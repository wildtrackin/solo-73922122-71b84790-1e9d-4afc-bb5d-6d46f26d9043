"""Per-process request admission and temporary-file byte accounting.

The demultiplexer never streams a partially built ZIP.  While a request is
being parsed, processed, or downloaded it owns:

* one of a small number of in-flight request slots, and
* a shared budget of *logical* temporary-file bytes.

Both limits are enforced before bytes are written.  The upload spool, the
per-group gzip files, and the final ZIP all consume the same budget, so two
concurrent requests can never together exceed the configured disk allowance
(as long as every writer goes through :class:`RequestLease`).
"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any, Callable

from .demux import DemuxError

DEFAULT_MAX_INFLIGHT = 2
DEFAULT_TMP_BUDGET_BYTES = 256 * 1024 * 1024


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < 1:
        raise ValueError(f"{name} must be at least 1, got {value}")
    return value


class RequestAborted(Exception):
    """Raised inside a request worker when the lease is being torn down."""


class BudgetedWriter:
    """A binary file wrapper that reserves space before extending the file.

    ``write()`` computes how many bytes the raw file will grow by and asks the
    lease for that many bytes *before* touching the file.  Header rewrites and
    seeks inside already allocated space therefore do not double-charge.
    """

    def __init__(self, raw: Any, lease: "RequestLease | None" = None):
        self._raw = raw
        self._lease = lease
        try:
            self._size = int(os.fstat(raw.fileno()).st_size)
        except (AttributeError, OSError, ValueError):
            self._size = 0

    def write(self, data: bytes) -> int:
        if not data:
            return 0
        size = len(data)
        position = int(self._raw.tell())
        end = position + size
        growth = end - self._size
        if growth > 0 and self._lease is not None:
            self._lease.reserve(growth, kind="output")
        written = self._raw.write(data)
        if end > self._size:
            self._size = end
        return written

    def __getattr__(self, name: str) -> Any:
        # tell/seek/flush/close/read/fileno/... belong to the real file.
        return getattr(self._raw, name)

    def __enter__(self) -> "BudgetedWriter":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self._raw.close()


class RequestLease:
    """Holds one request's slot, budget reservations, and temporary directory."""

    def __init__(
        self,
        manager: "ResourceManager",
        tmpdir: Path,
    ):
        self.manager = manager
        self.tmpdir = tmpdir
        self._state_lock = threading.Lock()
        self._closers: list[Callable[[], Any]] = []
        self._worker: threading.Thread | None = None
        self._cleanup_started = False
        self._cleanup_done = threading.Event()
        self.stop_event = threading.Event()

        # Budget bookkeeping, all mutated while holding manager._lock.
        self._reserved = 0
        self._upload_reserved = 0

    # ------------------------------------------------------------------ budget
    def reserve(self, amount: int, *, kind: str = "output") -> None:
        """Reserve ``amount`` logical bytes or raise a 503-capable error."""

        if amount <= 0:
            return
        with self.manager._lock:
            if self.stop_event.is_set():
                raise RequestAborted()
            used = self.manager._used
            limit = self.manager.budget_bytes
            if used + amount > limit:
                available = max(0, limit - used)
                raise self.manager.budget_error(
                    required=amount, used=used, available=available
                )
            self.manager._used += amount
            self._reserved += amount
            if kind == "upload":
                self._upload_reserved += amount

    def release_upload_budget(self) -> None:
        """Return budget for upload spools that have been closed and deleted."""

        with self._state_lock:
            if self._cleanup_started:
                # cleanup() will release everything that is still reserved.
                return
            with self.manager._lock:
                amount = self._upload_reserved
                self._upload_reserved = 0
                self._reserved -= amount
                self.manager._used -= amount

    # --------------------------------------------------------------- lifecycle
    def ensure_active(self) -> None:
        if self.stop_event.is_set():
            raise RequestAborted()

    def request_stop(self) -> None:
        self.stop_event.set()

    def set_worker(self, worker: threading.Thread) -> None:
        with self._state_lock:
            self._worker = worker

    def add_closer(self, closer: Callable[[], Any]) -> None:
        with self._state_lock:
            self._closers.append(closer)

    def cleanup(self) -> None:
        """Stop writers, close and remove files, then return budget and slot.

        The worker thread is joined while the stop flag is set.  Only after it
        has stopped can files be removed and reservations released, which is
        what prevents a request that is still writing from losing its slot or
        budget early.
        """

        with self._state_lock:
            if self._cleanup_started:
                owner = False
            else:
                owner = True
                self._cleanup_started = True
            worker = self._worker
        # Set the flag before waiting; writers check it under manager._lock.
        self.stop_event.set()

        if not owner:
            self._cleanup_done.wait()
            return

        try:
            if worker is not None and worker is not threading.current_thread():
                worker.join()

            with self._state_lock:
                closers = list(self._closers)
                self._closers.clear()
            for closer in closers:
                try:
                    closer()
                except Exception:
                    pass

            shutil.rmtree(self.tmpdir, ignore_errors=True)
            self.manager._finish_lease(self)
        finally:
            self._cleanup_done.set()


class ResourceManager:
    """Process-wide admission control and temporary-byte budget."""

    def __init__(
        self,
        *,
        max_inflight: int = DEFAULT_MAX_INFLIGHT,
        budget_bytes: int = DEFAULT_TMP_BUDGET_BYTES,
        tmp_root: str | os.PathLike[str] = "/tmp/demux",
    ):
        if max_inflight < 1:
            raise ValueError("max_inflight must be at least 1")
        if budget_bytes < 1:
            raise ValueError("budget_bytes must be at least 1")
        self.max_inflight = int(max_inflight)
        self.budget_bytes = int(budget_bytes)
        self.tmp_root = Path(tmp_root)
        self._lock = threading.Lock()
        self._inflight = 0
        self._used = 0

    @classmethod
    def from_env(
        cls, tmp_root: str | os.PathLike[str] = "/tmp/demux"
    ) -> "ResourceManager":
        return cls(
            max_inflight=_env_positive_int(
                "DEMUX_MAX_INFLIGHT", DEFAULT_MAX_INFLIGHT
            ),
            budget_bytes=_env_positive_int(
                "DEMUX_TMP_BUDGET_BYTES", DEFAULT_TMP_BUDGET_BYTES
            ),
            tmp_root=tmp_root,
        )

    # ------------------------------------------------------------- admission
    def try_acquire(self) -> RequestLease | None:
        """Take a slot without waiting, or return ``None`` immediately."""

        with self._lock:
            if self._inflight >= self.max_inflight:
                return None
            self._inflight += 1
        try:
            self.tmp_root.mkdir(parents=True, exist_ok=True)
            tmpdir = Path(
                tempfile.mkdtemp(prefix="demux-", dir=str(self.tmp_root))
            )
        except BaseException:
            with self._lock:
                self._inflight -= 1
            raise
        return RequestLease(self, tmpdir)

    def _finish_lease(self, lease: RequestLease) -> None:
        with self._lock:
            amount = lease._reserved
            lease._reserved = 0
            lease._upload_reserved = 0
            self._used -= amount
            if self._used < 0:  # defensive; never expected
                self._used = 0
            self._inflight -= 1
            if self._inflight < 0:  # defensive; never expected
                self._inflight = 0

    # ---------------------------------------------------------------- errors
    def budget_error(self, *, required: int, used: int, available: int) -> DemuxError:
        message = (
            "Temporary-file byte budget is exhausted: this request needs "
            f"{required} more bytes, but only {available} of "
            f"{self.budget_bytes} bytes are free ({used} currently in use). "
            "The request was stopped before the file was extended, so no "
            "partial ZIP was produced."
        )
        return DemuxError(
            "insufficient_tmp_budget",
            message,
            status_code=503,
            budget_bytes=self.budget_bytes,
            used_bytes=used,
            required_bytes=required,
            available_bytes=available,
        )

    # -------------------------------------------------------------- reporting
    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "max_inflight": self.max_inflight,
                "inflight": self._inflight,
                "budget_bytes": self.budget_bytes,
                "used_bytes": self._used,
                "available_bytes": max(0, self.budget_bytes - self._used),
            }
