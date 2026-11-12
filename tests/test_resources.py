from __future__ import annotations

import threading
import time

import pytest

from app.demux import DemuxError
from app.resources import BudgetedWriter, RequestAborted, ResourceManager


def test_budgeted_writer_reserves_before_extending(tmp_path):
    manager = ResourceManager(budget_bytes=10, tmp_root=tmp_path)
    lease = manager.try_acquire()
    assert lease is not None
    path = tmp_path / "out.bin"
    try:
        with BudgetedWriter(open(path, "wb"), lease) as writer:
            writer.write(b"123456")
            assert manager.snapshot()["used_bytes"] == 6
            # Seeking back inside allocated space does not reserve again.
            writer.seek(0)
            writer.write(b"12")
            assert manager.snapshot()["used_bytes"] == 6
            with pytest.raises(DemuxError) as ctx:
                writer.write(b"123456789")
            assert ctx.value.status_code == 503
            assert ctx.value.code == "insufficient_tmp_budget"
    finally:
        lease.cleanup()
    assert manager.snapshot()["inflight"] == 0
    assert manager.snapshot()["used_bytes"] == 0


def test_cleanup_waits_for_writing_worker_before_release(tmp_path):
    manager = ResourceManager(max_inflight=1, budget_bytes=1000, tmp_root=tmp_path)
    lease = manager.try_acquire()
    assert lease is not None
    reserved = threading.Event()
    stopped = threading.Event()
    finished = threading.Event()

    def worker():
        try:
            lease.reserve(100)
            reserved.set()
            # Simulate a write already in progress when cleanup starts.  The
            # cleanup must wait for this to finish before releasing anything.
            time.sleep(0.3)
        finally:
            finished.set()

    thread = threading.Thread(target=worker)
    lease.set_worker(thread)
    thread.start()
    assert reserved.wait(5)
    assert manager.snapshot()["used_bytes"] == 100

    cleaned = threading.Event()

    def cleanup():
        lease.cleanup()
        cleaned.set()

    cleaner = threading.Thread(target=cleanup)
    cleaner.start()
    # cleanup must not return (or free capacity) while the worker is alive.
    time.sleep(0.1)
    assert not cleaned.is_set()
    assert manager.snapshot()["inflight"] == 1
    assert manager.snapshot()["used_bytes"] == 100

    thread.join(5)
    cleaner.join(5)
    assert finished.is_set()
    assert cleaned.is_set()
    assert manager.snapshot()["inflight"] == 0
    assert manager.snapshot()["used_bytes"] == 0


def test_stop_flag_prevents_new_reservations(tmp_path):
    manager = ResourceManager(budget_bytes=100, tmp_root=tmp_path)
    lease = manager.try_acquire()
    assert lease is not None
    try:
        lease.request_stop()
        with pytest.raises(RequestAborted):
            lease.reserve(1)
    finally:
        lease.cleanup()
    assert manager.snapshot()["used_bytes"] == 0
    assert manager.snapshot()["inflight"] == 0


def test_independent_leases_do_not_clean_each_other(tmp_path):
    manager = ResourceManager(tmp_root=tmp_path)
    first = manager.try_acquire()
    second = manager.try_acquire()
    assert first is not None and second is not None
    first.tmpdir.mkdir(parents=True, exist_ok=True)
    second.tmpdir.mkdir(parents=True, exist_ok=True)
    (first.tmpdir / "first.bin").write_bytes(b"x")
    (second.tmpdir / "second.bin").write_bytes(b"y")

    first.cleanup()
    assert not first.tmpdir.exists()
    assert second.tmpdir.exists()
    assert (second.tmpdir / "second.bin").exists()

    second.cleanup()
    assert not second.tmpdir.exists()
    assert manager.snapshot()["inflight"] == 0
