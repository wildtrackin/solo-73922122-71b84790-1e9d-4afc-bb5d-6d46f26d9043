"""Streaming paired-end FASTQ demultiplexing.

The public entry point is :func:`prepare_package`.  It reads the two FASTQ
streams one record pair at a time, writes each pair to per-barcode gzip
sinks, and finally creates a ZIP file.  No list of reads is retained in
memory, so memory use is independent of the number of read pairs.
"""

from __future__ import annotations

import gzip
import json
import re
import shutil
import tempfile
import zipfile
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Iterable, Sequence

MAX_BARCODES = 16
BARCODE_LENGTH = 8
MAX_READ_LENGTH = 1000
MAX_HEADER_BYTES = 64 * 1024
# Enough for a read just over the limit plus a CR/LF, so validation can
# report ``read_too_long`` rather than a generic over-long line.
MAX_READ_LINE_BYTES = MAX_READ_LENGTH + 4
UNASSIGNED = "unassigned"

_BARCODE_RE = re.compile(r"^[ACGT]{8}$")


@dataclass
class DemuxError(Exception):
    """A source-data or request error with a stable machine-readable code."""

    code: str
    message: str
    file: str | None = None
    record: int | None = None
    line: int | None = None
    locations: dict[str, dict[str, int]] | None = None
    status_code: int = 400

    def __str__(self) -> str:  # pragma: no cover - convenience only
        return self.message

    def as_detail(self) -> dict:
        detail: dict = {"code": self.code, "message": self.message}
        if self.file is not None:
            detail["file"] = self.file
        if self.record is not None:
            detail["record"] = self.record
        if self.line is not None:
            detail["line"] = self.line
        if self.locations is not None:
            detail["locations"] = self.locations
        return detail


def normalize_barcodes(raw_values: Sequence[str]) -> list[str]:
    """Normalize, validate, and de-duplicate submitted sample barcodes.

    A form value may contain several barcodes separated by whitespace,
    commas, or semicolons.  This makes both repeated form fields and a single
    pasted newline/comma-separated field workable in Swagger UI.
    """

    values: list[str] = []
    for raw in raw_values:
        for token in re.split(r"[\s,;]+", raw.strip()):
            if token:
                values.append(token)

    if not values:
        raise DemuxError(
            "invalid_barcodes",
            "At least one sample barcode is required.",
            status_code=422,
        )
    if len(values) > MAX_BARCODES:
        raise DemuxError(
            "invalid_barcodes",
            f"At most {MAX_BARCODES} sample barcodes are allowed; got {len(values)}.",
            status_code=422,
        )

    seen: set[str] = set()
    for value in values:
        if not _BARCODE_RE.fullmatch(value):
            raise DemuxError(
                "invalid_barcodes",
                f"Barcode {value!r} is invalid: each barcode must be exactly "
                f"{BARCODE_LENGTH} uppercase A/C/G/T bases.",
                status_code=422,
            )
        if value in seen:
            raise DemuxError(
                "invalid_barcodes",
                f"Barcode {value!r} was supplied more than once; sample barcodes must be unique.",
                status_code=422,
            )
        seen.add(value)
    return values


@dataclass(frozen=True)
class RawRecord:
    """The four original FASTQ lines, kept as bytes for lossless copying."""

    lines: tuple[bytes, bytes, bytes, bytes]
    read_id: bytes
    observed_barcode: bytes | None


def _without_line_ending(line: bytes) -> bytes:
    if line.endswith(b"\n"):
        line = line[:-1]
    if line.endswith(b"\r"):
        line = line[:-1]
    return line


def _parse_header(
    header: bytes, role: str, record_no: int, line_no: int
) -> tuple[bytes, bytes | None]:
    text = _without_line_ending(header)
    if not text.startswith(b"@"):
        raise DemuxError(
            "bad_record",
            f"{role} header must start with '@'.",
            file=role,
            record=record_no,
            line=line_no,
        )

    body = text[1:]
    if not body or body[:1].isspace():
        raise DemuxError(
            "bad_record",
            f"{role} header is missing a read identifier.",
            file=role,
            record=record_no,
            line=line_no,
        )

    parts = body.split()
    if not parts:
        raise DemuxError(
            "bad_record",
            f"{role} header is missing a read identifier.",
            file=role,
            record=record_no,
            line=line_no,
        )

    suffix = b"/1" if role == "R1" else b"/2"
    token = parts[0]
    if not token.endswith(suffix) or len(token) == len(suffix):
        raise DemuxError(
            "bad_record",
            f"{role} header identifier must end with {suffix.decode()!r}.",
            file=role,
            record=record_no,
            line=line_no,
        )
    read_id = token[: -len(suffix)]

    if role == "R2":
        # The observed barcode is carried by R1 only.
        return read_id, None

    barcode_fields = [part for part in parts[1:] if part.startswith(b"barcode=")]
    if len(barcode_fields) != 1:
        raise DemuxError(
            "bad_record",
            "R1 header must contain exactly one 'barcode=<8 ACGT bases>' field.",
            file=role,
            record=record_no,
            line=line_no,
        )
    observed = barcode_fields[0][len(b"barcode=") :]
    try:
        observed_text = observed.decode("ascii")
    except UnicodeDecodeError as exc:
        raise DemuxError(
            "bad_record",
            "R1 barcode field must contain 8 A/C/G/T bases.",
            file=role,
            record=record_no,
            line=line_no,
        ) from exc
    if not _BARCODE_RE.fullmatch(observed_text):
        raise DemuxError(
            "bad_record",
            "R1 barcode field must contain exactly 8 A/C/G/T bases.",
            file=role,
            record=record_no,
            line=line_no,
        )
    return read_id, observed


class FastqStream:
    """A bounded-line, four-line FASTQ reader over a gzip stream.

    ``gzip.GzipFile`` validates the CRC32 and uncompressed size in the gzip
    trailer when it reaches end-of-stream.  Reading a record beyond the last
    record (which the pair loop always does) therefore forces trailer
    validation.
    """

    def __init__(self, fileobj: BinaryIO, role: str):
        self.role = role
        self.lines_read = 0
        self.records_read = 0
        self._eof = False
        try:
            self._gz = gzip.GzipFile(fileobj=fileobj, mode="rb")
        except (OSError, EOFError, zlib.error) as exc:
            raise DemuxError(
                "gzip_error",
                f"{role} is not a readable gzip stream.",
                file=role,
                record=1,
                line=1,
            ) from exc

    def _source_error(self, kind: str, record_no: int, line_no: int, exc: BaseException):
        return DemuxError(
            "gzip_error",
            f"Compressed data for {self.role} is corrupt or truncated while "
            f"reading the {kind} at line {line_no}.",
            file=self.role,
            record=record_no,
            line=line_no,
        )

    def _readline(
        self, limit: int, kind: str, record_no: int, line_no: int
    ) -> bytes | None:
        if self._eof:
            return None
        try:
            raw = self._gz.readline(limit)
        except (EOFError, gzip.BadGzipFile, zlib.error, OSError) as exc:
            raise self._source_error(kind, record_no, line_no, exc) from exc

        if raw == b"":
            return None
        if len(raw) == limit and not raw.endswith(b"\n"):
            raise DemuxError(
                "bad_record",
                f"{self.role} {kind} line is longer than the allowed limit "
                f"({limit} bytes).",
                file=self.role,
                record=record_no,
                line=line_no,
            )
        self.lines_read += 1
        return raw

    def read_record(self) -> RawRecord | None:
        """Return one original four-line record, or ``None`` at clean EOF."""

        if self._eof:
            return None

        record_no = self.records_read + 1
        header_line = self.lines_read + 1
        header = self._readline(MAX_HEADER_BYTES, "header", record_no, header_line)
        if header is None:
            self._eof = True
            return None

        read_id, observed = _parse_header(header, self.role, record_no, header_line)

        seq_line = self.lines_read + 1
        seq = self._readline(MAX_READ_LINE_BYTES, "sequence", record_no, seq_line)
        if seq is None:
            raise DemuxError(
                "truncated_record",
                f"{self.role} ended after the header; a complete FASTQ record "
                "needs four lines.",
                file=self.role,
                record=record_no,
                line=seq_line,
            )

        plus_line = self.lines_read + 1
        plus = self._readline(MAX_HEADER_BYTES, "plus", record_no, plus_line)
        if plus is None:
            raise DemuxError(
                "truncated_record",
                f"{self.role} ended before the '+' line; a complete FASTQ record "
                "needs four lines.",
                file=self.role,
                record=record_no,
                line=plus_line,
            )
        if not _without_line_ending(plus).startswith(b"+"):
            raise DemuxError(
                "bad_record",
                f"{self.role} third line must start with '+'.",
                file=self.role,
                record=record_no,
                line=plus_line,
            )

        qual_line = self.lines_read + 1
        qual = self._readline(MAX_READ_LINE_BYTES, "quality", record_no, qual_line)
        if qual is None:
            raise DemuxError(
                "truncated_record",
                f"{self.role} ended before the quality line; a complete FASTQ "
                "record needs four lines.",
                file=self.role,
                record=record_no,
                line=qual_line,
            )

        seq_text = _without_line_ending(seq)
        qual_text = _without_line_ending(qual)
        seq_len = len(seq_text)
        if seq_len > MAX_READ_LENGTH:
            raise DemuxError(
                "read_too_long",
                f"{self.role} read length is {seq_len}; the maximum is "
                f"{MAX_READ_LENGTH} bases.",
                file=self.role,
                record=record_no,
                line=seq_line,
            )
        if len(qual_text) != seq_len:
            raise DemuxError(
                "length_mismatch",
                f"{self.role} sequence length ({seq_len}) does not equal quality "
                f"length ({len(qual_text)}).",
                file=self.role,
                record=record_no,
                line=qual_line,
            )

        self.records_read += 1
        return RawRecord(
            lines=(header, seq, plus, qual),
            read_id=read_id,
            observed_barcode=observed,
        )

    def close(self) -> None:
        try:
            self._gz.close()
        except (OSError, EOFError, zlib.error):
            # A close error after the streams were fully consumed is not a
            # source-data error.  All trailer validation happens in readline.
            pass


@dataclass
class Group:
    name: str
    barcode: str | None
    count: int = 0
    r1_path: Path | None = None
    r2_path: Path | None = None


class _GzipSink:
    def __init__(self, path: Path):
        self.path = path
        self._fp = open(path, "wb")
        try:
            self._gz = gzip.GzipFile(
                fileobj=self._fp, mode="wb", compresslevel=1, mtime=0
            )
        except BaseException:
            self._fp.close()
            raise

    def write(self, raw: bytes) -> None:
        self._gz.write(raw)

    def close(self) -> None:
        try:
            self._gz.close()
        finally:
            self._fp.close()


class _GroupSinks:
    def __init__(self, tmpdir: Path, barcodes: Sequence[str]):
        self.groups: dict[str, Group] = {}
        self.sinks: dict[tuple[str, str], _GzipSink] = {}
        for barcode in barcodes:
            self.groups[barcode] = Group(
                name=barcode,
                barcode=barcode,
                r1_path=tmpdir / f"{barcode}_R1.fastq.gz",
                r2_path=tmpdir / f"{barcode}_R2.fastq.gz",
            )
        self.groups[UNASSIGNED] = Group(
            name=UNASSIGNED,
            barcode=None,
            r1_path=tmpdir / f"{UNASSIGNED}_R1.fastq.gz",
            r2_path=tmpdir / f"{UNASSIGNED}_R2.fastq.gz",
        )
        try:
            for name, group in self.groups.items():
                assert group.r1_path is not None and group.r2_path is not None
                self.sinks[(name, "R1")] = _GzipSink(group.r1_path)
                self.sinks[(name, "R2")] = _GzipSink(group.r2_path)
        except BaseException:
            self.close()
            raise

    def write_pair(self, group_name: str, r1: RawRecord, r2: RawRecord) -> None:
        for raw in r1.lines:
            self.sinks[(group_name, "R1")].write(raw)
        for raw in r2.lines:
            self.sinks[(group_name, "R2")].write(raw)
        self.groups[group_name].count += 1

    def close(self) -> None:
        first_error: BaseException | None = None
        for sink in self.sinks.values():
            try:
                sink.close()
            except BaseException as exc:  # keep closing other members
                if first_error is None:
                    first_error = exc
        self.sinks.clear()
        if first_error is not None:
            raise first_error


def _hamming_distance(left: bytes, right: bytes) -> int:
    return sum(a != b for a, b in zip(left, right))


def _classify(observed: bytes, barcodes: Sequence[str]) -> str:
    """Assign only when exactly one sample barcode is within one mismatch."""

    candidates = [
        barcode
        for barcode in barcodes
        if _hamming_distance(observed, barcode.encode("ascii")) <= 1
    ]
    if len(candidates) == 1:
        return candidates[0]
    return UNASSIGNED


@dataclass
class DemuxPackage:
    """Owns one request's temporary directory and finished ZIP file."""

    tmpdir: Path
    zip_path: Path
    manifest: dict
    _cleaned: bool = field(default=False, init=False)

    def cleanup(self) -> None:
        if self._cleaned:
            return
        self._cleaned = True
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def __del__(self):  # pragma: no cover - safety net for abandoned responses
        try:
            self.cleanup()
        except Exception:
            pass


def _build_zip(tmpdir: Path, groups: Iterable[Group], manifest: dict) -> Path:
    zip_path = tmpdir / "package.zip"
    tsv = ["group\tbarcode\tpair_count\tr1_file\tr2_file"]
    for group in groups:
        barcode_text = group.barcode if group.barcode is not None else ""
        prefix = f"{group.name}/{group.name}"
        tsv.append(
            f"{group.name}\t{barcode_text}\t{group.count}\t"
            f"{prefix}_R1.fastq.gz\t{prefix}_R2.fastq.gz"
        )

    with zipfile.ZipFile(
        zip_path, mode="w", compression=zipfile.ZIP_STORED, allowZip64=True
    ) as archive:
        archive.writestr(
            "manifest.json",
            json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        )
        archive.writestr("pair_counts.tsv", ("\n".join(tsv) + "\n").encode("utf-8"))
        for group in groups:
            assert group.r1_path is not None and group.r2_path is not None
            archive.write(
                group.r1_path, arcname=f"{group.name}/{group.name}_R1.fastq.gz"
            )
            archive.write(
                group.r2_path, arcname=f"{group.name}/{group.name}_R2.fastq.gz"
            )
    return zip_path


def prepare_package(
    r1_fileobj: BinaryIO,
    r2_fileobj: BinaryIO,
    barcodes: Sequence[str],
    *,
    tmp_root: str | Path | None = None,
) -> DemuxPackage:
    """Validate and demultiplex two gzip FASTQ streams into a ZIP package.

    On success the caller owns the returned package and must call
    ``cleanup()``.  On every error the temporary directory is removed before
    the exception is raised.
    """

    # Revalidate here as well: this function is useful without the HTTP layer.
    normalized = normalize_barcodes(list(barcodes))
    tmp_root_path = Path(tmp_root) if tmp_root is not None else None
    if tmp_root_path is not None:
        tmp_root_path.mkdir(parents=True, exist_ok=True)
    tmpdir = Path(
        tempfile.mkdtemp(prefix="demux-", dir=str(tmp_root_path) if tmp_root_path else None)
    )

    r1_stream: FastqStream | None = None
    r2_stream: FastqStream | None = None
    sinks: _GroupSinks | None = None
    keep_tmpdir = False
    try:
        r1_stream = FastqStream(r1_fileobj, "R1")
        r2_stream = FastqStream(r2_fileobj, "R2")
        sinks = _GroupSinks(tmpdir, normalized)

        pair_count = 0
        while True:
            next_pair = pair_count + 1
            r1_record = r1_stream.read_record()
            r2_record = r2_stream.read_record()

            if r1_record is None or r2_record is None:
                if r1_record is None and r2_record is None:
                    break

                longer = "R1" if r1_record is not None else "R2"
                next_line = pair_count * 4 + 1
                # Both readers are positioned at the same four-line slot,
                # whether they contain a record there or just hit EOF.
                raise DemuxError(
                    "count_mismatch",
                    f"{longer} has an extra record at pair {next_pair}, but the "
                    f"other file ended after {pair_count} reads. R1 and R2 must "
                    "contain the same number of reads.",
                    file=longer,
                    record=next_pair,
                    line=next_line,
                    locations={
                        "R1": {"record": next_pair, "line": next_line},
                        "R2": {"record": next_pair, "line": next_line},
                    },
                )

            if r1_record.read_id != r2_record.read_id:
                line_no = pair_count * 4 + 1
                raise DemuxError(
                    "id_mismatch",
                    f"Read identifiers differ at pair {next_pair}: R1 has "
                    f"{r1_record.read_id!r}, R2 has {r2_record.read_id!r}.",
                    file="R1/R2",
                    record=next_pair,
                    line=line_no,
                    locations={
                        "R1": {"record": next_pair, "line": line_no},
                        "R2": {"record": next_pair, "line": line_no},
                    },
                )

            assert r1_record.observed_barcode is not None
            group_name = _classify(r1_record.observed_barcode, normalized)
            sinks.write_pair(group_name, r1_record, r2_record)
            pair_count += 1

        # finish gzip streams before packaging them
        sinks.close()
        groups = list(sinks.groups.values())
        sinks = None

        manifest = {
            "format": "paired-end-demultiplex-v1",
            "barcodes": list(normalized),
            "matching": {
                "max_mismatches": 1,
                "rule": "assign only when exactly one sample barcode is within "
                "max_mismatches; otherwise place the pair in 'unassigned'",
            },
            "total_pairs": pair_count,
            "groups": [
                {
                    "name": group.name,
                    "barcode": group.barcode,
                    "pair_count": group.count,
                    "r1_file": f"{group.name}/{group.name}_R1.fastq.gz",
                    "r2_file": f"{group.name}/{group.name}_R2.fastq.gz",
                }
                for group in groups
            ],
        }
        zip_path = _build_zip(tmpdir, groups, manifest)
        keep_tmpdir = True
        return DemuxPackage(tmpdir=tmpdir, zip_path=zip_path, manifest=manifest)
    finally:
        try:
            if sinks is not None:
                sinks.close()
        except BaseException:
            pass
        try:
            if r1_stream is not None:
                r1_stream.close()
            if r2_stream is not None:
                r2_stream.close()
        except BaseException:
            pass
        if not keep_tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)
