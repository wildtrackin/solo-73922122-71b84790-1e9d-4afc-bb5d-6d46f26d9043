"""Regenerate the gzip FASTQ fixtures under examples/.

Run from the repository root:
    python3 examples/generate_examples.py
"""

from __future__ import annotations

import gzip
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def record(read_id: str, barcode: str | None, seq: str, mate: int) -> bytes:
    header = f"@{read_id}/{mate}"
    if barcode is not None:
        header += f" barcode={barcode}"
    quality = "I" * len(seq)
    return f"{header}\n{seq}\n+\n{quality}\n".encode("ascii")


def write_case(
    case: str,
    barcodes: list[str],
    r1_records: list[bytes],
    r2_records: list[bytes],
) -> None:
    directory = ROOT / case
    directory.mkdir(parents=True, exist_ok=True)
    r1 = b"".join(r1_records)
    r2 = b"".join(r2_records)
    (directory / "R1.fastq").write_bytes(r1)
    (directory / "R2.fastq").write_bytes(r2)
    with gzip.GzipFile(
        filename=str(directory / "R1.fastq.gz"), mode="wb", mtime=0
    ) as handle:
        handle.write(r1)
    with gzip.GzipFile(
        filename=str(directory / "R2.fastq.gz"), mode="wb", mtime=0
    ) as handle:
        handle.write(r2)
    (directory / "barcodes.txt").write_text("\n".join(barcodes) + "\n", encoding="ascii")


def main() -> None:
    # Normal: exact assignments, one-mismatch assignments, and unassigned.
    normal_r1 = [
        record("read0001", "ACGTACGT", "ACGTACGTACGTACG", 1),
        record("read0002", "ACGTACCT", "ACGTACCTACGTACG", 1),
        record("read0003", "TTTTAAAA", "TTTTAAAATTTTAAA", 1),
        record("read0004", "TTTTAAAT", "TTTTAAATTTTTAAA", 1),
        record("read0005", "CCCCCCCC", "CCCCCCCCCCCCCCC", 1),
        record("read0006", "ACGTACGT", "ACGTACGTACGTACG", 1),
    ]
    normal_r2 = [
        record("read0001", None, "GGGGGGGGGGGGGGG", 2),
        record("read0002", None, "GGGGGGGGGGGGGGG", 2),
        record("read0003", None, "AAAAAAAAAAAAAAA", 2),
        record("read0004", None, "AAAAAAAAAAAAAAA", 2),
        record("read0005", None, "GGGGGGGGGGGGGGG", 2),
        record("read0006", None, "GGGGGGGGGGGGGGG", 2),
    ]
    write_case("normal", ["ACGTACGT", "TTTTAAAA"], normal_r1, normal_r2)

    # Ambiguous: every observation is within one base of both sample barcodes.
    ambiguous_r1 = [
        record("read0001", "ACGTACGG", "ACGTACGGACGTACG", 1),
        record("read0002", "ACGTACGA", "ACGTACGAACGTACG", 1),
        record("read0003", "ACGTACGC", "ACGTACGCACGTACG", 1),
    ]
    ambiguous_r2 = [
        record("read0001", None, "GGGGGGGGGGGGGGG", 2),
        record("read0002", None, "GGGGGGGGGGGGGGG", 2),
        record("read0003", None, "GGGGGGGGGGGGGGG", 2),
    ]
    write_case("ambiguous", ["ACGTACGT", "ACGTACGA"], ambiguous_r1, ambiguous_r2)

    # Missing mate: R1 has read0003 but R2 ends after read0002.
    missing_r1 = [
        record("read0001", "ACGTACGT", "ACGTACGTACGTACG", 1),
        record("read0002", "ACGTACGT", "ACGTACGTACGTACG", 1),
        record("read0003", "TTTTAAAA", "TTTTAAAATTTTAAA", 1),
    ]
    missing_r2 = [
        record("read0001", None, "GGGGGGGGGGGGGGG", 2),
        record("read0002", None, "GGGGGGGGGGGGGGG", 2),
    ]
    write_case("missing_pair", ["ACGTACGT", "TTTTAAAA"], missing_r1, missing_r2)


if __name__ == "__main__":
    main()
