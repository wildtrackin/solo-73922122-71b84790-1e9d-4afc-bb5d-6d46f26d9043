from __future__ import annotations

import gzip
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from app.demux import DemuxError, normalize_barcodes, prepare_package


def gz(data: bytes) -> bytes:
    return gzip.compress(data, mtime=0)


def rec(read_id: str, barcode: str | None, seq: str, mate: int) -> bytes:
    header = f"@{read_id}/{mate}"
    if barcode is not None:
        header += f" barcode={barcode}"
    return f"{header}\n{seq}\n+\n{'I' * len(seq)}\n".encode("ascii")


def errors_detail(exc: DemuxError) -> dict:
    return exc.as_detail()


class DemuxCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_root = Path(self.tmp.name)

    def tearDown(self) -> None:
        # Any package should already have been cleaned by the test.
        self.assertEqual(list(self.tmp_root.iterdir()), [])
        self.tmp.cleanup()

    def run_case(self, r1: bytes, r2: bytes, barcodes=("ACGTACGT", "TTTTAAAA")):
        return prepare_package(
            io.BytesIO(gz(r1)),
            io.BytesIO(gz(r2)),
            list(barcodes),
            tmp_root=self.tmp_root,
        )

    def test_normal_exact_mismatch_unassigned_and_order(self) -> None:
        r1 = b"".join(
            [
                rec("r1", "ACGTACGT", "ACGTACGTAC", 1),
                rec("r2", "ACGTACCT", "ACGTACCTAC", 1),
                rec("r3", "TTTTAAAA", "TTTTAAAATT", 1),
                rec("r4", "CCCCCCCC", "CCCCCCCCCC", 1),
                rec("r5", "ACGTACGT", "ACGTACGTAC", 1),
            ]
        )
        r2 = b"".join([rec(f"r{i}", None, "GGGGGGGGGG", 2) for i in range(1, 6)])
        package = self.run_case(r1, r2)
        self.assertEqual(package.manifest["total_pairs"], 5)
        counts = {g["name"]: g["pair_count"] for g in package.manifest["groups"]}
        self.assertEqual(counts, {"ACGTACGT": 3, "TTTTAAAA": 1, "unassigned": 1})

        with zipfile.ZipFile(package.zip_path) as archive:
            self.assertEqual(
                set(archive.namelist()),
                {
                    "manifest.json",
                    "pair_counts.tsv",
                    "ACGTACGT/ACGTACGT_R1.fastq.gz",
                    "ACGTACGT/ACGTACGT_R2.fastq.gz",
                    "TTTTAAAA/TTTTAAAA_R1.fastq.gz",
                    "TTTTAAAA/TTTTAAAA_R2.fastq.gz",
                    "unassigned/unassigned_R1.fastq.gz",
                    "unassigned/unassigned_R2.fastq.gz",
                },
            )
            group_r1 = gzip.decompress(archive.read("ACGTACGT/ACGTACGT_R1.fastq.gz"))
            group_r2 = gzip.decompress(archive.read("ACGTACGT/ACGTACGT_R2.fastq.gz"))
            self.assertEqual(
                group_r1,
                rec("r1", "ACGTACGT", "ACGTACGTAC", 1)
                + rec("r2", "ACGTACCT", "ACGTACCTAC", 1)
                + rec("r5", "ACGTACGT", "ACGTACGTAC", 1),
            )
            self.assertEqual(
                group_r2,
                rec("r1", None, "GGGGGGGGGG", 2)
                + rec("r2", None, "GGGGGGGGGG", 2)
                + rec("r5", None, "GGGGGGGGGG", 2),
            )
            manifest = json.loads(archive.read("manifest.json"))
            self.assertEqual(manifest["groups"][-1]["name"], "unassigned")
            self.assertIn("ACGTACGT\tACGTACGT\t3", archive.read("pair_counts.tsv").decode())

        package.cleanup()
        self.assertFalse(package.zip_path.exists())

    def test_ambiguous_is_unassigned(self) -> None:
        barcodes = ["ACGTACGT", "ACGTACGA"]
        r1 = b"".join(
            [
                rec("r1", "ACGTACGG", "ACGT", 1),  # 1 from each
                rec("r2", "ACGTACGA", "ACGT", 1),  # exact second, 1 from first
                rec("r3", "ACGTACGC", "ACGT", 1),  # 1 from each
            ]
        )
        r2 = b"".join([rec(f"r{i}", None, "TTTT", 2) for i in range(1, 4)])
        package = self.run_case(r1, r2, barcodes)
        counts = {g["name"]: g["pair_count"] for g in package.manifest["groups"]}
        self.assertEqual(counts, {"ACGTACGT": 0, "ACGTACGA": 0, "unassigned": 3})
        with zipfile.ZipFile(package.zip_path) as archive:
            # Empty groups still contain valid empty gzip members.
            self.assertEqual(gzip.decompress(archive.read("ACGTACGT/ACGTACGT_R1.fastq.gz")), b"")
        package.cleanup()

    def test_count_mismatch_reports_pair_and_line(self) -> None:
        r1 = rec("r1", "ACGTACGT", "ACGT", 1) + rec("r2", "ACGTACGT", "ACGT", 1)
        r2 = rec("r1", None, "ACGT", 2)
        with self.assertRaises(DemuxError) as ctx:
            self.run_case(r1, r2)
        detail = errors_detail(ctx.exception)
        self.assertEqual(detail["code"], "count_mismatch")
        self.assertEqual(detail["file"], "R1")
        self.assertEqual(detail["record"], 2)
        self.assertEqual(detail["line"], 5)

    def test_id_mismatch_reports_both_locations(self) -> None:
        r1 = rec("r1", "ACGTACGT", "ACGT", 1)
        r2 = rec("other", None, "ACGT", 2)
        with self.assertRaises(DemuxError) as ctx:
            self.run_case(r1, r2)
        detail = ctx.exception.as_detail()
        self.assertEqual(detail["code"], "id_mismatch")
        self.assertEqual(detail["record"], 1)
        self.assertEqual(detail["line"], 1)
        self.assertEqual(detail["locations"]["R1"]["line"], 1)
        self.assertEqual(detail["locations"]["R2"]["line"], 1)

    def test_bad_record_header_and_length(self) -> None:
        cases = [
            (rec("r1", "ACGTACGN", "ACGT", 1), "bad_record"),
            (rec("r1", None, "ACGT", 1), "bad_record"),
            (rec("r1", "ACGTACGT", "ACGT", 2), "bad_record"),
        ]
        for r1, code in cases:
            with self.assertRaises(DemuxError) as ctx:
                self.run_case(r1, rec("r1", None, "ACGT", 2))
            self.assertEqual(ctx.exception.code, code)
            self.assertEqual(ctx.exception.file, "R1")
            self.assertEqual(ctx.exception.record, 1)

        bad_qual = b"@r1/1 barcode=ACGTACGT\nACGT\n+\nIII\n"
        with self.assertRaises(DemuxError) as ctx:
            self.run_case(bad_qual, rec("r1", None, "ACGT", 2))
        self.assertEqual(ctx.exception.code, "length_mismatch")
        self.assertEqual(ctx.exception.line, 4)

    def test_read_length_limit(self) -> None:
        ok_1000 = rec("r1", "ACGTACGT", "A" * 1000, 1)
        package = self.run_case(ok_1000, rec("r1", None, "T" * 1000, 2))
        self.assertEqual(package.manifest["total_pairs"], 1)
        package.cleanup()

        too_long = rec("r1", "ACGTACGT", "A" * 1001, 1)
        with self.assertRaises(DemuxError) as ctx:
            self.run_case(too_long, rec("r1", None, "T" * 1001, 2))
        self.assertEqual(ctx.exception.code, "read_too_long")
        self.assertEqual(ctx.exception.line, 2)

    def test_crlf_is_preserved(self) -> None:
        r1 = b"@r1/1 barcode=ACGTACGT\r\nACGT\r\n+\r\nIIII\r\n"
        r2 = b"@r1/2\r\nTTTT\r\n+\r\nIIII\r\n"
        package = self.run_case(r1, r2)
        with zipfile.ZipFile(package.zip_path) as archive:
            out = gzip.decompress(archive.read("ACGTACGT/ACGTACGT_R1.fastq.gz"))
        self.assertEqual(out, r1)
        package.cleanup()

    def test_compressed_data_errors(self) -> None:
        good = gz(rec("r1", "ACGTACGT", "ACGT", 1))
        variants = [
            good[:-8] + bytes([good[-8] ^ 0xFF]) + good[-7:],  # CRC/FLG byte damage
            good[:-4],  # missing ISIZE trailer
            good + b"trailing garbage",
            b"not gzip at all",
        ]
        for bad in variants:
            with self.assertRaises(DemuxError) as ctx:
                prepare_package(
                    io.BytesIO(bad),
                    io.BytesIO(gz(rec("r1", None, "ACGT", 2))),
                    ["ACGTACGT"],
                    tmp_root=self.tmp_root,
                )
            self.assertEqual(ctx.exception.code, "gzip_error")
            self.assertEqual(ctx.exception.file, "R1")

    def test_barcode_validation(self) -> None:
        self.assertEqual(normalize_barcodes(["ACGTACGT, TTTTAAAA"]), ["ACGTACGT", "TTTTAAAA"])
        for values in ([], ["ACGTACGT", "ACGTACGT"], ["ACGTACGN"], ["ACGTACG"], ["acgtacgt"]):
            with self.assertRaises(DemuxError) as ctx:
                normalize_barcodes(values)
            self.assertEqual(ctx.exception.status_code, 422)

        too_many = ["".join(bits) for bits in [
            format(i, "08b").replace("0", "A").replace("1", "C") for i in range(17)
        ]]
        with self.assertRaises(DemuxError):
            normalize_barcodes(too_many)


if __name__ == "__main__":
    unittest.main()
