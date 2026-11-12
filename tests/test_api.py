from __future__ import annotations

import gzip
import io
import json
import time
import zipfile

import pytest
from fastapi.testclient import TestClient

from app import main as main_module


def gz(data: bytes) -> bytes:
    return gzip.compress(data, mtime=0)


def rec(read_id: str, barcode: str | None, seq: str, mate: int) -> bytes:
    header = f"@{read_id}/{mate}"
    if barcode is not None:
        header += f" barcode={barcode}"
    return f"{header}\n{seq}\n+\n{'I' * len(seq)}\n".encode("ascii")


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(main_module, "TMP_ROOT", tmp_path)
    with TestClient(main_module.app) as test_client:
        yield test_client


def upload(client, r1: bytes, r2: bytes, barcodes: list[str]):
    return client.post(
        "/api/demux",
        files={
            "r1": ("R1.fastq.gz", gz(r1), "application/gzip"),
            "r2": ("R2.fastq.gz", gz(r2), "application/gzip"),
        },
        data={"barcodes": barcodes},
    )


def test_normal_upload_returns_complete_zip(client, tmp_path):
    r1 = b"".join(
        [
            rec("r1", "ACGTACGT", "ACGT", 1),
            rec("r2", "ACGTACCT", "ACGT", 1),
            rec("r3", "CCCCCCCC", "ACGT", 1),
        ]
    )
    r2 = b"".join([rec(f"r{i}", None, "TTTT", 2) for i in range(1, 4)])
    response = upload(client, r1, r2, ["ACGTACGT", "TTTTAAAA"])

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/zip")
    assert response.headers["cache-control"] == "no-store"
    assert int(response.headers["x-demux-total-pairs"]) == 3
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["total_pairs"] == 3
        assert {g["name"]: g["pair_count"] for g in manifest["groups"]} == {
            "ACGTACGT": 2,
            "TTTTAAAA": 0,
            "unassigned": 1,
        }
    # Streaming response finalization cleans the request directory.
    deadline = time.time() + 2
    while list(tmp_path.iterdir()) and time.time() < deadline:
        time.sleep(0.01)
    assert list(tmp_path.iterdir()) == []


def test_ambiguous_and_missing_pair_errors(client, tmp_path):
    barcodes = ["ACGTACGT", "ACGTACGA"]
    ambiguous = upload(
        client,
        rec("r1", "ACGTACGG", "ACGT", 1),
        rec("r1", None, "TTTT", 2),
        barcodes,
    )
    assert ambiguous.status_code == 200
    with zipfile.ZipFile(io.BytesIO(ambiguous.content)) as archive:
        manifest = json.loads(archive.read("manifest.json"))
    assert {g["name"]: g["pair_count"] for g in manifest["groups"]}["unassigned"] == 1

    missing = upload(
        client,
        rec("r1", "ACGTACGT", "ACGT", 1) + rec("r2", "ACGTACGT", "ACGT", 1),
        rec("r1", None, "TTTT", 2),
        ["ACGTACGT"],
    )
    assert missing.status_code == 400
    detail = missing.json()["detail"]
    assert detail["code"] == "count_mismatch"
    assert detail["file"] == "R1"
    assert detail["record"] == 2
    assert detail["line"] == 5

    deadline = time.time() + 2
    while list(tmp_path.iterdir()) and time.time() < deadline:
        time.sleep(0.01)
    assert list(tmp_path.iterdir()) == []


def test_bad_barcodes_and_corrupt_gzip(client):
    response = upload(client, rec("r1", "ACGTACGT", "ACGT", 1), rec("r1", None, "TTTT", 2), ["ACGTACGT", "ACGTACGT"])
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "invalid_barcodes"

    bad_gzip = gz(rec("r1", "ACGTACGT", "ACGT", 1))[:-4]
    response = client.post(
        "/api/demux",
        files={
            "r1": ("R1.fastq.gz", bad_gzip, "application/gzip"),
            "r2": ("R2.fastq.gz", gz(rec("r1", None, "TTTT", 2)), "application/gzip"),
        },
        data={"barcodes": ["ACGTACGT"]},
    )
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "gzip_error"


def test_health_and_root(client):
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/", follow_redirects=False).status_code in {302, 307}
