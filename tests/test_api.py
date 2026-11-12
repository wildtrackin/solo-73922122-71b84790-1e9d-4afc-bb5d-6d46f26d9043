from __future__ import annotations

import gzip
import io
import json
import threading
import time
import zipfile

import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.resources import ResourceManager


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
    manager = ResourceManager(tmp_root=tmp_path)
    monkeypatch.setattr(main_module, "RESOURCES", manager)
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


def test_busy_returns_503_without_queueing(tmp_path, monkeypatch):
    manager = ResourceManager(max_inflight=1, tmp_root=tmp_path)
    monkeypatch.setattr(main_module, "RESOURCES", manager)
    started = threading.Event()
    release = threading.Event()
    real_prepare = main_module.prepare_package

    def slow_prepare(*args, **kwargs):
        started.set()
        release.wait(10)
        return real_prepare(*args, **kwargs)

    monkeypatch.setattr(main_module, "prepare_package", slow_prepare)
    r1 = rec("r1", "ACGTACGT", "ACGT", 1)
    r2 = rec("r1", None, "TTTT", 2)

    first_result = {}

    def first_request():
        with TestClient(main_module.app) as test_client:
            first_result["response"] = upload(test_client, r1, r2, ["ACGTACGT"])

    worker = threading.Thread(target=first_request)
    worker.start()
    try:
        assert started.wait(5)
        with TestClient(main_module.app) as test_client:
            second = upload(test_client, r1, r2, ["ACGTACGT"])
        assert second.status_code == 503
        assert second.json()["detail"]["code"] == "server_busy"
        assert second.headers["retry-after"] == "1"
        assert manager.snapshot()["inflight"] == 1
    finally:
        release.set()
        worker.join(10)

    assert first_result["response"].status_code == 200
    assert manager.snapshot()["inflight"] == 0
    assert manager.snapshot()["used_bytes"] == 0
    deadline = time.time() + 2
    while list(tmp_path.iterdir()) and time.time() < deadline:
        time.sleep(0.01)
    assert list(tmp_path.iterdir()) == []


def test_budget_exhaustion_returns_503_with_reason(tmp_path, monkeypatch):
    manager = ResourceManager(budget_bytes=120, tmp_root=tmp_path)
    monkeypatch.setattr(main_module, "RESOURCES", manager)
    r1 = rec("r1", "ACGTACGT", "ACGT", 1)
    r2 = rec("r1", None, "TTTT", 2)
    with TestClient(main_module.app) as test_client:
        response = upload(test_client, r1, r2, ["ACGTACGT"])

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["code"] == "insufficient_tmp_budget"
    assert detail["budget_bytes"] == 120
    assert detail["required_bytes"] > 0
    assert "budget" in detail["message"]
    assert manager.snapshot()["inflight"] == 0
    assert manager.snapshot()["used_bytes"] == 0
    deadline = time.time() + 2
    while list(tmp_path.iterdir()) and time.time() < deadline:
        time.sleep(0.01)
    assert list(tmp_path.iterdir()) == []
