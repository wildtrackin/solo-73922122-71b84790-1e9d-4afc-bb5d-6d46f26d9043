from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from tempfile import SpooledTemporaryFile
from typing import Any

import anyio.to_thread
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask
from starlette.datastructures import Headers, UploadFile
from starlette.formparsers import (
    MultiPartException,
    MultiPartParser,
    _user_safe_decode,
    parse_options_header,
)

from .demux import DemuxPackage, DemuxError, normalize_barcodes, prepare_package
from .resources import RequestLease, ResourceManager

logger = logging.getLogger("demux")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

TMP_ROOT = Path(os.getenv("DEMUX_TMP_ROOT", "/tmp/demux"))
STREAM_CHUNK = 128 * 1024

# One manager per API process.  Capacity is per process: run a single worker
# (the Docker image does) so both limits describe the whole service.
RESOURCES = ResourceManager.from_env(TMP_ROOT)
MAX_INFLIGHT = RESOURCES.max_inflight
TMP_BUDGET_BYTES = RESOURCES.budget_bytes

DESCRIPTION = """
按 8 位 A/C/G/T 条码把一个双端 FASTQ 混样拆分为样本包。

### 输入
* `r1`、`r2`：两份 **gzip** 压缩的 FASTQ，必须都是严格的四行一条记录。
* `barcodes`：1 到 16 个唯一的 8 位 A/C/G/T 样本条码。Swagger UI 中可点
  “Add item” 添加多个字段，也可以在一个字段里用逗号/换行分隔。
* R1 头必须为 `@ID/1 barcode=<观测条码>`，R2 头必须为 `@ID/2`；两条记录的
  ID 必须相同。
* 读段长度不超过 1000，序列与质量字符串长度必须相同。

### 条码判定
观测条码与每个样本条码逐位比较。**仅当恰好一个样本条码的 Hamming
距离 ≤ 1 时**，该读段对归入这个样本；距离为 0 的精确匹配也遵守这一
规则，所以如果精确匹配的同时又落入另一个条码的 1 位范围内，也会进入
`unassigned`。没有候选或候选多于一个也进入 `unassigned`。一对 R1/R2 永远
一起移动，各组内保持原始顺序和原始四行文本。

### 输出
完整读取两路、验证 gzip 尾部 CRC/长度后，才返回 ZIP。ZIP 包含
`manifest.json`、`pair_counts.tsv`，以及每个组（所有提交条码和
`unassigned`）的 R1/R2 gzip。空组也有合法的空 gzip 文件，便于下游
发现样本。

### 资源限制（HTTP 503）
每个 API 进程最多同时接纳 **2** 个在途请求（可配置
`DEMUX_MAX_INFLIGHT`），且所有请求共用 **256 MiB** 临时文件逻辑字节预算
（可配置 `DEMUX_TMP_BUDGET_BYTES`）。预算计入 multipart 上传暂存、各组
gzip 和最终 ZIP；每次扩写文件前先预留字节。

名额已满时立即返回 `503 server_busy`，不排队。预算不足时返回
`503 insufficient_tmp_budget`；请求在写入该扩展之前停止，因此不会返回
半成品 ZIP。正常下载期间仍然占用名额。上传断连、处理失败、下载结束或
下载断连后，先停止本请求写入并关闭/删除本请求文件，再归还预算和名额。

### 错误
坏记录、R1/R2 ID 错配、条数不相等、压缩损坏均返回 400 JSON，并在
`detail.file`、`detail.record`、`detail.line` 中给出位置；请求不会留下
半成品 ZIP。临时文件在失败、下载完成或客户端断开时删除。

### 示例文件
仓库的 `examples/` 目录包含 `normal`、`ambiguous`、`missing_pair` 三组
可直接上传的 gzip 文件；正常样例条码为 `ACGTACGT` 和 `TTTTAAAA`。
"""

app = FastAPI(
    title="Paired-end FASTQ barcode demultiplexer",
    version="1.1.0",
    description=DESCRIPTION,
    docs_url="/docs",
    redoc_url=None,
)


class ErrorDetail(BaseModel):
    code: str
    message: str
    file: str | None = None
    record: int | None = None
    line: int | None = None
    locations: dict[str, dict[str, int]] | None = None
    budget_bytes: int | None = None
    used_bytes: int | None = None
    required_bytes: int | None = None
    available_bytes: int | None = None


class ErrorResponse(BaseModel):
    detail: ErrorDetail


@app.exception_handler(DemuxError)
async def demux_error_handler(_request, exc: DemuxError) -> JSONResponse:
    headers = {}
    if exc.status_code == 503:
        headers["Retry-After"] = exc.retry_after or "1"
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.as_detail()},
        headers=headers,
    )


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health", summary="Health check")
def health() -> dict[str, str]:
    return {"status": "ok"}


# --------------------------------------------------------------------------
# Budget-aware multipart parsing
# --------------------------------------------------------------------------


class _BudgetedSpool:
    """Upload spool that reserves budget before every byte is staged.

    ``SpooledTemporaryFile`` keeps at most a fixed, small amount in memory and
    rolls over to the request directory.  Reserving on each write (rather than
    checking the resulting file size afterwards) means the budget always
    covers the logical upload, including bytes still in the fixed-size memory
    buffer.
    """

    def __init__(self, spool: SpooledTemporaryFile, lease: RequestLease):
        self._spool = spool
        self._lease = lease

    def write(self, data: bytes) -> int:
        if data:
            self._lease.reserve(len(data), kind="upload")
        return self._spool.write(data)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._spool, name)


class _BudgetedMultiPartParser(MultiPartParser):
    """Starlette multipart parser whose file spools use the request lease."""

    def __init__(
        self,
        headers: Headers,
        stream: Any,
        *,
        lease: RequestLease,
        max_files: int = 10,
        max_fields: int = 1000,
    ):
        super().__init__(headers, stream, max_files=max_files, max_fields=max_fields)
        self.lease = lease

    def on_headers_finished(self) -> None:
        # Mirrors Starlette 0.41's implementation, with a lease-owned spool.
        disposition, options = parse_options_header(self._current_part.content_disposition)
        try:
            self._current_part.field_name = _user_safe_decode(
                options[b"name"], self._charset
            )
        except KeyError:
            raise MultiPartException(
                'The Content-Disposition header field "name" must be provided.'
            )
        if b"filename" in options:
            self._current_files += 1
            if self._current_files > self.max_files:
                raise MultiPartException(
                    f"Too many files. Maximum number of files is {self.max_files}."
                )
            filename = _user_safe_decode(options[b"filename"], self._charset)
            spool = SpooledTemporaryFile(
                max_size=self.max_file_size, dir=str(self.lease.tmpdir)
            )
            budgeted = _BudgetedSpool(spool, self.lease)
            self._files_to_close_on_error.append(budgeted)
            self.lease.add_closer(budgeted.close)
            self._current_part.file = UploadFile(
                file=budgeted,  # type: ignore[arg-type]
                size=0,
                filename=filename,
                headers=Headers(raw=self._current_part.item_headers),
            )
        else:
            self._current_fields += 1
            if self._current_fields > self.max_fields:
                raise MultiPartException(
                    f"Too many fields. Maximum number of fields is {self.max_fields}."
                )
            self._current_part.file = None

    async def parse(self):
        try:
            return await super().parse()
        except BaseException:
            # Includes budget exhaustion and client disconnect, which the
            # Starlette implementation does not clean up itself.
            for spool in self._files_to_close_on_error:
                try:
                    spool.close()
                except Exception:
                    pass
            raise


def _single_upload(form, name: str) -> UploadFile:
    values = [value for key, value in form.multi_items() if key == name]
    if not values:
        raise DemuxError(
            "invalid_request",
            f"Missing required file field {name!r}.",
            status_code=422,
        )
    if len(values) > 1:
        raise DemuxError(
            "invalid_request",
            f"File field {name!r} was supplied more than once.",
            status_code=422,
        )
    value = values[0]
    if not isinstance(value, UploadFile):
        raise DemuxError(
            "invalid_request",
            f"Field {name!r} must be an uploaded file.",
            status_code=422,
        )
    return value


async def _parse_multipart(
    request: Request, lease: RequestLease
) -> tuple[UploadFile, UploadFile, list[str]]:
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type.lower():
        raise DemuxError(
            "invalid_request",
            "The request body must use multipart/form-data.",
            status_code=422,
        )

    parser = _BudgetedMultiPartParser(
        request.headers, request.stream(), lease=lease
    )
    try:
        form = await parser.parse()
    except MultiPartException as exc:
        raise DemuxError(
            "invalid_request", exc.message, status_code=422
        ) from exc

    r1 = _single_upload(form, "r1")
    r2 = _single_upload(form, "r2")

    barcode_values: list[str] = []
    for key, value in form.multi_items():
        if key != "barcodes":
            continue
        if isinstance(value, UploadFile):
            raise DemuxError(
                "invalid_request",
                "The 'barcodes' field must be text, not an uploaded file.",
                status_code=422,
            )
        barcode_values.append(value)
    return r1, r2, barcode_values


# --------------------------------------------------------------------------
# Request worker and response streaming
# --------------------------------------------------------------------------


async def _run_worker(lease: RequestLease, target) -> Any:
    """Run processing in a thread and wait for it to stop before unwinding.

    When the request task is cancelled (client disconnect / server shutdown),
    the stop flag is set and the thread is joined under a shielded scope.  The
    lease therefore never releases budget or a slot while the thread may still
    be writing.
    """

    holder: dict[str, Any] = {}

    def runner() -> None:
        try:
            holder["result"] = target()
        except BaseException as exc:  # store, then re-raise on the caller side
            holder["error"] = exc

    worker = threading.Thread(target=runner, name="demux-worker", daemon=True)
    lease.set_worker(worker)
    worker.start()

    try:
        await anyio.to_thread.run_sync(worker.join, abandon_on_cancel=True)
    except BaseException:
        lease.request_stop()
        with anyio.CancelScope(shield=True):
            await anyio.to_thread.run_sync(worker.join)
        raise

    if "error" in holder:
        raise holder["error"]
    return holder.get("result")


def _close_upload_spools(uploads: tuple[UploadFile, ...]) -> None:
    for upload in uploads:
        try:
            upload.file.close()
        except Exception:
            logger.warning("Could not close upload spool file", exc_info=True)


async def _stream_zip(package: DemuxPackage, lease: RequestLease):
    """Yield the already-complete ZIP, cleaning up in all exits."""

    try:
        with open(package.zip_path, "rb") as handle:
            while True:
                chunk = await anyio.to_thread.run_sync(handle.read, STREAM_CHUNK)
                if not chunk:
                    break
                yield chunk
    finally:
        # Runs after a normal response, a write error, or client disconnect
        # (Starlette closes the async iterator in those cases).  cleanup()
        # stops the worker, closes/deletes files, then returns budget and slot.
        lease.cleanup()


_ERROR_EXAMPLES = {
    "server_busy": {
        "summary": "Both in-flight request slots are occupied",
        "value": {
            "detail": {
                "code": "server_busy",
                "message": (
                    "Server is already processing 2 in-flight demux requests; "
                    "no queue is used. Try again later."
                ),
            }
        },
    },
    "insufficient_tmp_budget": {
        "summary": "The shared temporary-file byte budget is exhausted",
        "value": {
            "detail": {
                "code": "insufficient_tmp_budget",
                "message": (
                    "Temporary-file byte budget is exhausted: this request "
                    "needs 131072 more bytes, but only 65536 of 268435456 "
                    "bytes are free. The request was stopped before the file "
                    "was extended, so no partial ZIP was produced."
                ),
                "budget_bytes": 268435456,
                "used_bytes": 268369920,
                "required_bytes": 131072,
                "available_bytes": 65536,
            }
        },
    },
}

_REQUEST_BODY = {
    "required": True,
    "content": {
        "multipart/form-data": {
            "schema": {
                "type": "object",
                "required": ["r1", "r2", "barcodes"],
                "properties": {
                    "r1": {
                        "type": "string",
                        "format": "binary",
                        "description": "gzip-compressed R1 FASTQ",
                    },
                    "r2": {
                        "type": "string",
                        "format": "binary",
                        "description": "gzip-compressed R2 FASTQ",
                    },
                    "barcodes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "1-16 unique 8-base A/C/G/T barcodes (repeat the "
                            "field, or separate values with commas/newlines)"
                        ),
                    },
                },
            }
        }
    },
}


@app.post(
    "/api/demux",
    summary="Upload R1/R2 and download a demultiplexed ZIP",
    description=(
        "Upload two gzip FASTQ files and sample barcodes. The response is only "
        "produced after both streams have been fully read and validated. On "
        "success the body is `application/zip`; on a record, pairing, or gzip "
        "error the body is a JSON error with a file/record/line location. "
        "When no request slot is free the response is an immediate "
        "`503 server_busy`; when the shared temporary-file byte budget cannot "
        "reserve the next file extension it is `503 insufficient_tmp_budget`."
    ),
    response_class=StreamingResponse,
    openapi_extra={"requestBody": _REQUEST_BODY},
    responses={
        200: {
            "description": "Complete ZIP package",
            "content": {
                "application/zip": {
                    "schema": {"type": "string", "format": "binary"}
                }
            },
        },
        400: {"model": ErrorResponse, "description": "FASTQ or gzip error"},
        422: {"model": ErrorResponse, "description": "Invalid request"},
        500: {"model": ErrorResponse, "description": "Internal error"},
        503: {
            "model": ErrorResponse,
            "description": (
                "Capacity unavailable: `server_busy` when both in-flight slots "
                "are occupied, or `insufficient_tmp_budget` when the shared "
                "temporary-file byte budget cannot cover the next extension. "
                "No partial ZIP is returned."
            ),
            "content": {"application/json": {"examples": _ERROR_EXAMPLES}},
        },
    },
)
async def upload_and_demux(request: Request) -> StreamingResponse:
    # No multipart bytes have been read yet: acquire the slot first, without
    # waiting, so an over-capacity request gets an immediate 503.
    lease = RESOURCES.try_acquire()
    if lease is None:
        raise DemuxError(
            "server_busy",
            (
                f"Server is already processing {RESOURCES.max_inflight} "
                "in-flight demux requests; no queue is used. Try again later."
            ),
            status_code=503,
            retry_after="1",
        )

    transferred = False
    try:
        r1, r2, barcode_values = await _parse_multipart(request, lease)
        normalized = normalize_barcodes(barcode_values)

        def work() -> DemuxPackage:
            # UploadFile.file is a spooled temporary file; seek before reading.
            r1.file.seek(0)
            r2.file.seek(0)
            return prepare_package(
                r1.file,
                r2.file,
                normalized,
                tmpdir=lease.tmpdir,
                lease=lease,
            )

        package = await _run_worker(lease, work)

        # The source spools are no longer needed.  Close and delete them now,
        # before the download, and return only those upload bytes to the pool.
        _close_upload_spools((r1, r2))
        lease.release_upload_budget()

        filename = (
            "demux-"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + ".zip"
        )
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(package.zip_path.stat().st_size),
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Demux-Total-Pairs": str(package.manifest["total_pairs"]),
        }
        response = StreamingResponse(
            _stream_zip(package, lease),
            media_type="application/zip",
            headers=headers,
        )
        # Belt and braces: the generator's finally is the primary cleanup path,
        # and this background task makes cleanup happen even for unusual ASGI
        # response paths. cleanup() is idempotent.  The lease (and therefore
        # the slot) is held for the entire download.
        response.background = BackgroundTask(lease.cleanup)
        transferred = True
        return response
    finally:
        if not transferred:
            # Upload disconnect, validation error, processing failure, or a
            # failure while constructing the response: stop the worker, close
            # and delete this request's files, then release budget and slot.
            lease.cleanup()
