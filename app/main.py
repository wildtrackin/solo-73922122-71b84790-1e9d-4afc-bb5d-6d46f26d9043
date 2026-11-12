from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import anyio.to_thread
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

from .demux import DemuxError, DemuxPackage, normalize_barcodes, prepare_package

logger = logging.getLogger("demux")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

TMP_ROOT = Path(os.getenv("DEMUX_TMP_ROOT", "/tmp/demux"))
STREAM_CHUNK = 128 * 1024

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
    version="1.0.0",
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


class ErrorResponse(BaseModel):
    detail: ErrorDetail


@app.exception_handler(DemuxError)
async def demux_error_handler(_request, exc: DemuxError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.as_detail()})


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health", summary="Health check")
def health() -> dict[str, str]:
    return {"status": "ok"}


async def _stream_zip(package: DemuxPackage):
    """Yield the already-complete ZIP, deleting request files in all exits."""

    try:
        with open(package.zip_path, "rb") as handle:
            while True:
                chunk = await anyio.to_thread.run_sync(handle.read, STREAM_CHUNK)
                if not chunk:
                    break
                yield chunk
    finally:
        # Runs after a normal response, a write error, or client disconnect
        # (Starlette closes the async iterator in those cases).
        package.cleanup()


@app.post(
    "/api/demux",
    summary="Upload R1/R2 and download a demultiplexed ZIP",
    description=(
        "Upload two gzip FASTQ files and sample barcodes. The response is only "
        "produced after both streams have been fully read and validated. On "
        "success the body is `application/zip`; on a record, pairing, or gzip "
        "error the body is a JSON error with a file/record/line location."
    ),
    response_class=StreamingResponse,
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
    },
)
async def upload_and_demux(
    r1: UploadFile = File(..., description="gzip-compressed R1 FASTQ"),
    r2: UploadFile = File(..., description="gzip-compressed R2 FASTQ"),
    barcodes: list[str] = Form(
        ...,
        description="1-16 unique 8-base A/C/G/T barcodes (repeat field or separate with commas/newlines)",
    ),
) -> StreamingResponse:
    holder: dict[str, DemuxPackage] = {}

    def process(barcodes_for_run: list[str]) -> None:
        # UploadFile.file is a spooled temporary file; seek before reading.
        r1.file.seek(0)
        r2.file.seek(0)
        holder["package"] = prepare_package(
            r1.file,
            r2.file,
            barcodes_for_run,
            tmp_root=TMP_ROOT,
        )

    try:
        normalized = normalize_barcodes(barcodes)
        await anyio.to_thread.run_sync(process, normalized)
    except DemuxError:
        raise
    except Exception as exc:  # pragma: no cover - exercised by disk failures
        logger.exception("Unexpected demultiplexing failure")
        raise DemuxError(
            "internal_error",
            "An internal error occurred while building the package.",
            status_code=500,
        ) from exc
    except BaseException:
        # Request cancellation can arrive while the worker finishes. Reclaim
        # the package even though its future response will never be sent.
        package = holder.get("package")
        if package is not None:
            package.cleanup()
        raise
    finally:
        # Both source spool files are no longer needed: the ZIP is complete.
        try:
            r1.file.close()
            r2.file.close()
        except Exception:
            logger.warning("Could not close upload spool files", exc_info=True)

    package = holder.get("package")
    if package is None:  # defensive; prepare_package either returns or raises
        raise DemuxError("internal_error", "Package was not created.", status_code=500)

    filename = "demux-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + ".zip"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Length": str(package.zip_path.stat().st_size),
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
        "X-Demux-Total-Pairs": str(package.manifest["total_pairs"]),
    }
    response = StreamingResponse(
        _stream_zip(package),
        media_type="application/zip",
        headers=headers,
    )
    # Belt and braces: the generator's finally is the primary cleanup path,
    # and this background task makes cleanup happen even for unusual ASGI
    # response paths. cleanup() is idempotent.
    response.background = BackgroundTask(package.cleanup)
    return response
