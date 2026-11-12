# 双端 FASTQ 条码拆样服务

一个用 FastAPI 实现的上传/拆样/下载服务：接收 R1、R2 两份 gzip FASTQ
和最多 16 个唯一的 8 位 A/C/G/T 样本条码，流式拆分为样本包 ZIP。

服务对单进程资源做了硬限制，避免并行拆包时把临时磁盘占满：

* 最多 **2** 个在途请求；第 3 个请求立即返回 `503 server_busy`，不排队。
* 所有请求共用 **256 MiB** 临时文件逻辑字节预算；预算不足时返回
  `503 insufficient_tmp_budget`，不会返回半成品 ZIP。
* 预算覆盖 multipart 上传暂存、各组 gzip 和最终 ZIP；每次扩写文件前
  先预留字节。
* 正常下载期间继续占用名额；上传断连、处理失败、下载结束或下载断连后，
  先停止本请求写入、关闭并删除本请求文件，再归还预算和名额。

## 快速启动

```bash
docker compose up --build -d
```

启动后打开 <http://localhost:8000/docs>。`/health` 用于健康检查。停止并清理：

```bash
docker compose down
```

两个限制都可以通过环境变量配置（compose 会从宿主机环境读取）：

```bash
DEMUX_MAX_INFLIGHT=2 \
DEMUX_TMP_BUDGET_BYTES=268435456 \
docker compose up --build -d
```

| 变量 | 默认 | 说明 |
|---|---:|---|
| `DEMUX_MAX_INFLIGHT` | `2` | 单 API 进程最多同时接纳的在途请求数 |
| `DEMUX_TMP_BUDGET_BYTES` | `268435456`（256 MiB） | 单进程所有请求共享临时文件逻辑字节预算 |
| `DEMUX_TMP_ROOT` | `/tmp/demux` | 请求临时目录根 |

> 这些限制是**每进程**的。Docker 镜像固定使用一个 uvicorn worker
> （`--workers 1`），因此它们描述整个服务。若自行增加 worker，每个
> worker 都会独立拥有一份名额和预算。

也可以用本地 Python 启动（Python 3.11+）：

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
DEMUX_TMP_ROOT=/tmp/demux uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

## 在 /docs 演示上传和下载

1. 打开 `/docs`，展开 `POST /api/demux`，点 **Try it out**。
2. `r1` 选择 `examples/normal/R1.fastq.gz`，`r2` 选择
   `examples/normal/R2.fastq.gz`。
3. `barcodes` 点 **Add item** 两次，分别填 `ACGTACGT` 和 `TTTTAAAA`
   （也可以在一个字段里填 `ACGTACGT, TTTTAAAA`）。
4. 执行后响应是 ZIP 文件，浏览器会下载；用解压工具查看
   `manifest.json`、`pair_counts.tsv` 和每个组的 R1/R2 gzip。

`503` 的两个示例（`server_busy`、`insufficient_tmp_budget`）连同字段说明
都展示在该接口的响应文档中，可在 Swagger UI 的 **Responses** 区域直接看到。

等价的命令行调用：

```bash
curl -sS -D headers.txt \
  -F 'r1=@examples/normal/R1.fastq.gz' \
  -F 'r2=@examples/normal/R2.fastq.gz' \
  -F 'barcodes=ACGTACGT' \
  -F 'barcodes=TTTTAAAA' \
  -o normal.zip \
  http://localhost:8000/api/demux
```

`normal.zip` 的期望结果：

| 组 | 配对数 | 原因 |
|---|---:|---|
| `ACGTACGT` | 3 | 两个精确匹配，一个 1 位错配且只接近该样本 |
| `TTTTAAAA` | 2 | 一个精确匹配，一个 1 位错配且只接近该样本 |
| `unassigned` | 1 | `CCCCCCCC` 与两个样本都相差超过 1 位 |

### 演示繁忙（`server_busy`）

默认名额是 2。用限速上传同时占住两个名额，然后发第三个请求，第三个会
立即得到 503（不会等待前两个完成）。上传内容只需要足够大以让上传阶段
持续一段时间；演示结束后可以 `kill` 掉两个后台 curl：

```bash
# 准备两个较大的上传文件（内容不必是合法 FASTQ）
dd if=/dev/urandom of=/tmp/slow-r1.bin bs=1M count=2 status=none
dd if=/dev/urandom of=/tmp/slow-r2.bin bs=1M count=2 status=none

curl -sS --limit-rate 5K -o /dev/null \
  -F 'r1=@/tmp/slow-r1.bin' -F 'r2=@/tmp/slow-r2.bin' \
  -F 'barcodes=ACGTACGT' http://localhost:8000/api/demux &
curl -sS --limit-rate 5K -o /dev/null \
  -F 'r1=@/tmp/slow-r1.bin' -F 'r2=@/tmp/slow-r2.bin' \
  -F 'barcodes=ACGTACGT' http://localhost:8000/api/demux &
sleep 1

curl -sS -w '\nHTTP %{http_code}\n' \
  -F 'r1=@examples/normal/R1.fastq.gz' \
  -F 'r2=@examples/normal/R2.fastq.gz' \
  -F 'barcodes=ACGTACGT' http://localhost:8000/api/demux

kill %1 %2 2>/dev/null || true
```

响应示例：

```json
{
  "detail": {
    "code": "server_busy",
    "message": "Server is already processing 2 in-flight demux requests; no queue is used. Try again later."
  }
}
```

### 演示预算不足（`insufficient_tmp_budget`）

把预算调小后启动服务，再上传正常样例，就会在扩写文件前被拒绝：

```bash
DEMUX_TMP_BUDGET_BYTES=512 docker compose up --build -d

curl -sS -w '\nHTTP %{http_code}\n' \
  -F 'r1=@examples/normal/R1.fastq.gz' \
  -F 'r2=@examples/normal/R2.fastq.gz' \
  -F 'barcodes=ACGTACGT' \
  -F 'barcodes=TTTTAAAA' \
  http://localhost:8000/api/demux
```

响应示例（数值会随实际占用变化）：

```json
{
  "detail": {
    "code": "insufficient_tmp_budget",
    "message": "Temporary-file byte budget is exhausted: this request needs 42 more bytes, but only 0 of 512 bytes are free (512 currently in use). The request was stopped before the file was extended, so no partial ZIP was produced.",
    "budget_bytes": 512,
    "used_bytes": 512,
    "required_bytes": 42,
    "available_bytes": 0
  }
}
```

演示后恢复默认预算：

```bash
docker compose down
docker compose up --build -d
```

### 随附样例

| 目录 | 条码 | 期望 |
|---|---|---|
| `examples/normal` | `ACGTACGT`, `TTTTAAAA` | 正常拆分，包含精确匹配、1 位错配和未分配 |
| `examples/ambiguous` | `ACGTACGT`, `ACGTACGA` | 三个观测条码都同时落入两个样本的 1 位范围，全部进入 `unassigned` |
| `examples/missing_pair` | `ACGTACGT`, `TTTTAAAA` | R1 有 3 条、R2 只有 2 条，返回 `count_mismatch`，位置为 R1/record 3/line 9 |

每个目录都同时提供 `.fastq` 原文和可直接上传的 `.fastq.gz`，`barcodes.txt`
是建议的条码。重新生成样例：

```bash
python3 examples/generate_examples.py
```

## API

### `POST /api/demux`

`multipart/form-data`：

| 字段 | 类型 | 说明 |
|---|---|---|
| `r1` | file | gzip 压缩的 R1 FASTQ |
| `r2` | file | gzip 压缩的 R2 FASTQ |
| `barcodes` | string，可重复 | 1–16 个唯一条码，每个恰好 8 位 `A/C/G/T`。重复字段或以逗号/换行分隔均可 |

成功时返回 `application/zip`。ZIP 内容：

```text
manifest.json
pair_counts.tsv
<barcode>/<barcode>_R1.fastq.gz
<barcode>/<barcode>_R2.fastq.gz
unassigned/unassigned_R1.fastq.gz
unassigned/unassigned_R2.fastq.gz
```

所有提交的样本条码（包括 0 条的组）和 `unassigned` 都会有各自的
R1/R2 gzip；0 条组是合法的空 gzip，便于下游脚本统一处理。
`manifest.json` 里有总配对数、每个组的 `pair_count` 和文件路径；
`pair_counts.tsv` 提供同一信息的表格版本。

输入要求：

* 严格四行一条的 FASTQ，最后一条也应有换行。
* R1 头：`@ID/1 barcode=<8 位观测条码>`；R2 头：`@ID/2`。
* 两个文件按顺序记录 ID 相同、条数相等。
* 每个读段长度 ≤ 1000，序列和 quality 长度严格相等。
* R1/R2 都是完整、未损坏的 gzip（读取结束时会校验 CRC32 和 ISIZE 尾部）。

### 条码判定

观测条码与每个样本条码逐位（Hamming）比较：

```text
候选 = {样本条码 | 与观测条码的 Hamming 距离 <= 1}
候选恰好 1 个 -> 归入该样本
否则（0 个或多于 1 个） -> unassigned
```

精确匹配也参与这个规则：如果观测条码正好等于样本 A，但它同时与样本 B
只差 1 位，结果为 `unassigned`（歧义），不会强行归入 A。R1/R2 作为一整对
读段处理，不会拆开；每组内保持输入出现顺序，并逐字节保留原始四行文本。

### 资源限制与响应

| 状态 | `detail.code` | 含义 |
|---|---|---|
| `503` | `server_busy` | 在途请求名额已满；立即返回，未读取/解析 multipart，不排队 |
| `503` | `insufficient_tmp_budget` | 共享临时字节预算不足以在扩写前预留；请求停止，不返回半包 |
| `400` | `bad_record` 等 | FASTQ/gzip 数据错误 |
| `422` | `invalid_barcodes` / `invalid_request` | 条码或 multipart 请求参数错误 |

503 响应会带 `Retry-After: 1`。预算不足响应额外包含
`budget_bytes`、`used_bytes`、`required_bytes`、`available_bytes`，便于
定位是哪一个文件扩写被拒绝。

### 错误响应

坏记录、ID 错配、条数不等或 gzip 损坏返回 HTTP 400；条码等请求参数问题
返回 HTTP 422。响应格式：

```json
{
  "detail": {
    "code": "count_mismatch",
    "message": "R1 has an extra record at pair 3, ...",
    "file": "R1",
    "record": 3,
    "line": 9,
    "locations": {
      "R1": {"record": 3, "line": 9},
      "R2": {"record": 3, "line": 9}
    }
  }
}
```

`code` 包括 `bad_record`、`truncated_record`、`id_mismatch`、
`count_mismatch`、`length_mismatch`、`read_too_long`、`gzip_error`、
`invalid_barcodes`、`invalid_request`、`server_busy` 和
`insufficient_tmp_budget`。`record` 是从 1 开始的读段序号，`line` 是从 1
开始的物理行号（四行一条时第 N 条头行是 `4N-3`）。错误发生在构建阶段，
响应不会包含半成品 ZIP。

## 资源与内存

* 读取：每次只在内存中保留一对 R1/R2 记录（单行设上限），不建读段列表。
* 分组：最多 16 个样本加 `unassigned`，使用固定数量的 gzip 临时文件，
  逐个读段对追加；内存与读段总数无关。
* 打包：ZIP 先落盘，再以 128 KiB 块流式发送；`ZIP_STORED` 不重复压缩
  已压缩的组文件，内存同样不随读段数增长。
* 预算：`app/resources.py` 的 `ResourceManager` 在单个进程内共享名额与
  字节计数。multipart 的每个文件数据块在写入 `SpooledTemporaryFile`
  之前预留预算（内存缓冲上限固定为 1 MiB，超出后滚动到本请求目录）；
  组 gzip 和 ZIP 通过 `BudgetedWriter` 在文件真正扩写前预留增长字节。
* 名额：`try_acquire()` 是非阻塞的；`RequestLease.cleanup()` 先置停止标志，
  等待处理线程退出，关闭上传 spool、删除本请求临时目录，最后才归还预算
  和名额。每个请求有独立目录，清理不会影响其他请求。
* 生命周期：上传断连或处理失败时在返回响应前清理；正常下载期间继续持有
  名额；下载正常结束、写响应失败或客户端断开时，响应生成器的 `finally`
  和幂等后台任务都会触发清理。

临时目录默认 `/tmp/demux`，可用环境变量 `DEMUX_TMP_ROOT` 修改。

## 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

核心测试只用标准库；API 测试需要 FastAPI/httpx。当前仓库附带的
`tests/test_demux.py` 也可不装依赖直接运行：

```bash
python3 -m unittest tests.test_demux -v
```
