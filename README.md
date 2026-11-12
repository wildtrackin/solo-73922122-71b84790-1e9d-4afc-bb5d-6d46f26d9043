# 双端 FASTQ 条码拆样服务

一个用 FastAPI 实现的上传/拆样/下载服务：接收 R1、R2 两份 gzip FASTQ
和最多 16 个唯一的 8 位 A/C/G/T 样本条码，流式拆分为样本包 ZIP。

## 快速启动

```bash
docker compose up --build -d
```

启动后打开 <http://localhost:8000/docs>。`/health` 用于健康检查。停止并清理：

```bash
docker compose down
```

也可以用本地 Python 启动（Python 3.11+）：

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
DEMUX_TMP_ROOT=/tmp/demux uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 在 /docs 演示上传和下载

1. 打开 `/docs`，展开 `POST /api/demux`，点 **Try it out**。
2. `r1` 选择 `examples/normal/R1.fastq.gz`，`r2` 选择
   `examples/normal/R2.fastq.gz`。
3. `barcodes` 点 **Add item** 两次，分别填 `ACGTACGT` 和 `TTTTAAAA`
   （也可以在一个字段里填 `ACGTACGT, TTTTAAAA`）。
4. 执行后响应是 ZIP 文件，浏览器会下载；用解压工具查看
   `manifest.json`、`pair_counts.tsv` 和每个组的 R1/R2 gzip。

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
`count_mismatch`、`length_mismatch`、`read_too_long`、`gzip_error` 和
`invalid_barcodes`。`record` 是从 1 开始的读段序号，`line` 是从 1 开始的
物理行号（四行一条时第 N 条头行是 `4N-3`）。错误发生在构建阶段，响应
不会包含半成品 ZIP。

## 资源与内存

* 读取：每次只在内存中保留一对 R1/R2 记录（单行设上限），不建读段列表。
* 分组：最多 16 个样本加 `unassigned`，使用固定数量的 gzip 临时文件，
  逐个读段对追加；内存与读段总数无关。
* 打包：ZIP 先落盘，再以 128 KiB 块流式发送；`ZIP_STORED` 不重复压缩
  已压缩的组文件，内存同样不随读段数增长。
* 清理：构建失败时立即删除本请求临时目录；下载正常结束、写响应失败或
  客户端断开时，响应生成器的 `finally` 和幂等后台清理都会删除临时目录；
  上传 spool 文件也随即关闭。`DemuxPackage` 还有析构兜底。

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
