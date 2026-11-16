# protobuf-batch-ingestor

纯标准库实现的 Python Protobuf 批量接入组件。接入器自行处理 varint 长度前缀、
帧边界、消息类型与 schema 版本，单个坏帧只会进入 quarantine，不会丢失同批次或
后续批次中的有效记录。

## 接入协议

### 外层长度前缀帧

上游按帧推送，每一帧都是 Protobuf 风格的长度前缀编码：

```
+---------------------+-------------------------------+
| uvarint frame_len   | frame_len 字节的帧负载         |
+---------------------+-------------------------------+
```

- `frame_len` 是无符号 LEB128 varint（最多 10 字节，uint64 范围）。
- 读取不依赖任何 chunk 边界：varint 或帧体被拆到多个 chunk 也能正确重组。
- 声明长度超过 `max_frame_size` 或 varint 溢出属于帧级错误，该流无法再定位
  下一个帧边界，事件写入 quarantine 后将流标记为 poisoned 并停止读取。
- EOF 时仍残留不完整帧 → `TRUNCATED_FRAME` 隔离。

### 批次信封（外层帧负载）

帧负载本身是一个 Protobuf 消息：

| 字段 | wire 类型 | 含义 |
| --- | --- | --- |
| 1 | varint | `sequence`，来源内单调递增的批次序号 |
| 2 | varint | `record_count`，记录条数 |
| 3 | bytes | `digest`，记录区域的 SHA-256（32 字节） |
| 4 | bytes | 记录区域，内部同样是一串长度前缀记录帧 |

摘要对记录区域整体计算，因此同时覆盖**记录内容与记录顺序**。摘要不匹配
（`DIGEST_MISMATCH`）或计数不匹配（`RECORD_COUNT_MISMATCH`）的整个批次被隔离，
不写入 staging。

### 记录帧（记录区域内的嵌套帧）

每个记录帧负载是一个三字段 Protobuf 消息：

| 字段 | wire 类型 | 含义 |
| --- | --- | --- |
| 1 | varint | 消息类型 `msg_type` |
| 2 | varint | schema 版本 `schema_version` |
| 3 | bytes | 记录自身的 Protobuf 负载 |

记录帧之间相互独立：一条记录解析失败只隔离该记录（保留原始字节），同批次其余
记录照常入 staging，后续批次继续读取。

## Schema 与兼容策略

schema 由 `SchemaSpec` 声明，不依赖 `.proto` 编译产物：

- `field_wire_types`：已知字段号到 wire 类型（0/1/2/5）的映射。
- `required_tags`：必填字段，缺失 → `MISSING_REQUIRED_FIELD` 隔离。
- `unknown_required_tags`：新版本引入、当前消费者无法解析的**必填消息段**字段号；
  命中即 `UNKNOWN_REQUIRED_MESSAGE` 隔离，避免静默吞掉必填语义。
- `nested_schemas`：嵌套消息字段的子 schema，递归解码；内层失败包装为
  `NESTED_DECODE_ERROR`。

兼容性规则：

- **未知可选字段**：不在 schema 中的字段默认可选，按 `DecodedField(unknown=True)`
  保留原始值（varint 保留数值，长度前缀字段保留原始字节），staging 中以
  `bytes_hex` 形式持久化，后续升级 schema 后可回放。
- **未知 schema 版本**：`SchemaRegistry.get` 回退到该消息类型当前注册的最新版本
  进行解析，版本号原样保留；回退 schema 中声明的必填/必填未知规则继续生效。
- **未知消息类型**：`UNKNOWN_MESSAGE_TYPE` 隔离该记录。
- **wire 类型冲突 / 线格式损坏**：分别隔离为 `WIRE_TYPE_MISMATCH` /
  `MALFORMED_WIRE`。

## 幂等与数据顺序

SQLite 存储（`StagingStore`，默认内存库，也可落盘）包含三张表：

- `batch_ledger(source, sequence_num, digest_hex, record_count)`：幂等账本，
  主键 `(source, sequence)`。
- `staging`：有效记录，带 `frame_index` 保序，唯一键
  `(source, sequence_num, frame_index)`。
- `quarantine`：所有隔离事件、原因、详情与原始字节。

提交在单事务内完成（账本 + staging + quarantine）。重复提交判定：

- `(source, sequence)` 已存在且摘要一致 → `DuplicateBatch`，计数为 duplicate，
  **不会重复入 staging**。
- `(source, sequence)` 已存在但摘要不一致（包括仅记录顺序变化）→
  `SequenceConflict`，批次拒绝写入并记录 `SEQUENCE_CONFLICT` 事件。

## 快速上手

```python
from batch_ingestor import (
    Ingestor, SchemaRegistry, SchemaSpec, StagingStore,
    build_batch_body, encode_frame, encode_record,
)
from batch_ingestor.wire import encode_varint_field

registry = SchemaRegistry()
registry.register(SchemaSpec(
    msg_type=100, version=1,
    field_wire_types={1: 0}, required_tags=frozenset({1}),
))

record = encode_record(100, 1, encode_varint_field(1, 42))
body, digest, _ = build_batch_body(sequence=1, record_bodies=[record])
stream = encode_frame(body)

store = StagingStore("ingest.db")
ingestor = Ingestor(registry, store, source="upstream-a")
result = ingestor.ingest_stream(iter([stream]))  # 也支持 ingest_bytes / ingest_file
print(result.batches_committed, result.records_staged, result.events)
```

## 隔离原因一览

`TRUNCATED_FRAME`、`LENGTH_OVERFLOW`、`ENVELOPE_ERROR`、`DIGEST_MISMATCH`、
`RECORD_COUNT_MISMATCH`、`TRUNCATED_RECORD`、`UNKNOWN_MESSAGE_TYPE`、
`UNKNOWN_REQUIRED_MESSAGE`、`MISSING_REQUIRED_FIELD`、`WIRE_TYPE_MISMATCH`、
`MALFORMED_WIRE`、`NESTED_DECODE_ERROR`、`SEQUENCE_CONFLICT`。

帧级错误（长度溢出/超长/EOF 截断）无法仅凭长度前缀重新同步，会 poison 当前流；
记录级与批次级错误互不影响，后续完整帧继续处理。

## 测试

```bash
python -m unittest
```

覆盖：varint 边界与溢出、逐字节跨 chunk 帧重组、长度溢出、未知可选字段保留、
未知必填消息段隔离、混用 schema 版本与未知版本回退、坏帧后续读、重复提交幂等、
数据顺序变化导致的摘要冲突、截断帧与摘要篡改。

## 模块结构

```
batch_ingestor/
  varint.py    # LEB128 varint 编解码
  wire.py      # 最小 Protobuf 线格式读写
  framing.py   # 流式长度前缀帧缓冲
  record.py    # 记录帧（类型/版本/负载）
  batch.py     # 批次信封、摘要、嵌套记录切分
  schema.py    # schema 注册表与兼容解码
  store.py     # SQLite staging / 账本 / quarantine
  service.py   # 接入编排与隔离流程
  errors.py    # 异常与隔离原因码
tests/         # unittest 套件（43 个用例）
```
