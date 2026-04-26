# Iceberg-Flink (apache/iceberg)

> **외부 레포**: `external/iceberg/` (sparse-checkout: `flink/`, `api/`, `core/`)
> **Flink 측 진입점**: Sink V2 SPI + RecoverableWriter + Parquet BulkWriter

---

## 1. 무엇인가

Apache Iceberg = open table format for analytic datasets. Iceberg-Flink 모듈(`external/iceberg/flink/v2.0/`)이 Flink와의 통합 제공 — read (FLIP-27 Source), write (Sink V2), Catalog (REST/Hive 등).

## 2. Flink 본 레포 의존 매핑

| Iceberg 측 | Flink 측 SPI |
|----------|------------|
| `IcebergSink` | `Sink<RowData>` + `SupportsWriterState` + `SupportsCommitter` ([06-source-sink-spi/03-sink-v2-overview](../06-source-sink-spi/03-sink-v2-overview.md)) |
| `IcebergStreamWriter` | `StatefulSinkWriter<RowData, WriterStateT>` |
| `IcebergFilesCommitter` (parallelism=1) | `Committer<IcebergCommittable>` (2PC) |
| Data file write | Flink `RecoverableWriter` ([07-filesystem-checkpoint-store/02](../07-filesystem-checkpoint-store/02-recoverable-writer.md)) |
| Data file format | Flink `flink-formats/flink-parquet` ([08-file-formats/01](../08-file-formats/01-parquet-bulk-writer.md)) |
| Read source (Flink batch/streaming) | `Source<RowData>` (FLIP-27) ([06-source-sink-spi/01](../06-source-sink-spi/01-source-v2-overview.md)) |

## 3. 본인 환경에서의 흐름

[`../06-source-sink-spi/04-iceberg-kafka-mapping.md`](../06-source-sink-spi/04-iceberg-kafka-mapping.md)의 Iceberg sink 흐름 참조.

## 4. 외부 레포 코드 진입점

```
external/iceberg/flink/v2.0/
├── flink/src/main/java/org/apache/iceberg/flink/sink/
│   ├── IcebergSink.java                    # ★ Sink V2 진입
│   ├── IcebergStreamWriter.java            # writer
│   ├── IcebergFilesCommitter.java          # committer (parallelism=1)
│   └── ...
└── flink/src/main/java/org/apache/iceberg/flink/source/
    └── IcebergSource.java                   # Source V2 진입
```

## 5. Catalog

Iceberg `RESTCatalog`(코어, `external/iceberg/core/.../rest/`)가 Polaris와 통신. 본인 환경에선 RESTCatalog 사용 — 자세한 인증 흐름은 [`./02-polaris.md`](./02-polaris.md).
