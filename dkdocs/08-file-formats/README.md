# 08 — File Formats (Parquet)

> **다루는 영역**: 본인 환경에서 사용 중인 Parquet 형식 — write (sink), read (vectorized), 작은 파일 compaction.
> **선행**: [`../06-source-sink-spi/`](../06-source-sink-spi/), [`../07-filesystem-checkpoint-store/`](../07-filesystem-checkpoint-store/)

## 문서 목록

| # | 문서 | 한 줄 |
|---|------|------|
| 01 | [`01-parquet-bulk-writer.md`](./01-parquet-bulk-writer.md) | BulkWriter + ParquetWriterFactory + Iceberg sink 통합 |
| 02 | [`02-parquet-vectorized-reader.md`](./02-parquet-vectorized-reader.md) | column-oriented batch read + pushdown |
| 03 | [`03-compaction-rolling.md`](./03-compaction-rolling.md) | 작은 파일 누적 문제 + 운영 cron 패턴 |
