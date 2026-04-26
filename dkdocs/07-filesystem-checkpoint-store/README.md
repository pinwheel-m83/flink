# 07 — FileSystem & Checkpoint Storage

> **다루는 영역**: Flink가 어떻게 다양한 파일시스템(특히 S3/MinIO)을 추상화하고, 체크포인트/savepoint를 그 위에 영속화하는가.
> **선행**: [`../05-state-checkpoint/`](../05-state-checkpoint/) — RocksDB가 만든 SST를 어디에 저장하는가가 본 카테고리.
> **본인 환경 핵심**: MinIO + flink-s3-fs-presto plugin

## 문서 목록

| # | 문서 | 한 줄 |
|---|------|------|
| 01 | [`01-filesystem-abstraction.md`](./01-filesystem-abstraction.md) | Flink FileSystem 추상 + persistence contract |
| 02 | [`02-recoverable-writer.md`](./02-recoverable-writer.md) | Resume-able stream — 체크포인트 시점 이어쓰기 |
| 03 | [`03-s3-multipart-upload.md`](./03-s3-multipart-upload.md) | S3 multipart upload + Flink mapping (★ MinIO 핵심) |
| 04 | [`04-plugin-classloading.md`](./04-plugin-classloading.md) | Plugin jar 격리 (S3, Hadoop) |
| 05 | [`05-checkpoint-storage-on-s3.md`](./05-checkpoint-storage-on-s3.md) | ★ 종합 운영 가이드 (디렉토리 구조, retention, 복구) |
