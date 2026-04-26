# 06 — Source & Sink SPI

> **다루는 영역**: 본인 환경의 Kafka(외부 레포)와 Iceberg(외부 레포)가 의존하는 Flink 본 레포의 SPI — Source V2 (FLIP-27), Sink V2 (FLIP-143/191), Two-Phase Commit. ★ 환경 직결 영역.
> **선행**: [`../05-state-checkpoint/`](../05-state-checkpoint/), [`../07-filesystem-checkpoint-store/`](../07-filesystem-checkpoint-store/)

## 문서 목록

| # | 문서 | 한 줄 |
|---|------|------|
| 01 | [`01-source-v2-overview.md`](./01-source-v2-overview.md) | Source<T,SplitT,EnumChkT> + SplitEnumerator + SourceReader 분리 |
| 02 | [`02-source-coordinator.md`](./02-source-coordinator.md) | JM 측 SourceCoordinator + OperatorEvent 라우팅 |
| 03 | [`03-sink-v2-overview.md`](./03-sink-v2-overview.md) | Sink + SinkWriter + Committer 2PC commit |
| 04 | [`04-iceberg-kafka-mapping.md`](./04-iceberg-kafka-mapping.md) | ★ Iceberg-Flink + Kafka connector ↔ Flink SPI 매핑 + Polaris OAuth2 |
