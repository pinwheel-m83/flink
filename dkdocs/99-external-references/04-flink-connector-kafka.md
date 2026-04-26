# Flink Connector Kafka (apache/flink-connector-kafka)

> **외부 레포**: `external/flink-connector-kafka/`
> **Flink 측 진입점**: Source V2 + Sink V2 SPI

---

## 1. 무엇인가

Apache Flink Kafka connector — Kafka topic을 source/sink로 사용. 본인 환경의 streaming 잡의 input source.

## 2. Flink 본 레포 의존 매핑

| Kafka 측 | Flink 측 SPI |
|---------|------------|
| `KafkaSource<T>` | `Source<T, KafkaPartitionSplit, KafkaSourceEnumState>` ([06-source-sink-spi/01](../06-source-sink-spi/01-source-v2-overview.md)) |
| `KafkaPartitionSplit` | `SourceSplit` |
| `KafkaSourceEnumerator` | `SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>` (JM-side, hosted by `SourceCoordinator` ([02](../06-source-sink-spi/02-source-coordinator.md))) |
| `KafkaSourceReader` | `SourceReader<T, KafkaPartitionSplit>` (TM-side) |
| `KafkaSink<T>` | `Sink<T> + SupportsWriterState + SupportsCommitter` ([03](../06-source-sink-spi/03-sink-v2-overview.md)) |
| `KafkaWriter` | `StatefulSinkWriter` (transactional producer 활용) |
| `KafkaCommitter` | `Committer<KafkaCommittable>` (transaction commit) |

## 3. 외부 레포 진입점

```
external/flink-connector-kafka/
├── flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/
│   ├── source/
│   │   ├── KafkaSource.java              # ★ Source factory
│   │   ├── KafkaSourceBuilder.java       # 사용자 facing builder
│   │   ├── enumerator/
│   │   │   ├── KafkaSourceEnumerator.java
│   │   │   └── ...
│   │   ├── reader/
│   │   │   ├── KafkaSourceReader.java
│   │   │   └── ...
│   │   └── split/
│   │       └── KafkaPartitionSplit.java
│   └── sink/
│       ├── KafkaSink.java                # ★ Sink factory
│       ├── KafkaSinkBuilder.java
│       ├── KafkaWriter.java
│       └── KafkaCommitter.java
```

본인 환경 디버깅 시:
- Offset 안 진행 → `KafkaSourceReader.snapshotState` 또는 `KafkaSourceEnumerator.handleSplitRequest`
- Sink commit 실패 → `KafkaCommitter.commit` (transactional producer state)

## 4. Exactly-once 보장

`DeliveryGuarantee.EXACTLY_ONCE` 모드:
- Source: Flink checkpoint state에 last consumed offset 저장 → restore 시 그 offset부터 재시작
- Sink: Kafka transactional producer + Sink V2 2PC → checkpoint barrier 시 prepareCommit (transaction prepare), checkpoint complete 시 commit (transaction commit)

자세한 종단 흐름은 [`../06-source-sink-spi/04-iceberg-kafka-mapping.md#4-exactly-once-종합-본인-환경`](../06-source-sink-spi/04-iceberg-kafka-mapping.md#4-exactly-once-종합-본인-환경).

## 5. Operator autoscaler와의 관계

Kafka source의 parallelism = Kafka topic partition 수 이하 (효율 측면). Operator autoscaler가 source parallelism 변경 시:
1. AdaptiveScheduler가 새 ExecutionGraph (parallelism=N')
2. 새 reader subtask들 시작 → `SourceCoordinator`에 등록
3. `KafkaSourceEnumerator`가 partition을 N' reader에 재분배

→ Kafka rebalance 같지만 **Flink 자체 메커니즘** (Kafka consumer group rebalance와 별개).
