# Iceberg / Kafka — 본인 환경 외부 레포 매핑

> **요약**: 외부 레포 `apache/iceberg`(iceberg-flink 모듈)와 `apache/flink-connector-kafka`가 본 레포의 어떤 SPI를 어떻게 활용하는지 정리. Polaris(REST Catalog) 인증 흐름 포함.
> **선행**: [`./01-source-v2-overview.md`](./01-source-v2-overview.md), [`./03-sink-v2-overview.md`](./03-sink-v2-overview.md), [`../07-filesystem-checkpoint-store/02-recoverable-writer.md`](../07-filesystem-checkpoint-store/02-recoverable-writer.md)

---

## 1. 외부 레포 위치

```
~/code/flink/external/
├── iceberg/                       # apache/iceberg (sparse: flink/, api/, core/)
│   └── flink/                     # iceberg-flink integration
│       └── v2.0/                  # Flink 2.0 호환 모듈
└── flink-connector-kafka/         # apache/flink-connector-kafka
    ├── flink-connector-kafka/
    └── flink-sql-connector-kafka/
```

---

## 2. Kafka Source 매핑

본 레포 SPI ↔ 외부 구현체:

| Flink V2 SPI | Kafka 구현체 (external/flink-connector-kafka) |
|-------------|-------------------------------------------|
| `Source<T, SplitT, EnumChkT>` | `KafkaSource<T>` |
| `SourceSplit` | `KafkaPartitionSplit` (topic, partition, startingOffset, stoppingOffset) |
| `SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>` | `KafkaSourceEnumerator` |
| `SourceReader<T, KafkaPartitionSplit>` | `KafkaSourceReader` |
| `SimpleVersionedSerializer<KafkaPartitionSplit>` | `KafkaPartitionSplitSerializer` |
| `WatermarkStrategy<T>` | 사용자가 설정 (Source builder의 .setWatermarkStrategy) |

### 2.1 사용자 코드 패턴

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("my-topic")
    .setGroupId("my-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

DataStream<String> stream = env.fromSource(
    source, 
    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),
    "kafka-source");
```

### 2.2 Offset commit 메커니즘

Kafka source는 **Flink checkpoint와 연동된 offset commit**:
- 매 checkpoint complete 시 `notifyCheckpointComplete(chkId)` → KafkaSourceReader가 그 시점까지 처리한 offset을 Kafka에 commit (consumer group)
- Kafka 측에서 monitoring 가능 (offset lag 등)
- **Flink state(checkpoint)가 진실의 원천** — Kafka offset commit은 monitoring용

---

## 3. Iceberg Sink 매핑

본 레포 SPI ↔ Iceberg-Flink 구현체:

| Flink V2 SPI | Iceberg 구현체 (external/iceberg/flink) |
|-------------|--------------------------------------|
| `Sink<RowData>` (+`SupportsWriterState` +`SupportsCommitter`) | `IcebergSink` (정확히는 `IcebergSink.builder()` factory) |
| `StatefulSinkWriter<RowData, WriterStateT>` | `IcebergStreamWriter` |
| `Committer<IcebergCommittable>` | `IcebergFilesCommitter` (parallelism=1, single global) |
| (writer 측 Parquet) | `flink-formats/flink-parquet` (Flink 본 레포) → Parquet 파일 생성 |
| (file write) | `RecoverableWriter` via S3FileSystem (Flink 본 레포) → MinIO에 업로드 |
| Catalog | `RESTCatalog` (Iceberg 코어) → Polaris와 통신 |

### 3.1 사용자 코드 패턴 (Table API)

```sql
CREATE CATALOG polaris WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'https://polaris.example.com',
  'warehouse' = 's3://flink-warehouse/',
  'credential' = '<oauth-client-id>:<client-secret>',
  'token-refresh-enabled' = 'true',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'https://minio.example.com'
);

CREATE TABLE polaris.my_db.events (...) WITH ('format-version' = '2');

INSERT INTO polaris.my_db.events SELECT ... FROM kafka_table;
```

### 3.2 Commit 흐름 (2PC + Polaris)

```
[Iceberg Sink Writer] (parallelism=N)
   record → Parquet 파일 (MinIO에 RecoverableWriter로 multipart upload)
   ↓ checkpoint barrier
   prepareCommit() → IcebergCommittable (data file paths + stats)
   ↓ checkpoint state에 포함
   ↓ JM에 acknowledge
[JM CheckpointCoordinator] 모든 ack 수신
   ↓ notifyCheckpointComplete
[Iceberg Files Committer] (parallelism=1)
   commit(committables):
     1. manifest 파일 생성 (data file 메타 묶음) → MinIO 업로드
     2. snapshot 메타 생성 (manifest 묶음) → MinIO 업로드
     3. Polaris REST API 호출:
        POST /v1/{prefix}/namespaces/{ns}/tables/{table}/commit
        (with OAuth2 Bearer 토큰)
        body: {requirements: [...], updates: [{type: append-snapshot, ...}]}
     4. Polaris가 metadata.json 갱신 → 새 snapshot 노출
```

핵심: 단계 4가 atomic — Polaris가 OCC(Optimistic Concurrency Control)로 동시 commit 충돌 검출. 충돌 시 `IcebergFilesCommitter`가 retry (idempotent 보장).

### 3.3 Polaris OAuth2 인증 흐름

본인 환경 결정사항 ([`../00-overview/my-environment-map.md`](../00-overview/my-environment-map.md)): **OAuth2 Client Credentials grant** (가장 일반적):

```
[잡 시작 시]
RESTCatalog가 Polaris의 token endpoint 호출:
  POST /v1/oauth/tokens
  grant_type=client_credentials
  client_id=<id>&client_secret=<secret>&scope=<scope>
  ↓
Polaris → Bearer 토큰 (TTL 보통 1시간)
  ↓
[매 commit 시]
RESTCatalog가 Bearer 토큰을 Authorization 헤더에 포함
  ↓
TTL 만료 전에 토큰 자동 refresh ('token-refresh-enabled' = 'true')
```

K8s 측 secret 주입:
```yaml
env:
  - name: POLARIS_CLIENT_ID
    valueFrom:
      secretKeyRef: { name: polaris-oauth, key: client-id }
  - name: POLARIS_CLIENT_SECRET
    valueFrom:
      secretKeyRef: { name: polaris-oauth, key: client-secret }
```

---

## 4. Exactly-once 종합 (본인 환경)

```
[Kafka topic A]
    ↓ KafkaSource (FLIP-27, V2)
    ↓ KafkaPartitionSplit (per partition)
    ↓ checkpoint 매 30s
    ↓ split state에 last consumed offset 보존
    ↓
[process / window]
    ↓ RocksDB keyed state
    ↓ 매 checkpoint에 incremental snapshot → MinIO/chk-N/shared/*
    ↓
[Iceberg Sink Writer] (parallelism=N)
    ↓ Parquet 파일 (MinIO에 multipart upload)
    ↓ checkpoint 시 prepareCommit → IcebergCommittable
    ↓
[Iceberg Files Committer] (parallelism=1)
    ↓ checkpoint complete 시 commit → Polaris REST → snapshot atomic 갱신
```

종단 간 보장:
- 같은 checkpoint barrier 안에 묶인 (Kafka offset, RocksDB state, Iceberg data files) → atomicity
- checkpoint 실패 시 → 모두 rollback (Kafka offset 이전, Iceberg snapshot 미변경)
- checkpoint 성공 + commit 실패 시 → 다음 시도에서 idempotent retry

---

## 5. 외부 레포 코드 인용 형식

dkdocs에서 외부 레포 코드 인용 시:
```
external/iceberg/flink/v2.0/.../IcebergSink.java:42
external/flink-connector-kafka/.../KafkaSource.java:128
```

본 레포 코드와 명확히 구분.

---

## 6. 06 카테고리 마무리 + 다음

4개 문서로 Source/Sink V2 SPI 영역 완성:
- 01-source-v2-overview.md
- 02-source-coordinator.md
- 03-sink-v2-overview.md
- 04-iceberg-kafka-mapping.md (이 문서)

다음:
- [`../08-file-formats/`](../08-file-formats/) — Parquet (사용 중인 format)
- [`../09-kubernetes-integration/`](../09-kubernetes-integration/) — flink-kubernetes 모듈, K8s HA, Operator 경계
