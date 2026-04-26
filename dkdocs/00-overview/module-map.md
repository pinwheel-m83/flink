# 모듈 맵 — Flink internals 분석 우선순위

> Flink 2.0 (release-2.0) 기준. 본인 환경 (K8s + Operator + Kafka + MinIO + Iceberg/Polaris + Parquet) 가중치 반영.

---

## Tier 시스템

| Tier | 의미 |
|------|------|
| 🔴 **Tier 0** | 본인 환경 직결 — 가장 먼저, 가장 깊이 |
| 🟠 **Tier 1** | 토대 — Tier 0를 이해하기 위한 필수 |
| 🟡 **Tier 2** | 컨트리뷰터 목표를 위한 확장 |
| 🟢 **Tier 3** | 후순위 / 필요 시 참조 |
| ⚪ **Tier 4** | 분석 제외 |

---

## Tier 0 — 환경 직결

| # | 주제 | 모듈/패키지 | 본인 환경에서의 의미 |
|---|------|------------|-------------------|
| 1 | **Source V2 SPI** | `flink-core/api/connector/source/`, `flink-runtime/source/coordinator/`, `flink-connectors/flink-connector-base` | Kafka 커넥터(외부 레포)가 이 위에 구현. Split / Enumerator / Reader / SourceCoordinator |
| 2 | **Sink V2 + 2PC commit** | `flink-core/api/connector/sink2/`, `flink-streaming-java/api/operators/sink/` | Iceberg 커넥터(외부 레포)의 commit 보장. `Committer`, `GlobalCommitter` |
| 3 | **FileSystem + RecoverableWriter** | `flink-core/core/fs/`, `flink-filesystems/flink-s3-fs-{base,hadoop,presto}/` | MinIO 체크포인트 + 파일 Sink. multipart upload, in-progress→pending→committed |
| 4 | **Checkpoint** | `flink-runtime/checkpoint/` (`CheckpointCoordinator`, `CheckpointBarrier`, `BarrierAlignment`) | 모든 exactly-once의 뼈대. Kafka offset / Iceberg 스냅샷 commit 시점 |
| 5 | **State backend (RocksDB ★, ForSt 🧪 PoC)** | `flink-state-backends/{flink-statebackend-rocksdb, flink-statebackend-forst}`, `flink-dstl`, `flink-runtime/state/` | 운영은 RocksDB. ForSt는 향후 cloud-native 마이그레이션을 대비한 PoC |
| 6 | **K8s 통합** | `flink-kubernetes/` (`entrypoint/`, `kubeclient/`, `KubernetesResourceManagerDriver`, `highavailability/`) | Operator가 만드는 Pod/ConfigMap의 Flink 측 동작. JM HA/리더 선출 |
| 7 | **Parquet + 파일 I/O** | `flink-formats/flink-parquet/`, `flink-connectors/flink-connector-files/`, `flink-file-sink-common` | 직접 사용 중인 포맷 + Source v2 / Sink v2 레퍼런스 |
| 8 | **AdaptiveScheduler** | `flink-runtime/scheduler/adaptive/` | Operator autoscaler 활용에 필수 (declarative resource management) |

---

## Tier 1 — 토대

| 주제 | 모듈 |
|------|------|
| Graph 변환 (Stream→Job→Execution) | `flink-streaming-java/api/`, `flink-runtime/jobgraph/`, `flink-runtime/executiongraph/` |
| StreamTask 메인 루프 + mailbox 모델 | `flink-streaming-java/runtime/tasks/` |
| Operator 추상 (Source/Sink 포함) | `flink-streaming-java/api/operators/` |
| ResourceManager / Slot / TaskExecutor | `flink-runtime/resourcemanager/`, `flink-runtime/slots/`, `flink-runtime/taskexecutor/` |
| RPC (Pekko) | `flink-rpc/flink-rpc-core`, `flink-rpc/flink-rpc-akka` |
| MiniCluster (디버깅 베이스) | `flink-runtime/minicluster/` |

---

## Tier 2 — 확장

| 주제 | 모듈 | 메모 |
|------|------|------|
| DefaultScheduler (비교용) | `flink-runtime/scheduler/` | AdaptiveScheduler의 대조군 |
| Failover 전략 | `flink-runtime/executiongraph/failover/` | Operator의 자동 복구 동작 이해 |
| Network Shuffle | `flink-runtime/io/network/` | 백프레셔, latency 디버깅 시 |
| Watermark / 시간 | `flink-streaming-java/api/windowing/`, `flink-core/api/common/eventtime/`, `flink-runtime/operators/windowing/` | Kafka 이벤트 시간 처리 |
| 신 DataStream API (FLIP-409) | `flink-datastream-api/`, `flink-datastream/` | 향후 표준 가능성, 컨트리뷰션 노림수 |
| State V2 async API (FLIP-424) | `flink-runtime/asyncprocessing/`, `flink-core/core/state/` | ForSt 활용의 전제 |

---

## Tier 3 — 후순위 / 필요 시

- `flink-table/*` 전체 (별도 코스)
- `flink-libraries/{flink-cep, flink-state-processing-api, flink-gelly}`
- `flink-clients`, `flink-runtime-web`
- `flink-metrics/*` (디버깅 시)
- `flink-formats/{avro, json, orc, protobuf, csv}` (현재 미사용)
- `flink-filesystems/{flink-azure-fs-hadoop, flink-gs-fs-hadoop, flink-oss-fs-hadoop}` (다른 cloud)

---

## Tier 4 — 분석 제외

`flink-python`, `flink-quickstart`, `flink-examples` (인용용만), `flink-tests*`, `flink-end-to-end-tests`, `flink-dist*`, `flink-architecture-tests`, `flink-docs`

---

## `flink-runtime` 서브패키지 우선순위

(파일 수 기준, release-2.0 측정)

| # | 서브패키지 | Java 파일 | 다룰 주제 |
|---|------------|-----------|----------|
| 1 | `state/` | 340 | KeyedStateBackend, snapshot strategy |
| 2 | `io/network/` | 288 | ResultPartition/InputGate, NetworkBuffer, Netty stack, credit-based flow |
| 3 | `scheduler/` | 169 | DefaultScheduler, **AdaptiveScheduler**, SlotAllocator |
| 4 | `operators/` | 150 | 런타임 operator (sort/join/hash) |
| 5 | `checkpoint/` | 135 | CheckpointCoordinator, Barrier, alignment |
| 6 | `jobmaster/` | 108 | JobMaster (개별 Job 조율) |
| 7 | `executiongraph/` | 85 | ExecutionGraph, ExecutionVertex, failover |
| 8 | `taskexecutor/` | 69 | TaskExecutor, Slot, Task lifecycle |
| 9 | `resourcemanager/` | 62 | RM, SlotManager, declarative slot allocation |
| 10 | `dispatcher/` | 60 | Job 제출 진입점 |
| — | `minicluster/` | (보조) | 디버깅 베이스 ★★★ |
| — | `jobgraph/` | (보조) | JobGraph, JobVertex, IntermediateDataSet |
| — | `shuffle/` | (보조) | Shuffle service 추상화 |
| — | `source/coordinator/` | (보조) | SourceCoordinator (Tier 0 #1과 직결) |

---

## 모듈 ↔ 문서 매핑

| 문서 카테고리 | 주로 분석할 모듈 |
|--------------|------------------|
| `02-job-fundamentals/` | `flink-runtime/streaming/api/environment/` (★ release-2.0에서 streaming-java→runtime로 이동), `flink-core/core/execution/`, `flink-clients/client/deployment/`, `flink-kubernetes/entrypoint/` |
| `03-graph-transformation/` | `flink-runtime/streaming/api/`, `flink-runtime/{jobgraph, executiongraph}/` |
| `04-runtime-architecture/` | `flink-runtime/{dispatcher, resourcemanager, jobmaster, taskexecutor, minicluster}/`, `flink-rpc/` |
| `05-state-checkpoint/` | `flink-runtime/{state, checkpoint, asyncprocessing}/`, `flink-state-backends/{rocksdb, forst, changelog}`, `flink-dstl` |
| `06-source-sink-spi/` | `flink-core/api/connector/{source, sink2}/`, `flink-runtime/source/coordinator/`, `flink-streaming-java/api/operators/sink/`, `flink-connectors/flink-connector-base` |
| `07-filesystem-checkpoint-store/` | `flink-core/core/fs/`, `flink-filesystems/flink-s3-fs-*/`, `flink-runtime/state/filesystem/` |
| `08-file-formats/` | `flink-formats/flink-parquet/`, `flink-connectors/flink-connector-files/`, `flink-file-sink-common` |
| `09-kubernetes-integration/` | `flink-kubernetes/` |
| `10-scheduling-failover/` | `flink-runtime/scheduler/{, adaptive}/`, `flink-runtime/slots/`, `flink-runtime/executiongraph/failover/` |
| `11-network-shuffle/` | `flink-runtime/io/network/`, `flink-runtime/shuffle/`, `flink-streaming-java/runtime/io/` |
| `12-watermark-time/` | `flink-streaming-java/api/windowing/`, `flink-core/api/common/eventtime/`, `flink-runtime/operators/windowing/` |
| `13-new-datastream-v2/` | `flink-datastream-api/`, `flink-datastream/` |
