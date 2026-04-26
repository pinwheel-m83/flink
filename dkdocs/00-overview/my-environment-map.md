# 본인 환경 ↔ Flink 컴포넌트 매핑

> 본 문서는 운영 환경의 각 요소가 Flink internals의 어느 컴포넌트와 연결되는지 보여준다. 학습 우선순위와 디버깅 진입점의 단일 참조표 역할.

---

## 운영 스택 요약

| 영역 | 사용 기술 |
|------|----------|
| 실행 환경 | **Kubernetes** + **Apache Flink K8s Operator** (autoscaler 사용) |
| Source | **Apache Kafka** |
| Sink (object store) | **MinIO** (S3 API 호환) |
| Storage / Table format | **Apache Iceberg** + **Apache Parquet** |
| Iceberg Catalog | **Apache Polaris** (REST Catalog, OAuth2 Client Credentials) — *현재 HMS, 2주 내 마이그레이션* |
| State backend | **RocksDB** (운영) + **ForSt** 🧪 (PoC 트랙) |
| Scheduler | **AdaptiveScheduler** (Operator autoscaler 연동) |

---

## 환경 → Flink 매핑 (이 레포 안)

| 환경 요소 | 동작하는 Flink 컴포넌트 | 모듈 / 진입점 | dkdocs |
|----------|-----------------------|--------------|--------|
| K8s 위 JobManager/TaskManager 실행 | `KubernetesResourceManagerDriver`, kubeclient, Entrypoint | `flink-kubernetes/` (`entrypoint/`, `kubeclient/`) | [`09-kubernetes-integration/`](../09-kubernetes-integration/) |
| K8s ConfigMap 기반 HA / 리더 선출 | `KubernetesLeaderElectionDriver`, `KubernetesCheckpointRecoveryFactory` | `flink-kubernetes/highavailability/` | [`09-kubernetes-integration/`](../09-kubernetes-integration/) |
| Operator autoscaler가 parallelism 조정 | AdaptiveScheduler + Externalized Declarative Resource Management | `flink-runtime/scheduler/adaptive/AdaptiveScheduler.java` | [`10-scheduling-failover/`](../10-scheduling-failover/) |
| Kafka Source가 사용하는 SPI | Source V2 (Split/Enumerator/Reader), `SourceCoordinator` | `flink-core/api/connector/source/`, `flink-runtime/source/coordinator/` | [`06-source-sink-spi/`](../06-source-sink-spi/) |
| Iceberg Sink가 사용하는 SPI | Sink V2, `Committer`, `GlobalCommitter`, 2PC | `flink-core/api/connector/sink2/`, `flink-streaming-java/api/operators/sink/` | [`06-source-sink-spi/`](../06-source-sink-spi/) |
| MinIO 체크포인트 저장소 | `FileSystem` 추상, `RecoverableWriter`, S3 multipart upload | `flink-core/core/fs/`, `flink-filesystems/flink-s3-fs-{base,hadoop,presto}/` | [`07-filesystem-checkpoint-store/`](../07-filesystem-checkpoint-store/) |
| MinIO 직접 파일 Sink | FileSink (Sink V2 기반) | `flink-connectors/flink-connector-files/sink/`, `flink-file-sink-common` | [`06-source-sink-spi/`](../06-source-sink-spi/), [`07-filesystem-checkpoint-store/`](../07-filesystem-checkpoint-store/) |
| Parquet 포맷 (읽기/쓰기) | Vectorized reader, BulkWriter | `flink-formats/flink-parquet/` | [`08-file-formats/`](../08-file-formats/) |
| Kafka offset → Iceberg 스냅샷 exactly-once | CheckpointCoordinator + 2PC commit chain | `flink-runtime/checkpoint/` | [`05-state-checkpoint/`](../05-state-checkpoint/) |
| 큰 keyed state + 증분 체크포인트 | RocksDB state backend | `flink-state-backends/flink-statebackend-rocksdb/` | [`05-state-checkpoint/`](../05-state-checkpoint/) |
| 향후 cloud-native state (PoC) | ForSt + State V2 async API | `flink-state-backends/flink-statebackend-forst/`, `flink-runtime/asyncprocessing/` | [`05-state-checkpoint/`](../05-state-checkpoint/) |

---

## 환경 → 외부 레포 매핑

(레포는 [`../../external/`](../../external/) 아래에 클론)

| 외부 레포 | 본인 환경에서의 역할 | Flink 측 진입점 (이 레포 내) |
|----------|--------------------|---------------------------|
| `apache/flink-kubernetes-operator` | 실제 배포/스케일링 컨트롤러. CR(`FlinkDeployment`) → Pod | `flink-kubernetes/entrypoint/` (Operator가 띄우는 JM/TM 엔트리포인트) |
| `apache/flink-connector-kafka` | Kafka Source/Sink 구현체 | `flink-core/api/connector/source/`, `flink-runtime/source/coordinator/` |
| `apache/iceberg` (sparse: `flink/`) | Iceberg-Flink 통합 (Sink + Catalog) | `flink-core/api/connector/sink2/`, `flink-streaming-java/api/operators/sink/` |
| `apache/polaris` | Iceberg REST Catalog 서버. HMS 대체 | (Flink 측은 iceberg-flink의 RESTCatalog를 통해 호출) |

각 외부 레포의 자세한 분석은 [`../99-external-references/`](../99-external-references/) 참조.

---

## 주요 결정 사항 (이 환경에 한정)

| 결정 | 내용 | 근거 |
|------|------|------|
| State backend (운영) | **RocksDB** | 안정성 검증, 사용자 환경에 충분 |
| State backend (PoC) | **ForSt** + State V2 async API | Flink 2.x master 문서 기준 여전히 `@Experimental`. 향후 GA 대비 학습 |
| Scheduler | **AdaptiveScheduler** | Operator autoscaler 동작에 필수 (Externalized Declarative Resource Management) |
| Iceberg Catalog | **Polaris** (REST Catalog) | HMS는 2주 내 마이그레이션 예정 |
| Polaris 인증 | **OAuth2 Client Credentials** | 가장 일반적, 추후 다른 방식으로 확장 가능 |
| Table API | 1차 학습 범위에서 제외 | 별도 코스로 분리 (Calcite 기반, 거대 영역) |

---

## 디버깅 진입점 단축 표

본인 환경에서 무언가가 잘못됐을 때 어디부터 봐야 하는지:

| 증상 | 1차 의심 모듈 | 핵심 클래스 |
|------|-------------|-----------|
| 체크포인트가 느림/실패 | `flink-runtime/checkpoint/` + `flink-filesystems/flink-s3-fs-*` | `CheckpointCoordinator`, `S3RecoverableWriter` |
| Iceberg commit 실패 | Sink V2 commit 체인 | `Committer`, `CommittingSinkWriter` (외부: iceberg-flink) |
| Kafka에서 메시지 누락/중복 | Source V2 + Checkpoint | `SourceCoordinator`, `SplitEnumerator` |
| Pod이 자꾸 재시작 | K8s integration + Failover | `KubernetesResourceManagerDriver`, `ExecutionFailureHandler` |
| 오토스케일이 동작 안 함 | AdaptiveScheduler | `AdaptiveScheduler`, `SlotAllocator` |
| Backpressure | Network shuffle | `ResultPartition`, `LocalBufferPool`, credit-based flow |
| State 메모리 부족 | RocksDB state backend | `RocksDBKeyedStateBackend`, `OpaqueMemoryResource` |
