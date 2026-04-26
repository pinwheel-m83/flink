# `JobGraph` → `ExecutionGraph` — Subtask 단위로 펼치기

> **요약**: JM이 받은 `JobGraph`를 `DefaultExecutionGraphBuilder.buildGraph(...)`가 어떻게 `ExecutionGraph`로 펼치는지, `ExecutionJobVertex`/`ExecutionVertex`/`Execution`/`IntermediateResult`/`IntermediateResultPartition` 5단 구조의 의미와 그 위에서 시작되는 `ExecutionState` 머신을 코드 레벨로 정리한다.
> **모듈**: `flink-runtime` (`runtime/executiongraph/*`)
> **기준 버전**: Flink `release-2.0`
> **운영 권장 여부**: ★Production
> **선행**: [`./02-job-graph.md`](./02-job-graph.md)

---

## 1. TL;DR (3문장)

`JobGraph`는 vertex의 **logical 정의**(parallelism은 메타데이터)인 반면, `ExecutionGraph`는 그것을 실제로 실행할 **runtime 인스턴스 트리** — JobVertex 1개당 `ExecutionJobVertex` 1개, 그 안에 `parallelism`만큼의 `ExecutionVertex`(=subtask), 각 `ExecutionVertex`가 `Execution`(=한 시도)을 갖는다. `JobGraph`의 `IntermediateDataSet`도 `IntermediateResult`로 풀리고, 그 아래 `IntermediateResultPartition`이 producer subtask 1개당 1개씩 만들어진다. 이 펼침은 `DefaultExecutionGraphBuilder.buildGraph(...)`가 `attachJobGraph(sortedTopology, ...)` 호출 시 일어나며, 그 결과 위에서 [`Scheduler`](../10-scheduling-failover/) 가 동작을 시작한다.

---

## 2. 사전 지식

### 2.1 동시성 — Execution 클래스의 lock-free 상태 전이

`Execution`의 javadoc(`Execution.java:96+`)이 명시: "Lock free state transitions" — task deploy 중에 cancel이 들어오는 등 동시성 케이스를 lock 대신 **atomic state update + 검증 + idempotent action**으로 처리한다. 분산 시스템에서 lock의 cost와 deadlock 위험을 피하기 위함. 이 패턴은 Flink runtime 전반에 퍼져 있다 ([`../04-runtime-architecture/`](../04-runtime-architecture/) 예정).

### 2.2 `Either<L, R>` (대안 합집합 타입)

`ExecutionJobVertex.taskInformationOrBlobKey: Either<SerializedValue<TaskInformation>, PermanentBlobKey>` — TaskInformation을 직렬화한 raw bytes 자체를 가지거나, BLOB로 offload된 후의 key를 가지거나 **둘 중 하나**. Scala의 영향을 받은 `Either` 타입은 Flink util에 구현돼 있다.

### 2.3 `LinkedHashMap`의 insertion order (다시 강조)

`ExecutionVertex.resultPartitions = new LinkedHashMap<>(producedDataSets.length, 1)` — partition을 vertex가 만든 순서대로 보존. shuffle 시 partition index 결정에 영향.

---

## 3. 핵심 클래스 / 진입점

| 역할 | 클래스 | 위치 |
|------|------|------|
| 빌더 (메인 진입) | `DefaultExecutionGraphBuilder` | `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/DefaultExecutionGraphBuilder.java` |
| 결과 그래프 | `DefaultExecutionGraph` (`implements ExecutionGraph`) | `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/DefaultExecutionGraph.java` |
| JobVertex의 runtime 짝 | `ExecutionJobVertex` | `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/ExecutionJobVertex.java` |
| Subtask 1개 | `ExecutionVertex` | `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/ExecutionVertex.java` |
| 실행 시도 1번 | `Execution` | `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/Execution.java` |
| IntermediateDataSet의 runtime 짝 | `IntermediateResult` | `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/IntermediateResult.java` |
| Producer subtask 1개당 partition | `IntermediateResultPartition` | `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/IntermediateResultPartition.java` |
| 상태 enum | `ExecutionState` | `flink-runtime/src/main/java/org/apache/flink/runtime/execution/ExecutionState.java` |
| 빌더 팩토리 (스케줄러 진입) | `DefaultExecutionGraphFactory` | `flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/DefaultExecutionGraphFactory.java` |

---

## 4. 구조 펼침 (개념도)

```
JobGraph (cluster가 받은 것)                      ExecutionGraph (cluster가 만드는 것)
─────────────────────────                         ─────────────────────────────────
JobVertex A                          ─→         ExecutionJobVertex A
  parallelism = 3                                 ├─ ExecutionVertex A.0 ──→ Execution attempt#0
                                                   │   resultPartitions: [A→B partition 0]
                                                   ├─ ExecutionVertex A.1 ──→ Execution attempt#0
                                                   │   resultPartitions: [A→B partition 1]
                                                   └─ ExecutionVertex A.2 ──→ Execution attempt#0
                                                       resultPartitions: [A→B partition 2]

IntermediateDataSet (A→B)            ─→         IntermediateResult (A→B)
  resultType=PIPELINED                            ├─ partition[0]   (from A.0)
  distribution=ALL_TO_ALL                         ├─ partition[1]   (from A.1)
                                                   └─ partition[2]   (from A.2)

JobVertex B                          ─→         ExecutionJobVertex B
  parallelism = 2                                 ├─ ExecutionVertex B.0 ──→ Execution attempt#0
                                                   │   reads: [A→B partitions 0,1,2]   (ALL_TO_ALL)
                                                   └─ ExecutionVertex B.1 ──→ Execution attempt#0
                                                       reads: [A→B partitions 0,1,2]
```

핵심:
- **vertex × parallelism = subtask**. 3-parallel JobVertex는 3개의 `ExecutionVertex`로 풀린다.
- **Execution은 attempt 단위**. Subtask가 실패해 재시작되면 **새 `Execution` 객체**가 생기고, 이전 것은 `executionHistory`에 들어간다.
- **producer subtask마다 IntermediateResultPartition 1개**. 즉 IntermediateResult.partitions[i]는 i번째 producer subtask가 만들어내는 출력.
- consumer 측은 distribution pattern에 따라 partitions의 부분집합 또는 전체를 읽는다 (위 다이어그램은 ALL_TO_ALL).

---

## 5. 코드 워크스루

### 5.1 진입 — `DefaultExecutionGraphBuilder.buildGraph(...)`

`flink-runtime/.../DefaultExecutionGraphBuilder.java:76-323` (구조 발췌):

```java
public static DefaultExecutionGraph buildGraph(
        JobGraph jobGraph,
        Configuration jobManagerConfig,
        ScheduledExecutorService futureExecutor,
        Executor ioExecutor,
        ClassLoader classLoader,
        CompletedCheckpointStore completedCheckpointStore,
        CheckpointsCleaner checkpointsCleaner,
        CheckpointIDCounter checkpointIdCounter,
        Duration rpcTimeout,
        BlobWriter blobWriter,
        Logger log,
        ShuffleMaster<?> shuffleMaster,
        JobMasterPartitionTracker partitionTracker,
        TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
        ExecutionDeploymentListener executionDeploymentListener,
        ExecutionStateUpdateListener executionStateUpdateListener,
        long initializationTimestamp,
        VertexAttemptNumberStore vertexAttemptNumberStore,
        VertexParallelismStore vertexParallelismStore,
        CheckpointStatsTracker checkpointStatsTracker,
        boolean isDynamicGraph,
        ExecutionJobVertex.Factory executionJobVertexFactory,
        MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
        boolean nonFinishedHybridPartitionShouldBeUnknown,
        JobManagerJobMetricGroup jobManagerJobMetricGroup,
        ExecutionPlanSchedulingContext executionPlanSchedulingContext)
        throws JobExecutionException, JobException {

    checkNotNull(jobGraph, "job graph cannot be null");

    // ─── (a) JobInformation 만들기 (jar BLOB key, classpaths, etc.) ───
    final JobInformation jobInformation = new JobInformation(
        jobId, jobGraph.getJobType(), jobName,
        jobGraph.getSerializedExecutionConfig(),
        jobGraph.getJobConfiguration(),
        jobGraph.getUserJarBlobKeys(),
        jobGraph.getClasspaths());

    // ─── (b) TaskDeploymentDescriptorFactory 준비 (TM에게 보낼 정보 묶음) ───
    final TaskDeploymentDescriptorFactory taskDeploymentDescriptorFactory =
            new TaskDeploymentDescriptorFactory( /* ... offload via blob ... */ );

    // ─── (c) DefaultExecutionGraph 생성 (아직 비어 있음) ───
    final DefaultExecutionGraph executionGraph = new DefaultExecutionGraph(
            jobInformation, futureExecutor, ioExecutor, rpcTimeout,
            executionHistorySizeLimit, classLoader, blobWriter,
            partitionGroupReleaseStrategyFactory,
            shuffleMaster, partitionTracker,
            executionDeploymentListener, executionStateUpdateListener,
            initializationTimestamp, vertexAttemptNumberStore, vertexParallelismStore,
            isDynamicGraph, executionJobVertexFactory,
            jobGraph.getJobStatusHooks(),
            markPartitionFinishedStrategy, taskDeploymentDescriptorFactory,
            jobStatusChangedListeners, executionPlanSchedulingContext);

    // ─── (d) JSON plan 생성 (Web UI Plan 탭) ───
    executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));

    // ─── (e) JobVertex별 master 측 초기화 (Source의 inputSplit 생성 등) ───
    initJobVerticesOnMaster(jobGraph.getVertices(), classLoader, log,
                            vertexParallelismStore, jobName, jobId);

    // ─── (f) 토폴로지 정렬 후 attachJobGraph (실제 펼침) ───
    List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
    executionGraph.attachJobGraph(sortedTopology, jobManagerJobMetricGroup);

    // ─── (g) checkpointing 활성화 (state backend, checkpoint storage 결정 + enableCheckpointing) ───
    if (isCheckpointingEnabled(jobGraph)) {
        // StateBackend 로드 (application + flink-conf 우선순위)
        final StateBackend rootBackend = StateBackendLoader.fromApplicationOrConfigOrDefault(...);
        // CheckpointStorage 로드 (S3/FileSystem/Memory 등)
        final CheckpointStorage rootStorage = CheckpointStorageLoader.load(...);
        // master hooks 인스턴스화
        // ...
        executionGraph.enableCheckpointing(
                chkConfig, hooks, checkpointIdCounter, completedCheckpointStore,
                rootBackend, rootStorage, checkpointStatsTracker, checkpointsCleaner,
                jobManagerConfig.get(STATE_CHANGE_LOG_STORAGE));
    }

    return executionGraph;
}
```

7단계로 정리:
- **(a)** `JobInformation` — JM이 TM에게 일관되게 보낼 잡 정보 묶음
- **(b)** `TaskDeploymentDescriptorFactory` — TM에게 보낼 deployment descriptor를 BLOB offload 포함해 만들어주는 팩토리
- **(c)** **빈 ExecutionGraph 인스턴스** 생성 (vertex 없음, partition 없음)
- **(d)** Web UI용 JSON plan 미리 굽기 (운영 디버깅에 매우 유용)
- **(e)** **`initJobVerticesOnMaster`** — JM 측에서만 해야 하는 초기화 (예: `InputSplitSource`로 source의 split 만들어두기)
- **(f)** **`attachJobGraph(sortedTopology)`** — 진짜 펼침. 다음 절.
- **(g)** Checkpointing 인프라 활성화 — 본인 환경의 RocksDB + MinIO 매핑이 여기서 일어남 (state backend `StateBackendLoader.fromApplicationOrConfigOrDefault(...)`, checkpoint storage `CheckpointStorageLoader.load(...)`). 이 부분은 [`../05-state-checkpoint/`](../05-state-checkpoint/) 에서 깊이 다룬다.

### 5.2 `DefaultExecutionGraph.attachJobGraph(...)` — 실제 펼침

`flink-runtime/.../DefaultExecutionGraph.java:855-879`:

```java
@Override
public void attachJobGraph(
        List<JobVertex> verticesToAttach, JobManagerJobMetricGroup jobManagerJobMetricGroup)
        throws JobException {

    assertRunningInJobMasterMainThread();   // (a) JM main thread 보장

    LOG.debug("Attaching {} topologically sorted vertices to existing job graph with {} "
                + "vertices and {} intermediate results.",
            verticesToAttach.size(), tasks.size(), intermediateResults.size());

    attachJobVertices(verticesToAttach, jobManagerJobMetricGroup);   // (b) 각 JobVertex → ExecutionJobVertex
    if (!isDynamic) {
        initializeJobVertices(verticesToAttach);                      // (c) parallelism 결정 + ExecutionVertex 배열 만들기
    }

    // (d) topology assigning은 새 vertex가 failoverStrategy에 통지되기 전에 수행
    executionTopology = DefaultExecutionTopology.fromExecutionGraph(this);

    partitionGroupReleaseStrategy =
            partitionGroupReleaseStrategyFactory.createInstance(getSchedulingTopology());  // (e)
}
```

- **(a)** JM main thread 강제 — Flink runtime의 lock-free 모델은 단일 스레드 가정에 의존
- **(b) `attachJobVertices`** — 각 JobVertex 1개당 `ExecutionJobVertex` 1개 생성 후 `tasks` 맵에 등록
- **(c) `initializeJobVertices`** — non-dynamic graph일 때, `ExecutionJobVertex` 안에서 `ExecutionVertex[parallelism]` 배열을 채움. dynamic graph(Adaptive Batch)는 나중에 채워짐.
- **(d)** `DefaultExecutionTopology` — 스케줄러가 사용할 가벼운 view (vertex/edge id만 노출). 별도 도메인 객체.
- **(e)** `partitionGroupReleaseStrategy` — pipelined region이 끝나면 그 결과 partition을 언제 해제할지 결정

### 5.3 `ExecutionJobVertex` — JobVertex의 runtime 짝

`flink-runtime/.../ExecutionJobVertex.java:80-132` (대표 필드):

```java
/**
 * An {@code ExecutionJobVertex} is part of the {@link ExecutionGraph}, and the peer to the {@link
 * JobVertex}.
 *
 * <p>The {@code ExecutionJobVertex} corresponds to a parallelized operation. It contains an {@link
 * ExecutionVertex} for each parallel instance of that operation.
 */
public class ExecutionJobVertex
        implements AccessExecutionJobVertex, Archiveable<ArchivedExecutionJobVertex> {

    private final InternalExecutionGraphAccessor graph;
    private final JobVertex jobVertex;                       // 원본 JobVertex 보존

    @Nullable private ExecutionVertex[] taskVertices;        // ★ parallelism 만큼 채워짐
    @Nullable private IntermediateResult[] producedDataSets; // 이 vertex가 만드는 결과들 (보통 1개)
    @Nullable private List<IntermediateResult> inputs;       // 읽는 결과들

    private final VertexParallelismInformation parallelismInfo;
    private final SlotSharingGroup slotSharingGroup;
    @Nullable private final CoLocationGroup coLocationGroup;
    @Nullable private InputSplit[] inputSplits;              // source면 split 미리 계산
    private final ResourceProfile resourceProfile;

    private int numExecutionVertexFinished;                  // Bounded source의 종료 카운팅

    /** TaskInformation을 직렬화한 결과 또는 BLOB로 offload된 키 (둘 중 하나). */
    private Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInformationOrBlobKey;

    private final Collection<OperatorCoordinatorHolder> operatorCoordinators;  // SourceCoordinator 등
    @Nullable private InputSplitAssigner splitAssigner;
    // ...
}
```

핵심:
- `taskVertices: ExecutionVertex[]` — 이 vertex의 모든 subtask 인스턴스
- `producedDataSets: IntermediateResult[]` — JobVertex의 `results` 맵에 대응
- `inputs: List<IntermediateResult>` — JobVertex의 `inputs` 리스트(JobEdge들의 producer 측 IntermediateDataSet에 대응)에 대응
- `taskInformationOrBlobKey` — TaskInformation 직렬화 결과를 들고 있거나 (작으면), BLOB로 offload하고 key만 들고 있음 (크면). 모든 subtask가 공유.
- `operatorCoordinators` — Source V2 의 `SourceCoordinator`, Sink V2 의 `CommitterOperatorCoordinator` 등. **JM에서 생성되어 subtask들과 RPC로 통신**.

### 5.4 `ExecutionVertex` — 1개 subtask

`flink-runtime/.../ExecutionVertex.java:60-130` (대표 필드 + 생성자):

```java
public class ExecutionVertex
        implements AccessExecutionVertex, Archiveable<ArchivedExecutionVertex> {

    final ExecutionJobVertex jobVertex;
    private final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitions;
    private final int subTaskIndex;
    private final ExecutionVertexID executionVertexId;
    final ExecutionHistory executionHistory;        // 과거 Execution들 (재시도 기록)
    private final Duration timeout;
    private final String taskNameWithSubtask;       // "myTask (2/7)" 형식

    /** The current or latest execution attempt of this vertex's task. */
    Execution currentExecution;                     // null 아님 보장
    final ArrayList<InputSplit> inputSplits;
    private int nextAttemptNumber;
    private long inputBytes;

    /** This field holds the allocation id of the last successful assignment. */
    @Nullable private TaskManagerLocation lastAssignedLocation;
    @Nullable private AllocationID lastAssignedAllocationID;

    public ExecutionVertex(
            ExecutionJobVertex jobVertex,
            int subTaskIndex,
            IntermediateResult[] producedDataSets,
            ...) {
        this.jobVertex = jobVertex;
        this.subTaskIndex = subTaskIndex;
        this.executionVertexId = new ExecutionVertexID(jobVertex.getJobVertexId(), subTaskIndex);
        this.taskNameWithSubtask = String.format(
                "%s (%d/%d)",
                jobVertex.getJobVertex().getName(),
                subTaskIndex + 1,
                jobVertex.getParallelism());
        this.resultPartitions = new LinkedHashMap<>(producedDataSets.length, 1);

        for (IntermediateResult result : producedDataSets) {
            IntermediateResultPartition irp =
                    new IntermediateResultPartition(
                            result, this, subTaskIndex,
                            getExecutionGraphAccessor().getEdgeManager());
            result.setPartition(subTaskIndex, irp);
            resultPartitions.put(irp.getPartitionId(), irp);
        }
        // ...
    }
}
```

핵심 관찰:
- 생성자에서 **producer 역할의 `IntermediateResult`마다 `IntermediateResultPartition`을 만들고** 그것을 `result.partitions[subTaskIndex]` 자리에 박는다. 즉 IntermediateResult.partitions[i] ↔ ExecutionVertex(i).resultPartitions가 양방향.
- `executionVertexId = (JobVertexID, subTaskIndex)` — 잡 안에서 subtask 유일 식별.
- `currentExecution`은 vertex 생성 시 1개 `Execution`으로 초기화되고, 재시도마다 새 인스턴스로 교체. 과거는 `executionHistory`에 보관 (제한된 크기 — `MAX_ATTEMPTS_HISTORY_SIZE`).
- `lastAssignedLocation` — 직전 성공 배치의 TM 위치. 재시도 시 같은 위치 우선 (state local recovery 도움).

### 5.5 `Execution` — 한 번의 실행 시도

`flink-runtime/.../Execution.java:96-130` (개념 정리):

```java
/**
 * A single execution of a vertex. While an {@link ExecutionVertex} can be executed multiple times
 * (for recovery, re-computation, re-configuration), this class tracks the state of a single
 * execution of that vertex and the resources.
 *
 * <h2>Lock free state transitions</h2>
 * ...
 */
public class Execution
        implements AccessExecution, Archiveable<ArchivedExecution>, LogicalSlot.Payload {

    private final Executor executor;
    private final ExecutionVertex vertex;
    private ExecutionAttemptID attemptId;       // 잡 안 + 재시도 중 유일
    private final long[] stateTimestamps;       // ExecutionState.ordinal() 인덱스로 시작 시각
    // ... (deployment, slot, current state, future, ...)
}
```

핵심:
- **`ExecutionAttemptID`** — JobID + JobVertexID + subTaskIndex + attemptNumber 를 묶은 유일 ID. TM 측에서 task를 식별하는 키.
- 상태 전이는 atomic CAS로 처리. lock 없음.
- 실패 시 새 `Execution` 객체가 생기고 attempt 번호가 +1 — `ExecutionVertex.executionHistory`에 옛 것이 옮겨짐.
- `LogicalSlot.Payload`를 구현 — slot이 자기에게 어떤 task가 배치됐는지 알 수 있게 함 (slot 해제 시 cleanup 호출 등).

### 5.6 `ExecutionState` — 상태 머신

`flink-runtime/.../ExecutionState.java:20-58`:

```
CREATED  ──→ SCHEDULED ──→ DEPLOYING ──→ INITIALIZING ──→ RUNNING ──→ FINISHED
   │            │              │              │              │
   │            │              │              ┴──────┬───────┘
   │            │              │                     │
   │            │              ↓              ↓
   │            │           CANCELING ──┬──→ CANCELED
   │            │                       │
   │            └──────────────────────┘
   │
   │
   ↓                                              ... ──→ FAILED
RECONCILING ──→ INITIALIZING | RUNNING | FINISHED | CANCELED | FAILED
```

- **`CREATED`** → ExecutionVertex 생성 직후
- **`SCHEDULED`** → Scheduler가 slot 요청 진행
- **`DEPLOYING`** → TM에게 RPC로 task descriptor 보냄
- **`INITIALIZING`** → TM 측에서 task 객체 생성, state 복원, network 연결
- **`RUNNING`** → 실제 record 처리 시작
- **`FINISHED`** → bounded input 완료 또는 정상 종료
- **`CANCELING` / `CANCELED`** → 사용자/JM이 cancel 요청
- **`FAILED`** → 어디서든 발생 가능 (일반적으로 재시도 → 새 Execution)
- **`RECONCILING`** → JM failover 후 재기동 시 TM에 살아있는 task와 매칭하는 단계

본인 환경(K8s + Operator)에선 JM Pod failover 시 `RECONCILING`을 자주 보게 된다.

### 5.7 `IntermediateResult` & `IntermediateResultPartition`

`flink-runtime/.../IntermediateResult.java`:

```java
public class IntermediateResult {
    private final IntermediateDataSet intermediateDataSet;     // 원본 JobGraph 측
    private final IntermediateDataSetID id;
    private final ExecutionJobVertex producer;
    private final IntermediateResultPartition[] partitions;   // ★ producer subtask 수만큼
    private final HashMap<IntermediateResultPartitionID, Integer> partitionLookupHelper;
    private final int numParallelProducers;
    private final ResultPartitionType resultType;
    // ...
}
```

`flink-runtime/.../IntermediateResultPartition.java`:

```java
public class IntermediateResultPartition {
    private final IntermediateResult totalResult;
    private final ExecutionVertex producer;             // 어떤 subtask가 만들어내는지
    private final IntermediateResultPartitionID partitionId;
    private final EdgeManager edgeManager;              // partition ↔ consumer subtask 매핑 관리
    private final int numberOfSubpartitionsForDynamicGraph;
    private boolean dataAllProduced = false;
    private final Set<ConsumedPartitionGroup> releasablePartitionGroups;
}
```

- producer subtask 수 = `IntermediateResultPartition` 수.
- `dataAllProduced` — 이 partition의 모든 데이터가 produce 완료 (Bounded source면 true 가능, streaming은 끝나지 않음).
- `releasablePartitionGroups` — pipelined region scheduling에서 partition이 더 필요 없으면 release 가능.

ALL_TO_ALL distribution일 때, 한 consumer subtask는 모든 IntermediateResultPartition으로부터 자기 몫(=subpartition)을 읽는다. 즉:
- 출력 partition 수 = producer parallelism
- 각 partition 안의 subpartition 수 = consumer parallelism (또는 동적 결정)

이 구조의 자세한 데이터 흐름은 [`../11-network-shuffle/`](../11-network-shuffle/) 에서 다룬다.

---

## 6. 사용자 환경 매핑

### 6.1 본인 잡 (Kafka→keyBy→process→Iceberg)의 ExecutionGraph 펼침

JobGraph가 (앞 문서 6.1 절) 3 vertex로 chained된 결과를 가정하고 parallelism = 8:

| ExecutionJobVertex | parallelism | ExecutionVertex 개수 | 만드는 IntermediateResultPartition |
|-------|-------------|---------------------|-------------------|
| Source | 8 (Kafka 파티션 수에 맞춤) | 8 | 8개 (ALL_TO_ALL → keyBy) |
| Process | 8 | 8 | 8개 (Forward → Sink writer) |
| Iceberg writer | 8 | 8 | 8개 (Forward → Committer) |
| Iceberg committer | 1 | 1 | (terminal) |

→ 총 25개 `Execution` 인스턴스 (각 attempt 0). 본인 환경에서 한 잡이 띄우는 task 수.

### 6.2 AdaptiveScheduler ↔ ExecutionGraph 재구성

본인 환경의 핵심 설정 ([`../00-overview/my-environment-map.md`](../00-overview/my-environment-map.md)):

- AdaptiveScheduler가 가용 슬롯 변동을 감지 → **ExecutionGraph를 통째로 다시 빌드** → 새 attempt로 Execution 시작.
- 즉 parallelism 변경은 ExecutionGraph의 in-place 수정이 아니라 **새 ExecutionGraph 생성**. 기존 state는 savepoint/checkpoint를 통해 자동 restore.
- `maxParallelism`(=key group 수)이 새 parallelism의 상한이므로, 첫 배포 시 충분히 크게 잡는 것이 중요 (앞 문서들 강조).

이 메커니즘은 [`../10-scheduling-failover/adaptive-scheduler.md`](../10-scheduling-failover/) 에서 상세히 다룬다.

### 6.3 K8s Operator autoscaler가 보는 메트릭

Autoscaler는 ExecutionVertex 단위 메트릭(processing rate, busy time, backpressure)을 REST로 조회 → 새 parallelism 산출 → REST로 변경 명령 → AdaptiveScheduler가 ExecutionGraph 재구성.

→ Operator 측 동작은 외부 레포 (`external/flink-kubernetes-operator/`). dkdocs는 Flink 측 진입점만 다룸 ([`../09-kubernetes-integration/`](../09-kubernetes-integration/) 예정).

---

## 7. 관련 FLIP / JIRA

| 문서 | 무엇 |
|------|------|
| [FLIP-119: Pipelined Region Scheduling](https://cwiki.apache.org/confluence/display/FLINK/FLIP-119+Pipelined+Region+Scheduling) | `partitionGroupReleaseStrategy`의 배경 |
| [FLIP-160: Adaptive Scheduler](https://cwiki.apache.org/confluence/display/FLINK/FLIP-160:+Adaptive+Scheduler) | ExecutionGraph 재구성 메커니즘 |
| [FLIP-187: Adaptive Batch Scheduler](https://cwiki.apache.org/confluence/display/FLINK/FLIP-187%3A+Adaptive+Batch+Scheduler) | dynamic graph (`isDynamic`) |
| [FLIP-217: Support speculative execution](https://cwiki.apache.org/confluence/display/FLINK/FLIP-217%3A+Support+speculative+execution+of+arbitrary+operators) | 한 ExecutionVertex의 동시 다중 attempt |

---

## 8. 디버깅 & 실험

### 8.1 ExecutionGraph 상태 REST로 보기

```bash
curl http://<jm-rest-endpoint>/jobs/<jobId>
```

응답에 `vertices` 배열 — 각 vertex의 `subtasks`마다 `status`(=`ExecutionState`), `attempt`, `host`, `metrics`. 본인 환경에서 가장 자주 보는 endpoint.

### 8.2 코드로 따라가기

브레이크포인트:
- `DefaultExecutionGraphBuilder.java:76` — buildGraph 진입
- `DefaultExecutionGraph.java:855` — attachJobGraph
- `ExecutionVertex.java` 생성자 — 매 subtask 생성 시
- `Execution.transitionState(...)` (`Execution.java`) — 상태 전이마다

### 8.3 JSON plan 보기

`executionGraph.setJsonPlan(...)` 결과는 REST `/jobs/<jobId>/plan`로 조회. Web UI Plan 탭이 이걸 렌더링.

### 8.4 그래프 그래프 (MCP)

```
mcp__codebase-memory-mcp__search_graph(
  project="home-donamk-code-flink-flink-runtime",
  qn_pattern=".*ExecutionVertex\\.(scheduleForExecution|deploy|resetForNewExecution)$"
)
```

deploy/cancel/reset 같은 핵심 메서드를 따라가면 ExecutionVertex의 lifecycle을 파악할 수 있다.

---

## 9. 자주 묻는 질문 / 함정

**Q1. `ExecutionJobVertex.taskInformationOrBlobKey`가 `Either`인 이유?**
A. TaskInformation이 작으면 직렬화한 raw bytes를 들고 있고, 크면 BLOB(`PermanentBlobKey`)로 offload. 임계는 `BlobWriter.serializeAndTryOffload` 안에서 결정. 큰 task info(예: 사용자 함수가 무거운 경우)가 모든 deployment RPC에 실리면 JM heap/RPC payload가 폭발하므로 회피.

**Q2. `subTaskIndex`는 0부터 시작? `(2/7)`은 1부터?**
A. `subTaskIndex`는 0-based (코드). UI/log의 `(2/7)`은 1-based (사람용). 헷갈리지 말 것.

**Q3. `ExecutionState.RECONCILING`은 언제 보나?**
A. JM이 죽었다 살아난 직후, 살아있는 TM의 task와 다시 매칭해야 할 때. 본인 환경(K8s + HA)에서 JM Pod 재시작 시 발생. 매칭 후 RUNNING으로 직진할 수도 있고, 매칭 실패면 FAILED.

**Q4. Speculative execution이 켜지면 한 vertex에 Execution이 여러 개?**
A. 그렇다 (FLIP-217). 같은 ExecutionVertex의 여러 attempt가 동시 실행되어 가장 빠른 결과를 채택. 본인 환경(streaming) 기본 설정에선 비활성화.

**Q5. 왜 `attachJobGraph`가 vertex를 토폴로지 정렬해서 전달하나?**
A. ExecutionJobVertex 초기화 시 producer가 먼저 만들어져 있어야 consumer의 `inputs`(IntermediateResult 참조)를 채울 수 있다. 토폴로지 정렬이 이 dependency를 자연스럽게 해소.

**Q6. JobGraph 한 번 빌드하면 같은 ExecutionGraph가 영원히 살아있나?**
A. 아니. AdaptiveScheduler는 parallelism 변경 시 **새 ExecutionGraph를 통째로 만들고 옛 것을 폐기**한다. ExecutionGraph는 "이번 attempt 라이프"의 스냅샷.

---

## 10. 다음에 읽을 문서

| 다음 단계 | 문서 |
|----------|------|
| Scheduler가 ExecutionGraph 위에서 어떻게 동작하는가 (slot 요청 → deploy) | [`../10-scheduling-failover/adaptive-scheduler.md`](../10-scheduling-failover/01-adaptive-scheduler.md) |
| TaskManager 측에서 `Execution`이 어떻게 `Task`로 살아나는가 | [`../04-runtime-architecture/task-executor.md`](../04-runtime-architecture/04-task-executor.md) |
| `OperatorCoordinator` (Source/Sink V2의 JM 측 컴포넌트) | [`../06-source-sink-spi/source-coordinator.md`](../06-source-sink-spi/02-source-coordinator.md) |
| Checkpoint 메커니즘이 ExecutionGraph 위에서 어떻게 시작되는가 | [`../05-state-checkpoint/checkpoint-coordinator.md`](../05-state-checkpoint/01-checkpoint-coordinator.md) |
| IntermediateResultPartition의 데이터가 실제로 어떻게 전송되는가 (network shuffle) | [`../11-network-shuffle/`](../11-network-shuffle/) |
