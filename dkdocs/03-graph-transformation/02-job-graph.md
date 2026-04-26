# `StreamGraph` → `JobGraph` — Operator Chaining과 JobVertex

> **요약**: 클라이언트가 보낸 `StreamGraph`(또는 ApplicationMode에서는 같은 JVM 내)가 어떻게 `StreamingJobGraphGenerator.createJobGraph()`를 거쳐 **operator chaining**으로 노드를 합치고 `JobVertex` + `IntermediateDataSet` + `JobEdge`로 구성된 `JobGraph`로 변환되는지 코드 레벨로 따라간다.
> **모듈**: `flink-runtime` (`StreamingJobGraphGenerator`, `JobGraph`, `JobVertex`, `IntermediateDataSet`, `JobEdge`)
> **기준 버전**: Flink `release-2.0`
> **운영 권장 여부**: ★Production
> **선행**: [`./01-stream-graph.md`](./01-stream-graph.md)

---

## 1. TL;DR (3문장)

`StreamGraph`는 사용자 표현에 가까운 그래프이고, 이를 JobManager가 받을 수 있는 형태인 `JobGraph`로 변환하는 마지막 client-side(또는 client-equivalent) 단계가 `StreamingJobGraphGenerator.createJobGraph()`다. 핵심 작업은 **operator chaining** — 통신 비용을 줄이기 위해 forward + 동일 parallelism + 동일 slot sharing group + chainable인 연속된 `StreamNode`들을 한 `JobVertex` 안으로 합쳐 같은 subtask 안에서 in-thread call로 실행되게 만든다. 결과 `JobGraph`는 `Map<JobVertexID, JobVertex>` + 각 vertex가 만든 `IntermediateDataSet`들 + 그것들을 소비하는 `JobEdge`들로 구성된 정상적 DAG.

---

## 2. 사전 지식

### 2.1 Operator Chaining의 의미

Flink는 연속된 operator를 가능하면 **같은 thread 안에서 호출**하도록 묶는다 — 직렬화/네트워크/스케줄링 비용을 0으로 만든다. 비용이 중요한 이유는 streaming 잡의 throughput이 종종 record 직렬화 + 네트워크 buffer 큐에 좌우되기 때문. Chaining은 본인 환경의 Kafka→keyBy→process→Iceberg 파이프라인에서 `process→Iceberg writer` 부분이 한 task에서 in-thread로 도는 결정을 내린다.

### 2.2 `LinkedHashMap` (insertion order 보존)

`JobGraph.taskVertices = new LinkedHashMap<JobVertexID, JobVertex>()` (`JobGraph.java:78`). Vertex가 등록된 순서를 보존한다. 이는 디버깅 가능성(예측 가능한 plan dump)과 일부 알고리즘이 topological order에 의존하기 때문.

### 2.3 `LinkedHashMap` vs `HashMap` 의 차이를 Flink가 신경 쓰는 이유

분산 시스템에서 같은 입력에 대해 **노드 간 같은 결정**을 보장해야 할 때(예: chain hash 계산, distribution 결정) 순서가 중요해진다. 자세한 내용은 추후 `01-java-prerequisites/` 시리즈에서 다룸.

---

## 3. 핵심 클래스 / 진입점

| 역할 | 클래스 | 위치 |
|------|------|------|
| 변환 엔진 | `StreamingJobGraphGenerator` | `flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamingJobGraphGenerator.java` |
| 변환 진입 (StreamGraph 측) | `StreamGraph.getJobGraph(...)` | `flink-runtime/.../StreamGraph.java:1179` |
| 결과 그래프 | `JobGraph` (`implements ExecutionPlan`) | `flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/JobGraph.java` |
| 그래프 노드 (= chain 1개) | `JobVertex` | `flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/JobVertex.java` |
| 노드의 출력 데이터셋 | `IntermediateDataSet` | `flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/IntermediateDataSet.java` |
| Vertex 간 엣지 | `JobEdge` | `flink-runtime/.../jobgraph/JobEdge.java` |
| Operator 식별자 페어 | `OperatorIDPair` | `flink-runtime/.../jobgraph/OperatorIDPair.java` |
| Chaining 단계 컨텍스트 | `OperatorChainInfo` | `flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/util/OperatorChainInfo.java` |

---

## 4. 변환은 어디서 일어나는가 (release-2.0)

```
[Client/EmbeddedExecutor 측]                     [Cluster: Dispatcher / JobMaster]
StreamGraph 생성                                
   ↓
PipelineExecutor.execute(streamGraph, ...)
   ↓ (REST 또는 DispatcherGateway 직접 호출)
                                                Dispatcher.submitJob(streamGraph)
                                                   ↓
                                                streamGraph.getJobGraph(classLoader, jobID)
                                                   ↓
                                                StreamingJobGraphGenerator.createJobGraph()
                                                   ↓
                                                JobGraph → JobMaster 생성 → ExecutionGraph로 추가 변환
```

**핵심 사실**: release-2.0의 모든 PipelineExecutor (`LocalExecutor`, `RemoteExecutor` via `AbstractSessionClusterExecutor`, `EmbeddedExecutor`)는 **`StreamGraph` 자체를 `submitJob`에 넘긴다**. 즉 `StreamGraph` → `JobGraph` 변환은 **client가 아니라 cluster (또는 ApplicationMode의 같은 JVM 내 Dispatcher)** 가 한다. 두 클래스 모두 `ExecutionPlan`을 구현하기 때문에 wire 호환.

이 사실의 함의:
- 사용자 main이 죽어도 cluster는 자체적으로 `JobGraph`를 만들 수 있다
- `JobGraph` 변환 비용이 큰 잡(수천 노드)은 JM CPU에 부담을 준다
- chaining 결정은 cluster의 클래스로더로 이뤄진다 → factory 클래스가 cluster 측에 있어야 함

---

## 5. 코드 워크스루

### 5.1 진입 — `StreamGraph.getJobGraph(...)`

`flink-runtime/.../StreamGraph.java:1179-1181`:

```java
public JobGraph getJobGraph(ClassLoader userClassLoader, @Nullable JobID jobID) {
    return StreamingJobGraphGenerator.createJobGraph(userClassLoader, this, jobID);
}
```

`Dispatcher`가 `submitJob(streamGraph)`를 받으면 이 메서드(or 그 1-arg 오버로드)를 호출. 이후 작업은 generator에서.

### 5.2 `StreamingJobGraphGenerator.createJobGraph()` — 14단계 파이프라인

`flink-runtime/.../StreamingJobGraphGenerator.java:212-247`:

```java
private JobGraph createJobGraph() {
    preValidate(streamGraph, userClassloader);                 // (1)

    setChaining();                                              // (2) ★ 핵심: chaining 결정 + JobVertex 생성

    if (jobGraph.isDynamic()) {                                 // (3)
        setVertexParallelismsForDynamicGraphIfNecessary();
    }

    final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs =
            new HashMap<>();
    setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);  // (4)
    setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs);                            // (5)

    setPhysicalEdges(jobVertexBuildContext);                    // (6) JobEdge 실제 생성

    markSupportingConcurrentExecutionAttempts(jobVertexBuildContext);   // (7) speculative execution
    validateHybridShuffleExecuteInBatchMode(jobVertexBuildContext);     // (8) hybrid shuffle 검증
    setSlotSharingAndCoLocation(jobVertexBuildContext);                 // (9) slot 배치
    setManagedMemoryFraction(jobVertexBuildContext);                    // (10) operator별 managed mem 비율
    addVertexIndexPrefixInVertexName(jobVertexBuildContext, new AtomicInteger(0));  // (11)
    setVertexDescription(jobVertexBuildContext);                        // (12)

    serializeOperatorCoordinatorsAndStreamConfig(serializationExecutor, jobVertexBuildContext);  // (13) 직렬화

    return jobGraph;                                            // (14)
}
```

가장 중요한 두 단계:
- **(2) `setChaining()`** — `JobVertex` 자체의 형성. 어떤 `StreamNode`들이 한 vertex에 들어갈지 결정.
- **(6) `setPhysicalEdges(...)`** — chaining되지 않은 경계마다 `JobEdge`를 만들어 `IntermediateDataSet`를 통해 vertex 간 연결.

### 5.3 Chaining 결정 — `isChainable()`

`flink-runtime/.../StreamingJobGraphGenerator.java:1720-1726`:

```java
public static boolean isChainable(
        StreamEdge edge, StreamGraph streamGraph, boolean allowChainWithDefaultParallelism) {
    StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

    return downStreamVertex.getInEdges().size() == 1
            && isChainableInput(edge, streamGraph, allowChainWithDefaultParallelism);
}
```

먼저: **downstream이 in-edge 1개만 가져야 한다** (n-input operator는 chain 종료점). 그다음 `isChainableInput`이 추가 조건들을 검사:

`isChainableInput(...)`(같은 파일)이 검사하는 조건들 (요지):
- 두 노드의 **slot sharing group 동일**
- 두 노드의 **parallelism 동일** (또는 dynamic graph + allowChainWithDefaultParallelism)
- `StreamPartitioner`가 **`ForwardPartitioner` 계열** (forward 의미상 1:1)
- `StreamExchangeMode`가 streaming 호환 (PIPELINED 등)
- upstream operator의 chaining strategy가 **`HEAD` 또는 `ALWAYS`**, downstream이 **`ALWAYS`**
- batch 모드 chain 호환성

만족 → 같은 `JobVertex`로 합침. 불만족 → 새 `JobVertex` + `IntermediateDataSet` + `JobEdge`.

### 5.4 Chaining의 효과

Before (StreamGraph):

```
Source → map → keyBy → process → Sink (writer)
  N       N      ↓       N         N       (parallelism=N)
                hash
```

After (JobGraph) — 일반적 결과:

```
[Source → map]                 (chained: 같은 forward, 같은 parallelism, 1 in-edge)
   ↓ IntermediateDataSet (KeyGroup partition, hash partitioner)
   ↓ JobEdge
[process → Sink writer]        (chained: forward, 1 in-edge)
```

→ JobVertex 5개 → 2개로 축약. **2개의 task만 스케줄링되고, chain 내부는 in-thread call**.

만약 사용자가 `process` 다음에 `disableChaining()`을 호출했다면:

```
[Source → map]
   ↓
[process]
   ↓
[Sink writer]
```

→ 3 vertex. process와 Sink 사이 통신은 같은 subtask 내 forward여도 별도 vertex 경계가 생긴다 (에러 격리, 메모리 분리, 프로파일링 등 목적).

### 5.5 `JobGraph` 자체

`flink-runtime/.../JobGraph.java:57-70` (대표 필드):

```java
/**
 * The JobGraph represents a Flink dataflow program, at the low level that the JobManager accepts.
 * All programs from higher level APIs are transformed into JobGraphs.
 *
 * <p>The JobGraph is a graph of vertices and intermediate results that are connected together to
 * form a DAG.
 */
public class JobGraph implements ExecutionPlan {

    private long initialClientHeartbeatTimeout;

    /** List of task vertices included in this job graph. */
    private final Map<JobVertexID, JobVertex> taskVertices = new LinkedHashMap<>();

    private Configuration jobConfiguration = new Configuration();
    private JobID jobID;
    private final String jobName;
    private JobType jobType = JobType.BATCH;
    // ... (savepoint settings, classpaths, user jars, blob keys, ...)
}
```

**`JobGraph`가 들고 있는 것**:
- vertex 컬렉션 (insertion order 보존)
- jobID, jobName, jobType (STREAMING/BATCH)
- job-wide configuration
- savepoint restore settings
- classpaths, user JAR BLOB keys (cluster가 어디서 사용자 코드를 가져올지)

`JobType`이 `STREAMING`/`BATCH` 이후 ExecutionGraph 빌드 시 분기점이 된다.

### 5.6 `JobVertex` 자체 (chain 1개의 모든 정보)

`flink-runtime/.../JobVertex.java:60-130` (대표 필드):

```java
private final JobVertexID id;

/**
 * The IDs of all operators contained in this vertex.
 * <p>The ID pairs are stored depth-first post-order; for the forking chain below the ID's would
 * be stored as [D, E, B, C, A].
 *  A - B - D
 *   \    \
 *    C    E
 * <p>This is the same order that operators are stored in the {@code StreamTask}.
 */
private final List<OperatorIDPair> operatorIDs;        // ★ chain된 모든 operator의 ID

/** Produced data sets, one per writer. */
private final Map<IntermediateDataSetID, IntermediateDataSet> results = new LinkedHashMap<>();

/** List of edges with incoming data. One per Reader. */
private final List<JobEdge> inputs = new ArrayList<>();

/** The list of factories for operator coordinators. */
private final List<SerializedValue<OperatorCoordinator.Provider>> operatorCoordinators =
        new ArrayList<>();

/** Number of subtasks to split this task into at runtime. */
private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

private int maxParallelism = MAX_PARALLELISM_DEFAULT;
private ResourceSpec minResources = ResourceSpec.DEFAULT;
private ResourceSpec preferredResources = ResourceSpec.DEFAULT;

private Configuration configuration;          // task-level config (모든 chained operator의 직렬화 결과 등)
private String invokableClassName;            // ★ StreamTask 종류 (OneInputStreamTask 등)

private boolean isStoppable = false;
private InputSplitSource<?> inputSplitSource;
private String name;
@Nullable private SlotSharingGroup slotSharingGroup;
@Nullable private CoLocationGroupImpl coLocationGroup;
private String operatorName;
// ...
```

핵심:
- **`operatorIDs`**: 이 vertex 안의 모든 chained operator. **순서는 depth-first post-order** — 즉 `StreamTask`가 처리하는 순서와 같다 (가장 아래 operator부터). 사용자가 `setUidHash(...)`/`setUid(...)`로 명시한 ID는 여기에 저장되어 savepoint 호환성을 보장.
- **`results`**: 이 vertex가 만들어내는 출력 (보통 1개, fork되면 여러 개). 각 `IntermediateDataSet`은 하나 이상의 `JobEdge`에 의해 소비됨.
- **`inputs`**: 이 vertex가 읽는 입력 엣지들.
- **`operatorCoordinators`**: Source V2의 `SourceCoordinator` 등, JM 측에서 동작하는 coordinator들의 factory를 직렬화해 보관. (Sink V2의 committer coordinator도 여기.)
- **`invokableClassName`**: vertex를 실제로 실행하는 `StreamTask` 서브클래스 (`OneInputStreamTask`, `TwoInputStreamTask`, `SourceOperatorStreamTask`, `MultipleInputStreamTask` 중 하나). 이 결정은 `StreamGraph`의 `StreamNode.jobVertexClass`에서 옴 (이전 문서 5.7절).

### 5.7 `IntermediateDataSet` (vertex의 출력 그릇)

`flink-runtime/.../IntermediateDataSet.java`:

```java
/**
 * An intermediate data set is the data set produced by an operator - either a source or any
 * intermediate operation.
 */
public class IntermediateDataSet implements java.io.Serializable {

    private final IntermediateDataSetID id;
    private final JobVertex producer;             // 어떤 vertex가 만들어내는가

    // All consumers must have the same partitioner and parallelism
    private final List<JobEdge> consumers = new ArrayList<>();

    private final ResultPartitionType resultType; // PIPELINED / BLOCKING / HYBRID 등
    private DistributionPattern distributionPattern;
    private boolean isBroadcast;
    private boolean isForward;

    /** The number of job edges that need to be created. */
    private int numJobEdgesToCreate;
}
```

**놓치면 안 되는 항목**:
- `resultType` (`ResultPartitionType`):
  - `PIPELINED` — streaming 기본 (consumer가 producer와 동시 실행)
  - `BLOCKING` — batch (producer가 끝난 뒤 consumer 시작, 디스크 spill)
  - `HYBRID_FULL`/`HYBRID_SELECTIVE` — Adaptive Batch (FLIP-187)
  - 본인 환경(streaming)은 거의 항상 `PIPELINED`
- `distributionPattern`:
  - `ALL_TO_ALL` — 모든 producer subtask → 모든 consumer subtask (keyBy 등)
  - `POINTWISE` — 1:1 또는 N:M (forward, rescale)
- `numJobEdgesToCreate` — 이 데이터셋을 소비할 `JobEdge` 개수 (consumer 역할 vertex가 여러 개일 때)

### 5.8 `JobEdge` (vertex 연결)

각 `JobEdge`는 한 `IntermediateDataSet`(producer)과 한 `JobVertex`(consumer)를 연결. consumer 측에서 보면 `inputs` 리스트의 한 원소.

엣지의 핵심 속성: 어떤 `IntermediateDataSet`을 어떤 `DistributionPattern`으로 소비할지. 실제 partitioning 로직은 producer 측에 있고, consumer는 그 결과를 받기만 한다.

---

## 6. 사용자 환경 매핑

### 6.1 일반적 chain 구조

본인의 Kafka→keyBy→process→Iceberg 잡의 chained `JobGraph` (보통):

| JobVertex | 안에 포함된 operator 체인 | invokableClassName | 출력 IntermediateDataSet |
|-----------|--------------------------|-------------------|------------------------|
| Source | `[KafkaSource]` | `SourceOperatorStreamTask` | `PIPELINED, ALL_TO_ALL` (keyBy 때문) |
| Process | `[process → Iceberg writer]` (chained) | `OneInputStreamTask` | (보통 sink committer가 별도 vertex로 분리) |
| Committer | `[Iceberg committer]` | `OneInputStreamTask` | (terminal) |

→ 5 stream-node 잡이 보통 3 JobVertex로 축약.

### 6.2 Iceberg Sink와 chaining 경계

Iceberg-Flink 통합(외부 레포 `apache/iceberg`)은 Sink V2 + GlobalCommitter 패턴을 사용하므로 보통 **Writer와 Committer가 분리된 vertex**가 된다 (committer는 parallelism=1로 강제되는 경우가 많음 → 다른 parallelism이라서 chainable 아님).

이 분리는 의도된 것: writer는 N개 subtask가 병렬로 파일을 쓰지만, Iceberg 메타데이터 commit은 atomicity가 필요해 single-writer로 모인다. → 자세한 동작은 [`../06-source-sink-spi/04-iceberg-kafka-mapping.md`](../06-source-sink-spi/04-iceberg-kafka-mapping.md).

### 6.3 chaining을 명시적으로 끊고 싶을 때

```java
stream.process(new MyHeavyFunction())
      .disableChaining();   // ← 이 operator는 별도 JobVertex로
```

용도:
- CPU 집약적 operator를 다른 thread로 분리
- 실패 격리 (한 operator의 OOM이 다음 operator를 죽이지 않게)
- 프로파일링 시 metric 분리

---

## 7. 관련 FLIP / JIRA

| 문서 | 무엇 |
|------|------|
| [FLIP-126: Unify (and rework) watermark assigners](https://cwiki.apache.org/confluence/display/FLINK/FLIP-126%3A+Unify+%28and+rework%29+watermark+assigners) | watermark operator의 chain 위치 결정 영향 |
| [FLIP-187: Adaptive Batch Scheduler](https://cwiki.apache.org/confluence/display/FLINK/FLIP-187%3A+Adaptive+Batch+Scheduler) | `ResultPartitionType.HYBRID_*`, dynamic graph 모드 |
| [FLIP-191: Extend unified Sink interface to support small file compaction](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) | Sink V2 GlobalCommitter — 별도 vertex 결정 |
| [FLIP-217: Support speculative execution](https://cwiki.apache.org/confluence/display/FLINK/FLIP-217%3A+Support+speculative+execution+of+arbitrary+operators) | `markSupportingConcurrentExecutionAttempts` 단계의 배경 |

---

## 8. 디버깅 & 실험

### 8.1 chaining 결과 확인

가장 빠른 방법은 Flink Web UI의 "Job Graph" 탭. UI 없이는:

```java
// IDE에서
StreamGraph sg = env.getStreamGraph();
JobGraph jg = sg.getJobGraph(Thread.currentThread().getContextClassLoader(), null);
for (JobVertex v : jg.getVertices()) {
    System.out.println(v.getName() + " : " + v.getOperatorIDs().size() + " operators chained");
}
```

`v.getName()`은 chain된 operator 이름들이 화살표로 연결된 형태로 보인다 (예: `Source: KafkaSource -> Map -> Filter`).

### 8.2 chaining 결정을 코드로 따라가기

`StreamingJobGraphGenerator.java:1720` (`isChainable`)에 브레이크포인트. 또는 `StreamingJobGraphGenerator.setChaining()`를 따라가서 어떤 `StreamNode`가 어떤 chain group에 들어가는지 관찰.

### 8.3 그래프 그래프(MCP)로 vertex 종류 빠르게 확인

```
mcp__codebase-memory-mcp__search_graph(
  project="home-donamk-code-flink-flink-runtime",
  qn_pattern=".*StreamingJobGraphGenerator\\.(setChaining|connect|createJobVertex)$"
)
```

### 8.4 사용자 잡에서 vertex 이름 확인 (운영)

JM 로그 또는 REST `/jobs/<jobId>` 응답의 `vertices` 필드. 본인 환경의 잡이 어떻게 chain되는지 매번 확인하는 습관이 운영 디버깅의 시작점.

---

## 9. 자주 묻는 질문 / 함정

**Q1. `StreamGraph`와 `JobGraph` 둘 다 `ExecutionPlan`을 구현하는데, cluster는 무엇을 받나?**
A. release-2.0의 모든 `PipelineExecutor`는 `StreamGraph` 자체를 `submitJob`에 넘긴다. cluster의 `Dispatcher`가 `streamGraph.getJobGraph(...)`로 변환을 수행한다. 두 클래스 모두 `ExecutionPlan`을 구현해 wire 호환.

**Q2. `disableChaining()` vs `startNewChain()` 차이?**
A. `disableChaining()`: 이 operator를 어느 chain에도 넣지 말 것 (양 옆에서 다 끊김). `startNewChain()`: 이 operator부터 새 chain 시작 (downstream과는 chain 가능, upstream과는 강제 분리). `slotSharingGroup("...")`: chain 결정에 영향 (다른 group이면 chain 불가).

**Q3. `JobVertex.invokableClassName`이 결정되는 시점은?**
A. `StreamNode.jobVertexClass`에서 옴. 이는 transformation translator가 `addOperator(...)` 시 결정 (1-input → `OneInputStreamTask`, source → `SourceOperatorStreamTask`, 다중 입력 → `MultipleInputStreamTask`). chain되어도 vertex 측 `invokableClassName`은 chain의 첫 번째 operator 종류로 결정 (head operator).

**Q4. `OperatorIDPair`는 왜 pair인가?**
A. 자동 생성 ID + 사용자 명시 ID(uidHash)의 두 가지를 함께 보관. savepoint 매칭 시 사용자 ID를 우선, 없으면 자동 ID로 fallback.

**Q5. `keyBy`가 만든 IntermediateDataSet의 distribution pattern?**
A. `ALL_TO_ALL` + `KeyGroupStreamPartitioner`. 즉 producer의 모든 subtask가 consumer의 모든 subtask와 잠재적 연결을 가지지만, 실제 데이터는 key의 hash → key group → consumer subtask 매핑으로 1개 subtask로만 흘러간다. `key group 수 = maxParallelism`.

**Q6. `ResultPartitionType.PIPELINED`인데 streaming 잡이 갑자기 `BLOCKING`이 보이면?**
A. Hybrid shuffle (FLIP-187, batch에서) 또는 사용자가 명시적으로 `BatchExecutionOptions`로 변경한 경우. streaming 잡에서 의도하지 않은 `BLOCKING`이 보이면 잡 설정 검토.

---

## 10. 다음에 읽을 문서

| 다음 단계 | 문서 |
|----------|------|
| `JobGraph` → `ExecutionGraph` (subtask 단위 펼치기, scheduling 시작) | [`./03-execution-graph.md`](03-execution-graph.md) |
| `OperatorCoordinator` (Source/Sink V2 의 JM 측 컴포넌트) | [`../06-source-sink-spi/source-coordinator.md`](../06-source-sink-spi/02-source-coordinator.md) |
| `StreamTask` 메인 루프 (chained operator를 어떻게 실행하는가) | [`../04-runtime-architecture/stream-task-mailbox.md`](../04-runtime-architecture/05-stream-task-mailbox.md) |
| Slot sharing group / co-location 의 스케줄링 영향 | [`../10-scheduling-failover/`](../10-scheduling-failover/) |
| Operator chaining 설정 옵션 (전역 disable, optimizer 힌트) | (위 문서들과 함께 다룸) |
