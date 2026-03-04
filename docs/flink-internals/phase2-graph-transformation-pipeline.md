# Phase 2: Graph 변환 파이프라인

> Flink 내부 동작의 핵심 중의 핵심.
> 사용자 코드가 분산 실행 가능한 형태로 변환되는 3단계 과정을 코드 레벨에서 추적합니다.

```
User Code → StreamGraph → JobGraph → ExecutionGraph
  (논리적)     (토폴로지)    (물리적)    (실행 단위)
```

---

## 2.1 StreamGraph — 논리적 실행 계획

`StreamGraph`는 사용자의 DataStream API 호출을 **방향성 비순환 그래프(DAG)**로 표현한 것입니다.
각 노드(`StreamNode`)는 하나의 연산자(operator)를, 각 간선(`StreamEdge`)은 데이터 흐름을 나타냅니다.

### StreamGraph 클래스 구조

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamGraph.java

@Internal
public class StreamGraph implements Pipeline, ExecutionPlan {

    // ═══════════════════════════════════════════════
    // 핵심 데이터 구조
    // ═══════════════════════════════════════════════

    // 모든 노드를 ID로 관리하는 맵 — StreamGraph의 핵심
    private transient Map<Integer, StreamNode> streamNodes;

    // Source와 Sink 노드의 ID 집합
    private Set<Integer> sources;
    private Set<Integer> sinks;

    // 가상 노드 — 실제 연산자가 아닌, 파티셔닝/사이드출력을 위한 논리적 노드
    private transient Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes;
    private transient Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
            virtualPartitionNodes;

    // Job 설정
    private String jobName;
    private JobID jobId;
    private final Configuration jobConfiguration;
    private transient ExecutionConfig executionConfig;
    private final CheckpointConfig checkpointConfig;
}
```

### StreamNode — 그래프의 노드

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamNode.java

@Internal
public class StreamNode {

    private final int id;                           // 고유 ID
    private int parallelism;                        // 병렬도
    private int maxParallelism;                     // 최대 병렬도
    private Long bufferTimeout;                     // 버퍼 플러시 타임아웃

    // 이 노드에 연결된 입/출력 간선
    private List<StreamEdge> inEdges = new ArrayList<>();
    private List<StreamEdge> outEdges = new ArrayList<>();

    private final String operatorName;              // 연산자 이름 (예: "Map", "Filter")
    private String slotSharingGroup;                // Slot 공유 그룹

    // 핵심: 이 노드가 실행할 연산자 팩토리
    private transient StreamOperatorFactory<?> operatorFactory;

    // 타입 정보 (입력은 배열 — 다중 입력 연산자 지원)
    private TypeSerializer<?>[] typeSerializersIn = new TypeSerializer[0];
    private TypeSerializer<?> typeSerializerOut;
}
```

**StreamNode가 가진 핵심 정보:**
- `operatorFactory`: 실제로 실행될 연산자를 생성하는 팩토리 (예: `MapOperator`, `FilterOperator`)
- `parallelism`: 이 연산자의 병렬 인스턴스 수
- `inEdges/outEdges`: 데이터가 어디서 오고 어디로 가는지

### StreamEdge — 그래프의 간선

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamEdge.java

@Internal
public class StreamEdge implements Serializable {

    private final int sourceId;             // 출발 노드 ID
    private final int targetId;             // 도착 노드 ID

    // 데이터 파티셔닝 전략 — 어떻게 데이터를 분배할 것인가
    private StreamPartitioner<?> outputPartitioner;

    // 데이터 교환 모드
    private StreamExchangeMode exchangeMode;

    private final OutputTag outputTag;      // Side Output 태그 (있을 경우)
}
```

**StreamPartitioner 종류:**
| Partitioner | 설명 | 사용 시점 |
|-------------|------|-----------|
| `ForwardPartitioner` | 1:1 매핑 (같은 파티션으로) | `map()`, `filter()` 등 |
| `HashPartitioner` | 키의 해시값으로 분배 | `keyBy()` |
| `RebalancePartitioner` | 라운드 로빈 | `rebalance()` |
| `BroadcastPartitioner` | 모든 파티션으로 복제 | `broadcast()` |
| `ShufflePartitioner` | 랜덤 분배 | `shuffle()` |
| `RescalePartitioner` | 로컬 라운드 로빈 | `rescale()` |
| `GlobalPartitioner` | 모두 파티션 0으로 | `global()` |

### StreamGraph 생성 과정 — StreamGraphGenerator

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamGraphGenerator.java

@Internal
public class StreamGraphGenerator {

    // 사용자가 등록한 Transformation 리스트
    private final List<Transformation<?>> transformations;

    // 이미 변환된 Transformation을 추적 (중복 방지, IdentityHashMap 사용)
    private Map<Transformation<?>, Collection<Integer>> alreadyTransformed;
}
```

### generate() — 핵심 생성 메서드

```java
public StreamGraph generate() {
    streamGraph = new StreamGraph(configuration, executionConfig, checkpointConfig, savepointRestoreSettings);
    shouldExecuteInBatchMode = shouldExecuteInBatchMode();
    configureStreamGraph(streamGraph);

    alreadyTransformed = new IdentityHashMap<>();  // 객체 동일성(==) 기반 비교

    // ★ 핵심: 모든 Transformation을 순회하며 StreamGraph에 변환
    for (Transformation<?> transformation : transformations) {
        transform(transformation);
    }

    streamGraph.setSlotSharingGroupResource(slotSharingGroupResources);

    // StreamEdge에 고유 ID 할당
    setFineGrainedGlobalStreamExchangeMode(streamGraph);

    // virtual node들을 실제 edge로 변환
    for (StreamNode node : streamGraph.getStreamNodes()) {
        if (node.getInEdges().stream().anyMatch(this::shouldDisableUnalignedCheckpointing)) {
            for (StreamEdge edge : node.getInEdges()) {
                edge.setSupportsUnalignedCheckpoints(false);
            }
        }
    }

    return streamGraph;
}
```

### transform() — 개별 Transformation 변환

```java
private Collection<Integer> transform(Transformation<?> transform) {
    // 이미 변환된 것이면 스킵
    if (alreadyTransformed.containsKey(transform)) {
        return alreadyTransformed.get(transform);
    }

    // Transformation 타입별로 적절한 변환기(Translator) 선택
    final TransformationTranslator<?, Transformation<?>> translator =
            (TransformationTranslator<?, Transformation<?>>)
                    translatorMap.get(transform.getClass());

    Collection<Integer> transformedIds;
    if (translator != null) {
        // ★ Translator를 사용하여 변환
        transformedIds = translate(translator, transform);
    } else {
        // 레거시 변환 경로
        transformedIds = legacyTransform(transform);
    }

    // 변환 결과 캐싱
    if (!alreadyTransformed.containsKey(transform)) {
        alreadyTransformed.put(transform, transformedIds);
    }

    return transformedIds;
}
```

**Transformation → StreamNode 매핑 예시:**

```
OneInputTransformation("Map")
    ↓ translate()
streamGraph.addOperator(
    id = 2,
    operatorFactory = SimpleOperatorFactory.of(new StreamMap<>(mapFunction)),
    inTypeInfo = Types.STRING,
    outTypeInfo = Types.STRING,
    operatorName = "Map"
)
    ↓
StreamNode(id=2, operator=StreamMap, parallelism=4)
```

### addNode() — StreamGraph에 노드 추가

```java
// StreamGraph.java
protected StreamNode addNode(
        Integer vertexID,
        @Nullable String slotSharingGroup,
        @Nullable String coLocationGroup,
        Class<? extends TaskInvokable> vertexClass,
        StreamOperatorFactory<?> operatorFactory,
        String operatorName) {

    // 중복 체크
    if (streamNodes.containsKey(vertexID)) {
        throw new RuntimeException("Duplicate vertexID " + vertexID);
    }

    // StreamNode 생성
    StreamNode vertex = new StreamNode(
            vertexID,
            slotSharingGroup,
            coLocationGroup,
            operatorFactory,
            operatorName,
            vertexClass);

    streamNodes.put(vertexID, vertex);

    return vertex;
}
```

### addEdge() — 노드 간 간선 추가

```java
// StreamGraph.java
public void addEdge(int upStreamVertexID, int downStreamVertexID, int typeNumber) {
    addEdgeInternal(
            upStreamVertexID,
            downStreamVertexID,
            typeNumber,
            null,  // partitioner
            new ArrayList<>(),
            null,  // outputTag
            StreamExchangeMode.UNDEFINED);
}

private void addEdgeInternal(
        Integer upStreamVertexID,
        Integer downStreamVertexID,
        int typeNumber,
        StreamPartitioner<?> partitioner,
        List<String> outputNames,
        OutputTag outputTag,
        StreamExchangeMode exchangeMode) {

    // ★ 가상 노드(Virtual Node) 처리
    // keyBy()는 실제 연산자가 아니라 파티셔닝만 변경하므로 가상 노드로 표현됨
    if (virtualSideOutputNodes.containsKey(upStreamVertexID)) {
        // Side Output 가상 노드 → 실제 소스 노드로 치환
        int virtualId = upStreamVertexID;
        upStreamVertexID = virtualSideOutputNodes.get(virtualId).f0;
        outputTag = virtualSideOutputNodes.get(virtualId).f1;
        addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber,
                partitioner, outputNames, outputTag, exchangeMode);
    } else if (virtualPartitionNodes.containsKey(upStreamVertexID)) {
        // Partition 가상 노드 → Partitioner 정보를 edge에 설정
        int virtualId = upStreamVertexID;
        upStreamVertexID = virtualPartitionNodes.get(virtualId).f0;
        partitioner = virtualPartitionNodes.get(virtualId).f1;
        exchangeMode = virtualPartitionNodes.get(virtualId).f2;
        addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber,
                partitioner, outputNames, outputTag, exchangeMode);
    } else {
        // ★ 실제 Edge 생성
        createActualEdge(upStreamVertexID, downStreamVertexID, typeNumber,
                partitioner, outputTag, exchangeMode);
    }
}
```

> **가상 노드(Virtual Node)의 중요성:**
> `keyBy()`, `broadcast()`, `rebalance()` 등은 데이터 파티셔닝만 변경하고 별도의 연산을 수행하지 않습니다.
> 이들은 가상 노드로 표현되며, 최종적으로는 `StreamEdge`의 `partitioner` 속성으로 흡수됩니다.
> 이 설계 덕분에 불필요한 네트워크 홉이 제거됩니다.

---

## 2.2 StreamGraph → JobGraph 변환

`JobGraph`는 `StreamGraph`에 **Operator Chaining**을 적용한 물리적 실행 계획입니다.

### Operator Chaining이란?

연속된 연산자들 중 조건을 충족하는 것들을 하나의 Task로 합칩니다:

```
변환 전 (StreamGraph):
Source → Map → Filter → KeyBy → Reduce → Sink
  [1]    [2]    [3]              [4]      [5]

변환 후 (JobGraph) — Chaining 적용:
[Source → Map → Filter]  →  [Reduce → Sink]
     JobVertex 1       네트워크      JobVertex 2
                       셔플(KeyBy)
```

**Chaining 조건:**
- 같은 병렬도(parallelism)
- 같은 slot sharing group
- ForwardPartitioner 사용 (1:1 연결)
- 서로 다른 chaining 전략이 아닌 경우

### StreamingJobGraphGenerator — JobGraph 생성기

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamingJobGraphGenerator.java

@Internal
public class StreamingJobGraphGenerator {

    private final StreamGraph streamGraph;
    private final JobGraph jobGraph;

    // ★ Chaining 결과: 어떤 StreamNode들이 하나의 JobVertex로 합쳐졌는지
    private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

    // 각 StreamNode가 속한 JobVertex의 헤드 노드 ID
    private final Map<Integer, Integer> vertexHashes;
}
```

### createJobGraph() — JobGraph 생성 핵심 로직

```java
private JobGraph createJobGraph() {
    preValidate();

    jobGraph.setJobType(streamGraph.getJobType());

    // 설정 전달
    jobGraph.enableApproximateLocalRecovery(
            streamGraph.getCheckpointConfig()
                    .isApproximateLocalRecoveryEnabled());

    // ★★★ 핵심 단계 1: 해시값 계산 (각 노드의 고유 ID)
    // 이 해시는 Savepoint 복구 시 노드를 식별하는 데 사용됩니다
    Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);
    List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
    for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
        legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
    }

    // ★★★ 핵심 단계 2: Operator Chaining을 적용하여 JobVertex 생성
    setChaining(hashes, legacyHashes);

    // ★★★ 핵심 단계 3: 물리적 Edge 설정
    setPhysicalEdges();

    // 리소스 설정
    setSlotSharingAndCoLocation();

    // 체크포인트 설정
    configureCheckpointing();

    // Savepoint 설정
    jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());

    // 결과물 ID 할당
    final Map<Integer, OperatorIDPair> operatorsInOrder = new TreeMap<>();
    // ... operatorID 매핑

    jobGraph.setJobType(streamGraph.getJobType());

    return jobGraph;
}
```

### setChaining() — Operator Chaining 적용

```java
private void setChaining() {
    // Source 노드부터 시작하여 체인 구성
    final Map<Integer, OperatorChainInfo> chainEntryPoints =
            buildChainedInputsAndGetHeadInputs();

    // 토폴로지 순서로 정렬
    final Collection<OperatorChainInfo> initialEntryPoints =
            chainEntryPoints.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());

    // 각 체인의 시작점(head)에서 재귀적으로 체인 확장
    for (OperatorChainInfo info : initialEntryPoints) {
        createChain(
                info.getStartNodeId(),     // 체인 시작 노드
                1,                          // 체인 인덱스
                info,                       // 체인 정보
                chainEntryPoints,
                true,                       // 새 체인 생성 가능 여부
                serializationExecutor,
                jobVertexBuildContext,
                null);                      // visitor
    }
}
```

### createChain() — 재귀적 체인 생성

```java
private List<StreamEdge> createChain(
        final Integer currentNodeId,
        final int chainIndex,
        final OperatorChainInfo chainInfo,
        final Map<Integer, OperatorChainInfo> chainEntryPoints) {

    Integer startNodeId = chainInfo.getStartNodeId();
    StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

    List<StreamEdge> transitiveOutEdges = new ArrayList<>();

    // 출력 Edge들을 Chainable / Non-Chainable로 분류
    List<StreamEdge> chainableOutputs = new ArrayList<>();
    List<StreamEdge> nonChainableOutputs = new ArrayList<>();

    for (StreamEdge outEdge : currentNode.getOutEdges()) {
        if (isChainable(outEdge, streamGraph)) {
            chainableOutputs.add(outEdge);     // 체인 가능 → 같은 JobVertex에 포함
        } else {
            nonChainableOutputs.add(outEdge);  // 체인 불가 → 새로운 JobVertex 시작
        }
    }

    // ★ Chainable Edge: 재귀적으로 같은 체인에 추가
    for (StreamEdge chainableEdge : chainableOutputs) {
        transitiveOutEdges.addAll(
                createChain(chainableEdge.getTargetId(), chainIndex + 1,
                        chainInfo, chainEntryPoints));
    }

    // ★ Non-Chainable Edge: 새로운 체인 시작
    for (StreamEdge nonChainableEdge : nonChainableOutputs) {
        transitiveOutEdges.add(nonChainableEdge);
        createChain(nonChainableEdge.getTargetId(), 1,
                chainEntryPoints.computeIfAbsent(...),
                chainEntryPoints);
    }

    // 현재 노드가 체인의 시작점(head)이면 JobVertex 생성
    if (currentNodeId.equals(startNodeId)) {
        createJobVertex(startNodeId, chainInfo);
    }

    return transitiveOutEdges;
}
```

### isChainable() — 체인 가능 여부 판단

```java
public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
    StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

    // ★ 기본 조건: 다운스트림 노드의 입력이 하나뿐이어야 함
    return downStreamVertex.getInEdges().size() == 1
            && isChainableInput(edge, streamGraph, false);
}

private static boolean isChainableInput(
        StreamEdge edge, StreamGraph streamGraph, boolean allowChainWithDefaultParallelism) {
    StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
    StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

    // 모든 체이닝 조건을 확인
    if (!(streamGraph.isChainingEnabled()                               // 전역 체이닝 활성화
            && upStreamVertex.isSameSlotSharingGroup(downStreamVertex)   // 같은 Slot 공유 그룹
            && areOperatorsChainable(upStreamVertex, downStreamVertex, streamGraph,
                    allowChainWithDefaultParallelism)                    // 연산자 수준 호환
            && arePartitionerAndExchangeModeChainable(                   // 파티셔너/교환모드 호환
                    edge.getPartitioner(), edge.getExchangeMode(),
                    streamGraph.isDynamic()))) {
        return false;
    }

    // Union 연산 체크 — 같은 타입의 다른 입력 Edge가 없어야 함
    for (StreamEdge inEdge : downStreamVertex.getInEdges()) {
        if (inEdge != edge && inEdge.getTypeNumber() == edge.getTypeNumber()) {
            return false;
        }
    }
    return true;
}
```

### JobGraph 핵심 구조

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/JobGraph.java

public class JobGraph implements Serializable {

    private final JobID jobID;              // Job 고유 식별자
    private String jobName;                 // Job 이름

    // ★ 핵심: JobVertex 맵 — 체이닝 적용 후의 실행 단위
    private final Map<JobVertexID, JobVertex> taskVertices;

    private final List<URL> userJars;       // 사용자 JAR 파일들
    private final List<DistributedCache.DistributedCacheEntry> userArtifacts;

    private JobType jobType = JobType.STREAMING;  // STREAMING 또는 BATCH
}
```

### JobVertex — 물리적 실행 단위

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/JobVertex.java

public class JobVertex implements Serializable {

    private final JobVertexID id;                  // 고유 ID
    private final ArrayList<OperatorIDPair> operatorIDs;  // 체이닝된 연산자 ID들

    // 이 Vertex가 실행할 클래스 (보통 StreamTask의 서브클래스)
    private String invokableClassName;

    private int parallelism;                       // 병렬도
    private int maxParallelism;                    // 최대 병렬도

    // 입출력 — IntermediateDataSet으로 연결
    private final ArrayList<IntermediateDataSet> results;    // 출력 데이터셋
    private final ArrayList<JobEdge> inputs;                  // 입력 Edge

    // Slot 관련
    private String slotSharingGroup;
    private CoLocationGroupImpl coLocationGroup;

    // ★ StreamConfig: 체이닝된 모든 연산자의 설정 정보
    private Configuration configuration;
}
```

> **IntermediateDataSet:**
> `JobVertex` 간의 데이터 교환을 추상화한 것입니다.
> 하나의 `JobVertex`가 생산하는 출력 데이터를 나타내며, 다운스트림 `JobVertex`의 `JobEdge`가 이를 소비합니다.

---

## 2.3 JobGraph → ExecutionGraph 변환

`ExecutionGraph`는 `JobGraph`를 병렬도에 따라 **인스턴스화**한 것입니다.

```
JobGraph:
  JobVertex A (parallelism=3)  →  JobVertex B (parallelism=2)

ExecutionGraph:
  ExecutionVertex A[0] ──→ ExecutionVertex B[0]
  ExecutionVertex A[1] ──→ ExecutionVertex B[0]
  ExecutionVertex A[2] ──→ ExecutionVertex B[1]
                     (파티셔닝에 따라 연결)
```

### ExecutionGraph 핵심 구조

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/DefaultExecutionGraph.java

public class DefaultExecutionGraph implements ExecutionGraph, InternalExecutionGraphAccessor {

    // ★ JobVertex별 ExecutionJobVertex 맵
    private final Map<JobVertexID, ExecutionJobVertex> tasks;

    // 모든 ExecutionVertex (Task 인스턴스)
    private final Map<IntermediateDataSetID, IntermediateResult> intermediateResults;

    // 현재 Job 상태
    private volatile JobStatus state = JobStatus.CREATED;

    // 체크포인트 코디네이터 (Phase 4에서 상세히)
    private CheckpointCoordinator checkpointCoordinator;

    // Failover 전략 (Phase 5에서 상세히)
    private FailoverStrategy failoverStrategy;
}
```

### ExecutionVertex — 실제 실행 인스턴스

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/ExecutionVertex.java

public class ExecutionVertex implements AccessExecutionVertex, Archiveable<ArchivedExecutionVertex> {

    // 이 Vertex가 속한 JobVertex
    private final ExecutionJobVertex jobVertex;

    // 서브태스크 인덱스 (0부터 시작, parallelism 미만)
    private final int subTaskIndex;

    // 현재 실행 시도 (failover 시 새로운 Execution 생성)
    private Execution currentExecution;

    // 입력 분할(InputSplit) — 이 서브태스크가 처리할 데이터 범위
    private final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitions;
}
```

### ExecutionGraph 생성 과정

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/DefaultExecutionGraphBuilder.java

public class DefaultExecutionGraphBuilder {

    public static DefaultExecutionGraph buildGraph(
            ExecutionPlan executionPlan,        // JobGraph
            Configuration jobManagerConfig,
            ScheduledExecutorService futureExecutor,
            // ... 기타 파라미터
            ) throws JobExecutionException, JobException {

        // ① ExecutionGraph 인스턴스 생성
        DefaultExecutionGraph executionGraph = new DefaultExecutionGraph(
                jobId,
                jobName,
                jobConfiguration,
                // ...
        );

        // ② CheckpointCoordinator 설정
        // 체크포인트가 활성화된 경우 coordinator 생성
        // ...

        // ③ ★★★ 핵심: JobVertex들을 ExecutionGraph에 부착
        // 각 JobVertex를 병렬도만큼 ExecutionVertex로 확장
        List<JobVertex> sortedTopology = executionPlan.getVerticesSortedTopologicallyFromSources();
        executionGraph.attachJobGraph(sortedTopology);

        return executionGraph;
    }
}
```

### attachJobGraph() — JobVertex → ExecutionVertex 확장

```java
// DefaultExecutionGraph.java
public void attachJobGraph(
        List<JobVertex> verticesToAttach,
        JobManagerJobMetricGroup jobManagerJobMetricGroup)
        throws JobException {

    assertRunningInJobMasterMainThread();

    // ★ 내부적으로 attachJobVertices()를 호출
    attachJobVertices(verticesToAttach, jobManagerJobMetricGroup);

    // 동적 그래프가 아닌 경우 즉시 초기화
    if (!isDynamic) {
        initializeJobVertices(verticesToAttach);
    }

    // 실행 토폴로지 구축 — 스케줄링에 사용
    executionTopology = DefaultExecutionTopology.fromExecutionGraph(this);

    // 파티션 그룹 해제 전략 초기화
    partitionGroupReleaseStrategy =
            partitionGroupReleaseStrategyFactory.createInstance(getSchedulingTopology());
}

// attachJobVertices() 내부에서 각 JobVertex를:
// 1. ExecutionJobVertex 생성 (parallelism만큼 ExecutionVertex 포함)
// 2. IntermediateResult 연결 (선행 JobVertex의 출력과 연결)
// 3. tasks 맵에 등록
```

---

## 2.4 전체 변환 과정 시각화

### WordCount 예제의 변환 과정

```java
// 사용자 코드
env.fromSource(textSource, WatermarkStrategy.noWatermarks(), "Source")
   .flatMap(new Tokenizer())       // Transformation 1
   .keyBy(value -> value.f0)       // Transformation 2 (Virtual)
   .sum(1)                         // Transformation 3
   .print();                       // Transformation 4
```

**Step 1: StreamGraph**
```
StreamNode(id=1, op=Source)
    │ ForwardPartitioner
    ▼
StreamNode(id=2, op=FlatMap)
    │ HashPartitioner (keyBy에서 결정)
    ▼
StreamNode(id=4, op=Sum)
    │ ForwardPartitioner
    ▼
StreamNode(id=5, op=Print)

※ keyBy(id=3)는 가상 노드 → StreamEdge의 HashPartitioner로 흡수
```

**Step 2: JobGraph (Operator Chaining 적용)**
```
┌─────────────────────┐      ┌──────────────────┐
│ JobVertex 1         │      │ JobVertex 2       │
│ [Source → FlatMap]  │─────→│ [Sum → Print]     │
│ parallelism = 4     │ Hash │ parallelism = 4   │
└─────────────────────┘      └──────────────────┘

Source와 FlatMap: ForwardPartitioner + 같은 병렬도 → 체이닝!
FlatMap과 Sum: HashPartitioner (keyBy) → 체이닝 불가, 네트워크 셔플 필요
Sum과 Print: ForwardPartitioner + 같은 병렬도 → 체이닝!
```

**Step 3: ExecutionGraph (병렬도 = 4로 확장)**
```
ExecutionVertex [Source→FlatMap][0] ──┐
ExecutionVertex [Source→FlatMap][1] ──┼──→ ExecutionVertex [Sum→Print][0]
ExecutionVertex [Source→FlatMap][2] ──┤    ExecutionVertex [Sum→Print][1]
ExecutionVertex [Source→FlatMap][3] ──┘    ExecutionVertex [Sum→Print][2]
                                          ExecutionVertex [Sum→Print][3]

                (Hash Partitioning: 각 키가 해시값에 따라 특정 서브태스크로)
```

---

## 2.5 설계 배경: 관련 FLIP

> **[FLIP-92: N-Ary Stream Operator](https://cwiki.apache.org/confluence/display/FLINK/FLIP-92:+Add+N-Ary+Stream+Operator+in+Flink)**
> 기존의 OneInputStreamOperator/TwoInputStreamOperator를 넘어 **N개 입력을 받는 연산자**를 지원합니다.
> 이를 통해 Source Chaining과 Multiple Input Operator가 가능해졌고,
> 불필요한 네트워크 셔플을 제거하여 ~30% 성능 향상을 달성했습니다 (Flink 1.12).

> **[FLIP-411: Chaining-agnostic Operator ID Generation](https://cwiki.apache.org/confluence/display/FLINK/FLIP-411)**
> 이전에는 Operator ID 생성이 체이닝 결과에 의존했습니다. 즉, 병렬도 변경으로 체이닝이 달라지면
> Operator ID도 바뀌어 **Savepoint 호환성이 깨졌습니다**.
> FLIP-411은 `StreamGraphHasherV3`를 도입하여 체이닝과 무관한 ID 생성을 구현합니다.
> 이는 `createJobGraph()`의 해시 계산 단계에서 적용됩니다.

---

## 2.6 핵심 정리

| 그래프 | 역할 | 핵심 단위 | 생성 시점 |
|--------|------|-----------|-----------|
| **StreamGraph** | 논리적 DAG | StreamNode + StreamEdge | Client (execute() 호출 시) |
| **JobGraph** | Chaining 적용된 물리적 DAG | JobVertex + IntermediateDataSet | Client (execute() 호출 시) |
| **ExecutionGraph** | 병렬도 확장된 실행 DAG | ExecutionVertex + IntermediateResultPartition | JobMaster (Job 제출 후) |

1. **StreamGraph**: 사용자 API 호출을 1:1로 반영한 그래프. 가상 노드로 파티셔닝 표현
2. **JobGraph**: Operator Chaining으로 최적화. 네트워크 통신이 필요한 지점이 명확해짐
3. **ExecutionGraph**: 실제 스케줄링과 실행의 단위. 각 ExecutionVertex가 하나의 Task로 배포

---

## 다음 단계

Phase 3에서는 생성된 ExecutionGraph가 어떻게 클러스터 컴포넌트들(Dispatcher, JobMaster, TaskExecutor)에 의해
실제로 스케줄링되고 실행되는지를 추적합니다.
