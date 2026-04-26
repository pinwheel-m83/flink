# `StreamGraph` 생성 — `Transformation` 트리에서 DAG로

> **요약**: `env.execute()`가 호출하는 `getStreamGraph()`가 사용자의 `Transformation` 리스트를 어떻게 `StreamGraphGenerator`로 돌려 `StreamGraph`(=`Pipeline`) DAG로 변환하는지 코드 레벨로 따라간다. `StreamNode`/`StreamEdge`의 구조와 `TransformationTranslator` 디스패치 메커니즘이 핵심.
> **모듈**: `flink-runtime` (`StreamGraphGenerator`, `StreamGraph`, `StreamNode`, `StreamEdge`), `flink-core` (`Transformation`, `Pipeline` 마커)
> **기준 버전**: Flink `release-2.0`
> **운영 권장 여부**: ★Production (모든 DataStream 잡의 표준 그래프 모델)
> **선행 문서**: [`../02-job-fundamentals/01-execute-entrypoint.md`](../02-job-fundamentals/01-execute-entrypoint.md)

---

## 1. TL;DR (3문장)

`DataStream` API의 모든 연산(`map`, `filter`, `keyBy`, `addSink` …)은 사용자의 변환 의도를 `Transformation<T>` 객체로 누적한 트리(엄밀히는 forest)이고, `StreamGraphGenerator.generate()`가 이 트리를 한 번 순회하면서 각 `Transformation`을 등록된 `TransformationTranslator`로 위임해 `StreamNode`+`StreamEdge`로 구성된 `StreamGraph` DAG를 만든다. `StreamGraph`는 `Pipeline` 마커 인터페이스를 구현하므로 [이전 문서](../02-job-fundamentals/01-execute-entrypoint.md)의 `PipelineExecutor.execute(...)`가 받는 그 `Pipeline` 자체이며, 이후 단계에서 `JobGraph`로 추가 변환된다(다음 문서). 이 단계는 **순수 client 측 작업** — 어떤 클러스터 통신도 일어나지 않는다.

---

## 2. 사전 지식

### 2.1 `IdentityHashMap<K, V>`

`HashMap`은 `equals()`/`hashCode()`로 키 동일성을 판단하지만 `IdentityHashMap`은 **참조 동일성(`==`)**으로 판단한다. 같은 내용을 가진 두 `Transformation` 인스턴스가 있어도 다른 객체로 취급된다. `StreamGraphGenerator`가 "이 transformation을 이미 변환했나?"를 판정하는 `alreadyTransformed` 캐시에 이걸 쓰는 이유: Transformation은 사용자 프로그램 내에서 **객체 정체성**으로 식별돼야 하기 때문(`equals()`를 override한 두 transformation이 우연히 같은 결과를 내도 별개로 변환).

### 2.2 `AtomicInteger`로 글로벌 ID 발급

`Transformation.ID_COUNTER = new AtomicInteger(0)` (`flink-core/.../Transformation.java:115`). 모든 `Transformation`은 `ID_COUNTER.incrementAndGet()`로 process-wide 유일 ID를 할당받는다. **JVM 내 카운터**이므로 같은 JVM에서 두 번째 잡을 만들면 ID가 이어진다 — 이게 디버깅 시 헷갈리는 한 원인.

### 2.3 dispatch 패턴 (Map-based polymorphism)

`StreamGraphGenerator`는 `Map<Class<?>, TransformationTranslator>`를 정적 필드로 들고, `transform.getClass()`로 적합한 translator를 찾는다. **방문자 패턴(Visitor)의 변형** — 새 `Transformation` 타입을 추가하려면 서브클래스 + 그에 대응하는 translator를 등록하면 된다. 이 디자인 덕에 `StreamGraphGenerator` 본체는 transformation 종류가 늘어도 손대지 않는다.

---

## 3. 핵심 클래스 / 진입점

| 역할 | 클래스/인터페이스 | 위치 |
|------|------------------|------|
| 사용자 표현 노드 (logical) | `Transformation<T>` (Abstract Class) | `flink-core/src/main/java/org/apache/flink/api/dag/Transformation.java` |
| `Pipeline` 마커 | `Pipeline` (Interface) | `flink-core/src/main/java/org/apache/flink/api/dag/Pipeline.java` |
| Transformation → StreamGraph 변환 | `StreamGraphGenerator` (Class) | `flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamGraphGenerator.java` |
| 결과 그래프 | `StreamGraph` (Class) — `implements Pipeline, ExecutionPlan` | `flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamGraph.java` |
| 그래프 노드 (operator 단위) | `StreamNode` (Class) | `flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamNode.java` |
| 그래프 엣지 (데이터 흐름) | `StreamEdge` (Class) | `flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamEdge.java` |
| 변환 디스패치 | `TransformationTranslator<OUT, T>` (Interface) | `flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/TransformationTranslator.java` |
| 진입 메서드 | `StreamExecutionEnvironment.getStreamGraph(...)` | `flink-runtime/src/main/java/org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.java:2042` |

### Transformation 구현체 (대표)

`flink-core/src/main/java/org/apache/flink/streaming/api/transformations/` 아래에 모여 있다:

| 종류 | 클래스 | 언제 만들어지나 |
|------|--------|---------------|
| 1-input operator (map/filter 등) | `OneInputTransformation` | `DataStream.transform(...)` 등 |
| 2-input (connect+process) | `TwoInputTransformation` | `ConnectedStreams.process(...)` |
| 다중 입력 | `MultipleInputTransformation`, `KeyedMultipleInputTransformation` | `MultipleInputTransformation` API |
| Source V2 | `SourceTransformation` | `env.fromSource(...)` |
| Sink V2 | `SinkTransformation`, `GlobalCommitterTransform` | `stream.sinkTo(...)` |
| 파티셔닝 | `PartitionTransformation` | `keyBy`, `rebalance`, `forward` 등 |
| 합집합/사이드 출력 | `UnionTransformation`, `SideOutputTransformation` | `union`, `getSideOutput` |
| Reduce | `ReduceTransformation` | `KeyedStream.reduce(...)` |
| Broadcast state | `BroadcastStateTransformation`, `KeyedBroadcastStateTransformation` | `broadcast()` + `process()` |
| 캐시 | `CacheTransformation` | `DataStream.cache()` (Batch) |
| 레거시 (사라질 예정) | `LegacySinkTransformation`, `LegacySourceTransformation` | 이전 SourceFunction/SinkFunction API |

---

## 4. 데이터 / 제어 흐름

```mermaid
flowchart LR
    subgraph User["사용자 코드 (DataStream API)"]
        U1[fromSource]
        U2[map]
        U3[keyBy]
        U4[process]
        U5[sinkTo]
        U1 --> U2 --> U3 --> U4 --> U5
    end
    subgraph TransTree["Transformation 트리 (env.transformations)"]
        T1[SourceTransformation]
        T2[OneInputTransformation map]
        T3[PartitionTransformation key_group]
        T4[OneInputTransformation process]
        T5[SinkTransformation]
        T1 --> T2 --> T3 --> T4 --> T5
    end
    subgraph SGG["StreamGraphGenerator.generate()"]
        G1[for each transformation:<br/>transform tx]
        G2["transform tx<br/>translator = translatorMap.get(tx.getClass())<br/>translator.translateForStreaming(tx, ctx)"]
        G3[ctx.getStreamGraph().addOperator..<br/>ctx.getStreamGraph().addEdge..]
    end
    subgraph SG["StreamGraph DAG (output)"]
        N1[StreamNode source]
        N2[StreamNode map]
        N3[StreamNode process]
        N4[StreamNode sink]
        N1 -->|StreamEdge fwd| N2
        N2 -->|StreamEdge keyGroup| N3
        N3 -->|StreamEdge fwd| N4
    end
    User --> TransTree
    TransTree --> SGG
    SGG --> G1 --> G2 --> G3 --> SG
```

핵심 관찰:
- `Transformation` 트리는 **logical 표현** — `keyBy`도 자체 노드(`PartitionTransformation`)로 들어 있다.
- `StreamGraph`는 **physical에 가까운 표현** — `keyBy`는 별도 노드 없이 `StreamEdge`의 `StreamPartitioner` 속성으로 흡수된다.
- 1:1 매핑이 아니다. 한 transformation이 여러 `StreamNode`를 만들 수도 있고(예: `SinkTransformation` → writer + committer), 여러 transformation이 합쳐져 한 노드로 갈 수도 있다(union 류).

---

## 5. 코드 워크스루

### 5.1 진입 — `StreamExecutionEnvironment.getStreamGraph(...)`

이전 문서의 [`execute(String)` 메서드](../02-job-fundamentals/01-execute-entrypoint.md#52-execute--executestring--executestreamgraph)에서 호출되는 진입점.

`flink-runtime/src/main/java/org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.java:2042-2045`:

```java
private StreamGraph getStreamGraph(List<Transformation<?>> transformations) {
    synchronizeClusterDatasetStatus();
    return getStreamGraphGenerator(transformations).generate();
}
```

**놀랍도록 단순**하다 — `StreamGraphGenerator`를 만들고 `generate()` 한 번 호출. 모든 복잡도는 generator 안에 있다.

> `synchronizeClusterDatasetStatus()`는 `DataStream.cache()`(Batch) 결과의 클러스터 측 상태 일관성 유지용. 일반 streaming 잡에선 거의 영향 없음 — 본 문서 범위 밖.

### 5.2 `StreamGraphGenerator.generate()` — 메인 루프

`flink-runtime/.../StreamGraphGenerator.java:250-301`:

```java
public StreamGraph generate() {
    streamGraph =
            new StreamGraph(
                    configuration, executionConfig, checkpointConfig, savepointRestoreSettings);
    shouldExecuteInBatchMode = shouldExecuteInBatchMode();
    configureStreamGraph(streamGraph);

    alreadyTransformed = new IdentityHashMap<>();

    for (Transformation<?> transformation : transformations) {
        transform(transformation);   // ← 핵심 루프
    }
    streamGraph.setSlotSharingGroupResource(slotSharingGroupResources);

    setFineGrainedGlobalStreamExchangeMode(streamGraph);

    LineageGraph lineageGraph = LineageGraphUtils.convertToLineageGraph(transformations);
    streamGraph.setLineageGraph(lineageGraph);

    for (StreamNode node : streamGraph.getStreamNodes()) {
        if (node.getInEdges().stream()
                .anyMatch(e -> !e.getPartitioner().isSupportsUnalignedCheckpoint())) {
            for (StreamEdge edge : node.getInEdges()) {
                edge.setSupportsUnalignedCheckpoints(false);
            }
        }
    }

    final Map<String, DistributedCache.DistributedCacheEntry> distributedCacheEntries =
            ExecutionPlanUtils.prepareUserArtifactEntries(
                    /* ... cached files from PipelineOptions.CACHED_FILES ... */);

    for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
            distributedCacheEntries.entrySet()) {
        streamGraph.addUserArtifact(entry.getKey(), entry.getValue());
    }

    streamGraph.serializeAndSaveWatermarkDeclarations();

    final StreamGraph builtStreamGraph = streamGraph;

    alreadyTransformed.clear();
    alreadyTransformed = null;
    streamGraph = null;

    return builtStreamGraph;
}
```

5단계로 정리:

1. **빈 `StreamGraph` 생성** + `configuration`/`executionConfig`/`checkpointConfig`/`savepointRestoreSettings` 주입
2. **`alreadyTransformed = new IdentityHashMap<>()`** — 사이클/공유 노드 처리용 캐시 초기화
3. **루프**: 각 root `Transformation`에 대해 `transform(...)` 호출 → 실제 노드/엣지 생성
4. **후처리**: SlotSharingGroup 리소스 등록, fine-grained shuffle mode 설정, **lineage 그래프 계산**(데이터 출처 추적용), unaligned checkpoint 호환성 검사, distributed cache 엔트리 등록, watermark declaration 직렬화
5. **상태 리셋** + `StreamGraph` 반환 (이후 generator는 재사용 불가)

### 5.3 `transform(Transformation<?>)` — translator 디스패치의 본체

`flink-runtime/.../StreamGraphGenerator.java:461-527` (요지):

```java
private Collection<Integer> transform(Transformation<?> transform) {
    if (alreadyTransformed.containsKey(transform)) {
        return alreadyTransformed.get(transform);   // (a) 사이클/공유 처리
    }

    LOG.debug("Transforming " + transform);

    if (transform.getMaxParallelism() <= 0) {
        // (b) global maxParallelism fallback
        int globalMaxParallelismFromConfig = executionConfig.getMaxParallelism();
        if (globalMaxParallelismFromConfig > 0) {
            transform.setMaxParallelism(globalMaxParallelismFromConfig);
        }
    }

    transform.getSlotSharingGroup().ifPresent(slotSharingGroup -> {
        // (c) slot sharing group의 resource spec 등록 + 충돌 검증
        // ...
    });

    // (d) MissingTypeInfo 예외 강제로 끌어내기 위해 한 번 호출
    transform.getOutputType();

    @SuppressWarnings("unchecked")
    final TransformationTranslator<?, Transformation<?>> translator =
            (TransformationTranslator<?, Transformation<?>>)
                    translatorMap.get(transform.getClass());

    Collection<Integer> transformedIds;
    if (translator != null) {
        transformedIds = translate(translator, transform);   // (e) 등록된 translator 사용
    } else {
        transformedIds = legacyTransform(transform);          // (f) 레거시 fallback
    }

    // (g) iterate 변환은 자기 자신을 먼저 등록하므로 중복 방지
    if (!alreadyTransformed.containsKey(transform)) {
        alreadyTransformed.put(transform, transformedIds);
    }

    return transformedIds;
}
```

7개 책임:
- **(a) 멱등성**: 동일 transformation을 두 번 봐도 한 번만 변환. 결과 노드 ID 리스트를 반환해 caller가 엣지를 그릴 수 있게 함
- **(b) maxParallelism 기본값**: transformation별 미설정이면 잡 전체 설정 적용
- **(c) Slot sharing group**: 같은 그룹의 transformation들이 같은 슬롯에 co-locate되도록 그룹 리소스 spec을 그래프에 누적
- **(d) Type 추론 강제**: lambda 타입 erasure로 인한 `MissingTypeInfo`를 여기서 일찍 잡음
- **(e) Translator 디스패치**: `translatorMap`(아래)에서 클래스→translator 룩업
- **(f) Legacy 경로**: 등록되지 않은 (deprecated) transformation들 — 줄어들고 있음
- **(g) 캐시 등록**: iterate transformation은 본인을 미리 캐시에 박는 특수 케이스 처리

### 5.4 `translatorMap` — 어떤 translator가 등록돼 있나

`flink-runtime/.../StreamGraphGenerator.java:170-190` (정적 초기화):

```java
tmp.put(OneInputTransformation.class,         new OneInputTransformationTranslator<>());
tmp.put(TwoInputTransformation.class,         new TwoInputTransformationTranslator<>());
tmp.put(MultipleInputTransformation.class,    new MultiInputTransformationTranslator<>());
tmp.put(KeyedMultipleInputTransformation.class, new MultiInputTransformationTranslator<>());
tmp.put(SourceTransformation.class,           new SourceTransformationTranslator<>());
tmp.put(SinkTransformation.class,             new SinkTransformationTranslator<>());
tmp.put(GlobalCommitterTransform.class,       new GlobalCommitterTransformationTranslator<>());
tmp.put(LegacySinkTransformation.class,       new LegacySinkTransformationTranslator<>());
tmp.put(LegacySourceTransformation.class,     new LegacySourceTransformationTranslator<>());
tmp.put(UnionTransformation.class,            new UnionTransformationTranslator<>());
tmp.put(PartitionTransformation.class,        new PartitionTransformationTranslator<>());
tmp.put(SideOutputTransformation.class,       new SideOutputTransformationTranslator<>());
tmp.put(ReduceTransformation.class,           new ReduceTransformationTranslator<>());
// ...
tmp.put(BroadcastStateTransformation.class,        new BroadcastStateTransformationTranslator<>());
tmp.put(CacheTransformation.class,                 new CacheTransformationTranslator<>());
translatorMap = Collections.unmodifiableMap(tmp);   // immutable
```

**클래스↔translator 1:1 등록**. 새 transformation 종류를 추가하려면 (1) `Transformation` 서브클래스, (2) 거기에 맞는 `TransformationTranslator` 구현체, (3) 이 맵에 등록. 이 세 가지가 한 세트.

### 5.5 `TransformationTranslator` 인터페이스 — 변환 계약

`flink-runtime/.../TransformationTranslator.java:34-92`:

```java
@Internal
public interface TransformationTranslator<OUT, T extends Transformation<OUT>> {

    /** BATCH-style 실행용 변환 */
    Collection<Integer> translateForBatch(final T transformation, final Context context);

    /** STREAMING-style 실행용 변환 */
    Collection<Integer> translateForStreaming(final T transformation, final Context context);

    /** translator에게 주어지는 컨텍스트 */
    @Internal
    interface Context {
        StreamGraph getStreamGraph();   // 누적되는 StreamGraph에 노드/엣지 추가
        Collection<Integer> getStreamNodeIds(final Transformation<?> transformation);
        String getSlotSharingGroup();
        long getDefaultBufferTimeout();
        ReadableConfig getGraphGeneratorConfig();
        Collection<Integer> transform(Transformation<?> transformation);  // 입력 transformation 재귀 변환
    }
}
```

핵심 패턴:
- 반환값 `Collection<Integer>` = 이 transformation이 만들어낸 **마지막 `StreamNode` ID들** (= 다음 transformation이 엣지로 연결할 대상). ID가 여러 개일 수 있는 이유: union 같은 transformation은 input 여러 개가 모두 "마지막 노드" 역할을 함.
- `Context.transform(...)`을 통해 translator는 **자식 transformation들을 재귀 변환**한다 (트리를 따라 내려간다).
- BATCH/STREAMING 분기는 `executionConfig`/`shouldExecuteInBatchMode()` 결과로 `StreamGraphGenerator`에서 결정 → translator마다 두 메서드를 구현.

### 5.6 `Transformation<T>` — 추상 클래스의 핵심 필드

`flink-core/.../Transformation.java:109-140` (대표 필드만 발췌):

```java
@Internal
public abstract class Transformation<T> {

    public static final int UPPER_BOUND_MAX_PARALLELISM = 1 << 15;
    private static final AtomicInteger ID_COUNTER = new AtomicInteger(0);
    private boolean parallelismConfigured;

    protected final int id;                  // 글로벌 유일 ID (ID_COUNTER에서)
    protected String name;                   // 사용자가 본 ".name(...)"
    protected String description;
    protected TypeInformation<T> outputType; // 추론/명시된 출력 타입

    private int parallelism;                 // 사용자 또는 기본 parallelism
    private int maxParallelism = -1;         // dynamic scaling 상한 + key group 수
    private ResourceSpec minResources;
    private ResourceSpec preferredResources;

    // ... managed memory, slot sharing, co-location, uid, uidHash, bufferTimeout, attribute
}
```

**놓치면 안 되는 항목**:
- `id`: 매 `Transformation` 인스턴스가 받는 process-wide 카운터 ID. **`StreamNode`의 `id`가 여기서 옴** (보통 같음).
- `uid`: 사용자가 `.uid("...")`로 명시한 ID. **`StreamNode` → `JobVertex` 매핑 키**가 되어 savepoint 호환성을 보장 (next 문서 주제).
- `userProvidedNodeHash`: 사용자가 `setUidHash(...)`로 지정한 32자 hex 해시. 보통 잡 마이그레이션 트러블슈팅용.
- `maxParallelism`: dynamic scaling(=AdaptiveScheduler) 상한 + state의 **key group 수**. **변경 불가** (state 호환성에 직결). 본인 환경의 AdaptiveScheduler ↔ key group 관계는 [`../10-scheduling-failover/`](../10-scheduling-failover/)에서 다룰 예정.
- `slotSharingGroup`, `coLocationGroupKey`: 슬롯 배치 힌트.

### 5.7 `StreamNode` — 그래프 위의 한 operator

`flink-runtime/.../StreamNode.java:55-130` (대표 필드):

```java
/** Class representing the operators in the streaming programs, with all their properties. */
@Internal
public class StreamNode implements Serializable {

    private final int id;                                  // Transformation.id에서 유래
    private int parallelism;
    private int maxParallelism;
    private ResourceSpec minResources;
    private ResourceSpec preferredResources;
    // ... managed memory ...
    private long bufferTimeout;
    private final String operatorName;
    private String operatorDescription;
    private @Nullable String slotSharingGroup;
    private @Nullable String coLocationGroup;
    private KeySelector<?, ?>[] statePartitioners = new KeySelector[0];
    private TypeSerializer<?> stateKeySerializer;

    // 직렬화 시 별도로 처리하는 operator 본체
    private @Nullable transient StreamOperatorFactory<?> operatorFactory;

    private TypeSerializer<?>[] typeSerializersIn = new TypeSerializer[0];
    private TypeSerializer<?> typeSerializerOut;

    private List<StreamEdge> inEdges  = new ArrayList<>();
    private List<StreamEdge> outEdges = new ArrayList<>();

    private final Class<? extends TaskInvokable> jobVertexClass;  // 어떤 StreamTask로 실행할지

    // ... InputFormat, OutputFormat, transformationUID, userHash, inputRequirements ...
    private @Nullable IntermediateDataSetID consumeClusterDatasetId;
    private boolean supportsConcurrentExecutionAttempts = true;
    private boolean parallelismConfigured = false;
    private Attribute attribute;
}
```

**Transformation vs StreamNode 차이**:
- `Transformation`은 **사용자 facing API의 산물** — 사용자가 호출한 메서드 한 번이 거의 그대로 1개 transformation.
- `StreamNode`는 **runtime에 한 발 더 가까움** — 직렬화 가능, `StreamOperatorFactory`(실제 operator 인스턴스 만드는 팩토리)를 들고 있고, `TaskInvokable` 종류(`OneInputStreamTask`/`TwoInputStreamTask`/`SourceOperatorStreamTask`/`MultipleInputStreamTask` 등)를 결정.

### 5.8 `StreamEdge` — 노드 간 데이터 흐름

`flink-runtime/.../StreamEdge.java:40-80` (대표 필드):

```java
public class StreamEdge implements Serializable {

    private static final long ALWAYS_FLUSH_BUFFER_TIMEOUT = 0L;

    private final String edgeId;
    private final int sourceId;
    private final int targetId;
    private final int uniqueId;       // 같은 source/target 쌍 사이에서도 유일성 보장

    private int typeNumber;            // co-task에서 입력 슬롯 식별자 (1 또는 2)
    private final OutputTag outputTag; // side-output 식별
    private StreamPartitioner<?> outputPartitioner;  // ← 여기 keyBy/forward/rebalance가 들어감
    private final String sourceOperatorName;
    private final String targetOperatorName;
    private StreamExchangeMode exchangeMode;          // PIPELINED / BATCH / HYBRID 등
    private long bufferTimeout;
    private boolean supportsUnalignedCheckpoints = true;
}
```

**핵심**: `outputPartitioner` — `keyBy()`는 노드를 만들지 않고 **upstream 엣지의 partitioner**로 흡수된다. 종류:
- `ForwardPartitioner`: 같은 subtask로 forward (1:1)
- `KeyGroupStreamPartitioner`: keyBy → key group 기반 분배
- `RebalancePartitioner`: round-robin
- `RescalePartitioner`: subtask 그룹 안에서 round-robin
- `BroadcastPartitioner`: 모든 downstream subtask로 복제
- `ForwardForConsecutiveHashPartitioner`, `ForwardForUnspecifiedPartitioner`: 표시(marker)용 — 후속 단계에서 결정 보류

`exchangeMode`도 중요: `PIPELINED`(streaming 기본), `BATCH`, `HYBRID_FULL`/`HYBRID_SELECTIVE` (Adaptive Batch Scheduler용).

### 5.9 `StreamGraph` — 그릇

`flink-runtime/.../StreamGraph.java:124`:

```java
public class StreamGraph implements Pipeline, ExecutionPlan {
    // ...
}
```

두 인터페이스를 구현:
- **`Pipeline`**: `flink-core/.../Pipeline.java:25` — 그저 빈 마커. `PipelineExecutor.execute(Pipeline, ...)`에서 받는 그 타입. (이전 문서 5.6절)
- **`ExecutionPlan`**: 직렬화 가능한 잡 plan. K8s ApplicationMode 등에서 plan 자체를 BLOB로 업로드할 때 쓰인다.

주요 컬렉션:
- `Map<Integer, StreamNode>` (id → node) — `streamNodes`
- 사용자 metadata: `jobName`, `jobID`, `executionConfig`, `checkpointConfig`, `savepointRestoreSettings`
- 운영 관련: cached files, classpath, watermark declarations, lineage

`addOperator(...)` / `addEdge(...)` / `getStreamNodes()` 등의 메서드를 통해 generator가 그래프를 점진적으로 채운다.

---

## 6. 사용자 환경 매핑

### 6.1 K8s + Operator + Kafka + Iceberg 시나리오에서

본인의 일반적 잡 형태를 가정:

```
Kafka Source → keyBy(orderId) → Window → Iceberg Sink
```

생성되는 `Transformation` 트리(개념):

```
SourceTransformation                 (Kafka, FLIP-27 Source V2)
   ↓ (DataStream)
PartitionTransformation              (keyBy, KeyGroupStreamPartitioner)
   ↓
OneInputTransformation               (process function 또는 window operator)
   ↓
SinkTransformation                   (FLIP-191 Sink V2 → iceberg-flink)
```

`StreamGraphGenerator`가 만들어낼 `StreamGraph`(개념):

| StreamNode | jobVertexClass | inEdges (partitioner) | 비고 |
|-----------|---------------|---------------------|------|
| Source | `SourceOperatorStreamTask` | (none) | Kafka SplitEnumerator + Reader (FLIP-27) |
| Window operator | `OneInputStreamTask` | `KeyGroupStreamPartitioner` | `keyBy`가 엣지로 흡수됨 |
| Sink Writer | `OneInputStreamTask` | `ForwardPartitioner` | Iceberg 파일 쓰기 |
| (선택) Committer | (별도 vertex) | — | 체크포인트 시 Iceberg 메타 commit |

→ Source V2 / Sink V2 SPI 자체의 deep dive는 [`../06-source-sink-spi/`](../06-source-sink-spi/).

### 6.2 `maxParallelism` ↔ AdaptiveScheduler ↔ key group

본인 환경의 핵심 결정점 ([`../00-overview/my-environment-map.md`](../00-overview/my-environment-map.md)):

- AdaptiveScheduler가 잡 시작 후 parallelism을 가변 조정한다 → 그 **상한이 `maxParallelism`** (=key group 수).
- 한 번 잡을 띄운 뒤 `maxParallelism`을 바꾸려면 savepoint를 거쳐 새 잡을 띄워야 한다 (state migration 필요). 따라서 **첫 배포 시에 운영 가능한 최대 parallelism을 충분히 크게 잡는 것**이 본인 환경에선 특히 중요.

`Transformation.setMaxParallelism(...)`은 그래프 생성 단계에서 노드별로 박힌다.

---

## 7. 관련 FLIP / JIRA

| 문서 | 무엇 |
|------|------|
| [FLIP-25: Support User State TTL Natively](https://cwiki.apache.org/confluence/display/FLINK/FLIP-25%3A+Support+User+State+TTL+Natively) | (참고) state TTL — Transformation에 직접 영향 적지만 graph→runtime 매핑 이해의 측면 자료 |
| [FLIP-27: Refactor Source Interface](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) | `SourceTransformation` + `SourceTransformationTranslator`의 도입 배경 |
| [FLIP-143: Unified Sink API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API) | `SinkTransformation` 1세대 |
| [FLIP-191: Extend unified Sink interface to support small file compaction](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) | Sink V2 진화 — `GlobalCommitterTransform` 관련 |
| [FLIP-187: Adaptive Batch Scheduler](https://cwiki.apache.org/confluence/display/FLINK/FLIP-187%3A+Adaptive+Batch+Scheduler) | `StreamExchangeMode.HYBRID_*`의 용도 — 본 문서 범위 밖이지만 `StreamEdge.exchangeMode` 이해에 도움 |

---

## 8. 디버깅 & 실험

### 8.1 IDE에서 호출 흐름 따라가기

[이전 문서의 디버깅 가이드](../02-job-fundamentals/01-execute-entrypoint.md#81-ide에서-한-단계씩-따라가기-가장-빠른-학습법)에 더해, 그래프 생성 단계는 다음 위치에 브레이크포인트:

- `StreamExecutionEnvironment.java:2042` — `getStreamGraph(transformations)` 진입
- `StreamGraphGenerator.java:250` — `generate()` 시작
- `StreamGraphGenerator.java:476` — `transform(...)` 진입 (Transformation 1개당 한 번)
- `StreamGraphGenerator.java:511` — `translatorMap.get(transform.getClass())` — translator 디스패치 순간
- 각 translator 구현체의 `translateForStreaming(...)` — 노드/엣지가 실제로 추가되는 곳

WordCount 예제에서 실행하면 약 5~7개 transformation이 순회되는 모습을 볼 수 있다.

### 8.2 그래프 시각화 — JSON으로 떠보기

`StreamGraph.getStreamingPlanAsJSON()`가 그래프를 JSON으로 직렬화한다 (Web UI의 "Plan" 탭이 이걸 렌더링). 디버그 코드:

```java
StreamGraph sg = env.getStreamGraph();
System.out.println(sg.getStreamingPlanAsJSON());
// 또는 JSON을 https://flink.apache.org/visualizer/ 에 붙여 그래프로 보기
```

⚠️ `env.getStreamGraph()` 호출 후엔 `transformations`가 비워지지 않으므로(`execute()`와 달리), 같은 env에서 추가 transformation을 쌓고 다시 `execute()`하면 모두 합쳐서 제출된다.

### 8.3 그래프 그래프(MCP)에서 빠르게 보기

```
mcp__codebase-memory-mcp__search_graph(
  project="home-donamk-code-flink-flink-runtime",
  qn_pattern=".*StreamGraphGenerator\\.transform$"
)

mcp__codebase-memory-mcp__get_code_snippet(
  project="home-donamk-code-flink-flink-runtime",
  qualified_name="...SourceTransformationTranslator.translateForStreaming"
)
```

각 translator가 `addEdge`/`addOperator` 어디서 호출하는지 따라가면 그래프 형성 패턴이 클래스별로 명확해진다.

### 8.4 maxParallelism / key group 영향 실험

```java
env.setMaxParallelism(128);          // ← 글로벌 상한
DataStream<X> s = source.map(...).setMaxParallelism(64);  // ← 노드별 override
env.execute("test");
// → StreamGraph 안 해당 노드의 maxParallelism 필드가 64로 박힘
//   key state는 64개 key group으로 분할
```

이 값은 한 번 박히면 savepoint 안에 들어간다. 변경 시 state 호환 깨짐.

---

## 9. 자주 묻는 질문 / 함정

**Q1. `keyBy`는 왜 별도 `StreamNode`로 안 보이나?**
A. `keyBy`는 `PartitionTransformation`(Transformation 트리에는 노드로 존재)이지만, `PartitionTransformationTranslator`가 새 `StreamNode`를 만들지 않고 **upstream의 다음 `StreamEdge`에 `KeyGroupStreamPartitioner`를 박아서** 흡수한다. 네트워크 셔플은 엣지의 속성이지 노드가 아니라는 모델.

**Q2. 같은 `env`에서 `execute()`를 두 번 부르면?**
A. `transformations` 리스트가 누적되므로 **두 번째는 첫 번째 + 그 사이 추가된 모든 transformation까지 포함**되어 제출된다. `Transformation.id`도 process-wide AtomicInteger이므로 두 번째 잡의 노드 id는 첫 번째에서 이어진다. **테스트/REPL에서 자주 만나는 함정** — `getExecutionEnvironment()`를 매번 새로 받거나, 잡 사이에 `env`를 리셋하는 패턴 권장.

**Q3. `Transformation.equals()`가 override돼 있는데 generator는 왜 IdentityHashMap을 쓰나?**
A. `Transformation.equals()`는 `id, name, parallelism, outputType, bufferTimeout`만 비교. **두 다른 인스턴스가 우연히 같은 결과**일 수 있다. 그래프 생성에선 인스턴스 자체가 정체성이어야 하므로 `IdentityHashMap`(==) 사용.

**Q4. `MissingTypeInfo` 예외가 뜨면?**
A. lambda를 `map`/`filter`에 넘겼을 때 Java 타입 erasure로 출력 타입 추론 실패. `transform.getOutputType()` 호출 지점(`StreamGraphGenerator.java:506` 근처)에서 발생. 해결: `.returns(Types.STRING)` 등으로 명시 또는 `ResultTypeQueryable` 구현.

**Q5. `LegacySourceTransformation` / `LegacySinkTransformation`은 뭔가?**
A. 옛 `SourceFunction`/`SinkFunction` API(FLIP-27/FLIP-143 이전)용 transformation. 신규 코드는 `SourceTransformation`/`SinkTransformation`(V2)를 써야 한다. **본인 환경의 Kafka 커넥터(외부 레포 `apache/flink-connector-kafka`)는 V2 SourceTransformation을 사용**.

**Q6. `JobGraph`는 언제 만들어지나? (다음 문서 미리보기)**
A. `StreamGraph`까지가 client 측 작업. 클러스터로 제출되면 `Dispatcher`/`JobMaster`가 `StreamGraph` → `JobGraph` 변환을 수행한다 (operator chaining, JobVertex 통합 등). [`02-job-graph.md`](02-job-graph.md)에서 다룸.

---

## 10. 다음에 읽을 문서

| 다음 단계 | 문서 |
|----------|------|
| `StreamGraph` → `JobGraph` 변환 (operator chaining, JobVertex) | [`02-job-graph.md`](02-job-graph.md) |
| `JobGraph` → `ExecutionGraph` (parallelism × subtask 단위 펼치기) | [`03-execution-graph.md`](03-execution-graph.md) |
| `StreamOperator` / `StreamOperatorFactory` 본체 | [`../04-runtime-architecture/stream-task-mailbox.md`](../04-runtime-architecture/05-stream-task-mailbox.md) |
| Source V2 (`SourceTransformation` ↔ `SourceCoordinator` ↔ Reader) | [`../06-source-sink-spi/source-v2-overview.md`](../06-source-sink-spi/01-source-v2-overview.md) |
| Sink V2 (`SinkTransformation` ↔ Committer ↔ Iceberg) | [`../06-source-sink-spi/sink-v2-overview.md`](../06-source-sink-spi/03-sink-v2-overview.md) |
| `maxParallelism` ↔ AdaptiveScheduler ↔ key group | [`../10-scheduling-failover/adaptive-scheduler.md`](../10-scheduling-failover/01-adaptive-scheduler.md) |
