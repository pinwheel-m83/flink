# Phase 1: Flink Job 구조의 기본 이해

> 이 장에서는 Flink Job의 기본 구조를 코드 레벨에서 이해합니다.
> 핵심 질문: **"env.execute()를 호출하면 내부에서 무슨 일이 일어나는가?"**

---

## 1.1 가장 단순한 Flink Job — DataGenerator

모든 Flink Job은 동일한 패턴을 따릅니다: **Environment 생성 → Source → Transformation → Sink → Execute**.

```java
// 파일: flink-examples/flink-examples-streaming/.../datagen/DataGenerator.java

package org.apache.flink.streaming.examples.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenerator {

    public static void main(String[] args) throws Exception {

        // ① ExecutionEnvironment 생성 — 모든 Flink Job의 시작점
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ② Source 정의 — 데이터 생성기
        GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;
        long numberOfRecords = 10000000;

        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        generatorFunction,      // 각 인덱스를 문자열로 변환
                        numberOfRecords,        // 생성할 총 레코드 수
                        RateLimiterStrategy.perSecond(1000),  // 초당 1000개 제한
                        Types.STRING);          // 출력 타입 정보

        // ③ Source를 Environment에 등록하여 DataStream 생성
        DataStreamSource<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Data Generator");

        // ④ Sink — 콘솔에 출력
        stream.print();

        // ⑤ Job 실행 — 이 호출이 있어야 실제로 실행됨
        env.execute("Data Generator");
    }
}
```

### 코드 분석

| 단계 | 메서드 | 역할 |
|------|--------|------|
| ① | `getExecutionEnvironment()` | 실행 환경(로컬/클러스터)을 자동 감지하여 적절한 Environment 반환 |
| ② | `new DataGeneratorSource<>(...)` | FLIP-27 기반의 새로운 Source API로 데이터 소스 정의 |
| ③ | `env.fromSource(...)` | Source를 Environment에 등록, `DataStreamSource` 반환 |
| ④ | `stream.print()` | 내부적으로 `PrintSink`를 추가하는 shortcut |
| ⑤ | `env.execute(...)` | **실제 실행 트리거** — 이전 단계들은 실행 계획만 구성 |

> **핵심 개념: Lazy Evaluation**
> `env.execute()` 호출 전까지는 아무 데이터도 처리되지 않습니다.
> `map()`, `filter()`, `keyBy()` 등 모든 연산은 실행 계획(StreamGraph)을 구성할 뿐입니다.
> 이것은 Flink가 전체 파이프라인을 최적화할 수 있게 해주는 핵심 설계입니다.

---

## 1.2 실전 패턴 — SocketWindowWordCount

Source → Transform → KeyBy → Window → Aggregate → Sink의 전형적인 스트리밍 패턴입니다.

```java
// 파일: flink-examples/flink-examples-streaming/.../socket/SocketWindowWordCount.java

public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        final String hostname;
        final int port;

        // CLI 파라미터 파싱
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            return;
        }

        // ① Environment 생성
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ② Source — 소켓에서 텍스트 라인 읽기
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // ③ Transformation 파이프라인
        DataStream<WordWithCount> windowCounts =
                text
                    // FlatMap: 한 라인을 여러 단어로 분리
                    .flatMap(new FlatMapFunction<String, WordWithCount>() {
                        @Override
                        public void flatMap(String value, Collector<WordWithCount> out) {
                            for (String word : value.split("\\s")) {
                                out.collect(new WordWithCount(word, 1L));
                            }
                        }
                    })
                    // KeyBy: 단어별로 그룹핑 (같은 키는 같은 Task로)
                    .keyBy(value -> value.word)
                    // Window: 5초 텀블링 윈도우
                    .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                    // Reduce: 윈도우 내 카운트 합산
                    .reduce(
                            (a, b) -> new WordWithCount(a.word, a.count + b.count));

        // ④ Sink — 결과를 single thread로 콘솔에 출력
        windowCounts.print().setParallelism(1);

        // ⑤ 실행
        env.execute("Socket Window WordCount");
    }

    // POJO — Flink이 자동으로 직렬화 처리
    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
```

### 데이터 흐름 다이어그램

```
Socket Source ──→ FlatMap ──→ KeyBy ──→ Window(5s) ──→ Reduce ──→ Print Sink
  (text)        (word,1)    (파티셔닝)   (버퍼링)      (합산)     (출력)
```

### 핵심 개념 설명

**keyBy()의 의미:**
- 단순한 그룹핑이 아닙니다. 동일한 키를 가진 모든 레코드가 **같은 물리적 Task 인스턴스**로 라우팅됩니다
- 내부적으로 Hash Partitioning을 사용합니다
- `keyBy()` 호출 후 반환 타입이 `DataStream` → `KeyedStream`으로 바뀝니다
- 이는 상태(State)를 키별로 격리하기 위한 전제 조건입니다

**Window의 역할:**
- 무한 스트림을 유한한 청크로 분할합니다
- `TumblingProcessingTimeWindows.of(Duration.ofSeconds(5))`: 겹치지 않는 5초 단위 윈도우
- Processing Time 기반이므로 시스템 시계를 사용합니다 (Event Time과 대비)

---

## 1.3 StreamExecutionEnvironment — 모든 것의 시작점

`StreamExecutionEnvironment`는 Flink Job의 진입점이자 실행 계획 빌더입니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.java

public class StreamExecutionEnvironment implements AutoCloseable {

    // 변환(Transformation) 리스트 — Job의 실행 계획이 여기에 축적됩니다
    protected final List<Transformation<?>> transformations = new ArrayList<>();

    // 실행 설정
    private final ExecutionConfig config = new ExecutionConfig();
    private final CheckpointConfig checkpointCfg = new CheckpointConfig();

    // ...
}
```

### getExecutionEnvironment() — 환경 자동 감지

```java
public static StreamExecutionEnvironment getExecutionEnvironment() {
    return getExecutionEnvironment(new Configuration());
}

public static StreamExecutionEnvironment getExecutionEnvironment(Configuration configuration) {
    return Utils.resolveFactory(threadLocalContextEnvironmentFactory, contextEnvironmentFactory)
            .map(factory -> factory.createExecutionEnvironment(configuration, userClassloader))
            .orElseGet(
                    () -> StreamExecutionEnvironment.createLocalEnvironment(configuration));
}
```

**동작 원리:**
- 클러스터에서 실행 시: `contextEnvironmentFactory`가 설정되어 있어 원격 환경 반환
- 로컬 IDE에서 실행 시: `createLocalEnvironment()`이 호출되어 `MiniCluster` 기반 로컬 환경 반환
- 이 패턴 덕분에 동일한 코드가 로컬과 클러스터에서 모두 동작합니다

### fromSource() — Source를 Environment에 등록

```java
public <OUT> DataStreamSource<OUT> fromSource(
        Source<OUT, ?, ?> source,
        WatermarkStrategy<OUT> timestampsAndWatermarks,
        String sourceName) {
    return fromSource(source, timestampsAndWatermarks, sourceName, null);
}

public <OUT> DataStreamSource<OUT> fromSource(
        Source<OUT, ?, ?> source,
        WatermarkStrategy<OUT> timestampsAndWatermarks,
        String sourceName,
        TypeInformation<OUT> typeInfo) {

    // TypeInformation 추론
    final TypeInformation<OUT> resolvedTypeInfo =
            getTypeInfo(source, sourceName, Source.class, typeInfo);

    return new DataStreamSource<>(
            this,
            checkNotNull(source),         // Source 구현체
            checkNotNull(timestampsAndWatermarks),  // Watermark 전략
            checkNotNull(resolvedTypeInfo), // 타입 정보
            sourceName);
}
```

**핵심: TypeInformation**
- Flink은 Java의 제네릭 타입 소거(erasure) 문제를 우회하기 위해 자체 타입 시스템을 사용합니다
- `TypeInformation`은 직렬화/역직렬화, 비교, 해싱 등에 사용됩니다
- `Types.STRING`, `Types.INT` 같은 헬퍼로 지정하거나, Flink이 리플렉션으로 추론합니다

### execute() — Job 실행 트리거

```java
public JobExecutionResult execute(String jobName) throws Exception {
    final List<Transformation<?>> originalTransformations = new ArrayList<>(transformations);
    StreamGraph streamGraph = getStreamGraph();
    if (jobName != null) {
        streamGraph.setJobName(jobName);
    }
    try {
        return execute(streamGraph);
    } catch (Throwable t) {
        Optional<ClusterDatasetCorruptedException> clusterDatasetCorruptedException =
                ExceptionUtils.findThrowable(t, ClusterDatasetCorruptedException.class);
        if (!clusterDatasetCorruptedException.isPresent()) {
            throw t;
        }
        // 중간 데이터셋이 손상된 경우 재시도
        invalidateCacheTransformations(
                clusterDatasetCorruptedException.get().getDatasetIds());
        transformations.clear();
        transformations.addAll(originalTransformations);
        return execute(streamGraph);
    }
}

public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
    final JobClient jobClient = executeAsync(streamGraph);

    final JobExecutionResult jobExecutionResult;

    if (configuration.get(DeploymentOptions.ATTACHED)) {
        // Attached 모드 (기본): Job 완료까지 블로킹 대기
        jobExecutionResult = jobClient.getJobExecutionResult().get();
    } else {
        // Detached 모드: Job ID만 반환하고 즉시 리턴
        jobExecutionResult = new DetachedJobExecutionResult(jobClient.getJobID());
    }

    // Job 리스너에게 완료 통지
    jobListeners.forEach(
            jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));

    return jobExecutionResult;
}
```

**execute()의 내부 흐름 (가장 중요!):**

```
execute(jobName)
  │
  ├── ① getStreamGraph()
  │     └── transformations 리스트를 StreamGraph로 변환
  │
  ├── ② executeAsync(streamGraph)
  │     ├── StreamGraph → JobGraph 변환
  │     ├── JobGraph을 클러스터에 제출 (submit)
  │     └── JobClient 반환
  │
  └── ③ jobClient.getJobExecutionResult().get()
        └── Job 완료까지 블로킹 대기
```

### getStreamGraph() — Transformation → StreamGraph 변환

```java
public StreamGraph getStreamGraph() {
    return getStreamGraph(true);
}

public StreamGraph getStreamGraph(boolean clearTransformations) {
    final StreamGraph streamGraph = getStreamGraphGenerator(transformations)
            .generate();
    if (clearTransformations) {
        transformations.clear();
    }
    return streamGraph;
}

private StreamGraphGenerator getStreamGraphGenerator(List<Transformation<?>> transformations) {
    // ...
    return new StreamGraphGenerator(transformations, config, checkpointCfg, configuration)
            .setStateBackend(defaultStateBackend)
            .setChangelogStateBackendEnabled(changelogStateBackendEnabled)
            .setSavepointDir(defaultSavepointDirectory)
            .setChaining(isChainingEnabled)
            // ...;
}
```

> **이것이 Phase 2의 시작점입니다.**
> `StreamGraphGenerator.generate()`가 사용자의 transformation 체인을 내부 그래프 표현으로 변환합니다.

---

## 1.4 DataStream API — 변환(Transformation) 등록의 메커니즘

사용자가 `stream.map(...)`, `stream.flatMap(...)` 등을 호출할 때, 실제로는 `Transformation` 객체가 생성되어 리스트에 추가됩니다.

### map() 내부

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java

// 사용자 호출:
// stream.map(value -> value.toUpperCase())

// Step 1: map()이 호출되면 타입 추론 후 transform()으로 위임
public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper) {
    TypeInformation<R> outType =
            TypeExtractor.getMapReturnTypes(
                    clean(mapper), getType(), Utils.getCallLocationName(), true);
    return map(mapper, outType);
}

public <R> SingleOutputStreamOperator<R> map(
        MapFunction<T, R> mapper, TypeInformation<R> outputType) {
    return transform("Map", outputType, new StreamMap<>(clean(mapper)));
}

// Step 2: transform()에서 OneInputTransformation 생성
public <R> SingleOutputStreamOperator<R> transform(
        String operatorName,
        TypeInformation<R> outTypeInfo,
        OneInputStreamOperator<T, R> operator) {
    // ...
    OneInputTransformation<T, R> resultTransform =
            new OneInputTransformation<>(
                    this.transformation,         // 이전 Transformation (input)
                    operatorName,                // "Map"
                    SimpleOperatorFactory.of(operator),  // StreamMap 래핑
                    outTypeInfo,                 // 출력 타입
                    environment.getParallelism(), // 병렬도
                    false);

    SingleOutputStreamOperator<R> returnStream =
            new SingleOutputStreamOperator(environment, resultTransform);

    // ★ Environment에 Transformation 등록
    getExecutionEnvironment().addOperator(resultTransform);

    return returnStream;
}
```

### Transformation 체인의 구조

```
fromSource() → Transformation₁
    ↓
  .map() → Transformation₂ (input = Transformation₁)
    ↓
  .keyBy() → Transformation₃ (input = Transformation₂, + PartitionTransformation)
    ↓
  .reduce() → Transformation₄ (input = Transformation₃)
    ↓
  .print() → Transformation₅ (input = Transformation₄, + SinkTransformation)
```

**모든 Transformation은 자신의 input Transformation을 참조합니다.**
이것은 역방향 연결 리스트(linked list)와 같은 구조로, `StreamGraphGenerator`가 이를 순회하여 StreamGraph를 구축합니다.

### addOperator() — Environment에 Transformation 등록

```java
// StreamExecutionEnvironment.java
public void addOperator(Transformation<?> transformation) {
    Preconditions.checkNotNull(transformation, "transformation must not be null.");
    this.transformations.add(transformation);
}
```

---

## 1.5 전체 그림: Job 제출부터 실행까지

```
[사용자 코드]
    │
    ▼
StreamExecutionEnvironment
    │ transformations: List<Transformation>
    │
    ▼ execute()
StreamGraphGenerator.generate()
    │
    ▼
StreamGraph (논리적 실행 계획)
    │
    ▼ Phase 2에서 상세히
JobGraph (물리적 실행 계획, Operator Chaining 적용)
    │
    ▼
ClusterClient.submitJob(jobGraph)
    │
    ▼
Dispatcher.submitJob()          ← Phase 3: 런타임 아키텍처
    │
    ▼
JobMaster (JobGraph → ExecutionGraph)
    │
    ▼
TaskExecutor.submitTask()
    │
    ▼
Task.run() → StreamTask.invoke()  ← 실제 데이터 처리 시작
```

---

## 1.6 설계 배경: FLIP-27 Source API

DataGenerator 예제에서 사용한 `env.fromSource()`는 **FLIP-27**에 의해 도입된 새로운 Source API입니다.

> **[FLIP-27: Refactor Source Interface](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27:+Refactor+Source+Interface)**

기존의 `SourceFunction` 인터페이스는 파티션 검색과 데이터 읽기가 혼재되어 있었습니다.
FLIP-27은 이를 3개 컴포넌트로 분리했습니다:

```
┌─────────────────────────────────┐
│          Source<T>              │
│  (팩토리 — 아래 두 개를 생성)      │
└────────┬───────────┬────────────┘
         │           │
         ▼           ▼
┌──────────────┐ ┌──────────────┐
│SplitEnumerator│ │ SourceReader │ × parallelism
│(병렬도 1)     │ │(데이터 읽기)  │
│              │ │              │
│ Split 검색    │ │ Split 소비   │
│ Split 할당    │ │ 레코드 방출   │
└──────────────┘ └──────────────┘
```

- **SplitEnumerator**: 병렬도 1로 실행. 데이터 소스의 파티션(Split)을 검색하고 SourceReader에 할당
- **SourceReader**: 각 병렬 인스턴스에서 실행. 할당된 Split에서 데이터를 읽어 레코드 방출
- **Split**: 데이터의 논리적 단위 (예: Kafka 파티션, 파일 블록)

이 설계의 장점:
- 파티션 검색과 데이터 읽기의 관심사 분리
- SplitEnumerator가 체크포인트의 일부로 Split 할당을 관리 → Exactly-once 보장
- 다양한 Source(Kafka, File, Custom)에 일관된 API 제공

---

## 1.7 핵심 정리

1. **모든 Flink Job은 5단계**: Environment → Source → Transform → Sink → Execute
2. **Lazy Evaluation**: `execute()` 전까지 실행 계획만 구성 (Transformation 리스트)
3. **StreamExecutionEnvironment**: Job의 진입점이자 설정 컨테이너
4. **TypeInformation**: Java 제네릭의 타입 소거를 보완하는 Flink 자체 타입 시스템
5. **execute()의 핵심**: `Transformation → StreamGraph → JobGraph → 클러스터 제출`

---

## 다음 단계

Phase 2에서는 `StreamGraphGenerator.generate()`가 Transformation 리스트를 어떻게 `StreamGraph`로 변환하는지,
그리고 `StreamGraph → JobGraph → ExecutionGraph`의 3단계 변환 과정을 코드 레벨에서 추적합니다.
