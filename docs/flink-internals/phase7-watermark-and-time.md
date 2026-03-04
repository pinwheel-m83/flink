# Phase 7: Watermark & Time

> Flink의 이벤트 시간(Event Time) 처리의 핵심인 Watermark 메커니즘을 코드 레벨에서 추적합니다.
> Watermark의 생성, 전파, 그리고 Window 연산의 내부 동작을 다룹니다.

---

## 7.1 Time의 종류

Flink은 3가지 시간 개념을 제공합니다:

```
이벤트 발생        Flink 수신          Flink 처리
    │                 │                   │
    ▼                 ▼                   ▼
Event Time      Ingestion Time     Processing Time
(이벤트 시간)    (수집 시간)         (처리 시간)

Event Time: 이벤트가 실제로 발생한 시간 (레코드에 포함된 타임스탬프)
Processing Time: Flink이 레코드를 처리하는 시점의 시스템 시계
Ingestion Time: Flink에 레코드가 들어온 시점 (현재는 Event Time의 특수 케이스)
```

> **왜 Event Time이 중요한가?**
> Processing Time은 네트워크 지연, 장애 복구 등으로 비결정적입니다.
> Event Time을 사용하면 늦게 도착한 데이터도 정확한 윈도우에 배치할 수 있습니다.
> 재처리(reprocessing) 시에도 동일한 결과를 보장합니다.

---

## 7.2 WatermarkStrategy — Watermark 전략

`WatermarkStrategy`는 타임스탬프 추출과 Watermark 생성을 정의합니다.

```java
// 파일: flink-core/src/main/java/org/apache/flink/api/common/eventtime/WatermarkStrategy.java

@Public
public interface WatermarkStrategy<T> extends
        TimestampAssignerSupplier<T>,
        WatermarkGeneratorSupplier<T> {

    // ★ Watermark 생성기 제공
    @Override
    WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);

    // ★ 타임스탬프 추출기 제공
    @Override
    default TimestampAssigner<T> createTimestampAssigner(
            TimestampAssignerSupplier.Context context) {
        // 기본: 레코드에 이미 타임스탬프가 있다고 가정
        return new RecordTimestampAssigner<>();
    }
}
```

### 자주 사용되는 WatermarkStrategy

```java
// 1. 타임스탬프 순서가 보장되는 경우
WatermarkStrategy.forMonotonousTimestamps()

// 2. ★ 가장 일반적: 최대 N초의 지연 허용
WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, timestamp) -> event.getTimestamp())

// 3. Watermark 없음 (Processing Time 사용 시)
WatermarkStrategy.noWatermarks()
```

### 사용 예시

```java
DataStream<Event> stream = env
    .fromSource(source,
         WatermarkStrategy
             .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
             .withTimestampAssigner((event, timestamp) -> event.getEventTime()),
         "Event Source");

// 이 시점에서:
// - 각 레코드에 이벤트 타임스탬프가 할당됨
// - 주기적으로 Watermark가 생성됨 (현재까지 본 최대 타임스탬프 - 5초)
```

---

## 7.3 WatermarkGenerator — Watermark 생성

### BoundedOutOfOrdernessWatermarks (가장 널리 사용)

```java
// 파일: flink-core-api/src/main/java/org/apache/flink/api/common/eventtime/BoundedOutOfOrdernessWatermarks.java

@Public
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

    // 지금까지 관찰한 최대 타임스탬프
    private long maxTimestamp;

    // 허용하는 최대 지연 시간 (out-of-orderness)
    private final long outOfOrdernessMillis;

    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        // 초기값: Long.MIN_VALUE + outOfOrdernessMillis + 1
        // Watermark가 너무 일찍 방출되지 않도록
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    // ★ 각 레코드 처리 시 호출
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        // 최대 타임스탬프 갱신
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    // ★ 주기적으로 호출 (기본 200ms마다)
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // Watermark = 최대 타임스탬프 - 허용 지연 - 1
        // "-1"은 Watermark보다 같거나 작은 타임스탬프의 레코드는
        // 모두 도착했다는 의미이므로 경계값 처리
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }
}
```

**Watermark의 의미:**

```
Watermark(t) = "타임스탬프 ≤ t인 모든 이벤트가 도착했다"는 선언

예: outOfOrderness = 5초
    지금까지 본 최대 타임스탬프 = 10:00:30

    Watermark = 10:00:30 - 5초 = 10:00:25
    의미: "10:00:25 이전의 모든 이벤트가 도착했음"

    → 10:00:25 이전에 끝나는 윈도우는 이제 닫을 수 있음!
```

---

## 7.4 Watermark 전파 메커니즘

### StatusWatermarkValve — 다중 입력 Watermark 정렬

하나의 연산자가 여러 입력 채널에서 데이터를 받을 때, 각 채널의 Watermark를 정렬해야 합니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/runtime/watermarkstatus/StatusWatermarkValve.java

@Internal
public class StatusWatermarkValve {

    // 각 입력 채널의 현재 Watermark
    private final long[] channelStatuses;

    // 모든 활성 채널의 최소 Watermark
    private long lastOutputWatermark;

    // ★ 특정 채널에서 Watermark 수신
    public void inputWatermark(Watermark watermark, int channelIndex,
                                DataOutput<?> output) throws Exception {
        long watermarkMillis = watermark.getTimestamp();

        // 이 채널의 Watermark가 전진했는지 확인
        if (watermarkMillis > channelStatuses[channelIndex]) {
            channelStatuses[channelIndex] = watermarkMillis;

            // ★ 모든 채널의 최소값을 새 Watermark로
            long newMinWatermark = findMinWatermark();

            if (newMinWatermark > lastOutputWatermark) {
                // Watermark 전진! → 다운스트림으로 전파
                lastOutputWatermark = newMinWatermark;
                output.emitWatermark(new Watermark(newMinWatermark));
            }
        }
    }

    private long findMinWatermark() {
        long min = Long.MAX_VALUE;
        for (long channelStatus : channelStatuses) {
            min = Math.min(min, channelStatus);
        }
        return min;
    }
}
```

**Watermark 전파 규칙:**

```
입력 채널 0: ──W(3)──W(5)──W(7)──────→
입력 채널 1: ──W(2)──────W(4)──W(6)──→

연산자 출력 Watermark = min(채널0, 채널1):
         W(2)  W(3)  W(4)  W(5)  W(6)

★ 가장 느린 채널이 전체 Watermark를 결정합니다.
  이것은 "모든 입력에서 t 이전의 데이터가 도착했음"을 보장하기 위함입니다.
```

---

## 7.5 InternalTimerService — 타이머 서비스

Watermark는 **타이머(Timer)**와 밀접하게 연관됩니다.
윈도우는 타이머를 등록하고, Watermark가 타이머 시간을 넘어서면 윈도우가 트리거됩니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/api/operators/InternalTimerServiceImpl.java

public class InternalTimerServiceImpl<K, N> implements InternalTimerService<N> {

    // ★ Event Time 타이머 큐 — 우선순위 큐 (시간순)
    private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>>
            eventTimeTimersQueue;

    // ★ Processing Time 타이머 큐
    private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>>
            processingTimeTimersQueue;

    // 타이머 콜백 — 타이머 만료 시 호출
    private Triggerable<K, N> triggerTarget;

    // 현재 Watermark
    private long currentWatermark = Long.MIN_VALUE;
}
```

### registerEventTimeTimer() — Event Time 타이머 등록

```java
@Override
public void registerEventTimeTimer(N namespace, long time) {
    // ★ 타이머를 우선순위 큐에 추가
    eventTimeTimersQueue.add(
            new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
}
```

### advanceWatermark() — Watermark 전진 시 타이머 발화

```java
public void advanceWatermark(long time) throws Exception {
    currentWatermark = time;

    InternalTimer<K, N> timer;

    // ★★★ 핵심: Watermark보다 작거나 같은 모든 타이머를 발화!
    while ((timer = eventTimeTimersQueue.peek()) != null
            && timer.getTimestamp() <= time) {

        // 타이머 제거
        eventTimeTimersQueue.poll();

        // ★ 키 컨텍스트 설정 후 콜백 호출
        keyContext.setCurrentKey(timer.getKey());
        triggerTarget.onEventTime(timer);
        // → WindowOperator의 경우 윈도우 트리거!
    }
}
```

---

## 7.6 WindowOperator — 윈도우 연산의 내부

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperator.java

public class WindowOperator<K, IN, ACC, OUT, W extends Window>
        extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
        implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {

    // 윈도우 할당기 — 레코드가 어떤 윈도우에 속하는지 결정
    protected final WindowAssigner<? super IN, W> windowAssigner;

    // 트리거 — 윈도우를 언제 발화할지 결정
    protected final Trigger<? super IN, ? super W> trigger;

    // 윈도우 상태 — 각 윈도우에 축적된 데이터
    private transient InternalAppendingState<K, W, IN, ACC, ACC> windowState;

    // 타이머 서비스 — Watermark 기반 윈도우 트리거
    protected transient InternalTimerService<W> internalTimerService;
}
```

### processElement() — 레코드 처리

```java
@Override
public void processElement(StreamRecord<IN> element) throws Exception {
    // ① ★ 윈도우 할당: 이 레코드가 어떤 윈도우에 속하는지 결정
    final Collection<W> elementWindows =
            windowAssigner.assignWindows(
                    element.getValue(),
                    element.getTimestamp(),   // 이벤트 타임스탬프
                    windowAssignerContext);

    // ② 각 윈도우에 대해 처리
    for (W window : elementWindows) {
        // 늦은 데이터 체크 (Watermark보다 뒤처진 윈도우)
        if (isWindowLate(window)) {
            continue;  // 너무 늦은 데이터 → 버림 (또는 Side Output)
        }

        // ★ 윈도우 상태에 레코드 추가
        windowState.setCurrentNamespace(window);
        windowState.add(element.getValue());

        // ★ Event Time 타이머 등록 — 윈도우 종료 시점
        // 예: [10:00:00, 10:00:05) 윈도우 → 타이머 = 10:00:05 - 1
        if (windowAssigner.isEventTime()) {
            registerCleanupTimer(window);
            // → internalTimerService.registerEventTimeTimer(window, cleanupTime)
        }

        // ③ Trigger 평가 — 지금 윈도우를 발화할지 결정
        TriggerResult triggerResult = trigger.onElement(element.getValue(),
                element.getTimestamp(), window, triggerContext);

        if (triggerResult.isFire()) {
            // ★ 윈도우 발화! → 축적된 데이터로 결과 생성
            emitWindowContents(window, contents);
        }

        if (triggerResult.isPurge()) {
            // 윈도우 상태 제거
            windowState.clear();
        }
    }
}
```

### onEventTime() — Watermark에 의한 윈도우 트리거

```java
@Override
public void onEventTime(InternalTimer<K, W> timer) throws Exception {
    // ★ Watermark가 윈도우 종료 시점을 넘어섰을 때 호출됨

    W window = timer.getNamespace();
    triggerContext.window = window;

    // Trigger에게 이벤트 시간 타이머 만료 통지
    TriggerResult triggerResult = trigger.onEventTime(
            timer.getTimestamp(), window, triggerContext);

    if (triggerResult.isFire()) {
        // ★ 윈도우의 축적된 데이터로 결과 생성
        ACC contents = windowState.get();
        if (contents != null) {
            emitWindowContents(window, contents);
        }
    }

    if (triggerResult.isPurge()) {
        windowState.clear();
    }

    // 윈도우 정리 시간인 경우 상태 제거
    if (isCleanupTime(window, timer.getTimestamp())) {
        clearAllState(window, windowState, mergingWindows);
    }
}
```

### isWindowLate() — 늦은 데이터 판단

```java
protected boolean isWindowLate(W window) {
    // ★ 윈도우의 최대 타임스탬프가 현재 Watermark보다 작으면 "늦음"
    return (windowAssigner.isEventTime()
            && (cleanupTime(window) <= internalTimerService.currentWatermark()));
}

// cleanupTime: 윈도우 종료 시점 + allowedLateness
// allowedLateness를 설정하면 Watermark 이후에도 일정 시간 늦은 데이터 수용 가능
```

---

## 7.7 WindowAssigner — 윈도우 할당기

### TumblingEventTimeWindows (텀블링 윈도우)

```java
// 겹치지 않는 고정 크기 윈도우
// [00:00, 00:05), [00:05, 00:10), [00:10, 00:15), ...

@Override
public Collection<TimeWindow> assignWindows(Object element, long timestamp,
        WindowAssignerContext context) {
    long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
    // start = timestamp - (timestamp - offset + size) % size
    return Collections.singletonList(new TimeWindow(start, start + size));
}
```

### SlidingEventTimeWindows (슬라이딩 윈도우)

```java
// 겹치는 윈도우
// 예: 크기=10분, 슬라이드=5분
// [00:00, 00:10), [00:05, 00:15), [00:10, 00:20), ...
// 하나의 레코드가 여러 윈도우에 속할 수 있음

@Override
public Collection<TimeWindow> assignWindows(Object element, long timestamp,
        WindowAssignerContext context) {
    List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
    long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
    for (long start = lastStart; start > timestamp - size; start -= slide) {
        windows.add(new TimeWindow(start, start + size));
    }
    return windows;
}
```

### SessionWindows (세션 윈도우)

```java
// 동적 크기 윈도우 — 이벤트 간 간격(gap)으로 결정
// 일정 시간 동안 이벤트가 없으면 윈도우 종료
// 새 이벤트가 기존 윈도우의 gap 내에 있으면 윈도우 확장

// 세션 윈도우는 "병합(merge)" 가능:
// 두 세션 윈도우가 겹치면 하나로 합침
```

---

## 7.8 전체 Event Time 처리 흐름

```
① Source: 레코드에 타임스탬프 할당 + 주기적 Watermark 생성
    │
    ▼
② Watermark 전파: StatusWatermarkValve에서 min(채널별 Watermark)
    │
    ▼
③ WindowOperator.processElement():
    ├── 윈도우 할당 (WindowAssigner)
    ├── 윈도우 상태에 데이터 추가
    └── Event Time 타이머 등록 (윈도우 종료 시점)
    │
    ▼
④ Watermark 전진 → InternalTimerService.advanceWatermark()
    │
    ▼
⑤ 만료된 타이머 발화 → WindowOperator.onEventTime()
    │
    ▼
⑥ 윈도우 결과 방출 (emitWindowContents)
    │
    ▼
⑦ 다운스트림 연산자에 결과 전달
```

**타임라인 예시 (5초 텀블링 윈도우, 3초 out-of-orderness):**

```
시간  이벤트            최대 TS    Watermark    윈도우 동작
─────────────────────────────────────────────────────────
t=1   Event(ts=1)       1         -2           [0,5)에 추가
t=2   Event(ts=4)       4          1           [0,5)에 추가
t=3   Event(ts=2)       4          1           [0,5)에 추가 (늦었지만 OK)
t=4   Event(ts=7)       7          4           [5,10)에 추가
t=5   Event(ts=8)       8          5           [5,10)에 추가
                                    ↑
                        Watermark(5) >= 윈도우[0,5) 종료시점
                        → ★ [0,5) 윈도우 트리거! 결과 방출
t=6   Event(ts=3)       8          5           [0,5)에 추가 → 늦은 데이터!
                                                (allowedLateness 설정에 따라 처리 또는 버림)
```

---

## 7.9 설계 배경: Watermark 관련 FLIP

> **[FLIP-182: Watermark Alignment of FLIP-27 Sources](https://cwiki.apache.org/confluence/display/FLINK/FLIP-182:+Support+watermark+alignment+of+FLIP-27+Sources)**
> 여러 Source 연산자 간에 Watermark 방출 속도를 정렬합니다.
> 빠른 Source가 느린 Source보다 너무 앞서 나가지 않도록 합니다.
> 이를 통해 다운스트림 연산자의 과도한 버퍼링을 방지합니다.

> **[FLIP-217: Watermark Alignment of Source Splits](https://cwiki.apache.org/confluence/display/FLINK/FLIP-217:+Support+watermark+alignment+of+source+splits)**
> FLIP-182를 확장하여, 단일 Source 연산자 **내부**의 Split 간에도 Watermark를 정렬합니다.
> Source 커넥터가 `SourceReader#pauseOrResumeSplits`를 구현해야 합니다.

> **[FLIP-467: Generalized Watermarks](https://cwiki.apache.org/confluence/display/FLINK/FLIP-467:+Introduce+Generalized+Watermarks)** (Flink 2.x, DataStream V2 전용)
> 사용자 정의 Watermark 타입을 선언할 수 있게 합니다.
> Event Time Watermark는 일반화된 Watermark 프레임워크의 하나의 내장 인스턴스가 됩니다.
> 예: `INTERNAL_RUNTIME_BACKLOG`, `CONNECTOR_KAFKA_IDLE` 등의 커스텀 Watermark 가능.

---

## 7.10 Allowed Lateness와 Side Output

```java
// 늦은 데이터 처리 설정
stream
    .keyBy(event -> event.getKey())
    .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
    .allowedLateness(Duration.ofSeconds(10))   // 10초까지 늦은 데이터 허용
    .sideOutputLateData(lateOutputTag)          // 그 이후는 Side Output으로
    .reduce(new MyReduceFunction());

// Side Output에서 늦은 데이터 수집
DataStream<Event> lateEvents = result.getSideOutput(lateOutputTag);
```

```
Watermark 기준:
├── Watermark 이전 + allowedLateness 이내 → 윈도우에 추가 (재발화)
├── allowedLateness 이후 → Side Output으로 전달
└── Side Output도 없으면 → 버림
```

---

## 7.11 핵심 정리

1. **Event Time vs Processing Time**: Event Time은 결정론적, 재처리 시 동일 결과 보장
2. **WatermarkStrategy**: 타임스탬프 추출 + Watermark 생성 정의
3. **BoundedOutOfOrdernessWatermarks**: `Watermark = maxTimestamp - outOfOrderness - 1`
4. **StatusWatermarkValve**: 다중 입력 시 `min(채널별 Watermark)` = 출력 Watermark
5. **InternalTimerService**: Watermark 전진 시 만료된 Event Time 타이머 발화
6. **WindowOperator**: 레코드를 윈도우에 할당 → 타이머 등록 → Watermark에 의해 트리거
7. **WindowAssigner**: Tumbling(고정), Sliding(겹침), Session(동적) 윈도우 할당
8. **Allowed Lateness**: Watermark 이후에도 일정 시간 늦은 데이터 수용
9. **Side Output**: 너무 늦은 데이터를 별도 스트림으로 분리

---

## 전체 로드맵 완료!

Phase 1~7을 통해 Flink의 핵심 내부 구조를 코드 레벨에서 이해할 수 있는 기반을 마련했습니다.

| Phase | 주제 | 핵심 키워드 |
|-------|------|-------------|
| 1 | Job 구조 기초 | StreamExecutionEnvironment, Lazy Evaluation, execute() |
| 2 | Graph 변환 | StreamGraph → JobGraph → ExecutionGraph, Operator Chaining |
| 3 | 런타임 아키텍처 | Dispatcher, JobMaster, TaskExecutor, StreamTask, 메일박스 루프 |
| 4 | State & Checkpoint | CheckpointBarrier, Chandy-Lamport, State Backend, Exactly-once |
| 5 | Scheduling & Failover | PipelinedRegion, Slot Sharing, Region-based Failover |
| 6 | Network Shuffle | ResultPartition, InputGate, Credit-based Flow Control |
| 7 | Watermark & Time | WatermarkStrategy, StatusWatermarkValve, WindowOperator, Timer |
