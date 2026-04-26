# Event Time & Watermark

> **요약**: streaming 잡의 시간 의미론 — processing time(현재 시각) vs event time(record의 timestamp). watermark가 "이 시각 이전의 모든 event는 도착했다"는 약속을 stream에 inject. window/timer 처리의 토대.

---

## 1. TL;DR

`WatermarkStrategy<T>`로 source가 watermark 발급 정책 결정 — `forBoundedOutOfOrderness(Duration.ofSeconds(20))`이 가장 일반적 (out-of-order tolerance). 매 record processing 후 strategy가 새 watermark를 발급할 수 있고, periodic emit이면 일정 주기로 자동 emit. Watermark는 RuntimeEvent로 record 사이에 흘러 downstream operator가 window/timer trigger 결정에 사용.

---

## 2. 핵심 인터페이스

| 역할 | 클래스 | 위치 |
|------|------|------|
| 정책 추상 | `WatermarkStrategy<T>` (Interface) | `flink-core/.../api/common/eventtime/WatermarkStrategy.java` |
| Watermark 발급 | `WatermarkGenerator<T>` (Interface) | 같은 패키지 |
| Timestamp 추출 | `TimestampAssigner<T>` (Interface) | 같은 패키지 |
| Window | `WindowAssigner`, `Trigger`, `Evictor` | `flink-runtime/streaming/api/windowing/` |
| Timer service | `InternalTimerService`, `InternalTimer` | `flink-runtime/streaming/api/operators/` |

---

## 3. 사용자 코드

```java
WatermarkStrategy<Event> strategy = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(20))
    .withTimestampAssigner((event, ts) -> event.getEventTime())
    .withIdleness(Duration.ofMinutes(1));   // source idle 처리

DataStream<Event> events = env.fromSource(kafkaSource, strategy, "events");

events
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
    .aggregate(new MyAggregator())
    .sinkTo(icebergSink);
```

---

## 4. Watermark 흐름

```
[Kafka Source]
  record 받음 → eventTime 추출 → 내부 max event time 갱신
  ↓ periodic (default 200ms 마다)
  WatermarkGenerator.onPeriodicEmit → Watermark(maxEventTime - 20s) emit
  ↓ output stream에 inject (record와 같은 channel)
[Downstream Window operator]
  Watermark(t) 받음 → endTime ≤ t 인 모든 window의 trigger 호출 → output emit
  ↓
  Watermark forward
```

핵심:
- Watermark는 단조 증가 — backwards 안 감
- Multi-input operator는 모든 input의 min watermark 채택 (boundary)
- Idle source는 watermark progress 멈춤 → `withIdleness`로 mark idle해 무시

---

## 5. 본인 환경 (Kafka + Iceberg)

```
[Kafka Source]
  매 record processing → max event time 갱신
  KafkaSourceReader가 partition별 watermark 합산 (min)
  ↓ Watermark emit
[keyBy → Window]
  TumblingEventTimeWindows(5min) → 매 5분 boundary마다 trigger
  ↓ aggregated result
[Iceberg Sink]
  한 5분-window의 결과를 한 batch로 sink (또는 streaming append)
```

Out-of-order tolerance(20s)가 짧으면 late event drop, 길면 latency 증가. 본인 데이터 특성에 맞춰 튜닝.

---

## 6. 12 카테고리 (이 문서로 충분)

watermark/window는 깊은 영역이지만 본인 환경의 표준 사용 패턴은 위로 충분. window 종류별 trigger 정책은 사용자 코드 레벨이라 internals 영역 밖.

---

## 7. 다음

- [`../13-new-datastream-v2/`](../13-new-datastream-v2/) — FLIP-409 신규 DataStream API
