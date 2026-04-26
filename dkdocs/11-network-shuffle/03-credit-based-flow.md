# Credit-based Flow Control — Backpressure 메커니즘

> **요약**: producer가 buffer를 무작정 보내지 않고 consumer가 announce한 가용 buffer 수(=credit)만큼만 send → 네트워크 버퍼 overflow 방지 + backpressure를 channel 단위로 자연스럽게 표현. Flink 1.5+의 표준 모델.
> **모듈**: `flink-runtime/runtime/io/network/`
> **선행**: [`./01-overview.md`](./01-overview.md), [`./02-result-partition-input-gate.md`](./02-result-partition-input-gate.md)

---

## 1. TL;DR

각 `RemoteInputChannel`이 자기 buffer pool에서 빌릴 수 있는 buffer 수를 producer에게 **credit**으로 통보 → producer는 그 credit만큼만 buffer를 send → consume 후 credit을 다시 보충. 한 channel이 backpressure 발생하면 credit이 0이 되어 producer가 그 channel로 send 멈춤 → **다른 channel은 영향 안 받음** (channel-level isolation). 이전 모델(TCP-only flow control)은 head-of-line blocking이 있었음.

---

## 2. 동작 (개념도)

```
[Producer]                                   [Consumer]
ResultSubpartition #0                         RemoteInputChannel #0
  ↓ has 100 buffers ready                       buffer pool: 5 free
  
                ← credit announcement (5)
  send up to 5 buffers
  ↓ Netty TCP
                buffer received → enqueued in input gate
                                  consumer reads → buffer freed
                ← additional credit (3)
  send 3 more
  ...
```

핵심:
- **credit = 가용 buffer 수** (per-channel, per-direction)
- producer는 **credit > 0 일 때만 send**
- credit 0 → 그 channel로의 send 멈춤 → backpressure
- 다른 channel은 자기 credit이 있으면 계속 send (격리)

---

## 3. 핵심 클래스

| 역할 | 클래스 | 위치 |
|------|------|------|
| Credit 주는 측 (consumer) | `RemoteInputChannel` | `flink-runtime/.../io/network/partition/consumer/RemoteInputChannel.java` |
| Credit 받는 측 (producer Netty handler) | `CreditBasedSequenceNumberingViewReader` | `flink-runtime/.../io/network/netty/` |
| Buffer 통계 / backpressure 메트릭 | `BufferDebloater`, `LocalBufferPool` | 같은 위치 |

---

## 4. 본인 환경 backpressure 디버깅

REST `/jobs/<id>/vertices/<vid>/backpressure`:
```json
{
  "status": "ok",
  "backpressure-level": "ok",   // ok / low / high
  "subtasks": [
    {"subtask": 0, "ratio": 0.0, "backPressureLevel": "ok"},
    {"subtask": 1, "ratio": 0.85, "backPressureLevel": "high"}  // ← 이 subtask가 문제
  ]
}
```

`ratio` = 한 subtask가 buffer request에서 block된 시간 비율. **0.5 이상 = backpressure 의심**.

원인 추적 순서:
1. 어느 vertex가 backpressure인지 식별 (보통 downstream sink)
2. 그 vertex의 metric으로 throughput / latency 확인
3. external system (MinIO, Polaris, Iceberg) 측 latency 확인
4. unaligned checkpoint 활성화로 checkpoint는 살아있게 ([`../05-state-checkpoint/02-checkpoint-barrier.md`](../05-state-checkpoint/02-checkpoint-barrier.md))

---

## 5. FLIP

- [FLIP-1: Credit-based flow control](https://cwiki.apache.org/confluence/display/FLINK/FLIP-1+%3A+Credit-based+Flow+Control) — 도입
- [FLIP-183: Dynamic buffer size adjustment](https://cwiki.apache.org/confluence/display/FLINK/FLIP-183%3A+Dynamic+buffer+size+adjustment) — buffer size 자동 조정 (BufferDebloater)
