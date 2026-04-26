# Network Shuffle — Netty 기반 데이터 채널

> **요약**: TM 간 record가 어떻게 전송되는지 — Netty 기반, NetworkBuffer + LocalBufferPool, credit-based flow control. 본인 환경에서 backpressure 디버깅 시 핵심.
> **모듈**: `flink-runtime/runtime/io/network/`

---

## 1. TL;DR

각 ExecutionVertex의 출력은 `ResultPartition`(N subpartition) → 같은 TM 내 consumer면 `LocalInputChannel`(zero-copy), 원격 TM이면 `RemoteInputChannel`(Netty TCP). 모든 buffer는 `LocalBufferPool`에서 빌려 사용 — pool이 가득 차면 backpressure 발생. **credit-based flow control**: consumer가 가용 buffer 수(=credit)를 producer에게 주기적 통보 → producer는 credit만큼만 send (overflow 방지).

---

## 2. 핵심 클래스 (overview)

| 역할 | 클래스 | 위치 |
|------|------|------|
| Producer 측 partition | `ResultPartition` (Abstract), `PipelinedResultPartition`, `BoundedBlockingResultPartition` | `flink-runtime/runtime/io/network/partition/` |
| Subpartition | `ResultSubpartition` | 같은 패키지 |
| Consumer 측 input gate | `SingleInputGate` | `flink-runtime/runtime/io/network/partition/consumer/` |
| Input channel | `LocalInputChannel`, `RemoteInputChannel`, `RecoveredInputChannel` | 같은 패키지 |
| Buffer | `Buffer`, `NetworkBuffer` | `flink-runtime/runtime/io/network/buffer/` |
| Buffer pool | `LocalBufferPool`, `NetworkBufferPool` | 같은 패키지 |
| Netty server/client | `NettyServer`, `NettyClient` | `flink-runtime/runtime/io/network/netty/` |
| Shuffle service 추상 | `ShuffleEnvironment` (Interface), `NettyShuffleEnvironment` | `flink-runtime/runtime/shuffle/` |

---

## 3. 데이터 흐름

```
[Producer Task]
  StreamRecord → RecordWriter → serializer → Buffer (LocalBufferPool에서 borrow)
  ↓
  Subpartition 1 / 2 / ... / N (consumer subtask 수만큼)
  
  [Local consumer (같은 TM)]
  LocalInputChannel.requestSubpartition → Buffer pass (zero-copy)
  
  [Remote consumer (다른 TM)]
  Netty 서버가 partition request 수신
  ↓
  Buffer → Netty channel → TCP (credit control)
  ↓
  Remote TM: Netty client → InputChannel → CheckpointedInputGate → record
```

---

## 4. Credit-based Flow Control

매 InputChannel이 자기 가용 buffer 수를 producer에게 통보:

```
Consumer: "내 input channel 1번에 buffer 5개 여유 있음" (credit=5)
   ↓ 비동기 메시지
Producer: 그 channel로 최대 5 buffer 전송
   ↓ buffer 받을 때마다 credit 감소
   ↓ consumer가 buffer 처리 후 다시 credit 통보
```

장점:
- buffer overflow 방지 → backpressure 자연스럽게 전파
- per-channel granularity → 한 채널이 느려도 다른 채널은 영향 없음

---

## 5. Backpressure 발생 시 일어나는 일

```
[느린 Consumer]
  buffer 처리 못 함 → LocalBufferPool 가득
  ↓
  credit 0 통보 → producer가 send 멈춤
  ↓
[Producer]
  RecordWriter가 buffer 못 빌림 → write block
  ↓
  StreamTask mailbox의 default action(record processing) block
  ↓
[메트릭]
  busyTimeMsPerSecond 증가
  backPressureTimeMsPerSecond 증가
  
  → 이게 Operator autoscaler가 감지하는 신호
```

---

## 6. 본인 환경 튜닝

```yaml
# Buffer 수
taskmanager.network.numberOfBuffers: 32768   # default 8192
taskmanager.network.memory.fraction: 0.1     # 또는 size 명시
taskmanager.memory.network.min: 256mb
taskmanager.memory.network.max: 1gb

# Buffer debloat (FLIP-183, alignment latency 단축)
taskmanager.network.memory.buffer-debloat.enabled: true
taskmanager.network.memory.buffer-debloat.target: 1s
```

주의: buffer 너무 많으면 unaligned checkpoint 시 channel state 크기 증가.

---

## 7. 11 카테고리 (이 문서로 충분)

network shuffle은 매우 깊은 영역이지만 본인 환경에서 직접 만질 일은 적음 — overview만으로 충분.

자세한 구현은 추후 필요 시 별도 문서. 운영 중 backpressure 디버깅엔 위 메트릭으로 충분.

---

## 8. 다음

- [`../12-watermark-time/`](../12-watermark-time/) — EventTime, Window
- [`../13-new-datastream-v2/`](../13-new-datastream-v2/) — FLIP-409
