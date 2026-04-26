# ResultPartition & InputGate — Producer/Consumer 측 자료구조

> **요약**: 한 ExecutionVertex의 출력 = `ResultPartition`(N subpartition) ↔ consumer의 입력 = `SingleInputGate`(N input channel). channel 종류로 local/remote 분기, buffer는 NetworkBufferPool에서 관리.
> **모듈**: `flink-runtime/runtime/io/network/`
> **선행**: [`./01-overview.md`](./01-overview.md)

---

## 1. ResultPartition (Producer 측)

`ResultPartition` 추상 — 구현체:
- `PipelinedResultPartition` — streaming 기본, consumer가 동시 read
- `BoundedBlockingResultPartition` — batch, producer 끝난 뒤 consumer
- `HybridShuffleResultPartition` — Adaptive Batch (FLIP-187)

각 ResultPartition은 N개 `ResultSubpartition`으로 나뉨 (N = consumer parallelism). `RecordWriter`가 record를 직렬화 → partitioner(`ForwardPartitioner`/`KeyGroupStreamPartitioner` 등)가 어느 subpartition으로 갈지 결정 → buffer에 append.

---

## 2. SingleInputGate (Consumer 측)

```java
SingleInputGate {
    InputChannel[] inputChannels;     // upstream subpartition마다 1 channel
    LocalBufferPool bufferPool;       // remote channel용 buffer pool
    BufferOrEvent pollNext();         // mailbox loop가 호출
}
```

InputChannel 종류:
- **`LocalInputChannel`** — producer가 같은 TM → 직접 메모리 참조 (zero-copy)
- **`RemoteInputChannel`** — producer가 다른 TM → Netty TCP로 buffer 수신
- **`RecoveredInputChannel`** — restore 시 임시 (channel state 재생용)

---

## 3. Buffer 흐름

```
[Producer subtask]
record → serializer → byte buffer
    ↓
ResultSubpartition.add(buffer)
    ↓ (네트워크면)
Netty server.send(buffer) → 원격 TM
    ↓
RemoteInputChannel.onBuffer
    ↓
SingleInputGate.notifyDataAvailable
    ↓
[Consumer subtask] mailbox loop가 pollNext → 처리
```

local channel은 같은 단계에서 메모리 직접 참조 (Netty 거치지 않음).

---

## 4. NetworkBufferPool

전 TM 공유 buffer pool. JVM 시작 시 `taskmanager.memory.network.fraction`(default 0.1)으로 전체 buffer 개수 결정. 각 LocalBufferPool이 NetworkBufferPool에서 빌려 사용. backpressure 시 buffer 부족 → request blocking.

본인 환경 권장:
```yaml
taskmanager.memory.network.fraction: 0.1     # default
taskmanager.network.numberOfBuffers: <auto>
taskmanager.memory.segment-size: 32kb
```

---

## 5. 다음

- Credit-based flow control: [`./03-credit-based-flow.md`](./03-credit-based-flow.md) (예정 — 본 문서가 마지막)
- 또는 [`../05-state-checkpoint/02-checkpoint-barrier.md`](../05-state-checkpoint/02-checkpoint-barrier.md) (barrier가 buffer로 어떻게 흐르는지)
