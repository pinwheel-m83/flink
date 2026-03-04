# Phase 6: Network Shuffle & 데이터 교환

> Task 간 데이터가 네트워크를 통해 어떻게 전달되는지를 코드 레벨에서 추적합니다.
> Netty 기반 네트워크 스택, 버퍼 관리, Credit-based 흐름 제어(백프레셔)를 다룹니다.

---

## 6.1 네트워크 스택 아키텍처 개요

```
Producer Task                              Consumer Task
┌──────────────────────┐                  ┌──────────────────────┐
│ StreamOperator       │                  │ StreamOperator       │
│     │                │                  │     ▲                │
│     ▼                │                  │     │                │
│ RecordWriter         │                  │ StreamInputProcessor │
│     │                │                  │     ▲                │
│     ▼                │                  │     │                │
│ ResultPartition      │   Netty 통신     │ InputGate            │
│  ├── SubPartition[0] │ ═══════════════> │  ├── InputChannel[0] │
│  ├── SubPartition[1] │                  │  ├── InputChannel[1] │
│  └── SubPartition[2] │                  │  └── InputChannel[2] │
└──────────────────────┘                  └──────────────────────┘
```

---

## 6.2 RecordWriter — 레코드 출력

`RecordWriter`는 연산자가 출력하는 레코드를 직렬화하고, 적절한 SubPartition으로 분배합니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/api/writer/RecordWriter.java

public abstract class RecordWriter<T extends IOReadableWritable> implements AvailabilityProvider {

    protected final ResultPartitionWriter targetPartition;  // 출력 파티션
    protected final int numberOfSubpartitions;              // 서브파티션 수
    protected final DataOutputSerializer serializer;        // 직렬화기

    // ★ 각 서브파티션별 버퍼 빌더
    // 레코드는 먼저 버퍼에 축적되고, 버퍼가 가득 차면 네트워크로 전송
    protected final BufferBuilder[] bufferBuilders;
}
```

### emit() — 레코드 전송

```java
// ChannelSelectorRecordWriter.java (RecordWriter의 구현)
@Override
public void emit(T record) throws IOException {
    // ★ ChannelSelector가 레코드의 목적지 서브파티션 결정
    // HashPartitioner의 경우: key.hashCode() % numberOfSubpartitions
    emit(record, channelSelector.selectChannel(record));
}

// RecordWriter.java (기반 클래스)
public void emit(T record, int targetSubpartition) throws IOException {
    checkErroneous();

    // ① 레코드를 직렬화하여 ByteBuffer로 변환
    // ② ResultPartition에 직접 전달
    targetPartition.emitRecord(serializeRecord(serializer, record), targetSubpartition);

    // flushAlways가 설정된 경우 즉시 플러시
    if (flushAlways) {
        targetPartition.flush(targetSubpartition);
    }
}
```

### ChannelSelector — 파티셔닝 전략

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/api/writer/ChannelSelector.java

public interface ChannelSelector<T extends IOReadableWritable> {
    // 레코드를 어떤 서브파티션(채널)으로 보낼지 결정
    int selectChannel(T record);
}

// 구현체 예시: KeyGroupStreamPartitioner (keyBy() 사용 시)
// key의 해시값을 기반으로 서브파티션 결정
// key → murmurHash(key) → keyGroup → subpartition
```

---

## 6.3 ResultPartition — 출력 데이터 파티션

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/ResultPartition.java

public abstract class ResultPartition implements ResultPartitionWriter {

    protected final ResultPartitionID partitionId;         // 파티션 고유 ID
    protected final ResultPartitionType partitionType;     // 파티션 유형
    protected final ResultSubpartition[] subpartitions;    // 서브파티션 배열
    protected final BufferPool bufferPool;                 // 버퍼 풀
}
```

### ResultPartitionType — 파티션 유형

```java
public enum ResultPartitionType {

    // ★ 파이프라인 파티션 — 스트리밍 처리의 기본
    // 데이터가 생성되는 즉시 소비자에게 전달 (bounded 아님)
    PIPELINED,

    // 파이프라인 + bounded (유한 데이터)
    PIPELINED_BOUNDED,

    // ★ 블로킹 파티션 — 배치 처리
    // 모든 데이터가 생성 완료된 후에야 소비 가능
    BLOCKING,

    // 하이브리드 — 스트리밍과 배치의 중간
    HYBRID_FULL,
    HYBRID_SELECTIVE;

    // 파이프라인 여부
    public boolean isPipelinedOrPipelinedBoundedResultPartition() {
        return this == PIPELINED || this == PIPELINED_BOUNDED;
    }

    // 블로킹 여부
    public boolean isBlockingOrBlockingPersistentResultPartition() {
        return this == BLOCKING;
    }
}
```

---

## 6.4 InputGate — 입력 데이터 게이트

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/SingleInputGate.java

public class SingleInputGate extends IndexedInputGate {

    // 입력 채널 배열 — 각 채널이 하나의 업스트림 서브파티션에 연결
    private final Map<IntermediateResultPartitionID, InputChannel> inputChannels;

    // 데이터가 도착한 채널들의 큐
    private final PrioritizedDeque<InputChannel> inputChannelsWithData;

    // 버퍼 풀
    private BufferPool bufferPool;
}
```

### getNext() — 다음 버퍼 읽기

```java
@Override
public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
    return getNextBufferOrEvent(true);  // blocking
}

private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking)
        throws IOException, InterruptedException {

    // ★ 데이터가 도착한 채널에서 버퍼 읽기
    while (true) {
        // ① 데이터가 있는 채널 선택
        Optional<InputWithData<InputChannel, BufferAndAvailability>> next =
                waitAndGetNextData(blocking);

        if (!next.isPresent()) {
            return Optional.empty();
        }

        InputChannel inputChannel = next.get().input;
        BufferAndAvailability bufferAndAvailability = next.get().data;

        // ② 버퍼인지 이벤트인지 구분
        Buffer buffer = bufferAndAvailability.buffer();

        if (buffer.isBuffer()) {
            // ★ 일반 데이터 버퍼 → 역직렬화 후 연산자에 전달
            return Optional.of(new BufferOrEvent(buffer, inputChannel.getChannelInfo()));
        } else {
            // ★ 이벤트 (CheckpointBarrier, EndOfPartition 등)
            AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
            return Optional.of(new BufferOrEvent(event, inputChannel.getChannelInfo()));
        }
    }
}
```

### InputChannel 종류

```java
// LocalInputChannel — 같은 TaskManager 내 통신
// 네트워크를 거치지 않고 메모리 직접 참조
public class LocalInputChannel extends InputChannel {
    // ResultSubpartition을 직접 참조
    private ResultSubpartitionView subpartitionView;
}

// RemoteInputChannel — 다른 TaskManager와 네트워크 통신
// Netty를 통한 TCP 통신
public class RemoteInputChannel extends InputChannel {
    private final ConnectionID connectionId;    // 원격 TaskManager 주소
    private final PartitionRequestClient partitionRequestClient;  // Netty 클라이언트
}
```

---

## 6.5 Buffer 관리

### NetworkBuffer — 네트워크 버퍼

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/NetworkBuffer.java

public class NetworkBuffer extends AbstractReferenceCountedByteBuf implements Buffer {

    // ★ 고정 크기의 메모리 세그먼트
    private final MemorySegment memorySegment;

    // 버퍼 타입 (데이터 vs 이벤트)
    private DataType dataType;

    // 재활용을 위한 콜백
    private BufferRecycler recycler;

    // ★ 참조 카운트 기반 관리
    // 참조가 0이 되면 BufferPool에 반환되어 재사용됨
}
```

### BufferPool — 버퍼 풀

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/buffer/LocalBufferPool.java

public class LocalBufferPool implements BufferPool {

    // 이 풀에 할당된 최소/최대 버퍼 수
    private final int minNumberOfMemorySegments;
    private int maxNumberOfMemorySegments;

    // 사용 가능한 버퍼 큐
    private final ArrayDeque<MemorySegment> availableMemorySegments;

    // ★ 버퍼 요청
    @Override
    public Buffer requestBuffer() throws IOException {
        // 가용 버퍼가 있으면 즉시 반환
        // 없으면 null 반환 (non-blocking)
        // 또는 requestBufferBlocking()으로 대기 가능
    }

    // ★ 버퍼 반환 (재활용)
    @Override
    public void recycle(MemorySegment segment) {
        // 반환된 버퍼를 다시 가용 큐에 추가
        availableMemorySegments.add(segment);
        // 대기 중인 요청이 있으면 깨움
    }
}
```

**메모리 관리 계층:**

```
NetworkBufferPool (전역)
    │ 전체 네트워크 메모리를 MemorySegment로 관리
    │ TaskManager 시작 시 고정 크기 할당
    │
    ├── LocalBufferPool (Task A의 출력용)
    │     └── MemorySegment × N개
    │
    ├── LocalBufferPool (Task A의 입력용)
    │     └── MemorySegment × M개
    │
    ├── LocalBufferPool (Task B의 출력용)
    │     └── MemorySegment × N개
    │
    └── ...

기본 MemorySegment 크기: 32KB
```

---

## 6.6 Netty 네트워크 통신

### NettyConnectionManager — Netty 연결 관리

```java
// Flink의 네트워크 통신은 Netty 기반입니다.
// 각 TaskManager 간에 하나의 TCP 연결을 공유합니다 (multiplexing).

// Producer 측 (서버):
// PartitionRequestServerHandler가 소비자의 파티션 요청을 처리

// Consumer 측 (클라이언트):
// CreditBasedPartitionRequestClientHandler가 응답을 처리
```

### CreditBasedPartitionRequestClientHandler — 클라이언트 핸들러

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/netty/CreditBasedPartitionRequestClientHandler.java

class CreditBasedPartitionRequestClientHandler
        extends ChannelInboundHandlerAdapter implements NetworkClientHandler {

    // 채널별 핸들러 매핑
    private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            decodeMsg(msg);
        } catch (Throwable t) {
            notifyAllChannelsOfErrorAndClose(t);
        }
    }

    private void decodeMsg(Object msg) throws Throwable {
        if (msg instanceof NettyMessage.BufferResponse) {
            // ★ 데이터 버퍼 수신
            NettyMessage.BufferResponse bufferResponse = (NettyMessage.BufferResponse) msg;
            RemoteInputChannel inputChannel =
                    inputChannels.get(bufferResponse.receiverId);

            // 수신한 버퍼를 InputChannel에 전달
            inputChannel.onBuffer(bufferResponse.getBuffer(), bufferResponse.sequenceNumber);

        } else if (msg instanceof NettyMessage.ErrorResponse) {
            // 에러 처리
        }
    }
}
```

---

## 6.7 Credit-based Flow Control (백프레셔)

Flink는 **Credit-based 흐름 제어**를 사용하여 백프레셔를 구현합니다.
이는 TCP 레벨의 흐름 제어보다 훨씬 세밀한 제어를 가능하게 합니다.

### 동작 원리

```
Producer                                    Consumer
┌──────────────────┐                       ┌──────────────────┐
│ ResultSubpartition│                       │ RemoteInputChannel│
│                  │                       │                  │
│ ┌──────────────┐ │   ① Credit 전송       │ ┌──────────────┐ │
│ │ 대기 버퍼 큐  │ │ <─────────────────── │ │ 가용 버퍼 수  │ │
│ │ [buf][buf]   │ │                       │ │ Credit = 3   │ │
│ └──────────────┘ │                       │ └──────────────┘ │
│                  │   ② Credit만큼 전송    │                  │
│                  │ ──────────────────→   │                  │
│                  │   [buf][buf][buf]     │                  │
│                  │                       │                  │
│ 남은 Credit = 0  │                       │ 수신 후 처리      │
│ → 전송 중단!    │                       │ → 버퍼 반환       │
│ (백프레셔)      │                       │ → Credit 재전송   │
└──────────────────┘                       └──────────────────┘
```

### CreditBasedSequenceNumberingViewReader — Producer 측 Credit 관리

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/netty/CreditBasedSequenceNumberingViewReader.java

class CreditBasedSequenceNumberingViewReader
        implements BufferAvailabilityListener, NetworkSequenceViewReader {

    private final InputChannelID receiverId;
    private final PartitionRequestQueue requestQueue;
    private final int initialCredit;

    // ★ 소비자로부터 받은 Credit (= 소비자가 수신 가능한 버퍼 수)
    private int numCreditsAvailable;

    // Credit 추가 (소비자가 버퍼를 처리하고 Credit을 보내올 때)
    void addCredit(int creditDeltas) {
        numCreditsAvailable += creditDeltas;
    }

    // 전송 가능 여부 — Credit이 있어야만 데이터 전송 가능
    boolean isAvailable() {
        return numCreditsAvailable > 0 && subpartitionView.isAvailable();
    }

    // 데이터 전송
    BufferAndAvailability getNextBuffer() throws IOException {
        Buffer buffer = subpartitionView.getNextBuffer();
        if (buffer != null && buffer.isBuffer()) {
            // ★ 버퍼 전송 시 Credit 차감
            numCreditsAvailable--;
        }
        return new BufferAndAvailability(buffer, isAvailable(), buffersInBacklog);
    }
}
```

### RemoteInputChannel — Consumer 측 Credit 관리

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RemoteInputChannel.java

public class RemoteInputChannel extends InputChannel {

    // ★ 가용 Credit (= 버퍼 풀에서 사용 가능한 빈 버퍼 수)
    private final AtomicInteger unannouncedCredit = new AtomicInteger(0);

    // 버퍼 수신 시 (4개 파라미터)
    public void onBuffer(Buffer buffer, int sequenceNumber, int backlog, int subpartitionId)
            throws IOException {
        // ① 수신한 버퍼를 큐에 추가
        receivedBuffers.add(buffer);

        // ② 데이터 도착 알림 → InputGate에 통지
        notifyChannelNonEmpty();
    }

    // ★ 버퍼 처리 후 Credit 반환
    // 처리가 끝난 버퍼가 BufferPool에 반환되면
    // 새 Credit이 생기고, 이를 Producer에게 알림
    public void notifyCreditAvailable() {
        // Producer에게 Credit 추가 메시지 전송
        partitionRequestClient.notifyCreditAvailable(this);
    }
}
```

### 백프레셔의 전파

```
Sink가 느림 → Sink의 입력 버퍼 소진
    │
    ▼
Sink의 InputChannel: Credit = 0 → Producer에게 Credit 없음 알림
    │
    ▼
Reduce의 ResultSubpartition: Credit = 0 → 전송 중단
    │
    ▼
Reduce의 출력 버퍼가 가득 참 → Reduce 처리 중단
    │
    ▼
Reduce의 InputChannel: Credit = 0 → Map에 전파
    │
    ▼
Map 처리 중단 → Source까지 역전파
    │
    ▼
★ Source 읽기 속도 저하 → 전체 파이프라인 속도 조절

Credit-based 흐름 제어 덕분에:
- TCP 레벨 백프레셔보다 빠르게 반응
- 채널별 독립적 제어 가능 (하나의 느린 채널이 다른 채널을 블록하지 않음)
- 데드락 방지
```

---

## 6.8 설계 배경: Credit-based 흐름 제어 (FLINK-7282)

> **[FLINK-7282: Network Credit-based Flow Control](https://issues.apache.org/jira/browse/FLINK-7282)**
> 참고: [A Deep-Dive into Flink's Network Stack](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/)

Flink 1.5 이전에는 TCP 레벨의 흐름 제어에 의존했습니다.
이 방식의 문제점:

```
TCP 기반 (Flink 1.4 이전):
┌──────────────────┐     하나의 TCP 연결     ┌──────────────────┐
│ TaskManager A     │ ══════════════════════> │ TaskManager B     │
│ Channel 1 (빠름) │                          │ Channel 1         │
│ Channel 2 (빠름) │                          │ Channel 2 (느림!) │
│ Channel 3 (빠름) │                          │ Channel 3         │
└──────────────────┘                          └──────────────────┘

문제: Channel 2가 느리면 TCP 윈도우가 줄어들어
      Channel 1, 3도 함께 느려짐 (Head-of-Line Blocking)

Credit 기반 (Flink 1.5+):
- 각 Channel이 독립적인 Credit을 관리
- Channel 2가 느려도 Channel 1, 3은 영향 없음
- 체크포인트 배리어도 데이터와 독립적으로 전달 가능
```

---

## 6.9 데이터 직렬화/역직렬화

### StreamRecord의 네트워크 전송

```java
// 연산자 출력
// StreamOperator.processElement(StreamRecord) →
//   Output.collect(StreamRecord) →
//     RecordWriterOutput.collect() →
//       RecordWriter.emit()

// ★ 직렬화 과정:
// 1. StreamRecord → TypeSerializer.serialize() → byte[]
// 2. byte[] → NetworkBuffer에 기록
// 3. NetworkBuffer 가득 참 → ResultSubpartition에 추가
// 4. Credit 있으면 → Netty를 통해 네트워크 전송

// ★ 역직렬화 과정:
// 1. 네트워크 수신 → RemoteInputChannel의 버퍼 큐
// 2. InputGate.getNext() → BufferOrEvent 반환
// 3. StreamTaskNetworkInput에서 TypeSerializer.deserialize()
// 4. StreamRecord로 복원 → 연산자에 전달
```

---

## 6.10 핵심 정리

1. **RecordWriter**: 레코드를 직렬화하고 ChannelSelector로 목적지 서브파티션 결정
2. **ResultPartition**: Task의 출력을 서브파티션으로 관리. PIPELINED(스트리밍) vs BLOCKING(배치)
3. **InputGate**: 여러 InputChannel에서 데이터 수신. Local(같은 JVM) vs Remote(Netty)
4. **NetworkBuffer**: 32KB 고정 크기 메모리 세그먼트. 참조 카운트로 재활용
5. **BufferPool**: 계층적 버퍼 관리. NetworkBufferPool(전역) → LocalBufferPool(Task별)
6. **Credit-based Flow Control**: 소비자가 Credit(가용 버퍼 수)을 생산자에게 알림. 세밀한 백프레셔
7. **Netty Multiplexing**: TaskManager 간 하나의 TCP 연결로 여러 채널 다중화

---

## 다음 단계

Phase 7에서는 **Watermark와 시간(Time)**이 어떻게 관리되고 전파되는지,
그리고 Window 연산이 내부적으로 어떻게 동작하는지를 코드 레벨에서 추적합니다.
