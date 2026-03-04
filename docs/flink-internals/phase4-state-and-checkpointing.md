# Phase 4: State & Checkpointing — Flink의 차별화 포인트

> Flink의 핵심 경쟁력인 **Exactly-once** 처리 보장의 메커니즘을 코드 레벨에서 추적합니다.
> Chandy-Lamport 분산 스냅샷 알고리즘의 실제 구현을 이해합니다.

---

## 4.1 Checkpointing 개요

체크포인트는 **분산 스트림 처리에서 일관된 상태 스냅샷**을 만드는 메커니즘입니다.

```
                    CheckpointCoordinator (JobMaster)
                           │
                    ① triggerCheckpoint()
                           │
                           ▼
            ┌──────────── Barrier ────────────┐
            │              │                   │
            ▼              ▼                   ▼
     ┌──────────┐   ┌──────────┐       ┌──────────┐
     │ Source[0] │   │ Source[1] │       │ Source[2] │
     │ snapshot  │   │ snapshot  │       │ snapshot  │
     └────┬─────┘   └────┬─────┘       └────┬─────┘
          │ barrier       │ barrier          │ barrier
          ▼               ▼                  ▼
     ┌──────────┐   ┌──────────┐       ┌──────────┐
     │  Map[0]  │   │  Map[1]  │       │  Map[2]  │
     │ snapshot  │   │ snapshot  │       │ snapshot  │
     └────┬─────┘   └────┬─────┘       └────┬─────┘
          │               │                  │
          ▼               ▼                  ▼
     ② acknowledgeCheckpoint() → CheckpointCoordinator
     ③ completePendingCheckpoint() → 영속화
```

---

## 4.2 CheckpointCoordinator — 체크포인트 오케스트레이터

`CheckpointCoordinator`는 JobMaster에서 실행되며, 체크포인트의 전체 라이프사이클을 관리합니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/CheckpointCoordinator.java

public class CheckpointCoordinator {

    // ★ Flink 2.x에서는 체크포인트 대상을 CheckpointPlanCalculator로 관리합니다.
    //    이전 버전의 tasksToTrigger/tasksToWaitFor/tasksToCommitTo 필드는
    //    CheckpointPlan 추상화로 대체되었습니다.
    private final CheckpointPlanCalculator checkpointPlanCalculator;

    // 진행 중인 체크포인트
    private final Map<Long, PendingCheckpoint> pendingCheckpoints;

    // 완료된 체크포인트 이력
    private final CompletedCheckpointStore completedCheckpointStore;

    // 체크포인트 ID 카운터
    private final CheckpointIDCounter checkpointIdCounter;

    // 주기적 체크포인트 스케줄러
    private ScheduledFuture<?> currentPeriodicTrigger;

    // 체크포인트 설정
    private final long checkpointTimeout;    // 타임아웃 (밀리초)
    private final long minPauseBetweenCheckpoints;  // 최소 간격
}

// CheckpointPlan이 제공하는 메서드:
// - getTasksToTrigger()   → 체크포인트 시작 대상 (Source)
// - getTasksToWaitFor()   → ACK를 기다릴 대상 (모든 Task)
// - getTasksToCommitTo()  → 완료 통지 대상
```

### triggerCheckpoint() — 체크포인트 트리거

```java
public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(boolean isPeriodic) {
    return triggerCheckpointFromCheckpointThread(checkpointProperties, null, isPeriodic);
}

// 타입별 트리거
public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {
    final SnapshotType snapshotType;
    switch (checkpointType) {
        case CONFIGURED:
            snapshotType = checkpointProperties.getCheckpointType();
            break;
        case FULL:
            snapshotType = FULL_CHECKPOINT;    // 전체 스냅샷 (증분 무시)
            break;
        case INCREMENTAL:
            snapshotType = CHECKPOINT;         // 증분 체크포인트
            break;
        default:
            throw new IllegalArgumentException("unknown checkpointType: " + checkpointType);
    }

    final CheckpointProperties properties = new CheckpointProperties(/* ... */);
    return triggerCheckpointFromCheckpointThread(properties, null, false);
}
```

### startTriggeringCheckpoint() — 실제 트리거 로직

```java
private void startTriggeringCheckpoint(CheckpointTriggerRequest request) {
    // ① 사전 조건 검사
    //    - 동시 체크포인트 수 제한
    //    - 최소 간격 확인
    //    - Job 상태 확인

    // ② 체크포인트 ID 생성
    final long checkpointID = checkpointIdCounter.getAndIncrement();

    // ③ PendingCheckpoint 생성
    final PendingCheckpoint checkpoint = new PendingCheckpoint(
            job,
            checkpointID,
            checkpointTimestamp,
            ackTasks,           // ACK를 기다릴 Task 목록
            masterHooks,
            checkpointProperties,
            checkpointStorageLocation,  // 스냅샷 저장 위치
            onCompletionPromise);

    pendingCheckpoints.put(checkpointID, checkpoint);

    // ④ 타임아웃 타이머 설정
    ScheduledFuture<?> cancellerHandle = timer.schedule(
            () -> abortPendingCheckpoint(checkpoint,
                    new CheckpointException(CHECKPOINT_EXPIRED)),
            checkpointTimeout, TimeUnit.MILLISECONDS);
    checkpoint.setCancellerHandle(cancellerHandle);

    // ⑤ ★★★ 핵심: Source Task들에게 체크포인트 배리어 주입!
    for (Execution execution : executions) {
        execution.triggerCheckpoint(
                checkpointID, checkpointTimestamp, checkpointOptions);
        // 이 호출은 RPC를 통해 TaskExecutor.triggerCheckpoint()을 호출
        // TaskExecutor는 해당 Task에 체크포인트 이벤트를 전달
        // Source Task는 배리어를 데이터 스트림에 삽입
    }
}
```

### receiveAcknowledgeMessage() — ACK 수신

```java
public boolean receiveAcknowledgeMessage(
        AcknowledgeCheckpoint message, String taskManagerLocationInfo)
        throws CheckpointException {

    final long checkpointId = message.getCheckpointId();

    // PendingCheckpoint 찾기
    PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

    if (checkpoint == null) {
        // 이미 완료되었거나 만료된 체크포인트
        return false;
    }

    // ★ 개별 Task의 상태 스냅샷 정보를 ACK에 포함
    switch (checkpoint.acknowledgeTask(
            message.getTaskExecutionId(),
            message.getSubtaskState(),        // Task의 상태 스냅샷
            message.getCheckpointMetrics(),   // 체크포인트 성능 메트릭
            getStatsCallback(checkpoint))) {

        case SUCCESS:
            // ★ 모든 Task가 ACK했는지 확인
            if (checkpoint.isFullyAcknowledged()) {
                // 모든 Task 완료! → 체크포인트 완성
                completePendingCheckpoint(checkpoint);
            }
            break;

        case DUPLICATE:
            // 중복 ACK — 무시
            break;

        case UNKNOWN:
            // 알 수 없는 Task — 경고
            break;

        case DISCARDED:
            // 이미 폐기된 체크포인트
            break;
    }

    return true;
}
```

### completePendingCheckpoint() — 체크포인트 완료

```java
private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint)
        throws CheckpointException {

    final long checkpointId = pendingCheckpoint.getCheckpointId();

    // ① PendingCheckpoint → CompletedCheckpoint 변환
    CompletedCheckpoint completedCheckpoint;
    try {
        completedCheckpoint = pendingCheckpoint.finalizeCheckpoint(
                checkpointsCleaner, this::scheduleTrigger, executor);
    } catch (Exception e) {
        // 실패 시 폐기
        throw new CheckpointException("Could not finalize checkpoint", e);
    }

    // ② 완료된 체크포인트를 영속 저장소에 저장
    completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
            completedCheckpoint, checkpointsCleaner, this::scheduleTrigger);

    // ③ Pending 목록에서 제거
    pendingCheckpoints.remove(checkpointId);

    // ④ ★ 모든 Task에게 체크포인트 완료 통지
    //    Task들은 이 통지를 받고 임시 상태를 커밋
    for (ExecutionVertex ev : tasksToCommitTo) {
        ev.getCurrentExecutionAttempt().notifyCheckpointComplete(checkpointId, checkpointTimestamp);
    }

    // ⑤ 이 체크포인트 이전의 pending 체크포인트 정리
    abortPendingCheckpointsSubsumedBy(checkpointId);
}
```

---

## 4.3 CheckpointBarrier — 스트림 속의 신호

`CheckpointBarrier`는 일반 레코드와 함께 스트림을 흐르는 **특수 이벤트**입니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/io/network/api/CheckpointBarrier.java

public class CheckpointBarrier extends RuntimeEvent {

    private final long id;              // 체크포인트 ID
    private final long timestamp;       // 체크포인트 타임스탬프
    private final CheckpointOptions checkpointOptions;  // 옵션 (정렬/비정렬 등)

    public CheckpointBarrier(long id, long timestamp, CheckpointOptions checkpointOptions) {
        this.id = id;
        this.timestamp = timestamp;
        this.checkpointOptions = checkNotNull(checkpointOptions);
    }
}
```

**배리어의 핵심 특성:**
- 데이터 스트림에 삽입되어 레코드와 함께 흐릅니다
- 배리어 **이전**의 모든 레코드는 현재 체크포인트에 포함됩니다
- 배리어 **이후**의 모든 레코드는 다음 체크포인트에 포함됩니다
- 이것이 **Chandy-Lamport 알고리즘**의 핵심 아이디어입니다

### 배리어 정렬 (Barrier Alignment)

```
입력 채널 1:  ──record──record──│barrier│──record──record──
입력 채널 2:  ──record──record──record──│barrier│──record──

Task 처리:
1. 채널1의 barrier 도착 → 채널1 블록 (배리어 이후 레코드 버퍼링)
2. 채널2의 레코드 계속 처리
3. 채널2의 barrier 도착 → ★ 모든 입력의 barrier 도착!
4. 상태 스냅샷 수행
5. 채널1 블록 해제, 다운스트림으로 barrier 전파
```

> **Unaligned Checkpoint (비정렬 체크포인트) — [FLIP-76](https://cwiki.apache.org/confluence/display/FLINK/FLIP-76:+Unaligned+Checkpoints)**
>
> Flink 1.11에서 FLIP-76에 의해 도입. 배리어 정렬 없이 즉시 스냅샷을 수행합니다.
> 대신 "in-flight" 레코드(아직 처리되지 않은 버퍼의 레코드)도 함께 스냅샷합니다.
> 백프레셔가 심한 상황에서 체크포인트 지연을 방지합니다.
>
> **정렬 vs 비정렬 비교:**
> | | 정렬 체크포인트 | 비정렬 체크포인트 (FLIP-76) |
> |--|----------------|---------------------------|
> | 배리어 대기 | 모든 입력 채널의 배리어 도착 대기 | 첫 배리어 도착 즉시 스냅샷 |
> | in-flight 데이터 | 스냅샷에 미포함 | 스냅샷에 포함 |
> | 스냅샷 크기 | 작음 | 클 수 있음 |
> | 백프레셔 영향 | 체크포인트가 지연될 수 있음 | 영향 없음 |
> | 요구 조건 | - | EXACTLY_ONCE 모드 필수 |

---

## 4.4 Task 측 체크포인트 처리

### SubtaskCheckpointCoordinator — Task 내 체크포인트 처리

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/runtime/tasks/SubtaskCheckpointCoordinatorImpl.java

public class SubtaskCheckpointCoordinatorImpl implements SubtaskCheckpointCoordinator {

    // 비동기 스냅샷을 위한 스레드 풀
    private final ExecutorService asyncOperationsThreadPool;

    // 체크포인트 저장소
    private final CheckpointStorage checkpointStorage;
}
```

### checkpointState() — 상태 스냅샷 수행

```java
public void checkpointState(
        CheckpointMetaData metadata,
        CheckpointOptions options,
        CheckpointMetricsBuilder metrics,
        OperatorChain<?, ?> operatorChain,
        boolean isTaskFinished,
        Supplier<Boolean> isRunning)
        throws Exception {

    // ① 체크포인트 저장 위치 결정
    CheckpointStreamFactory storage =
            checkpointStorage.resolveCheckpointStorageLocation(
                    metadata.getCheckpointId(),
                    options.getTargetLocation());

    // ② ★★★ 모든 연산자의 상태 스냅샷 수행
    // 동기 단계: 연산자에게 스냅샷 시작 요청
    Map<OperatorID, OperatorSnapshotFutures> snapshotFutures = new HashMap<>();

    for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperatorsReverse()) {
        // 각 연산자의 snapshotState() 호출
        OperatorSnapshotFutures futures =
                operatorWrapper.getStreamOperator().snapshotState(
                        metadata.getCheckpointId(),
                        metadata.getTimestamp(),
                        options,
                        storage);
        snapshotFutures.put(operatorWrapper.getStreamOperator().getOperatorID(), futures);
    }

    // ③ ★ 비동기 단계: 상태를 영속 저장소에 기록
    // 이 부분은 별도 스레드에서 실행되어 메인 처리를 블로킹하지 않습니다
    AsyncCheckpointRunnable asyncCheckpointRunnable =
            new AsyncCheckpointRunnable(
                    snapshotFutures,
                    metadata,
                    metrics,
                    // ACK 콜백 → CheckpointCoordinator에 보고
                    (jobId, executionAttemptID, checkpointId, subtaskState, checkpointMetrics) -> {
                        checkpointResponder.acknowledgeCheckpoint(
                                jobId, executionAttemptID, checkpointId,
                                subtaskState, checkpointMetrics);
                    },
                    // ...
            );

    asyncOperationsThreadPool.execute(asyncCheckpointRunnable);
}
```

**체크포인트 처리의 2단계:**

```
동기 단계 (메인 스레드):                    비동기 단계 (별도 스레드):
├── 배리어 정렬                             ├── 상태 데이터를 저장소에 기록
├── 연산자 snapshotState() 호출             │   (FileSystem, RocksDB 등)
├── 상태의 "포인터" 획득                    ├── 기록 완료 확인
└── 비동기 작업 제출                        └── CheckpointCoordinator에 ACK 전송
    ↓                                         ↓
메인 처리 계속 →                            완료 후 정리
```

> **성능 포인트:**
> 동기 단계에서는 상태의 "스냅샷 포인터"만 획득하고, 실제 데이터 기록은 비동기로 수행합니다.
> 이것이 체크포인트가 처리 성능에 미치는 영향을 최소화하는 핵심 설계입니다.

---

## 4.5 State Backend — 상태 저장소

### StateBackend 인터페이스

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/state/StateBackend.java

@PublicEvolving
public interface StateBackend extends Serializable {

    // ★ Keyed State Backend 생성
    // keyBy() 이후의 상태를 관리
    // 반환 타입: CheckpointableKeyedStateBackend (스냅샷 기능 포함)
    <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws Exception;

    // ★ Operator State Backend 생성
    // 연산자 전체에 걸친 상태를 관리
    OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) throws Exception;
}
// ★ Flink 2.x에서는 파라미터 번들 패턴(Parameter Bundle Pattern)을 사용합니다.
// 개별 파라미터 대신 KeyedStateBackendParameters / OperatorStateBackendParameters
// 객체 하나에 모든 설정을 담아 전달합니다.
```

### HashMapStateBackend — 힙 메모리 기반 상태

```java
// 파일: flink-state-backends/flink-statebackend-hashmap/.../HashMapStateBackend.java

@PublicEvolving
public class HashMapStateBackend implements StateBackend, ConfigurableStateBackend {

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws Exception {

        // HeapKeyedStateBackend 생성
        // 상태를 Java HashMap에 저장
        return new HeapKeyedStateBackendBuilder<>(
                parameters.getKvStateRegistryListener(),
                parameters.getKeySerializer(),
                parameters.getUserCodeClassLoader(),
                parameters.getNumberOfKeyGroups(),
                parameters.getKeyGroupRange(),
                parameters.getExecutionConfig(),
                parameters.getTtlTimeProvider(),
                parameters.getLatencyTrackingStateConfig(),
                parameters.getStateHandles(),    // 복구할 상태 핸들
                parameters.getCancelStreamRegistry(),
                // ...
        ).build();
    }
}
```

> **State Backend 비교:**
> | Backend | 상태 위치 | 스냅샷 방식 | 용도 |
> |---------|----------|------------|------|
> | HashMapStateBackend | JVM Heap | 전체 스냅샷 | 작은~중간 상태 |
> | EmbeddedRocksDBStateBackend | RocksDB (디스크) | 증분 스냅샷 | 큰 상태 |

### KeyedStateBackend — 키별 상태 관리

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/state/KeyedStateBackend.java

@PublicEvolving
public interface KeyedStateBackend<K> extends KeyedStateFactory, Disposable {

    // 현재 처리 중인 키 설정
    void setCurrentKey(K newKey);

    // 현재 키 조회
    K getCurrentKey();

    // ★ 파티셔닝된 상태 접근
    // 내부적으로 현재 키에 해당하는 상태를 반환
    <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, T> stateDescriptor) throws Exception;

    // ★ 스냅샷 수행
    RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions) throws Exception;
}
```

### 상태 등록과 접근 — AbstractStreamOperator에서

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/api/operators/AbstractStreamOperator.java

public abstract class AbstractStreamOperator<OUT>
        implements StreamOperator<OUT>, SetupableStreamOperator<OUT>, Serializable {

    // ★ Keyed State 접근 메서드
    protected <T> ValueState<T> getRuntimeContext().getState(
            ValueStateDescriptor<T> stateDescriptor) {
        // 내부적으로:
        // 1. StateDescriptor에서 상태 이름과 타입 추출
        // 2. KeyedStateBackend에서 해당 상태 조회 또는 생성
        // 3. 현재 키(setCurrentKey에 의해 설정)에 바인딩된 상태 반환
    }

    // ★ snapshotState() — 체크포인트 시 호출
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // 사용자가 오버라이드하여 커스텀 스냅샷 로직 구현 가능
        // 기본적으로는 등록된 상태들이 자동으로 스냅샷됨
    }

    // ★ initializeState() — 복구 시 호출
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        // 체크포인트/세이브포인트에서 상태를 복구
    }
}
```

---

## 4.6 State의 종류

### Keyed State (키별 상태)

```java
// 사용자 코드 예시
public class CountFunction extends RichFlatMapFunction<String, Tuple2<String, Long>> {

    // ★ Keyed State 선언 — keyBy() 이후에만 사용 가능
    private transient ValueState<Long> counter;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 상태 초기화 — StateDescriptor로 이름과 타입 지정
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("counter", Types.LONG);
        counter = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        // ★ 현재 키에 해당하는 상태 접근
        // keyBy()에 의해 현재 키가 자동으로 설정되어 있음
        Long currentCount = counter.value();
        if (currentCount == null) {
            currentCount = 0L;
        }
        currentCount++;
        counter.update(currentCount);
        out.collect(Tuple2.of(value, currentCount));
    }
}
```

**Keyed State 종류:**
| 타입 | 설명 | 사용 예 |
|------|------|---------|
| `ValueState<T>` | 단일 값 | 카운터, 최근 값 |
| `ListState<T>` | 값 리스트 | 이벤트 축적 |
| `MapState<K, V>` | 키-값 맵 | 사용자별 속성 |
| `ReducingState<T>` | 자동 집계 | 합계, 최대값 |
| `AggregatingState<IN, OUT>` | 커스텀 집계 | 평균, 중앙값 |

### Operator State (연산자 상태)

```java
// Operator State — keyBy() 없이도 사용 가능
// 보통 Source/Sink에서 오프셋 관리 등에 사용

public class MySource implements SourceFunction<String>,
        CheckpointedFunction {  // ★ CheckpointedFunction 구현

    private transient ListState<Long> offsetState;
    private long offset = 0;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // ★ Operator State 초기화
        ListStateDescriptor<Long> descriptor =
                new ListStateDescriptor<>("offset", Types.LONG);
        offsetState = context.getOperatorStateStore()
                .getListState(descriptor);

        // 복구 시 상태 로드
        for (Long o : offsetState.get()) {
            offset = o;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // ★ 체크포인트 시 현재 오프셋 저장
        offsetState.clear();
        offsetState.add(offset);
    }
}
```

---

## 4.7 설계 배경: Disaggregated State — FLIP-423

> **[FLIP-423: Disaggregated State Storage and Management](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=293046855)**

Flink 2.x의 가장 큰 아키텍처 변화 중 하나입니다. 기존에는 상태가 TaskManager의 로컬 디스크/메모리에 저장되었지만,
FLIP-423에서는 **원격 분산 파일시스템(S3, HDFS 등)을 기본 상태 저장소**로 사용합니다.

```
기존 (Flink 1.x):                    Disaggregated (Flink 2.x):
┌─────────────────┐                  ┌─────────────────┐
│ TaskManager      │                  │ TaskManager      │
│ ┌─────────────┐ │                  │ ┌─────────────┐ │
│ │ RocksDB     │ │                  │ │ ForSt       │ │
│ │ (로컬 디스크) │ │                  │ │ (로컬 캐시) │ │
│ └──────┬──────┘ │                  │ └──────┬──────┘ │
│        │        │                  │        │        │
│   체크포인트 시   │                  │   상시 읽기/쓰기 │
│   전체 스냅샷    │                  │        │        │
└────────┼────────┘                  └────────┼────────┘
         │                                    │
         ▼                                    ▼
  ┌──────────────┐                    ┌──────────────┐
  │ DFS (S3/HDFS)│                    │ DFS (S3/HDFS)│
  │ 체크포인트만  │                    │ 기본 저장소   │
  └──────────────┘                    └──────────────┘
```

**ForSt (For Streaming)**: RocksDB의 분리형 버전으로, 원격 저장소를 기본으로 사용하고 로컬은 캐시로만 활용합니다.

**핵심 하위 FLIP:**
- **FLIP-424**: 비동기 상태 접근 API — 원격 상태 접근의 지연을 숨기기 위한 non-blocking API
- **FLIP-425**: 비동기 실행 모델 — FLIP-424 기반의 non-blocking 실행 프레임워크
- **FLIP-427**: ForSt State Store — 실제 구현체
- **FLIP-428**: Checkpoint/Rescale 통합 — 복구 시간 10초 이내 (기존 대비 40배 개선)

이 모듈은 코드베이스의 `flink-dstl/` 디렉토리에 위치합니다.

---

## 4.8 체크포인트 복구 과정

장애 발생 시 가장 최근의 완료된 체크포인트로 복구합니다:

```
장애 발생!
    │
    ▼
① JobMaster: 최신 CompletedCheckpoint 로드
    │
    ▼
② ExecutionGraph 재스케줄링 (실패한 Task들 재시작)
    │
    ▼
③ Task 시작 시 StateInitializationContext에 체크포인트 핸들 전달
    │
    ▼
④ StateBackend: 핸들에서 상태 복원
    │
    ├── HashMapStateBackend: 파일에서 상태를 역직렬화하여 힙에 로드
    └── RocksDB: SST 파일을 로컬에 복사하여 DB 복원
    │
    ▼
⑤ Source: 체크포인트 시점의 오프셋부터 데이터 재읽기
    │
    ▼
⑥ 정상 처리 재개 (Exactly-once 보장!)
```

---

## 4.9 핵심 정리

1. **CheckpointCoordinator**: JobMaster에서 체크포인트 전체 조율. Source에 배리어 주입 → ACK 수집 → 완료
2. **CheckpointBarrier**: 데이터 스트림에 삽입되는 특수 이벤트. Chandy-Lamport 알고리즘의 마커
3. **배리어 정렬**: 다중 입력 시 모든 채널의 배리어가 도착할 때까지 대기 → 일관된 상태 보장
4. **비동기 스냅샷**: 동기 단계(포인터 획득) + 비동기 단계(데이터 기록)로 성능 영향 최소화
5. **State Backend**: HashMapStateBackend(힙) vs EmbeddedRocksDBStateBackend(디스크)
6. **Keyed State**: `keyBy()` 이후 키별로 격리된 상태. ValueState, ListState, MapState 등
7. **Operator State**: 연산자 레벨 상태. Source/Sink의 오프셋 관리에 주로 사용
8. **복구**: 최신 체크포인트에서 상태 복원 + Source 오프셋 되감기 → Exactly-once

---

## 다음 단계

Phase 5에서는 Task들이 어떻게 **스케줄링**되고, 장애 발생 시 어떤 **Failover 전략**으로 복구되는지를 추적합니다.
