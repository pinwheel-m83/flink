# Phase 3: 런타임 아키텍처 — 분산 실행의 뼈대

> Flink 클러스터를 구성하는 4대 핵심 컴포넌트와 그들의 상호작용을 코드 레벨에서 추적합니다.
> Job이 제출된 순간부터 Task가 실제로 데이터를 처리하기 시작할 때까지의 전체 흐름을 다룹니다.

---

## 3.1 아키텍처 개요

Flink 클러스터는 다음 4개의 핵심 컴포넌트로 구성됩니다:

```
┌─────────────────────────────────────────────────────┐
│                  Flink Cluster                       │
│                                                      │
│  ┌──────────────┐    ┌───────────────────┐          │
│  │  Dispatcher   │    │  ResourceManager   │          │
│  │  (Job 접수)   │    │  (리소스 관리)      │          │
│  └──────┬───────┘    └────────┬──────────┘          │
│         │                     │                      │
│         ▼                     │                      │
│  ┌──────────────┐            │                      │
│  │  JobMaster    │◄───────────┘                      │
│  │  (Job 조율)   │    Slot 할당                       │
│  └──────┬───────┘                                    │
│         │ Task 배포                                   │
│         ▼                                            │
│  ┌──────────────┐  ┌──────────────┐                 │
│  │ TaskExecutor  │  │ TaskExecutor  │  ...            │
│  │ (Task 실행)   │  │ (Task 실행)   │                 │
│  │ ┌────┐┌────┐ │  │ ┌────┐┌────┐ │                 │
│  │ │Slot││Slot│ │  │ │Slot││Slot│ │                 │
│  │ └────┘└────┘ │  │ └────┘└────┘ │                 │
│  └──────────────┘  └──────────────┘                 │
└─────────────────────────────────────────────────────┘
```

모든 컴포넌트는 **RPC(Remote Procedure Call)**로 통신합니다.

---

## 3.2 RPC 통신 메커니즘

Flink의 모든 분산 컴포넌트 통신은 자체 RPC 프레임워크를 기반으로 합니다 (내부적으로 Pekko/Akka 사용).

### RpcGateway — 원격 호출 인터페이스

```java
// 파일: flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcGateway.java

public interface RpcGateway {
    // 이 RPC 엔드포인트의 주소 반환
    String getAddress();

    // 이 RPC 엔드포인트의 호스트명 반환
    String getHostname();
}
```

**설계 패턴:** 각 컴포넌트는 `Gateway` 인터페이스와 `Endpoint` 구현으로 분리됩니다.
- `DispatcherGateway`: Dispatcher에 대한 원격 호출 인터페이스
- `Dispatcher`: 실제 구현 (`RpcEndpoint` 상속)

### RpcEndpoint — RPC 엔드포인트 기반 클래스

```java
// 파일: flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcEndpoint.java

public abstract class RpcEndpoint implements RpcGateway, AutoCloseableAsync {

    protected final RpcServer rpcServer;  // 실제 RPC 통신을 처리하는 서버

    // 이 엔드포인트의 "메인 스레드"에서 실행을 보장
    // ★ 모든 상태 변경은 이 스레드에서만 일어남 → 동시성 문제 방지
    protected void runAsync(Runnable runnable) {
        rpcServer.runAsync(runnable);
    }

    // 일정 시간 후 실행
    protected void scheduleRunAsync(Runnable runnable, Duration delay) {
        rpcServer.scheduleRunAsync(runnable, delay);
    }

    // 이 엔드포인트에 대한 Gateway(프록시) 획득
    public <C extends RpcGateway> C getSelfGateway(Class<C> selfGatewayType) {
        // 자기 자신에 대한 RPC 프록시를 반환
        // 이를 통해 자기 자신의 메서드도 RPC 스레드에서 안전하게 호출
        return rpcServer.getSelfGateway(selfGatewayType);
    }
}
```

> **핵심 설계: 단일 스레드 모델**
> `RpcEndpoint`는 내부적으로 **단일 메인 스레드**에서 모든 RPC 호출을 처리합니다.
> 이는 복잡한 동기화 코드 없이도 스레드 안전성을 보장합니다.
> `runAsync()`, `callAsync()`는 모두 이 메인 스레드의 메일박스에 작업을 넣는 것입니다.

### FencedRpcEndpoint — 리더 선출 기반 RPC

```java
// 파일: flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/FencedRpcEndpoint.java

public abstract class FencedRpcEndpoint<F extends Serializable> extends RpcEndpoint {

    private final F fencingToken;  // "울타리" 토큰 — 불변 (리더 변경 시 새 인스턴스 생성)

    // ★ Fencing의 의미:
    // HA(고가용성) 환경에서 리더가 변경되면 새 토큰이 할당됩니다.
    // 이전 리더의 잔여 메시지가 새 리더에게 처리되는 것을 방지합니다.
    // 메시지의 fencing token이 현재 token과 일치하지 않으면 무시됩니다.
}
```

---

## 3.3 Dispatcher — Job 제출의 관문

Dispatcher는 클라이언트로부터 Job을 받아 `JobMaster`를 생성하는 역할을 합니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java

public abstract class Dispatcher extends FencedRpcEndpoint<DispatcherId>
        implements DispatcherGateway {

    // 핵심 필드
    private final ExecutionPlanWriter executionPlanWriter;              // Job 영속화
    private final JobResultStore jobResultStore;                        // 결과 저장소
    private final HighAvailabilityServices highAvailabilityServices;    // HA 서비스
    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    private final OnMainThreadJobManagerRunnerRegistry jobManagerRunnerRegistry;  // JobMaster 관리
    private final JobManagerRunnerFactory jobManagerRunnerFactory;      // JobMaster 팩토리
}
```

### submitJob() — Job 제출 처리

```java
@Override
public CompletableFuture<Acknowledge> submitJob(ExecutionPlan executionPlan, Duration timeout) {
    final JobID jobID = executionPlan.getJobID();

    log.info("Received job submission '{}' ({}).", executionPlan.getName(), jobID);

    // ① 이미 완료(Terminal State)된 Job인지 확인
    return isInGloballyTerminalState(jobID)
            .thenComposeAsync(
                    isTerminated -> {
                        if (isTerminated) {
                            log.warn("Ignoring job submission '{}' ({}) because "
                                    + "the job already reached a globally terminal state.",
                                    executionPlan.getName(), jobID);
                            return FutureUtils.completedExceptionally(
                                    DuplicateJobSubmissionException
                                            .ofGloballyTerminated(jobID));
                        }

                        // ② 이미 실행 중인 Job인지 확인
                        if (jobManagerRunnerRegistry.isRegistered(jobID)) {
                            return FutureUtils.completedExceptionally(
                                    DuplicateJobSubmissionException.of(jobID));
                        }

                        // ③ ★ 핵심: 실제 Job 실행 시작
                        return internalSubmitJob(executionPlan);
                    },
                    getMainThreadExecutor());
}
```

### internalSubmitJob() — 내부 제출 로직

```java
private CompletableFuture<Acknowledge> internalSubmitJob(ExecutionPlan executionPlan) {
    // JobGraph인 경우 병렬도 오버라이드 적용
    if (executionPlan instanceof JobGraph) {
        applyParallelismOverrides((JobGraph) executionPlan);
    }

    log.info("Submitting job '{}' ({}).", executionPlan.getName(), executionPlan.getJobID());

    // 제출 대기 목록에 추가
    submittedAndWaitingTerminationJobIDs.add(executionPlan.getJobID());

    // ★ 핵심: Job 영속화 및 실행
    return waitForTerminatingJob(
                    executionPlan.getJobID(), executionPlan, this::persistAndRunJob)
            .handle(
                    (ignored, throwable) ->
                            handleTermination(executionPlan.getJobID(), throwable))
            .thenCompose(Function.identity())
            .whenComplete(
                    (ignored, throwable) ->
                            submittedAndWaitingTerminationJobIDs.remove(
                                    executionPlan.getJobID()));
}
```

> **참고**: `submitJob()` 메서드의 파라미터 타입이 `JobGraph`가 아닌 `ExecutionPlan` 인터페이스입니다.
> Flink 2.x에서 `JobGraph`은 `ExecutionPlan`의 구현체이며, 이 추상화 덕분에 향후 다른 실행 계획 형태도 지원 가능합니다.

**흐름 요약:**
```
Client → Dispatcher.submitJob(ExecutionPlan)
            │
            ├── 중복 검사 (이미 실행 중/완료된 Job?)
            ├── persistAndRunJob() → HA 저장소에 영속화 + JobMaster 생성 및 시작
            └── 완료 대기 및 정리
```

---

## 3.4 ResourceManager — 리소스 관리자

ResourceManager는 클러스터의 리소스(TaskManager의 Slot)를 관리합니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/resourcemanager/ResourceManager.java

public abstract class ResourceManager<WorkerType extends ResourceIDRetrievable>
        extends FencedRpcEndpoint<ResourceManagerId>
        implements DelegationTokenManager.Listener, ResourceManagerGateway {

    // TaskManager 등록 정보
    private final Map<ResourceID, WorkerRegistration<WorkerType>> taskExecutors;

    // Slot 매니저 — 어떤 Slot이 어떤 Job에 할당되었는지 관리
    private final SlotManager slotManager;

    // Job별 리소스 요청 추적
    private final Map<JobID, JobManagerRegistration> jmResourceIdRegistrations;
}
```

### registerTaskManager() — TaskManager 등록

```java
@Override
public CompletableFuture<RegistrationResponse> registerTaskManager(
        final TaskManagerRegistrationInformation taskManagerRegistrationInformation,
        final Duration timeout) {

    // TaskManager 등록 처리
    CompletableFuture<TaskExecutorGateway> taskExecutorGatewayFuture =
            getRpcService().connect(
                    taskManagerRegistrationInformation.getTaskManagerRpcAddress(),
                    TaskExecutorGateway.class);

    return taskExecutorGatewayFuture.handleAsync(
            (gateway, throwable) -> {
                // ★ TaskManager의 RPC Gateway를 획득하고 등록
                final WorkerRegistration<WorkerType> registration =
                        new WorkerRegistration<>(
                                gateway,
                                worker,
                                taskManagerRegistrationInformation.getDataPort(),
                                taskManagerRegistrationInformation.getJmxPort(),
                                hardwareDescription);

                taskExecutors.put(resourceID, registration);

                // ★ SlotManager에 새 TaskManager의 Slot들을 보고
                slotManager.registerTaskManager(
                        registration, registration.getSlotReport(), resourceProfile);

                return new TaskExecutorRegistrationSuccess(/* ... */);
            },
            getMainThreadExecutor());
}
```

### 리소스 할당 흐름

```
JobMaster가 Slot 요청
    │
    ▼
ResourceManager.requestSlot()
    │
    ▼
SlotManager: 사용 가능한 Slot 찾기
    │
    ├── 가용 Slot 있음 → TaskExecutor에 Slot 할당 지시
    │
    └── 가용 Slot 없음 → 새 TaskManager 시작 요청
                          (YARN/K8s에서만, Standalone은 고정)
```

---

## 3.5 JobMaster — Job 실행의 조율자

`JobMaster`는 하나의 Job에 대한 전체 라이프사이클을 관리합니다.
JobGraph → ExecutionGraph 변환, Slot 요청, Task 배포, 체크포인트 트리거 등을 담당합니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobMaster.java

public class JobMaster extends FencedRpcEndpoint<JobMasterId>
        implements JobMasterGateway, JobMasterService {

    // ★ 핵심 필드
    private final JobGraph jobGraph;                       // 원본 JobGraph
    private final SchedulerNG scheduler;                   // 스케줄러 (ExecutionGraph 포함)
    private final SlotPoolService slotPoolService;         // Slot 풀 관리

    // TaskManager 연결 관리
    private final Map<ResourceID, TaskManagerRegistration> registeredTaskManagers;

    // 하트비트 매니저 — TaskManager/ResourceManager 생존 확인
    private final HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload,
            AllocatedSlotReport> taskManagerHeartbeatManager;
    private final HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;
}
```

### startJobExecution() — Job 실행 시작

```java
private void startJobExecution() throws Exception {
    validateRunsInMainThread();

    // ① Shuffle 서비스에 Job 등록
    JobShuffleContext context = new JobShuffleContextImpl(executionPlan.getJobID(), this);
    shuffleMaster.registerJob(context);

    // ② JobMaster 서비스 시작 (SlotPool, HeartbeatManager 등)
    startJobMasterServices();

    log.info(
            "Starting execution of job '{}' ({}) under job master id {}.",
            executionPlan.getName(),
            executionPlan.getJobID(),
            getFencingToken());

    // ③ ★★★ 스케줄러를 통해 Job 스케줄링 시작
    // ExecutionGraph 생성 → Slot 요청 → Task 배포
    startScheduling();
}
```

> **참고**: 이전 버전에서는 `resetAndStartScheduler()`라는 메서드가 있었지만,
> Flink 2.x에서는 `startScheduling()`으로 단순화되었습니다.
```

### offerSlots() — TaskExecutor로부터 Slot 제안 수신

```java
@Override
public CompletableFuture<Collection<SlotOffer>> offerSlots(
        final ResourceID taskManagerId,
        final Collection<SlotOffer> slots,
        final Duration timeout) {

    // TaskExecutor가 "이 Slot들을 사용할 수 있습니다"라고 제안
    // JobMaster는 필요한 Slot을 수락하고 나머지는 거절

    TaskManagerRegistration registration = registeredTaskManagers.get(taskManagerId);
    if (registration == null) {
        // 등록되지 않은 TaskManager → 거절
        return FutureUtils.completedExceptionally(
                new Exception("Unknown TaskManager " + taskManagerId));
    }

    // ★ SlotPool에 Slot 제안 전달
    return slotPoolService.offerSlots(
            registration.getTaskExecutorGateway(),
            taskManagerId,
            slots);
}
```

---

## 3.6 TaskExecutor — 실제 Task 실행자

`TaskExecutor`는 Flink의 워커 프로세스로, 실제로 Task를 실행하는 JVM입니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/taskexecutor/TaskExecutor.java

public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway {

    // ★ 현재 실행 중인 Task들
    private final Map<ExecutionAttemptID, Task> tasks;

    // Slot 관리
    private final TaskSlotTable<Task> taskSlotTable;

    // 네트워크 스택
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;

    // 메모리 매니저
    private final MemoryManager memoryManager;
}
```

### submitTask() — Task 제출 처리

```java
@Override
public CompletableFuture<Acknowledge> submitTask(
        TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Duration timeout) {

    final JobID jobId = tdd.getJobId();
    final ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();

    // ① 권한 검증 — 해당 Job의 JobMaster가 보낸 요청인지 확인
    final JobTable.Connection jobConnection =
            jobTable.getConnection(jobId)
                    .orElseThrow(() -> new TaskSubmissionException("No job connection for " + jobId));

    if (!Objects.equals(jobMasterId, jobConnection.getJobMasterId())) {
        throw new TaskSubmissionException("JobMaster ID mismatch");
    }

    // ② ★★★ Task 객체 생성
    Task task = new Task(
            jobInformation,
            taskInformation,
            executionAttemptID,
            tdd.getAllocationId(),
            tdd.getProducedPartitions(),        // 출력 파티션 정보
            tdd.getInputGates(),                 // 입력 게이트 정보
            memoryManager,
            sharedResources,
            shuffleEnvironment,                  // 네트워크 통신
            kvStateService,
            broadcastVariableManager,
            taskEventDispatcher,
            externalResourceInfoProvider,
            taskStateManager,                    // 상태 복구
            taskManagerActions,
            inputSplitProvider,
            checkpointResponder,                 // 체크포인트 응답
            aggregateManager,
            // ...
    );

    // ③ Slot에 Task 할당
    taskSlotTable.addTask(task);

    // ④ ★ Task 실행 시작!
    task.startTaskThread();

    return CompletableFuture.completedFuture(Acknowledge.get());
}
```

---

## 3.7 Task — Task의 라이프사이클

`Task`는 하나의 스레드에서 실행되며, 실제 연산자(Operator)를 호출합니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/taskmanager/Task.java

public class Task implements Runnable, TaskSlotPayload, TaskActions,
        PartitionProducerStateProvider, CheckpointableTask {

    // Task 상태 머신
    private volatile ExecutionState executionState = ExecutionState.CREATED;

    // ★ 실제 실행할 코드 — StreamTask의 서브클래스
    private final String nameOfInvokableClass;
    private volatile TaskInvokable invokable;

    // Task 스레드
    private final Thread executingThread;
}
```

### 상태 전이 다이어그램

```
CREATED → DEPLOYING → INITIALIZING → RUNNING → FINISHED
                                        │
                                        ▼
                                     CANCELING → CANCELED
                                        │
                                        ▼
                                      FAILED
```

### doRun() — Task 실행의 핵심

```java
private void doRun() {
    // ★ 상태 전이: CREATED → DEPLOYING
    transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING);

    // ① 사용자 코드 ClassLoader 설정
    final ClassLoader userCodeClassLoader = createUserCodeClassloader();

    // ② ResultPartition과 InputGate 초기화
    //    (네트워크 통신 채널 설정)
    setupPartitionsAndGates(resultPartitionWriters, inputGates);

    // ③ ★★★ TaskInvokable 인스턴스 생성 (보통 StreamTask)
    Environment env = new RuntimeEnvironment(/* ... */);
    invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);

    // ★ 상태 전이: DEPLOYING → INITIALIZING
    transitionState(ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);

    // ④ ★★★ Task 실행! (이 안에서 데이터 처리가 시작됨)
    invokable.invoke();

    // ⑤ 정상 완료
    // ★ 상태 전이: → FINISHED
    transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED);
}
```

> **`invokable.invoke()`가 핵심입니다.**
> `nameOfInvokableClass`는 보통 `OneInputStreamTask`, `TwoInputStreamTask`, `SourceStreamTask` 등이며,
> 이들은 모두 `StreamTask`의 서브클래스입니다.

---

## 3.8 StreamTask — 스트리밍 처리의 메인 루프

`StreamTask`는 실제 데이터를 처리하는 메인 루프를 구현합니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/runtime/tasks/StreamTask.java

@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
        implements TaskInvokable, CheckpointableTask, CoordinatedTask,
                   AsyncExceptionHandler, ContainingTaskDetails {

    // ★ 핵심 필드
    protected final OperatorChain<OUT, OP> operatorChain;    // 체이닝된 연산자 체인
    private final StreamTaskMailboxProcessor mailboxProcessor;  // 메일박스 (이벤트 루프)
    protected final StreamConfig configuration;               // 스트림 설정
    private final CheckpointStorage checkpointStorage;        // 체크포인트 저장소
}
```

### invoke() — StreamTask 실행 진입점

```java
@Override
public final void invoke() throws Exception {
    // ① 초기화 단계
    restoreInternal();           // 상태 복구 (체크포인트/세이브포인트에서)
    isRunning = true;

    // ② ★★★ 메인 처리 루프!
    // 이 메서드 안에서 데이터가 계속 처리됩니다
    runMailboxLoop();

    // ③ 종료 처리
    afterInvoke();
}
```

### runMailboxLoop() — 메일박스 이벤트 루프

```java
// 파일: flink-runtime/.../streaming/runtime/tasks/mailbox/MailboxProcessor.java
public void runMailboxLoop() throws Exception {
    suspended = !mailboxLoopRunning;
    final TaskMailbox localMailbox = mailbox;

    checkState(localMailbox.isMailboxThread(),
            "Method must be executed by declared mailbox thread!");

    final MailboxController mailboxController = new MailboxController(this);

    // ★ 핵심 루프 — Flink 데이터 처리의 심장
    while (isNextLoopPossible()) {
        // ① 메일박스의 모든 메일 처리 (블로킹)
        //    체크포인트 트리거, 타이머 콜백, 연산자 이벤트 등
        //    메일이 없으면 default action이 가능해질 때까지 대기
        processMail(localMailbox, false);

        if (isNextLoopPossible()) {
            // ② ★ 기본 동작: 입력에서 레코드 하나 읽어서 처리
            //    이것이 실제 데이터 처리!
            mailboxDefaultAction.runDefaultAction(mailboxController);
        }
    }
}
```

**defaultAction은 무엇인가?**
- `StreamTask`에서 `defaultAction`은 `processInput()` 메서드입니다
- `processInput()`은 입력 채널에서 레코드를 읽고 → 연산자 체인을 통해 처리합니다

### processInput() — 레코드 처리

```java
// StreamTask 내부
protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
    // ★ 입력 프로세서에서 레코드 하나를 읽어 처리
    DataInputStatus status = inputProcessor.processInput();

    switch (status) {
        case MORE_AVAILABLE:
            // 더 많은 데이터가 있음 → 루프 계속
            if (taskIsAvailable()) {
                return;
            }
            break;
        case NOTHING_AVAILABLE:
            // 현재 데이터 없음 → 대기
            break;
        case END_OF_RECOVERY:
            // 복구 완료 (이 메서드에서는 발생하지 않아야 함)
            throw new IllegalStateException("We should not receive this event here.");
        case STOPPED:
            // 정지 → 드레인 없이 종료
            endData(StopMode.NO_DRAIN);
            return;
        case END_OF_DATA:
            // 데이터 끝 → 드레인 모드로 종료
            endData(StopMode.DRAIN);
            notifyEndOfData();
            return;
        case END_OF_INPUT:
            // 입력 종료 → 메일박스 루프 중단
            controller.suspendDefaultAction();
            mailboxProcessor.suspend();
            return;
    }
}
```

### OperatorChain — 체이닝된 연산자 실행

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/streaming/runtime/tasks/OperatorChain.java

// 데이터 처리 흐름 (체이닝된 경우):
// Record → HeadOperator → ChainedOperator1 → ChainedOperator2 → Output

// 체인 내에서는 메서드 호출(function call)로 데이터 전달
// 체인 간에는 네트워크 통신(network shuffle)으로 데이터 전달
```

---

## 3.9 설계 배경: 메일박스 모델 (FLINK-12477)

> **[FLINK-12477: Mailbox Threading Model](https://issues.apache.org/jira/browse/FLINK-12477)**

Flink 1.9 이전의 `StreamTask`는 다중 스레드 모델을 사용했습니다:
- 메인 스레드: 레코드 처리
- 체크포인트 스레드: 체크포인트 배리어 주입
- 타이머 스레드: 타이머 콜백 실행

이로 인해 복잡한 동기화 코드가 필요했고, 데드락과 경쟁 조건의 원인이 되었습니다.

**메일박스 모델**은 Actor 모델에서 영감을 받아 **모든 작업을 단일 스레드에서 실행**합니다:

```
메일박스 루프:
┌─────────────────────────────────────────┐
│ while (running) {                        │
│   ① processMail()  ← 체크포인트, 타이머  │
│   ② defaultAction() ← 레코드 처리       │
│ }                                        │
└─────────────────────────────────────────┘

외부 스레드 → mailbox.put(mail) → 메인 스레드에서 처리

장점:
- 동기화 코드 불필요 (단일 스레드)
- 체크포인트/타이머/레코드 처리의 순서 보장
- 핫 패스(메일 없는 경우)의 오버헤드 최소화
```

---

## 3.11 ClusterEntrypoint — 클러스터 부트스트랩

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/entrypoint/ClusterEntrypoint.java

public abstract class ClusterEntrypoint implements AutoCloseableAsync, FatalErrorHandler {

    // 클러스터 시작 시 모든 컴포넌트를 생성하고 연결합니다
    protected DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration);

    // 내부적으로:
    // 1. Dispatcher 생성 및 시작
    // 2. ResourceManager 생성 및 시작
    // 3. WebMonitorEndpoint (REST API) 시작
    // 4. HA 서비스 초기화
}
```

---

## 3.12 전체 실행 흐름 정리

```
① Client: env.execute()
    │
    ├── StreamGraph → JobGraph 변환
    │
    ▼
② Dispatcher.submitJob(JobGraph)
    │
    ├── JobGraph 영속화 (HA)
    ├── JobMaster 생성
    │
    ▼
③ JobMaster.startJobExecution()
    │
    ├── JobGraph → ExecutionGraph 변환
    ├── ResourceManager에 Slot 요청
    │
    ▼
④ ResourceManager: Slot 할당
    │
    ├── TaskExecutor에 Slot 할당 지시
    │
    ▼
⑤ TaskExecutor.submitTask(TaskDeploymentDescriptor)
    │
    ├── Task 객체 생성
    ├── 네트워크 채널 설정
    │
    ▼
⑥ Task.doRun()
    │
    ├── StreamTask 인스턴스 생성
    ├── 상태 복구 (있을 경우)
    │
    ▼
⑦ StreamTask.runMailboxLoop()
    │
    └── 무한 루프: 레코드 읽기 → 연산자 처리 → 출력
        (체크포인트, 타이머 등 메일박스 이벤트도 처리)
```

---

## 3.13 핵심 정리

1. **Dispatcher**: Job 제출 관문. JobMaster를 생성하고 관리
2. **ResourceManager**: TaskExecutor의 Slot을 관리. 필요시 새 TaskManager 시작 요청
3. **JobMaster**: 하나의 Job 전담. ExecutionGraph 생성, Slot 요청, Task 배포, 체크포인트 조율
4. **TaskExecutor**: 워커 프로세스. Task를 실행하고 네트워크 통신 처리
5. **Task**: 하나의 스레드. StreamTask를 통해 실제 데이터 처리
6. **StreamTask**: 메일박스 루프 기반. 레코드 처리 + 체크포인트 + 타이머를 하나의 스레드에서 처리
7. **RPC**: 모든 컴포넌트 통신은 RPC 기반. 단일 스레드 모델로 동시성 단순화

---

## 다음 단계

Phase 4에서는 StreamTask의 메일박스 루프 안에서 **체크포인트**가 어떻게 트리거되고 처리되는지,
그리고 **State**가 어떻게 관리되고 스냅샷되는지를 코드 레벨에서 추적합니다.
