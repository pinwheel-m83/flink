# Phase 5: Scheduling & Failover

> Job의 Task가 어떻게 스케줄링되어 실행되는지, 장애 발생 시 어떻게 복구되는지를 코드 레벨에서 추적합니다.

---

## 5.1 스케줄링 아키텍처 개요

```
JobMaster
    │
    ▼
SchedulerNG (인터페이스)
    │
    ├── DefaultScheduler (기본 스케줄러)
    │       │
    │       ├── SchedulingStrategy (스케줄링 전략)
    │       │       ├── PipelinedRegionSchedulingStrategy
    │       │       └── ...
    │       │
    │       ├── ExecutionSlotAllocator (Slot 할당)
    │       │       └── SlotSharingExecutionSlotAllocator
    │       │
    │       └── FailoverStrategy (장애 복구 전략)
    │               └── RestartPipelinedRegionFailoverStrategy
    │
    └── AdaptiveScheduler (적응형 스케줄러)
            └── 가용 리소스에 따라 병렬도 동적 조정
```

---

## 5.2 DefaultScheduler — 기본 스케줄러

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/DefaultScheduler.java

protected DefaultScheduler(
        final Logger log,
        final JobGraph jobGraph,
        final Executor ioExecutor,
        final Configuration jobMasterConfiguration,
        // ... 많은 파라미터
        ExecutionPlanSchedulingContext executionPlanSchedulingContext)
        throws Exception {

    super(/* 부모 SchedulerBase 초기화 */);

    this.log = log;
    this.delayExecutor = checkNotNull(delayExecutor);
    this.userCodeLoader = checkNotNull(userCodeLoader);
    this.executionOperations = checkNotNull(executionOperations);
    this.shuffleMaster = checkNotNull(shuffleMaster);

    // ★ Failover 전략 초기화
    this.failoverStrategy =
            failoverStrategyFactory.create(
                    getSchedulingTopology(), getResultPartitionAvailabilityChecker());
    log.info(
            "Using failover strategy {} for {} ({}).",
            failoverStrategy,
            jobGraph.getName(),
            jobGraph.getJobID());
}
```

### startScheduling() — 스케줄링 시작

```java
@Override
protected void startSchedulingInternal() {
    log.info("Starting scheduling with scheduling strategy [{}]",
            schedulingStrategy.getClass().getName());

    // ★ SchedulingStrategy에게 스케줄링 시작을 위임
    schedulingStrategy.startScheduling();
}
```

---

## 5.3 PipelinedRegionSchedulingStrategy — 파이프라인 리전 기반 스케줄링

Flink는 ExecutionGraph를 **Pipelined Region**으로 분할합니다.
같은 Region 내의 Task들은 파이프라인으로 연결되어 있어 동시에 실행되어야 합니다.

```
Region 1              Region 2
┌─────────────┐       ┌──────────────┐
│Source → Map │ ════> │Reduce → Sink │
│(파이프라인)  │ 셔플  │(파이프라인)   │
└─────────────┘       └──────────────┘

Region 내: ForwardPartitioner (네트워크 불필요, 동시 실행 필수)
Region 간: HashPartitioner (네트워크 셔플, 독립 스케줄링 가능)
```

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/strategy/PipelinedRegionSchedulingStrategy.java

public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;
    private final SchedulingTopology schedulingTopology;

    // 모든 Pipelined Region
    private final Set<SchedulingPipelinedRegion> regions;

    @Override
    public void startScheduling() {
        // ★ Source가 포함된 Region부터 스케줄링 시작
        final Set<SchedulingPipelinedRegion> sourceRegions =
                IterableUtils.toStream(schedulingTopology.getAllPipelinedRegions())
                        .filter(this::isSourceRegion)
                        .collect(Collectors.toSet());

        maybeScheduleRegions(sourceRegions);
    }
}
```

### maybeScheduleRegions() — Region 스케줄링

```java
private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {
    final List<SchedulingPipelinedRegion> regionsSorted =
            SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                    schedulingTopology, regions);

    for (SchedulingPipelinedRegion region : regionsSorted) {
        // ★ 이 Region의 모든 입력 데이터가 준비되었는지 확인
        if (areRegionInputsAllConsumable(region)) {
            // 모든 입력 준비됨 → 스케줄링!
            schedulerOperations.allocateSlotsAndDeploy(
                    regionVerticesToSchedule(region));
        }
        // 입력이 아직 준비되지 않으면 대기
        // (업스트림 Region이 완료되면 콜백으로 재시도)
    }
}
```

### allocateSlotsAndDeploy() — Slot 할당 및 배포

```java
// DefaultScheduler.java
@Override
public void allocateSlotsAndDeploy(final List<ExecutionVertexID> verticesToDeploy) {

    // ① 버전 기록 — 동시 수정 감지를 위한 Vertex 버전 추적
    final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
            executionVertexVersioner.recordVertexModifications(verticesToDeploy);

    // ② 배포할 Execution 수집
    final List<Execution> executionsToDeploy =
            verticesToDeploy.stream()
                    .map(this::getCurrentExecutionOfVertex)
                    .collect(Collectors.toList());

    // ③ ★ ExecutionDeployer에게 Slot 할당 및 배포 위임
    executionDeployer.allocateSlotsAndDeploy(executionsToDeploy, requiredVersionByVertex);
}
```

### deployTaskSafe() — 실제 배포

```java
private void deployTaskSafe(final ExecutionVertexID executionVertexId) {
    try {
        // ★ ExecutionVertex의 현재 Execution을 배포
        // 이것이 TaskExecutor.submitTask()를 호출합니다
        executionOperations.deploy(executionVertexId);
    } catch (Exception e) {
        handleTaskDeploymentFailure(executionVertexId, e);
    }
}
```

---

## 5.4 SlotPool — Slot 관리

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/slotpool/DeclarativeSlotPoolService.java

// Declarative Slot Pool (선언적 Slot 풀):
// JobMaster가 "이 Job에 N개의 Slot이 필요합니다"라고 선언하면,
// ResourceManager가 가용한 Slot을 할당합니다.

// ★ Slot Sharing:
// 같은 Slot Sharing Group에 속한 다른 Task들이 하나의 Slot을 공유합니다.
// 기본적으로 모든 Task가 같은 그룹에 속하므로,
// 하나의 파이프라인(Source → Map → Reduce → Sink)이 하나의 Slot에서 실행될 수 있습니다.
```

**Slot Sharing의 의미:**

```
Slot 1:                     Slot 2:
┌────────────────────┐     ┌────────────────────┐
│ Source[0]          │     │ Source[1]          │
│ Map[0]             │     │ Map[1]             │
│ Reduce[0]          │     │ Reduce[1]          │
│ Sink[0]            │     │ Sink[1]            │
└────────────────────┘     └────────────────────┘

하나의 Slot에 전체 파이프라인의 서브태스크가 들어감
→ 필요한 Slot 수 = max(parallelism) (모든 연산자의 최대 병렬도)
→ 네트워크 통신 최소화 (같은 JVM 내 메모리 전달 가능)
```

---

## 5.5 Failover — 장애 복구

### RestartPipelinedRegionFailoverStrategy

장애 발생 시, 실패한 Task가 속한 **Pipelined Region**만 재시작합니다.

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/failover/RestartPipelinedRegionFailoverStrategy.java

public class RestartPipelinedRegionFailoverStrategy implements FailoverStrategy {

    private final SchedulingTopology topology;

    @Override
    public Set<ExecutionVertexID> getTasksNeedingRestart(
            ExecutionVertexID executionVertexId,
            Throwable cause) {

        // ① 실패한 Task가 속한 Pipelined Region 찾기
        SchedulingPipelinedRegion failedRegion =
                topology.getPipelinedRegionOfVertex(executionVertexId);

        // ② 이 Region과 영향받는 다운스트림 Region 모두 찾기
        Set<SchedulingPipelinedRegion> regionsToRestart =
                getAllRegionsToRestart(failedRegion);

        // ③ 재시작할 모든 Task 수집
        Set<ExecutionVertexID> tasksToRestart = new HashSet<>();
        for (SchedulingPipelinedRegion region : regionsToRestart) {
            for (SchedulingExecutionVertex vertex : region.getVertices()) {
                tasksToRestart.add(vertex.getId());
            }
        }

        return tasksToRestart;
    }
}
```

**Failover 범위:**

```
Region 1         Region 2         Region 3
┌─────────┐     ┌──────────┐    ┌──────────┐
│Source→Map│ ──> │Reduce    │ ──>│Sink      │
└─────────┘     └──────────┘    └──────────┘

Reduce에서 장애 발생 시:
- Region 2 재시작 (Reduce)
- Region 3 재시작 (Reduce의 출력을 소비하므로)
- Region 1은 재시작 불필요 (중간 결과가 아직 유효하면)
```

### ExecutionFailureHandler — 장애 처리 핸들러

```java
// 파일: flink-runtime/src/main/java/org/apache/flink/runtime/scheduler/exceptionhistory/
//       및 DefaultScheduler.java 내부

// DefaultScheduler에서 장애 발생 시:
private void handleTaskFailure(
        final ExecutionVertexID executionVertexId,
        final Throwable error) {

    // ① Failover 전략에 재시작 대상 요청
    final Set<ExecutionVertexID> verticesToRestart =
            failoverStrategy.getTasksNeedingRestart(executionVertexId, error);

    // ② 재시작 전략에 따라 딜레이 결정
    final RestartBackoffTimeStrategy.CanRestart canRestart =
            restartBackoffTimeStrategy.canRestart();

    if (canRestart.isCanRestart()) {
        // ★ 지정된 딜레이 후 재시작
        long delay = canRestart.getBackoffTime();
        delayExecutor.schedule(
                () -> restartTasks(verticesToRestart),
                delay, TimeUnit.MILLISECONDS);
    } else {
        // ★ 재시작 횟수 초과 → Job 실패
        failJob(error);
    }
}
```

### RestartBackoffTimeStrategy — 재시작 전략

```java
// 재시작 전략 종류:

// 1. FixedDelayRestartBackoffTimeStrategy
//    → 고정 횟수만큼 고정 간격으로 재시작
//    예: 최대 3번, 10초 간격

// 2. ExponentialDelayRestartBackoffTimeStrategy
//    → 지수적으로 증가하는 간격으로 재시작
//    예: 1초 → 2초 → 4초 → 8초 ...

// 3. NoRestartBackoffTimeStrategy
//    → 재시작 없음 (장애 즉시 Job 실패)

// 4. InfiniteDelayRestartBackoffTimeStrategy
//    → 무한 재시작 (보통 프로덕션 환경)
```

---

## 5.6 설계 배경: 스케줄러 관련 FLIP

> **[FLIP-6: Flink Deployment and Process Model](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65147077)**
> Flink 1.5에서 스케줄러와 배포 모델의 근본적 재설계. 동적 리소스 획득/해제, "세션" vs "단일 Job" 클러스터 분리.

> **[FLIP-160: Adaptive Scheduler](https://cwiki.apache.org/confluence/display/FLINK/FLIP-160:+Adaptive+Scheduler)**
> 가용 Slot 수에 따라 Job의 병렬도를 **자동으로 조정**하는 적응형 스케줄러.
> 리소스가 부족하면 병렬도를 줄이고, 새 Slot이 추가되면 병렬도를 높입니다.
> Reactive Mode(FLIP-159)와 결합하여 탄력적 스케일링 가능.

> **[FLIP-187: Adaptive Batch Scheduler](https://cwiki.apache.org/confluence/display/FLINK/FLIP-187:+Adaptive+Batch+Scheduler)**
> 배치 Job 전용. 중간 데이터 크기를 기반으로 다운스트림 병렬도를 **동적으로 결정**.
> FLIP-283에 의해 배치 Job의 기본 스케줄러로 설정.

### RestartBackoffTimeStrategy 인터페이스

```java
// 파일: flink-runtime/.../executiongraph/failover/RestartBackoffTimeStrategy.java

public interface RestartBackoffTimeStrategy {
    // 재시작 가능 여부
    boolean canRestart();
    // 재시작까지 대기 시간 (밀리초)
    long getBackoffTime();
    // 장애 통지
    boolean notifyFailure(Throwable cause);
}
```

**구현체:**
| 전략 | 설명 | 설정 |
|------|------|------|
| `FixedDelayRestartBackoffTimeStrategy` | 고정 횟수, 고정 간격 | `restart-strategy: fixed-delay` |
| `ExponentialDelayRestartBackoffTimeStrategy` | 지수 증가 간격 | `restart-strategy: exponential-delay` |
| `NoRestartBackoffTimeStrategy` | 재시작 없음 | `restart-strategy: none` |
| `InfiniteDelayRestartBackoffTimeStrategy` | 무한 재시작 | 기본값 (체크포인트 활성화 시) |

---

## 5.7 스케줄링 전체 흐름

```
① JobMaster.startJobExecution()
    │
    ▼
② DefaultScheduler.startScheduling()
    │
    ▼
③ PipelinedRegionSchedulingStrategy.startScheduling()
    │  Source Region부터 시작
    │
    ▼
④ allocateSlotsAndDeploy()
    │
    ├── ExecutionSlotAllocator: SlotPool에서 Slot 요청
    │     │
    │     ├── Slot 있음 → 즉시 할당
    │     └── Slot 없음 → ResourceManager에 요청
    │           │
    │           └── TaskManager 시작 → Slot 확보 → 콜백으로 할당
    │
    ▼
⑤ deploy() → TaskExecutor.submitTask()
    │
    ▼
⑥ Task.run() → StreamTask.invoke()
    │
    ▼
⑦ 데이터 처리 시작
    │
    │ (장애 발생 시)
    ▼
⑧ handleTaskFailure()
    │
    ├── failoverStrategy.getTasksNeedingRestart()  → 재시작 범위 결정
    ├── restartBackoffTimeStrategy.canRestart()      → 재시작 가능 여부
    └── restartTasks()                               → 체크포인트에서 복구 후 재시작
```

---

## 5.8 핵심 정리

1. **DefaultScheduler**: 스케줄링의 총괄. SchedulingStrategy + SlotAllocator + FailoverStrategy 조합
2. **PipelinedRegion**: 파이프라인으로 연결된 Task 그룹. 동시 실행이 필수
3. **Slot Sharing**: 다른 연산자의 서브태스크가 하나의 Slot 공유. 리소스 효율성 극대화
4. **Region-based Failover**: 실패한 Region과 영향받는 다운스트림 Region만 재시작
5. **Restart Strategy**: 고정 딜레이, 지수 딜레이, 무한 재시작 등 유연한 복구 전략

---

## 다음 단계

Phase 6에서는 Task 간 데이터가 **네트워크를 통해 어떻게 교환**되는지,
특히 Netty 기반 네트워크 스택과 백프레셔 메커니즘을 코드 레벨에서 추적합니다.
