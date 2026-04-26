# `KubernetesResourceManagerDriver` 깊이

> **요약**: 본인 환경의 RM 측 K8s driver — TM Pod 생성/회수, Pod state watch (autoscaler 연동의 기반).
> **모듈**: `flink-kubernetes/`
> **선행**: [`../04-runtime-architecture/02-resource-manager.md`](../04-runtime-architecture/02-resource-manager.md)

---

## 1. TL;DR

`KubernetesResourceManagerDriver` (`extends AbstractResourceManagerDriver<KubernetesWorkerNode>`)가 `ActiveResourceManager`의 driver 역할 — `requestResource(spec)` 호출 시 podTemplate + spec으로 Pod yaml 생성 → fabric8 K8s API에 POST → uploadId 비슷한 podName으로 추적. `watchTaskManagerPods()`가 Pod의 ADDED/MODIFIED/DELETED 이벤트를 stream으로 받아 `onWorkerTerminated` 콜백 → RM이 잡 자원 상태 갱신.

---

## 2. 핵심 코드 (앞서 04-runtime-architecture/02에서 다뤘으나 한 번 더)

`flink-kubernetes/.../KubernetesResourceManagerDriver.java:71-`:

```java
/** Implementation of {@link ResourceManagerDriver} for Kubernetes deployment. */
public class KubernetesResourceManagerDriver
        extends AbstractResourceManagerDriver<KubernetesWorkerNode> {

    private static final String TASK_MANAGER_POD_FORMAT = "%s-taskmanager-%d-%d";
    private static final Long TERMINATION_WAIT_SECOND = 5L;

    private final String clusterId;
    private final String webInterfaceUrl;
    private final FlinkKubeClient flinkKubeClient;
    
    /** Request resource futures, keyed by pod names. */
    private final Map<String, CompletableFuture<KubernetesWorkerNode>> requestResourceFutures;
    
    private long currentMaxAttemptId = 0;
    private long currentMaxPodId = 0;
    
    private CompletableFuture<KubernetesWatch> podsWatchOptFuture;
    private volatile boolean running;
    private FlinkPod taskManagerPodTemplate;

    @Override
    protected void initializeInternal() throws Exception {
        podsWatchOptFuture = watchTaskManagerPods();   // Pod state watch 시작
        final File podTemplateFile = KubernetesUtils.getTaskManagerPodTemplateFileInPod();
        if (podTemplateFile.exists()) {
            taskManagerPodTemplate = KubernetesUtils.loadPodFromTemplateFile(...);
        } else {
            taskManagerPodTemplate = new FlinkPod.Builder().build();
        }
    }
}
```

---

## 3. `requestResource` 흐름

```java
public CompletableFuture<KubernetesWorkerNode> requestResource(TaskExecutorProcessSpec spec) {
    // 1. 새 podName 생성 ({clusterId}-taskmanager-{attemptId}-{podId})
    final String podName = String.format(TASK_MANAGER_POD_FORMAT, clusterId, currentMaxAttemptId, ++currentMaxPodId);
    
    // 2. taskManagerPodTemplate base + spec(memory/CPU/env)으로 실제 Pod spec 생성
    //    (Decorator 체인 적용)
    final KubernetesPod taskManagerPod = createTaskManagerPodSpec(podName, spec);
    
    // 3. future 등록
    final CompletableFuture<KubernetesWorkerNode> future = new CompletableFuture<>();
    requestResourceFutures.put(podName, future);
    
    // 4. fabric8로 K8s API 호출
    flinkKubeClient.createTaskManagerPod(taskManagerPod)
        .whenComplete((ignore, throwable) -> {
            if (throwable != null) {
                requestResourceFutures.remove(podName);
                future.completeExceptionally(throwable);
            }
        });
    
    return future;
}
```

---

## 4. Pod state watch (`onPodEvent` 류)

```
KubernetesWatch (long-poll API)
   ↓ ADDED 이벤트
Pod이 PENDING → 무시 (아직 시작 안 됨)

   ↓ MODIFIED 이벤트
Pod이 RUNNING → onPodAdded → future complete (KubernetesWorkerNode)
Pod이 FAILED/SUCCEEDED → onWorkerTerminated → ActiveResourceManager에 알림

   ↓ DELETED 이벤트
Pod 삭제됨 → onWorkerTerminated → 워커 회수
```

`onWorkerTerminated`가 호출되면 `ActiveResourceManager`가 잡 자원 상태를 재계산해 필요 시 새 Pod 요청 (auto recovery).

---

## 5. Operator autoscaler 연동

```
[Operator]
  메트릭 수집 → 새 parallelism 결정 → REST PATCH (FLIP-291)
  ↓
[JM AdaptiveScheduler]
  새 ResourceRequirements 선언 → SlotManager
  ↓
[SlotManager]
  pending request 발생 → ResourceAllocator.declareResourceNeeded
  ↓
[ActiveResourceManager]
  driver.requestResource (또는 release)
  ↓
[KubernetesResourceManagerDriver]
  flinkKubeClient.createTaskManagerPod (or stopPod)
  ↓
[K8s API]
  Pod create/delete → autoscaler가 본 메트릭 변화 사이클 종결
```

---

## 6. 다음

- HA & leader election: [`./04-k8s-ha-leader-election.md`](./04-k8s-ha-leader-election.md)
- Operator-Flink 경계: [`./05-operator-flink-boundary.md`](./05-operator-flink-boundary.md)
