# Operator ↔ Flink 경계

> **요약**: Apache Flink K8s Operator(외부 레포)는 CR을 watch해 K8s 리소스를 생성/관리. Flink 본 레포의 `flink-kubernetes` 모듈이 그 안에서 동작 — autoscaler/savepoint 등 Operator 기능은 Flink REST API를 호출해 수행.
> **외부 레포**: `external/flink-kubernetes-operator/`

---

## 1. 책임 분담

| 책임 | Operator | Flink 본 레포 (flink-kubernetes) |
|------|---------|------------------------------|
| FlinkDeployment CR watch | ✓ | |
| JM/TM Pod, ConfigMap, Service, RBAC 생성 | ✓ | |
| 두 entrypoint 중 하나를 컨테이너 command로 지정 | ✓ | |
| 잡 lifecycle 관리 (create, suspend, upgrade) | ✓ | |
| Savepoint trigger (REST 호출) | ✓ | |
| Autoscaler (메트릭 수집 → REST PATCH) | ✓ | |
| | | |
| 두 entrypoint의 main() | | ✓ |
| Dispatcher / RM / JobMaster 동작 | | ✓ |
| `KubernetesResourceManagerDriver` (TM Pod 요청) | | ✓ |
| K8s ConfigMap 기반 HA, leader election | | ✓ |
| AdaptiveScheduler (slot 변동에 적응) | | ✓ |

---

## 2. CR(`FlinkDeployment`) 예시

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: my-streaming-job
  namespace: flink-jobs
spec:
  image: flink:2.0.0
  flinkVersion: v2_0
  flinkConfiguration:
    state.backend.type: rocksdb
    state.checkpoints.dir: s3://flink-checkpoints/
    s3.endpoint: https://minio.example.com
    high-availability.type: kubernetes
    high-availability.storageDir: s3://flink-ha/
    jobmanager.scheduler: adaptive
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2g"
      cpu: 1
  taskManager:
    resource:
      memory: "4g"
      cpu: 2
    podTemplate: { ... }
  job:
    jarURI: local:///opt/flink/usrlib/my-app.jar
    parallelism: 8
    upgradeMode: savepoint
    state: running
  podTemplate: { ... }
```

Operator가 이 CR을 watch → JM Pod (with ApplicationMode entrypoint) + Service + ConfigMaps + RBAC 생성.

---

## 3. 흐름 — 잡 시작

```
1. 사용자: kubectl apply -f flinkdeployment.yaml
2. Operator: CR 감지 → Reconciliation
3. Operator: ServiceAccount, Role, RoleBinding 생성
4. Operator: ConfigMap (flink-conf.yaml, log4j 등) 생성
5. Operator: JM Pod (with KubernetesApplicationClusterEntrypoint) 생성
6. JM Pod: entrypoint main() → ClusterEntrypoint.runClusterEntrypoint
7. Flink: KubernetesResourceManagerFactory → KubernetesResourceManagerDriver
8. Flink: HA services 시작 → ConfigMap leader election
9. Flink: leader 획득 → MiniDispatcher → 사용자 main() 실행
10. 사용자 코드: env.execute() → EmbeddedExecutor → 잡 실행
11. Flink: ResourceRequirements 선언 → driver → fabric8 → K8s에 TM Pod 요청
12. TM Pod 생성 → TaskExecutor 등록 → Task 배치
```

Operator는 5번까지 + 모니터링. 6~12는 Flink 자체 동작.

---

## 4. Operator의 Autoscaler ↔ Flink AdaptiveScheduler

```
[Operator의 Autoscaler]
  Flink REST `/jobs/<id>/metrics?get=...` 폴링
  ↓
  utilization, busy time, backpressure 분석
  ↓
  새 parallelism 결정 (e.g. 8 → 16)
  ↓
  Flink REST `PATCH /jobs/<id>/resource-requirements` (FLIP-291)
  
[Flink AdaptiveScheduler]
  새 ResourceRequirements 받음
  ↓
  새 ExecutionGraph 빌드 (parallelism=16)
  ↓
  ResourceManager에 ResourceRequirements 선언
  ↓
  SlotManager: 부족 → driver.requestResource → 새 TM Pod 요청
  ↓
  새 Pod 시작 → 새 ExecutionGraph로 transition
```

→ Operator-측 메트릭 → REST PATCH → Flink-측 ExecutionGraph 재구성 → Pod 변화. 양측이 잘 분리되어 있음.

---

## 5. 잡 upgrade

CR의 `spec.image` 변경 시:
1. Operator가 변경 감지 → savepoint trigger (REST POST `/jobs/<id>/savepoints`)
2. savepoint 완료 후 잡 cancel
3. 새 image로 JM Pod 재생성
4. 새 JM이 savepoint에서 시작

`upgradeMode: savepoint` 시 위 흐름. `upgradeMode: last-state` 시 마지막 retained checkpoint 사용 — 더 빠르지만 일부 상태 손실 위험.

---

## 6. 다음

- Pod template & config 운영: [`./06-pod-template-and-config.md`](./06-pod-template-and-config.md)
