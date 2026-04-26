# Flink Kubernetes Operator (apache/flink-kubernetes-operator)

> **외부 레포**: `external/flink-kubernetes-operator/`
> **Flink 측 진입점**: `flink-kubernetes` 모듈의 entrypoint 두 종류 + REST API.

---

## 1. 무엇인가

Apache Flink K8s Operator = K8s에서 Flink 잡 lifecycle을 관리하는 Operator (Custom Resource Definition + controller). 본인 환경의 잡 배포·업그레이드·스케일링 표준 도구.

## 2. 책임 분담 (자세한 내용은 [`../09-kubernetes-integration/05-operator-flink-boundary.md`](../09-kubernetes-integration/05-operator-flink-boundary.md))

| Operator 측 | Flink 본 레포 (flink-kubernetes) |
|----------|------------------------------|
| `FlinkDeployment` / `FlinkSessionJob` CR watch | |
| JM/TM Pod, ConfigMap, Service, RBAC 생성 | |
| Autoscaler (메트릭 → REST PATCH) | |
| Savepoint trigger (REST POST) | |
| Upgrade (savepoint → 재시작) | |
| | KubernetesSessionClusterEntrypoint, KubernetesApplicationClusterEntrypoint |
| | KubernetesResourceManagerDriver (TM Pod 요청) |
| | ConfigMap 기반 HA (KubernetesLeaderElectionDriver) |
| | AdaptiveScheduler (REST PATCH 받아 ExecutionGraph 재구성) |

## 3. 외부 레포 진입점

```
external/flink-kubernetes-operator/
├── flink-kubernetes-operator/src/main/java/org/apache/flink/kubernetes/operator/
│   ├── controller/
│   │   ├── FlinkDeploymentController.java   # ★ CR reconciliation 메인
│   │   ├── FlinkSessionJobController.java
│   │   └── ...
│   ├── reconciler/                           # 잡 lifecycle 관리
│   │   ├── deployment/
│   │   ├── session/
│   │   └── ...
│   └── ...
└── flink-autoscaler/                         # ★ autoscaler 모듈
    └── src/main/java/org/apache/flink/autoscaler/
        ├── JobAutoScalerImpl.java
        ├── ScalingMetricCollector.java
        └── ...
```

본인 환경 디버깅 시:
- 잡이 안 뜸 → `FlinkDeploymentController` 측 reconciliation log
- Autoscale 안 됨 → `JobAutoScalerImpl` log + Flink REST 응답 확인

## 4. Operator REST 호출 (Flink 측)

Operator가 잡을 제어할 때 사용하는 Flink REST endpoint:
- `GET /jobs/<id>` — 잡 상태
- `GET /jobs/<id>/metrics?get=...` — 메트릭 (autoscaler가 폴링)
- `POST /jobs/<id>/savepoints` — savepoint trigger
- `PATCH /jobs/<id>/resource-requirements` — parallelism 변경 (FLIP-291)
- `PATCH /jobs/<id>?mode=cancel` — cancel

이 REST endpoint들은 모두 Flink 본 레포 측 `DispatcherGateway` / `JobMasterGateway`가 처리 ([`../04-runtime-architecture/01-dispatcher.md`](../04-runtime-architecture/01-dispatcher.md), [`../04-runtime-architecture/03-job-master.md`](../04-runtime-architecture/03-job-master.md)).

## 5. CR 예시

[`../09-kubernetes-integration/06-pod-template-and-config.md`](../09-kubernetes-integration/06-pod-template-and-config.md) 의 권장 CR 참조.
