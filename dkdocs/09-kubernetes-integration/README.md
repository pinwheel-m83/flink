# 09 — Kubernetes Integration

> **다루는 영역**: `flink-kubernetes` 모듈의 entrypoint, kubeclient, ResourceManagerDriver, HA(ConfigMap 기반), Operator와의 경계, podTemplate 구성. **본인 환경 핵심 영역**.
> **외부 레포**: [`../../external/flink-kubernetes-operator/`](../../external/flink-kubernetes-operator/) — Operator는 별도 레포

## 문서 목록

| # | 문서 | 한 줄 |
|---|------|------|
| 01 | [`01-entrypoints.md`](./01-entrypoints.md) | KubernetesSessionClusterEntrypoint vs KubernetesApplicationClusterEntrypoint |
| 02 | [`02-kubeclient-decorators.md`](./02-kubeclient-decorators.md) | FlinkKubeClient (fabric8) + 14개 decorator chain |
| 03 | [`03-k8s-resource-manager-driver.md`](./03-k8s-resource-manager-driver.md) | TM Pod 생성/회수, Pod state watch, autoscaler 연동 |
| 04 | [`04-k8s-ha-leader-election.md`](./04-k8s-ha-leader-election.md) | ConfigMap 기반 leader election, state handle store |
| 05 | [`05-operator-flink-boundary.md`](./05-operator-flink-boundary.md) | Operator(외부) ↔ Flink 책임 분담, autoscaler ↔ AdaptiveScheduler |
| 06 | [`06-pod-template-and-config.md`](./06-pod-template-and-config.md) | ★ 권장 podTemplate + flinkConfiguration 종합 운영 패턴 |
