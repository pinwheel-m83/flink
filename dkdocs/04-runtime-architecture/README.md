# 04 — Runtime Architecture

> **다루는 영역**: cluster 측 핵심 컴포넌트 — Dispatcher, ResourceManager, JobMaster, TaskExecutor, StreamTask, RPC.
> **선행**: [`../03-graph-transformation/`](../03-graph-transformation/) (Job/ExecutionGraph 빌드)
> **후속**: [`../05-state-checkpoint/`](../05-state-checkpoint/), [`../10-scheduling-failover/`](../10-scheduling-failover/)

## 컴포넌트 관계 (개관)

```
[클러스터 1개당]
ResourceManager ──── 슬롯 인벤토리 + 외부 자원 협상 (K8s/YARN driver)
       ↑
       │ register/declareResources
       ↓
Dispatcher ──── 잡 접수처, 잡당 JobManagerRunner spawn
       ↓
[잡 1개당]
JobManagerRunner ──── leader election + JobMaster lifecycle
       ↓
JobMaster ──── ExecutionGraph 빌드 + Scheduler + 체크포인트 트리거
       ↓ (slot offers, submit task)
TaskExecutor ──── slot 호스팅, Task 실행
       ↓
StreamTask ──── mailbox 메인 루프, operator chain 호출
       ↓
StreamOperator ──── 실제 record processing
```

## 문서 목록

| # | 문서 | 한 줄 요약 |
|---|------|----------|
| 01 | [`01-dispatcher.md`](./01-dispatcher.md) | Dispatcher — 잡 접수, JobMaster spawn |
| 02 | [`02-resource-manager.md`](./02-resource-manager.md) | RM + SlotManager + KubernetesResourceManagerDriver |
| 03 | [`03-job-master.md`](./03-job-master.md) | JobMaster — 잡 1개의 매니저 |
| 04 | [`04-task-executor.md`](./04-task-executor.md) | TaskExecutor — slot 호스팅, Task 실행 |
| 05 | [`05-stream-task-mailbox.md`](./05-stream-task-mailbox.md) | StreamTask 메인 루프, mailbox 모델 |
| 06 | [`06-rpc-pekko.md`](./06-rpc-pekko.md) | Pekko Actor 기반 Flink RPC |
