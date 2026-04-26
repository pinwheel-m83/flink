# 03 — Graph 변환 (Graph Transformation)

> **다루는 영역**: 사용자 `Transformation` 트리 → `StreamGraph` → `JobGraph` → `ExecutionGraph` 의 3단계 변환. Flink internals의 **50% 이상**을 이해하는 핵심 영역.
> **선행**: [`../02-job-fundamentals/01-execute-entrypoint.md`](../02-job-fundamentals/01-execute-entrypoint.md)
> **후속**: [`../04-runtime-architecture/`](../04-runtime-architecture/)

## 변환 단계 (개관)

```
Transformation 트리        ← 사용자 DataStream API의 산물 (logical)
       ↓ StreamGraphGenerator (client)
StreamGraph              ← Pipeline + ExecutionPlan 구현, executor에 전달되는 표현
       ↓ Dispatcher/JobMaster (cluster)
JobGraph                 ← operator chaining 후, JobVertex 단위 (vertex × parallelism = subtask)
       ↓ ExecutionGraphBuilder (cluster)
ExecutionGraph           ← runtime 인스턴스. ExecutionVertex × parallelism = Execution attempts
```

## 문서 목록

| # | 문서 | 한 줄 요약 |
|---|------|----------|
| 01 | [`01-stream-graph.md`](./01-stream-graph.md) | `Transformation` 트리 → `StreamGraph` (StreamNode/StreamEdge) — client 측 |
| 02 | `02-job-graph.md` (예정) | `StreamGraph` → `JobGraph` (operator chaining, JobVertex) |
| 03 | `03-execution-graph.md` (예정) | `JobGraph` → `ExecutionGraph` (subtask 단위 펼치기) |

## 사전에 한 번 읽어둘 자료

- [`../00-overview/architecture-snapshot.md`](../00-overview/architecture-snapshot.md) — 인덱싱된 모듈/프로젝트 매핑
- [`../00-overview/module-map.md`](../00-overview/module-map.md) — Tier별 모듈 우선순위
