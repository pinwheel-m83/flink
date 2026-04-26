# 02 — Job 기초 (Job Fundamentals)

> **다루는 영역**: 사용자가 작성한 Flink 코드의 **client 측 Job 제출 흐름**. 즉 `env.execute()`가 어떻게 `PipelineExecutor`를 거쳐 클러스터까지 도달하는가.
> **다루지 않는 영역**: `StreamGraph` 내부 구조 (→ [`03-graph-transformation/`](../03-graph-transformation/)), 클러스터 측 동작 (→ [`04-runtime-architecture/`](../04-runtime-architecture/))

## 문서 목록

| # | 문서 | 한 줄 요약 |
|---|------|----------|
| 01 | [`01-execute-entrypoint.md`](./01-execute-entrypoint.md) | `env.execute()` → SPI 로 `PipelineExecutor` 선택 → 클러스터 제출 → `JobClient` 반환 |

## 사전에 한 번 읽어둘 자료

- [`../00-overview/architecture-snapshot.md`](../00-overview/architecture-snapshot.md) — 인덱싱된 모듈/프로젝트 매핑
- [`../00-overview/my-environment-map.md`](../00-overview/my-environment-map.md) — 본인 운영 환경(K8s + Operator + Kafka + MinIO + Iceberg/Polaris) ↔ Flink 컴포넌트 매핑
- [`../00-overview/module-map.md`](../00-overview/module-map.md) — Tier별 모듈 우선순위
