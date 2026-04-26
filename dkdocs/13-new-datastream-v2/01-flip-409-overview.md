# FLIP-409 — Process Function Centric DataStream API V2

> **요약**: Flink 2.x의 차세대 DataStream API. 기존 API(map/filter/window 등)를 unified `ProcessFunction` 위에 재구성 — 더 명확한 모델, 더 적은 abstraction. 아직 진화 중, 본인 환경 도입은 후속 검토.
> **모듈**: `flink-datastream-api/`, `flink-datastream/`

---

## 1. TL;DR

기존 V1 DataStream API는 `MapFunction`, `FilterFunction`, `KeyedProcessFunction`, `WindowFunction` 등 다양한 사용자 함수 인터페이스 + 그 사이의 미묘한 차이로 학습 곡선이 가파름. V2는 모든 것을 **`ProcessFunction` 일원화**하여 record-by-record processing + state + timer + watermark를 단일 모델로. 새 잡 작성 시 V2 채택 가능, 기존 V1 잡은 유지 (병행 운영 가능).

---

## 2. 두 모듈

| 모듈 | 역할 |
|------|------|
| `flink-datastream-api` | V2 인터페이스 정의 |
| `flink-datastream` | V2 구현체 |

V1은 `flink-streaming-java` (또는 release-2.0의 `flink-runtime` 안 streaming 부분)에 있음.

---

## 3. V1 vs V2 비교

| | V1 | V2 |
|---|---|---|
| API 표면 | 다양한 function 인터페이스 | ProcessFunction 일원화 |
| Type inference | TypeHint 자주 필요 | 더 명시적 type binding |
| Async state | 별도 V2 API (FLIP-424) 필요 | V2와 같은 패키지에 통합 |
| 학습 곡선 | 높음 | 단순 |
| 안정성 | ★ Production 검증 | 진화 중 (Public Evolving) |
| 본인 환경 | 운영 잡 그대로 | 새 PoC 잡에서 시도 |

---

## 4. 본인 환경 의미

- **운영 잡**: V1 그대로 유지 — 안정성 검증된 API.
- **새 잡**: V2로 시작 시 향후 표준화 시 마이그레이션 부담 적음.
- **컨트리뷰션**: V2가 활발히 개발 중 → starter issue로 좋은 영역.

---

## 5. 13 카테고리 (이 문서로 충분)

V2는 진화 중이라 깊이 있는 internals 분석은 시기상조. 안정화 후 별도 문서 추가.

---

## 6. dkdocs 전체 마무리

```
00-overview/                       — 전체 가이드
01-java-prerequisites/             — (placeholder)
02-job-fundamentals/               — execute() 진입점
03-graph-transformation/           — Stream/Job/Execution Graph
04-runtime-architecture/           — Dispatcher/RM/JM/TE/StreamTask/RPC
05-state-checkpoint/               — Checkpoint/RocksDB/ForSt/Changelog
06-source-sink-spi/                — Source/Sink V2 + Iceberg/Kafka
07-filesystem-checkpoint-store/    — FS abstract + S3/MinIO
08-file-formats/                   — Parquet
09-kubernetes-integration/         — flink-kubernetes 모듈, Operator 경계
10-scheduling-failover/            — AdaptiveScheduler
11-network-shuffle/                — Netty stack overview
12-watermark-time/                 — EventTime, Watermark
13-new-datastream-v2/              — FLIP-409 (이 카테고리)
```

본인 환경(K8s + Operator + Kafka + MinIO + Iceberg/Polaris + Parquet)을 깊이 다룬 internals deep-dive 시리즈가 완성됨.
