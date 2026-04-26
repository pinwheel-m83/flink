# Flink Internals — Personal Deep-Dive Docs (`dkdocs`)

> **목표**: Flink internals를 코드 레벨에서 이해하여 컨트리뷰터가 되는 것
> **기준 브랜치**: `release-2.0` (Flink 2.0.x 계열)
> **대상 독자**: Java에는 익숙하지만 Flink internals 및 일부 기반 기술(Netty, Pekko 등)에 익숙하지 않은 본인
> **작성 우선순위**: 본인 운영 환경에 직결되는 영역부터 (K8s + Flink K8s Operator + Kafka + MinIO + Iceberg/Polaris + Parquet)

---

## 디렉토리 구조

| 카테고리 | 무엇을 다루는가 |
|----------|----------------|
| [`00-overview/`](./00-overview/) | 큰 그림 — 모듈 맵, 환경 매핑, 용어집 |
| [`01-java-prerequisites/`](./01-java-prerequisites/) | Flink가 사용하는 Java/JVM 사전 지식 (DataInputStream, ByteBuffer, Netty, Pekko 등) |
| [`02-job-fundamentals/`](./02-job-fundamentals/) | `env.execute()`부터 시작하는 Job 진입점 |
| [`03-graph-transformation/`](./03-graph-transformation/) | StreamGraph → JobGraph → ExecutionGraph 변환 |
| [`04-runtime-architecture/`](./04-runtime-architecture/) | Dispatcher / RM / JobMaster / TaskExecutor / StreamTask / RPC |
| [`05-state-checkpoint/`](./05-state-checkpoint/) | ★ 체크포인트, RocksDB, ForSt(PoC), Changelog, State V2 API |
| [`06-source-sink-spi/`](./06-source-sink-spi/) | ★ Source V2, Sink V2, Two-Phase Commit, Iceberg/Kafka 매핑 |
| [`07-filesystem-checkpoint-store/`](./07-filesystem-checkpoint-store/) | ★ FileSystem 추상화, RecoverableWriter, S3/MinIO multipart upload |
| [`08-file-formats/`](./08-file-formats/) | Parquet (vectorized reader, BulkWriter) |
| [`09-kubernetes-integration/`](./09-kubernetes-integration/) | ★ flink-kubernetes 모듈, K8s HA, Operator 경계 |
| [`10-scheduling-failover/`](./10-scheduling-failover/) | ★ AdaptiveScheduler (메인), DefaultScheduler (비교용), Failover 전략 |
| [`11-network-shuffle/`](./11-network-shuffle/) | Credit-based flow, Netty 스택, ResultPartition/InputGate |
| [`12-watermark-time/`](./12-watermark-time/) | Watermark, EventTime, Window |
| [`13-new-datastream-v2/`](./13-new-datastream-v2/) | FLIP-409 기반 신규 DataStream API |
| [`99-external-references/`](./99-external-references/) | 외부 레포(`flink-kubernetes-operator`, `iceberg-flink`, `flink-connector-kafka`, `polaris`)에서의 진입점 |

★ = 본인 환경 직결, 우선순위 최고

---

## 학습 진입 순서

```
Phase A (토대):  02 → 03 → 04
Phase B (환경):  05 → 07 → 06 → 08 → 09
Phase C (확장):  10 → 11 → 12 → 13
Phase D (선택):  Table/SQL (별도 코스, dkdocs 외부)
```

`01-java-prerequisites/`는 발견될 때마다 보강하는 횡단 카테고리. 본 문서를 읽다가 사전 지식이 필요한 항목이 나오면 해당 prerequisite 문서로 링크된다.

---

## 본인 환경 요약 (자세한 내용은 [`my-environment-map.md`](./00-overview/my-environment-map.md))

| 환경 | 사용 Flink 컴포넌트 | 외부 레포 |
|------|--------------------|----------|
| K8s 배포 | `flink-kubernetes`, K8s HA, AdaptiveScheduler | `apache/flink-kubernetes-operator` |
| Source: Kafka | Source V2 SPI (`flink-core/api/connector/source/`) + `flink-runtime/source/coordinator/` | `apache/flink-connector-kafka` |
| Sink: MinIO | `flink-filesystems/flink-s3-fs-*`, RecoverableWriter | — |
| Storage: Iceberg + Parquet | Sink V2 (`flink-core/api/connector/sink2/`), `flink-formats/flink-parquet/` | `apache/iceberg`, `apache/polaris` |

---

## 외부 레포

[`../external/`](../external/) (로컬에만 존재, `.git/info/exclude` 처리됨). [`external/README.md`](../external/README.md) 참고.

---

## 작성 규칙

1. 모든 신규 문서는 [`_template.md`](./_template.md)를 따른다.
2. 코드 인용은 반드시 `<file_path>:<line_number>` 형식 (예: `flink-runtime/.../StreamTask.java:425`).
3. 외부 레포 코드를 인용할 땐 `external/<repo>/.../File.java:42` 형식.
4. Java 사전 지식이 필요한 개념을 발견하면 `01-java-prerequisites/`에 보강하고 본문에서 앵커 링크.
5. FLIP / JIRA 인용은 항상 번호 + 링크.
6. 운영(production) 권장 여부와 실험적(experimental) 상태는 명시적으로 표시.

---

## 진행 상황

- [x] Phase 0: 골격 셋업 — `00-overview/` (architecture-snapshot, module-map, my-environment-map) + `_template.md` + 본 README
- [x] Phase A: 토대 (02-04) — 11 문서 (`02-job-fundamentals/`, `03-graph-transformation/`, `04-runtime-architecture/`)
- [x] Phase B: 환경 직결 (05, 07, 06, 08, 09) — 30 문서
- [x] Phase C: 확장 (10-13) — 13 문서
- [x] 99-external-references — 5 문서 (Iceberg/Polaris/Operator/Kafka 진입점 매핑)
- [ ] 01-java-prerequisites — 의도적 미작성. Java 사전지식은 본문 인라인으로 짧게 처리됨 (`CompletableFuture`, `ServiceLoader`, `ClassLoader`, `IdentityHashMap` 등). 별도 deep-dive 필요 시 후속 작성.

**현재 합계**: 66 문서, 약 10,700 라인 (2026-04-26 기준).

> 후속 작업 후보: ① Java prerequisites deep-dive, ② 컨트리뷰터 트랙(starter issue + FLIP bibliography 큐레이션), ③ 본인 환경 잡 실측 데이터로 각 문서 검증/보강.
