# Architecture Snapshot

> **인덱싱 일시**: 2026-04-26
> **도구**: `codebase-memory-mcp` v0.10.0 (binary)
> **모드**: `fast` (구조 + 호출/사용/계승 엣지 포함, semantic 엣지 제외)
> **인덱싱 방식**: **모듈별 분할 인덱싱** (전체 단일 인덱싱은 dump 단계에서 native segfault 재현됨, 약 400k 노드 임계점에서 발생)

이 문서는 `codebase-memory-mcp`로 색인된 18개 프로젝트의 구조 스냅샷이다. 향후 `search_graph` / `trace_path` / `query_graph` 호출 시 `project` 파라미터로 사용할 정확한 프로젝트명, 그래프 규모, 노드 라벨/엣지 타입 분포를 기록한다.

---

## 인덱싱된 프로젝트 (18개)

### Flink 본 레포 (14개 모듈, Tier 0/1)

| 모듈 | MCP project name | 노드 | 엣지 | 디스크 |
|------|------------------|-----:|-----:|------:|
| `flink-runtime` | `home-donamk-code-flink-flink-runtime` | 100,429 | 333,195 | 256.6 MB |
| `flink-core` | `home-donamk-code-flink-flink-core` | 21,813 | 61,404 | 50.6 MB |
| `flink-connectors` | `home-donamk-code-flink-flink-connectors` | 15,368 | 44,106 | 40.3 MB |
| `flink-state-backends` | `home-donamk-code-flink-flink-state-backends` | 8,259 | 21,574 | 22.4 MB |
| `flink-formats` | `home-donamk-code-flink-flink-formats` | 7,439 | 17,770 | 17.6 MB |
| `flink-streaming-java` | `home-donamk-code-flink-flink-streaming-java` | 6,140 | 17,804 | 17.9 MB |
| `flink-libraries` | `home-donamk-code-flink-flink-libraries` | 4,071 | 15,606 | 13.2 MB |
| `flink-kubernetes` | `home-donamk-code-flink-flink-kubernetes` | 2,989 | 7,666 | 8.9 MB |
| `flink-clients` | `home-donamk-code-flink-flink-clients` | 2,541 | 6,768 | 7.6 MB |
| `flink-core-api` | `home-donamk-code-flink-flink-core-api` | 2,082 | 3,480 | 5.4 MB |
| `flink-rpc` | `home-donamk-code-flink-flink-rpc` | 1,809 | 4,070 | 5.6 MB |
| `flink-datastream` | `home-donamk-code-flink-flink-datastream` | 1,611 | 4,178 | 5.8 MB |
| `flink-dstl` | `home-donamk-code-flink-flink-dstl` | 832 | 1,993 | 3.4 MB |
| `flink-datastream-api` | `home-donamk-code-flink-flink-datastream-api` | 550 | 969 | 2.6 MB |

**소계**: 175,933 노드 / 540,583 엣지

### 외부 레포 (`external/`, 4개)

| 레포 | MCP project name | 노드 | 엣지 | 디스크 |
|------|------------------|-----:|-----:|------:|
| `iceberg` (sparse: flink/) | `home-donamk-code-flink-external-iceberg` | 64,701 | 292,451 | 199.0 MB |
| `polaris` | `home-donamk-code-flink-external-polaris` | 24,798 | 80,964 | 66.6 MB |
| `flink-kubernetes-operator` | `home-donamk-code-flink-external-flink-kubernetes-operator` | 10,135 | 30,874 | 32.1 MB |
| `flink-connector-kafka` | `home-donamk-code-flink-external-flink-connector-kafka` | 6,096 | 15,626 | 18.1 MB |

**소계**: 105,730 노드 / 419,915 엣지

### **전체**: 18 projects, **281,663 노드**, **960,498 엣지** (디스크 ~774 MB)

---

## 학습 영역 → 프로젝트 매핑

문서 작성 시 어떤 프로젝트를 쿼리할지 결정하는 표:

| dkdocs 카테고리 | 1차 프로젝트 | 보조 프로젝트 |
|----------------|-------------|--------------|
| `02-job-fundamentals/` | `flink-runtime` (StreamExecutionEnvironment), `flink-core` (PipelineExecutor SPI) | `flink-clients`, `flink-kubernetes` |
| `03-graph-transformation/` | `flink-runtime`, `flink-streaming-java` | `flink-core-api` |
| `04-runtime-architecture/` | `flink-runtime` | `flink-rpc` |
| `05-state-checkpoint/` | `flink-runtime`, `flink-state-backends` | `flink-dstl`, `flink-core` |
| `06-source-sink-spi/` | `flink-core`, `flink-connectors`, `flink-streaming-java` | `external-iceberg`, `external-flink-connector-kafka` |
| `07-filesystem-checkpoint-store/` | `flink-core`, `flink-runtime` | (S3 fs 모듈은 미인덱싱 — 필요 시 추가) |
| `08-file-formats/` | `flink-formats`, `flink-connectors` | — |
| `09-kubernetes-integration/` | `flink-kubernetes` | `external-flink-kubernetes-operator` |
| `10-scheduling-failover/` | `flink-runtime` | — |
| `11-network-shuffle/` | `flink-runtime` | — |
| `12-watermark-time/` | `flink-streaming-java`, `flink-core` | `flink-runtime` |
| `13-new-datastream-v2/` | `flink-datastream-api`, `flink-datastream` | `flink-streaming-java` |
| `99-external-references/` | `external-iceberg`, `external-polaris`, `external-flink-kubernetes-operator`, `external-flink-connector-kafka` | — |

> **주의**: 위 표의 모든 프로젝트명에는 실제 사용 시 `home-donamk-code-flink-` prefix가 붙는다 (예: `home-donamk-code-flink-flink-runtime`, `home-donamk-code-flink-external-iceberg`). 위에선 가독성을 위해 prefix 생략. **정확한 이름은 위 두 표 참조**.

> **주의**: 단일 프로젝트 내 `search_graph` / `trace_path`만 가능. 프로젝트 경계를 넘는 호출 추적은 두 프로젝트에서 각각 검색 후 수동 매칭 필요.

---

## 그래프 스키마 (`flink-runtime` 기준 — 가장 풍부한 샘플)

### Node labels (`Method` 43k, `Class` 6k가 핵심)

| Label | Count | 용도 |
|-------|------:|------|
| `Method` | 43,179 | 메서드 정의 |
| `Field` | 18,748 | 필드/멤버 변수 |
| `Variable` | 17,777 | 로컬 변수 |
| `Class` | 6,244 | 클래스 정의 |
| `File` | 4,972 | 파일 노드 |
| `Module` | 4,972 | 모듈/네임스페이스 |
| `Function` | 2,897 | 정적/lambda 함수 |
| `Interface` | 823 | 인터페이스 |
| `Folder` | 556 | 디렉토리 |
| `Enum` | 209 | 열거형 |
| `Route` | 51 | HTTP/RPC 엔드포인트 |
| `Project` | 1 | 프로젝트 자체 |

### Edge types

| Type | Count | 의미 |
|------|------:|------|
| `CALLS` | 106,342 | 메서드 호출 — `trace_path(mode=calls)`의 토대 |
| `DEFINES` | 94,849 | 컨테이너→정의 |
| `USAGE` | 71,192 | 변수/필드 사용 |
| `DEFINES_METHOD` | 43,458 | 클래스→메서드 |
| `TESTS` | 10,486 | 테스트→피테스트 대상 |
| `CONTAINS_FILE` | 4,972 | 폴더→파일 |
| `WRITES` | 820 | 변수 쓰기 — `data_flow` 모드에서 활용 |
| `INHERITS` | 123 | 상속 관계 |
| `HTTP_CALLS` | 46 | HTTP 호출 (cross_service 모드) |

### `fast` 모드 미포함 엣지 (참고)

- `SIMILAR_TO`, `SEMANTICALLY_RELATED`: `moderate` 또는 `full` 모드 필요. 현 구성에서는 미수집. 필요 시 향후 모듈별로 재인덱싱 가능 (모듈당 여전히 단일 dump가 가능한지는 별도 검증 필요).
- `git history` 관련 엣지: `fast` 모드는 skip (`pass.skip pass=githistory reason=fast_mode`).

---

## 사용 예시 (CLI 모드)

`codebase-memory-mcp` 바이너리는 MCP stdio뿐 아니라 직접 CLI 호출도 지원한다. 디버깅이나 일회성 쿼리 시 유용:

```bash
BIN=/home/donamk/.npm/_npx/f6b5eb6a05eb020a/node_modules/codebase-memory-mcp/bin/codebase-memory-mcp

# 클래스 검색
$BIN cli search_graph '{"project":"home-donamk-code-flink-flink-runtime","name_pattern":"CheckpointCoordinator","limit":5}'

# 호출 그래프 (function_name은 단순 이름 또는 qn — 정확한 형식은 Phase A 첫 문서에서 확정)
$BIN cli trace_path '{"function_name":"CheckpointCoordinator.triggerCheckpoint","project":"home-donamk-code-flink-flink-runtime","direction":"outbound","depth":2,"mode":"calls"}'

# 프로젝트 메타
$BIN cli get_architecture '{"project":"home-donamk-code-flink-flink-runtime","aspects":["all"]}'
```

평소 사용은 Claude Code의 MCP 통합(`mcp__codebase-memory-mcp__*` 도구)을 통해서가 정상.

---

## 알려진 이슈 / 향후 조치

1. **단일 인덱싱 segfault**: Flink 전체(13,989 java + non-java) 단일 인덱싱은 `route_match` 직후 `gbuf.dump`에서 native segfault. 약 400k 노드 임계로 추정. 모듈 분할로 회피.
2. **`trace_path` qn 형식 미확정**: `name_pattern`만으로 검색은 잘 되나 `trace_path`의 `function_name` 정확한 형식 (단순 메서드명 vs `Class.method` vs FQN)을 첫 Phase A 문서 작성 시 확정 필요.
3. **인덱싱 누락 모듈**: `flink-filesystems/*` (S3/MinIO 분석 시 필요), `flink-table` (별도 코스). 필요 시 동일한 방식으로 추가 인덱싱.
4. **`mode=full` / `moderate` 시도 미완**: `fast` 모드는 `SIMILAR_TO` / `SEMANTICALLY_RELATED` 엣지를 제공하지 않음. 작은 모듈에 한해 향후 재인덱싱 가능.

---

## 재인덱싱이 필요한 시점

- Flink 코드 변경 후 (예: PR 작업 중) → 해당 모듈만 재인덱싱: `cli index_repository '{"repo_path":"...","mode":"fast"}'`
- 외부 레포 업데이트 후 (`external/clone.sh` 실행 후) → 변경된 외부 레포만 재인덱싱
- 스키마 활용 변경 시 (예: `moderate` 모드 시도) → 해당 프로젝트만 재인덱싱
