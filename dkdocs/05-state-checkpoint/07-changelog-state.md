# Changelog State Backend (FLIP-158)

> **요약**: 기존 backend(주로 RocksDB)를 wrapping해 모든 state 변경을 별도 changelog에 추가 기록 → 체크포인트는 changelog만 빠르게 영속화. 매우 짧은 체크포인트 interval이 필요한 잡에 유용. 본인 환경 (30s interval)에선 필수 아님.
> **모듈**: `flink-state-backends/flink-statebackend-changelog/`, `flink-dstl/`
> **기준 버전**: Flink `release-2.0`
> **운영 권장 여부**: ⭐ Production (특정 시나리오 — 짧은 interval/낮은 latency 필요 시)
> **선행**: [`./04-rocksdb-state-backend.md`](./04-rocksdb-state-backend.md)

---

## 1. TL;DR (3문장)

`ChangelogStateBackend`는 다른 backend(예: RocksDB)를 delegate로 들고 있고, 매 state put/update를 **changelog**(append-only log)에 추가 기록 → checkpoint 시 RocksDB SST 업로드 대신 **changelog의 marker만 영속화**하면 됨 → 체크포인트 latency 매우 짧음 (수 백 ms). 단점: storage 사용량 증가, 복구 시 changelog 재생 필요. 본인 환경(30s interval, 일반적 throughput)은 RocksDB만으로 충분 — changelog는 필요 시 wrap만 하면 됨.

---

## 2. 사전 지식 / 핵심 클래스

| 역할 | 클래스 | 위치 |
|------|------|------|
| Wrapper backend | `ChangelogStateBackend` (`extends AbstractChangelogStateBackend`) | `flink-state-backends/flink-statebackend-changelog/.../ChangelogStateBackend.java` |
| Delegated backend (RocksDB 등) | `AbstractChangelogStateBackend.delegatedStateBackend` | 같은 모듈 |
| Changelog storage 추상 | `StateChangelogStorage` (Interface) | `flink-dstl/.../StateChangelogStorage.java` |
| FS 기반 changelog 구현 | `FsStateChangelogStorage` | `flink-dstl/flink-dstl-dfs/` |

---

## 3. 동작 원리

```
[기존 RocksDB만]                            [Changelog wrapping RocksDB]
state.update(v)                              state.update(v)
  ↓                                            ↓
RocksDB put (in-memory mem-table)             RocksDB put + changelog append
체크포인트 30s마다:                          체크포인트 5s마다:
  - flush mem-table → SST                     - changelog의 마지막 marker만 persist (수백 ms)
  - 새 SST 파일 MinIO 업로드                  
복구:                                        복구:
  - SST 다운로드 → ingest                     - 마지막 SST snapshot + changelog 재생
```

핵심 trade-off:
- **체크포인트 latency**: changelog가 훨씬 빠름
- **storage**: changelog는 누적량이 클 수 있음 (주기적 materialization으로 회수)
- **복구 시간**: changelog 재생 필요 → 큰 changelog면 느림

---

## 4. `ChangelogStateBackend` 코드

`flink-state-backends/flink-statebackend-changelog/.../ChangelogStateBackend.java:35-50`:

```java
/**
 * This state backend holds the working state in the underlying delegatedStateBackend, and forwards
 * state changes to State Changelog.
 */
@Internal
public class ChangelogStateBackend extends AbstractChangelogStateBackend
        implements ConfigurableStateBackend {

    ChangelogStateBackend(StateBackend stateBackend) {
        super(stateBackend);
    }

    @Override
    public StateBackend configure(ReadableConfig config, ClassLoader classLoader) {
        if (delegatedStateBackend instanceof ConfigurableStateBackend) {
            return new ChangelogStateBackend(
                    ((ConfigurableStateBackend) delegatedStateBackend).configure(config, classLoader));
        }
        return this;
    }
}
```

**delegate 패턴** — 모든 state operation은 underlying backend에 위임 + changelog append.

---

## 5. 활성화 설정

```yaml
# 기존 RocksDB 설정 그대로
state.backend.type: rocksdb
state.backend.incremental: true

# Changelog 활성화
state.backend.changelog.enabled: true
state.backend.changelog.storage: filesystem

# Changelog 저장 위치 (보통 체크포인트와 같은 storage)
dstl.dfs.base-path: s3://flink-changelog/
dstl.dfs.compression.enabled: true

# Materialization 주기 (changelog 회수)
state.backend.changelog.periodic-materialize.enabled: true
state.backend.changelog.periodic-materialize.interval: 10min
```

`state.backend.type`은 그대로 `rocksdb` — Flink가 자동으로 wrap.

---

## 6. 본인 환경에서의 가치

### 6.1 언제 의미 있나?

| 시나리오 | 권장 |
|---------|------|
| 본인 일반 잡 (interval 30s) | RocksDB만 — changelog 안 씀 |
| **interval 5초 이하** 필요 (강력한 latency 요구) | RocksDB + Changelog ★ |
| backpressure 잦은 잡 | unaligned + RocksDB가 우선, changelog 추가 |
| recovery time 매우 중요 | changelog는 오히려 손해 — RocksDB만 |

### 6.2 비용 측면

- changelog 저장소 (MinIO bucket 별도 권장 — 회수 정책 다르게)
- materialization은 RocksDB SST 업로드 + changelog truncate를 백그라운드로 수행 → 추가 메모리/CPU

본인 환경의 일반 잡에 적용하면 **득보단 실** — 추가 storage/compute 부담만 늘어남.

---

## 7. 관련 FLIP

- [FLIP-158: Generalized incremental checkpoints](https://cwiki.apache.org/confluence/display/FLINK/FLIP-158%3A+Generalized+incremental+checkpoints) — Changelog 도입

---

## 8. FAQ

**Q1. 모든 backend에서 사용 가능?**
A. RocksDB가 주된 사용처. HashMap도 wrap 가능하나 의미 적음 (HashMap 자체가 빠름).

**Q2. ForSt + Changelog?**
A. 이론상 가능하나 ForSt도 Experimental → 두 experimental 조합은 위험. 권장 ❌.

**Q3. Materialization은 무엇?**
A. 누적된 changelog를 정기적으로 RocksDB SST로 변환해 저장하고 changelog를 truncate. 안 하면 changelog가 무한 증가.

**Q4. Recovery 시 동작?**
A. 마지막 materialized snapshot (= RocksDB SST set) 복구 + 그 이후 changelog 재생. changelog 길면 느림.

---

## 9. 05 카테고리 마무리 + 다음

이로써 `05-state-checkpoint/`가 7개 문서로 완성:

```
01-checkpoint-coordinator.md   - 체크포인트 trigger/조정
02-checkpoint-barrier.md       - barrier alignment + unaligned
03-state-backend-overview.md   - 4개 backend 비교
04-rocksdb-state-backend.md    ★ 운영 메인
05-forst-poc-guide.md          🧪 PoC 트랙
06-state-v2-async-api.md       - ForSt 활용 전제
07-changelog-state.md          - 짧은 interval용 (이 문서)
```

다음 카테고리: 
- [`../07-filesystem-checkpoint-store/`](../07-filesystem-checkpoint-store/) — FileSystem 추상 + S3/MinIO RecoverableWriter (체크포인트 storage 메커니즘)
- [`../06-source-sink-spi/`](../06-source-sink-spi/) — Source V2, Sink V2, Iceberg/Kafka 매핑
- [`../09-kubernetes-integration/`](../09-kubernetes-integration/) — flink-kubernetes 모듈
