# State Backend 비교 — HashMap, RocksDB, ForSt, Changelog

> **요약**: Flink는 working state를 어디에 어떻게 저장할지 `StateBackend` 추상으로 분리. release-2.0 기준 4개 주요 구현 — HashMap (메모리), RocksDB (디스크 임베디드), ForSt (cloud-native, **🧪 PoC**), Changelog (어떤 backend든 wrapping해 증분 변경 로그 추가).
> **모듈**: `flink-runtime/runtime/state/`, `flink-state-backends/{rocksdb, forst, changelog}/`
> **기준 버전**: Flink `release-2.0`
> **운영 권장 여부**: ★Production = HashMap (작은 state) / RocksDB (대규모 state, 본인 환경 메인) / 🧪 PoC = ForSt
> **선행**: [`./01-checkpoint-coordinator.md`](./01-checkpoint-coordinator.md), [`./02-checkpoint-barrier.md`](./02-checkpoint-barrier.md)

---

## 1. TL;DR (3문장)

`StateBackend`는 **working state의 저장 방식**(메모리 vs 디스크 vs 원격)을 결정하는 추상 — `CheckpointStorage`가 결정하는 **체크포인트 저장 방식**(JM 메모리 vs FileSystem)과는 독립적이고 직교한다. 본인 환경의 권장 조합: **`EmbeddedRocksDBStateBackend` + `FileSystemCheckpointStorage(s3://)` + 증분 체크포인트** — RocksDB가 disk-backed로 큰 state(>10GB)를 다루고, 매 체크포인트마다 새 SST 파일만 MinIO에 업로드. ForSt는 cloud-native disaggregated 미래 (state를 MinIO 직접 read/write, local disk 캐시) — 2026-04 기준 **`@Experimental`이며 운영 비권장**, PoC 트랙으로 학습 가치 있음.

---

## 2. 사전 지식

### 2.1 working state vs checkpoint state (직교 개념)

- **working state**: 잡 실행 중 operator가 read/write 하는 라이브 state (`ValueState`, `MapState` 등). `StateBackend` 결정.
- **checkpoint state**: working state의 스냅샷. `CheckpointStorage` 결정.

같은 RocksDB working state라도 체크포인트는 MinIO에 가거나 HDFS에 갈 수 있다. 두 결정은 별도.

### 2.2 keyed state vs operator state

- **keyed state**: `keyBy()` 후 사용 가능. key별로 분할 (`KeyGroupRange` 단위로 redistribute 가능). 대부분의 backend는 keyed state에 집중.
- **operator state**: subtask 단위 state (key 없음). 보통 `ListState<X>` (rescale 시 round-robin 또는 union 재분배). `OperatorStateBackend`가 담당, 기본 구현은 `DefaultOperatorStateBackend` (in-memory, snapshot 시 직렬화).

### 2.3 incremental checkpoint

증분 체크포인트 — 매번 전체 state가 아닌 변경분만 영속화. RocksDB의 LSM 구조와 자연스럽게 결합 (새 SST 파일만 업로드, 기존은 재사용). 본인 환경 필수 설정.

---

## 3. 핵심 클래스 / 인터페이스

| 역할 | 인터페이스/클래스 | 위치 |
|------|-----------------|------|
| Backend 추상 | `StateBackend` (Interface) | `flink-runtime/src/main/java/org/apache/flink/runtime/state/StateBackend.java` |
| Keyed state backend (작업 중인 state) | `KeyedStateBackend<K>` (Interface) | `flink-runtime/.../state/KeyedStateBackend.java` |
| Operator state backend | `OperatorStateBackend` (Interface) | `flink-runtime/.../state/OperatorStateBackend.java` |
| Loader (config → backend 인스턴스) | `StateBackendLoader` | `flink-runtime/.../state/StateBackendLoader.java` |
| HashMap backend | `HashMapStateBackend` | `flink-runtime/.../state/hashmap/HashMapStateBackend.java` |
| RocksDB backend | `EmbeddedRocksDBStateBackend` | `flink-state-backends/flink-statebackend-rocksdb/.../EmbeddedRocksDBStateBackend.java` |
| RocksDB keyed state | `RocksDBKeyedStateBackend<K>` | 같은 모듈 |
| ForSt backend (🧪 Experimental) | `ForStStateBackend` | `flink-state-backends/flink-statebackend-forst/.../ForStStateBackend.java` |
| Changelog wrapper | `ChangelogStateBackend` | `flink-state-backends/flink-statebackend-changelog/.../ChangelogStateBackend.java` |

---

## 4. `StateBackend` 인터페이스 (javadoc 정리)

`flink-runtime/.../state/StateBackend.java:39-78`:

```java
/**
 * A <b>State Backend</b> defines how the state of a streaming application is stored locally within
 * the cluster. Different State Backends store their state in different fashions, and use different
 * data structures to hold the state of a running application.
 *
 * <p>For example, the {@link HashMapStateBackend hashmap state backend} keeps working state in the
 * memory of the TaskManager. The backend is lightweight and without additional dependencies.
 *
 * <p>The {@code EmbeddedRocksDBStateBackend} stores working state in an embedded RocksDB and is
 * able to scale working state to many terabytes in size, only limited by available disk space
 * across all task managers.
 *
 * <h2>Raw Bytes Storage and Backends</h2>
 *
 * <p>The {@code StateBackend} creates services for keyed state and operator state.
 *
 * <p>The CheckpointableKeyedStateBackend and OperatorStateBackend created by this state backend
 * define how to hold the working state for keys and operators.
 *
 * <h2>Serializability</h2>
 *
 * <p>State Backends need to be Serializable, because they distributed across parallel processes
 * (for distributed execution) together with the streaming application code.
 *
 * <p>StateBackend implementations are meant to be like factories that create the proper states
 * stores. That way, the State Backend can be very lightweight (contain only configurations).
 *
 * <h2>Thread Safety</h2>
 *
 * <p>State backend implementations have to be thread-safe.
 */
@PublicEvolving
public interface StateBackend extends java.io.Serializable {
    // createKeyedStateBackend(...), createOperatorStateBackend(...) etc.
}
```

핵심:
- **factory 패턴** — backend 자체는 가벼운 config 묶음, 실제 working state store는 `createKeyedStateBackend(...)` 호출 시 생성
- **Serializable** — JM이 만든 backend 객체가 TM으로 직렬화 전송됨
- **Thread-safe** — 한 TM 안 다수의 task가 동시에 backend로 state store 생성

---

## 5. 4개 backend 비교

### 5.1 `HashMapStateBackend` — 메모리 기반

- **저장소**: TM JVM heap 안의 `HashMap` (`StateMap` 자료구조)
- **장점**: 빠름 (RAM 접근), GC 압박 적은 한도 내 가벼움
- **단점**: state가 heap 한도를 넘을 수 없음 (보통 수 GB), GC 오버헤드, OOM 위험
- **체크포인트**: state 전체를 직렬화해 한 번에 영속화 (full)
- **권장**: 작은 state (~수 GB 이하) 잡, 테스트, 분석

본인 환경에선 거의 안 씀.

### 5.2 `EmbeddedRocksDBStateBackend` — 디스크 임베디드 (★ 본인 환경 메인)

`flink-state-backends/flink-statebackend-rocksdb/.../EmbeddedRocksDBStateBackend.java:85-`:

```java
/**
 * A {@link StateBackend} that stores its state in an embedded RocksDB instance. This state backend
 * can store very large state that exceeds memory and spills to local disk. All key/value state
 * (including windows) is stored in the key/value index of RocksDB. For persistence against loss of
 * machines, please configure a {@link CheckpointStorage} instance for the Job.
 *
 * <p>The behavior of the RocksDB instances can be parametrized by setting RocksDB Options using the
 * methods setPredefinedOptions and setRocksDBOptions.
 */
@PublicEvolving
public class EmbeddedRocksDBStateBackend extends AbstractManagedMemoryStateBackend
        implements ConfigurableStateBackend {
    private static final long serialVersionUID = 1L;
    private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;
    private static boolean rocksDbInitialized = false;
    // ...
}
```

핵심:
- **저장소**: 각 TM의 로컬 디스크에 RocksDB 임베디드 (JNI 통한 native lib)
- **장점**: 거의 무제한 state (디스크 한도까지), 안정적 메모리 사용 (managed memory로 제한)
- **단점**: 디스크 read/write IO 비용, 직렬화/역직렬화 비용 (key/value bytes 단위 저장)
- **체크포인트**: **incremental 가능** — 새 SST 파일만 MinIO에 업로드
- **권장**: 큰 state 잡, exactly-once 보장, 본인 환경 메인 ★

자세한 동작은 [`./04-rocksdb-state-backend.md`](04-rocksdb-state-backend.md).

### 5.3 `ForStStateBackend` — Disaggregated (🧪 Experimental)

`flink-state-backends/flink-statebackend-forst/.../ForStStateBackend.java`:

```java
@Experimental
public class ForStStateBackend extends AbstractManagedMemoryStateBackend
        implements ConfigurableStateBackend {
    // ...
}
```

핵심:
- **저장소**: 원격 파일시스템 (S3/MinIO/HDFS)을 직접 read/write, 로컬 디스크는 캐시
- **장점**: 진정한 cloud-native — TM에 로컬 디스크 사실상 불필요, 빠른 rescale (state download 없음), 리커버리 빠름
- **단점**: ⚠ **Experimental** — production 권장 ❌ (Flink 2.4-SNAPSHOT 마스터 문서 명시), API 변경 가능성, SQL async state 미완성, 일부 operator만 async 지원
- **권장**: PoC만 (본인 환경 결정사항)

자세한 동작은 [`./05-forst-poc-guide.md`](05-forst-poc-guide.md).

### 5.4 `ChangelogStateBackend` — Wrapping decorator

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
}
```

핵심:
- **wrapper** — 다른 backend(주로 RocksDB)를 감싸고 모든 state 변경을 별도의 changelog에 추가 기록
- **장점**: 매 변경마다 changelog에 append → 체크포인트는 changelog만 영속화하면 됨 → **체크포인트 latency 매우 낮음**
- **단점**: storage 사용량 증가 (changelog), 복구 시 changelog 재생 필요
- **권장**: 매우 짧은 체크포인트 interval이 필요한 잡 (본인 환경에선 30s 정도라 필수 아님)

---

## 6. Backend 선택 의사결정 표

| 시나리오 | 권장 |
|---------|------|
| state ≤ 1GB, 단순 잡 | `HashMapStateBackend` |
| state 1GB~10TB, 안정적 운영 | **`EmbeddedRocksDBStateBackend` + incremental** ★ (본인 환경) |
| 매우 짧은 체크포인트 interval (5s 이하) | `ChangelogStateBackend(EmbeddedRocksDBStateBackend)` |
| Cloud-native, fast rescale 우선, **실험적** | `ForStStateBackend` 🧪 |
| 매우 큰 state (>10TB), local disk 부족 | ForSt가 GA되면 → 그때까진 RocksDB + 큰 disk |

---

## 7. 본인 환경 설정 (운영)

### 7.1 권장 `flink-conf.yaml`

```yaml
# State backend (working state)
state.backend.type: rocksdb
state.backend.incremental: true
state.backend.rocksdb.localdir: /flink/rocksdb-data    # 로컬 SSD 권장 (PVC mount)
state.backend.rocksdb.predefined-options: SPINNING_DISK_OPTIMIZED_HIGH_MEM   # 또는 FLASH_SSD_OPTIMIZED

# Checkpoint storage (체크포인트 영속화)
state.checkpoint-storage: filesystem
state.checkpoints.dir: s3://flink-checkpoints/
state.savepoints.dir: s3://flink-savepoints/

# Checkpointing
execution.checkpointing.interval: 30s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.unaligned: true                # backpressure 시 자동 unaligned
execution.checkpointing.aligned-checkpoint-timeout: 30s
execution.checkpointing.tolerable-failed-checkpoints: 3
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# Local recovery (TM 재시작 시 빠른 복구)
state.backend.local-recovery: true

# RocksDB 메모리 (managed memory에서 할당)
state.backend.rocksdb.memory.managed: true
```

### 7.2 K8s Pod 측 PVC

```yaml
# TM Pod에 RocksDB local dir용 emptyDir 또는 PVC 마운트
volumes:
  - name: rocksdb-data
    emptyDir:
      sizeLimit: 100Gi   # state 크기에 맞춤
volumeMounts:
  - name: rocksdb-data
    mountPath: /flink/rocksdb-data
```

`emptyDir` (Pod 죽으면 사라짐) vs `PVC` (영속) — local recovery를 활용하려면 PVC가 더 안전 (Pod 재시작 시 디스크 보존).

---

## 8. 관련 FLIP / JIRA

- [FLIP-50: Spill-able Heap Keyed State Backend](https://cwiki.apache.org/confluence/display/FLINK/FLIP-50%3A+Spill-able+Heap+Keyed+State+Backend) — heap-spillable 옵션 (deprecated)
- [FLIP-158: Generalized incremental checkpoints](https://cwiki.apache.org/confluence/display/FLINK/FLIP-158%3A+Generalized+incremental+checkpoints) — Changelog backend
- [FLIP-423: Disaggregated State Storage and Management](https://cwiki.apache.org/confluence/display/FLINK/FLIP-423%3A+Disaggregated+State+Storage+and+Management) — ForSt 도입 배경
- [FLIP-424: Asynchronous State APIs](https://cwiki.apache.org/confluence/display/FLINK/FLIP-424%3A+Asynchronous+State+APIs) — ForSt 활용을 위한 API
- [FLIP-427: ForSt — Cloud-Native State Store](https://cwiki.apache.org/confluence/display/FLINK/FLIP-427%3A+ForSt+-+Cloud-Native+State+Store) — ForSt 자체

---

## 9. 디버깅

### 9.1 현재 backend 확인

```bash
# JM 로그
kubectl logs <jm-pod> | grep -i "state backend\|checkpoint storage"
# 예: "State backend is set to org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend"
```

### 9.2 RocksDB 메트릭 (잡 메트릭)

```
state.backend.rocksdb.metrics.num-running-compactions
state.backend.rocksdb.metrics.num-running-flushes
state.backend.rocksdb.metrics.cur-size-active-mem-table
state.backend.rocksdb.metrics.size-all-mem-tables
state.backend.rocksdb.metrics.estimate-num-keys
```

자세한 옵션은 `RocksDBNativeMetricOptions`. 운영 디버깅 핵심.

### 9.3 체크포인트 크기 모니터링

REST `/jobs/<id>/checkpoints/details/<chkId>` 응답의 `state_size_total`, `incremental_state_size`, `checkpoint_state_size_per_task`. 갑자기 커지면 backend 동작 확인 필요.

---

## 10. 다음에 읽을 문서

- RocksDB state backend 운영 깊이: [`./04-rocksdb-state-backend.md`](04-rocksdb-state-backend.md)
- ForSt PoC 가이드 + production 비권장 근거: [`./05-forst-poc-guide.md`](05-forst-poc-guide.md)
- State V2 async API: [`./06-state-v2-async-api.md`](06-state-v2-async-api.md)
- FileSystem 추상 + S3 RecoverableWriter (체크포인트 저장 메커니즘): [`../07-filesystem-checkpoint-store/`](../07-filesystem-checkpoint-store/) (예정)
