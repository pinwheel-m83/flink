# Checkpoint Storage on S3/MinIO — 종합 운영 가이드

> **요약**: 본인 환경에서 RocksDB state + MinIO checkpoint storage 조합이 어떻게 일관 있게 동작하는지 종합 정리. 디렉토리 구조, retain 정책, 잡 cancellation 시 cleanup, savepoint 관계.
> **선행**: [`./01-filesystem-abstraction.md`](./01-filesystem-abstraction.md), [`./02-recoverable-writer.md`](./02-recoverable-writer.md), [`./03-s3-multipart-upload.md`](./03-s3-multipart-upload.md), [`./04-plugin-classloading.md`](./04-plugin-classloading.md), [`../05-state-checkpoint/04-rocksdb-state-backend.md`](../05-state-checkpoint/04-rocksdb-state-backend.md)

---

## 1. TL;DR

`state.checkpoint-storage: filesystem` + `state.checkpoints.dir: s3://flink-checkpoints/`로 설정 시 — `FileSystemCheckpointStorage`가 잡당 디렉토리(`<jobId>/`)를 만들고 그 안에 체크포인트별로 `chk-N/` 디렉토리 + RocksDB SST shared 폴더 `shared/` 둠. 매 체크포인트 메타는 `chk-N/_metadata` (직렬화된 `CompletedCheckpoint`). `state.checkpoints.num-retained` (default 1) 따라 옛 체크포인트 자동 회수, savepoint는 retain 정책과 무관하게 사용자가 명시적으로 관리.

---

## 2. 디렉토리 구조 (MinIO 측)

```
s3://flink-checkpoints/
└── <jobId-uuid>/                     # 잡 1개당
    ├── chk-100/                      # 체크포인트 100
    │   ├── _metadata                 # 직렬화된 CompletedCheckpoint (state handle ref들)
    │   ├── chunk-...                 # 작은 state는 직접 chunk 파일로
    │   └── ...
    ├── chk-101/
    ├── chk-102/                      # ← 최신 (num-retained=3이면 100, 101, 102만 유지)
    ├── shared/                       # RocksDB incremental — 여러 체크포인트가 공유
    │   ├── <uuid>.sst                # 매 SST 파일 (uuid로 unique)
    │   └── ...
    └── taskowned/                    # task별 owned state (rare)
```

핵심:
- **`chk-N/`**: 그 체크포인트만의 메타 + small state
- **`shared/`**: 여러 체크포인트가 공유하는 SST 파일 (incremental의 핵심)
- **`taskowned/`**: subtask별로 직접 관리하는 state (예: source의 split assignment)

---

## 3. `state.checkpoints.num-retained` 정책

```yaml
state.checkpoints.num-retained: 3   # default 1, 권장 3+
```

- 옛 chk-N 디렉토리는 `CompletedCheckpointStore`가 자동 삭제
- 단, `shared/` 안의 SST 파일은 ref counting — 어떤 체크포인트도 더 이상 안 쓰면 그때 삭제
- ref counting 누락 버그 시 `shared/` 무한 증가 가능 (드물지만 발생) → 모니터링 필요

---

## 4. Externalized Checkpoint Retention (잡 종료 시 보존)

```yaml
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
# 또는: DELETE_ON_CANCELLATION (default), NO_EXTERNALIZED_CHECKPOINTS
```

- `RETAIN_ON_CANCELLATION`: 잡 cancel 시 마지막 체크포인트 보존 → 새 잡으로 같은 체크포인트에서 시작 가능 (savepoint 대신 사용 가능)
- `DELETE_ON_CANCELLATION`: cancel 시 모든 체크포인트 삭제

본인 환경 권장: **`RETAIN_ON_CANCELLATION`** — operator autoscaler가 잡 재시작 시 fallback 가능.

---

## 5. Savepoint vs Checkpoint storage

```yaml
state.checkpoints.dir: s3://flink-checkpoints/    # 자동 (위)
state.savepoints.dir: s3://flink-savepoints/      # 사용자 trigger (REST/CLI)
```

같은 S3 endpoint, 다른 bucket 권장 — savepoint는 일반적으로 더 오래 보존 (잡 마이그레이션, 버전 업그레이드용).

```bash
# Savepoint trigger (REST)
curl -X POST http://<jm-rest>:8081/jobs/<jobId>/savepoints \
  -d '{"target-directory":"s3://flink-savepoints/jobX-v2/","cancel-job":false}'
```

---

## 6. 잡 시작 시 복구

세 가지 방식:

```bash
# (a) 마지막 자동 체크포인트에서 (HA enabled)
# Flink가 자동 — JM 재시작 시 CompletedCheckpointStore에서 마지막 chk-N 읽음

# (b) 외부 체크포인트 (RETAIN_ON_CANCELLATION으로 보존된 것)
flink run -s s3://flink-checkpoints/<jobId>/chk-N/_metadata my-job.jar

# (c) Savepoint
flink run -s s3://flink-savepoints/jobX-v2/savepoint-... my-job.jar
```

K8s Operator는 `FlinkDeployment` CR의 `state.savepointTriggerNonce` + `initialSavepointPath`로 관리.

---

## 7. 본인 환경 권장 종합 설정

```yaml
# === Storage ===
state.checkpoint-storage: filesystem
state.checkpoints.dir: s3://flink-checkpoints/
state.savepoints.dir: s3://flink-savepoints/

# === Retention ===
state.checkpoints.num-retained: 3
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# === S3/MinIO ===
s3.endpoint: https://minio.example.com
s3.access-key: ${MINIO_ACCESS_KEY}
s3.secret-key: ${MINIO_SECRET_KEY}
s3.path.style.access: true
s3.connection.maximum: 100

# === Backend ===
state.backend.type: rocksdb
state.backend.incremental: true
state.backend.local-recovery: true

# === Checkpointing ===
execution.checkpointing.interval: 30s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.unaligned: true
execution.checkpointing.aligned-checkpoint-timeout: 30s
execution.checkpointing.tolerable-failed-checkpoints: 3

# === Tuning ===
state.backend.rocksdb.checkpoint.transfer-thread.num: 4
```

---

## 8. MinIO bucket 운영

```bash
# Create
mc mb minio/flink-checkpoints
mc mb minio/flink-savepoints

# Lifecycle (필수)
mc ilm rule add minio/flink-checkpoints --abort-multipart-after 7d

# Access policy (Flink가 read/write할 수 있도록)
mc policy set readwrite minio/flink-checkpoints
mc policy set readwrite minio/flink-savepoints

# 모니터링
mc admin info minio                    # 클러스터 상태
mc du --depth=2 minio/flink-checkpoints  # 사용량
```

---

## 9. 흔한 운영 이슈

| 증상 | 원인 | 해결 |
|------|------|------|
| 체크포인트 storage 무한 증가 | (1) num-retained 너무 큼 (2) shared/ ref counting 누수 (3) 옛 잡 directory 안 지움 | num-retained 검토 + manual cleanup script + 모니터링 |
| 잡 fail "checkpoint not found" | externalized retention OFF + 잡 cancel 후 재시작 시도 | RETAIN_ON_CANCELLATION 활성화 |
| Restore 매우 느림 | local-recovery OFF + 큰 RocksDB SST를 MinIO에서 download | local-recovery ON + PVC |
| Savepoint 만들기 timeout | 큰 state + 단일 thread로 직렬화 | savepoint format을 native로 (FLIP-203) |
| Multipart upload 누적 | bucket lifecycle 미설정 | abort-multipart-after rule 추가 |

---

## 10. 07 카테고리 마무리

5개 문서로 구성:
- 01-filesystem-abstraction.md — Flink FileSystem persistence contract
- 02-recoverable-writer.md — resume-able stream
- 03-s3-multipart-upload.md — S3 protocol과 Flink mapping
- 04-plugin-classloading.md — plugin jar 격리
- 05-checkpoint-storage-on-s3.md — 종합 운영 가이드 (이 문서)

다음:
- [`../06-source-sink-spi/`](../06-source-sink-spi/) — Source V2 + Sink V2 + Iceberg/Kafka 매핑 (Iceberg sink가 RecoverableWriter를 어떻게 활용하는지 포함)
- [`../09-kubernetes-integration/`](../09-kubernetes-integration/) — K8s entrypoint, HA, Operator 경계
