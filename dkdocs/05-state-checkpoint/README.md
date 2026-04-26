# 05 — State & Checkpoint

> **다루는 영역**: working state 저장 (state backend) + 분산 일관 스냅샷 (checkpoint). 본인 환경(K8s + RocksDB + MinIO)의 핵심 영역.
> **선행**: [`../04-runtime-architecture/`](../04-runtime-architecture/)
> **후속**: [`../07-filesystem-checkpoint-store/`](../07-filesystem-checkpoint-store/) — 체크포인트가 실제로 어떻게 MinIO에 저장되는가

## 문서 목록

| # | 문서 | 한 줄 |
|---|------|------|
| 01 | [`01-checkpoint-coordinator.md`](./01-checkpoint-coordinator.md) | JM 측 체크포인트 trigger/ack 수집/완료 처리 |
| 02 | [`02-checkpoint-barrier.md`](./02-checkpoint-barrier.md) | Barrier alignment + unaligned (backpressure 친화) |
| 03 | [`03-state-backend-overview.md`](./03-state-backend-overview.md) | 4개 backend 비교: HashMap, RocksDB, ForSt, Changelog |
| 04 | [`04-rocksdb-state-backend.md`](./04-rocksdb-state-backend.md) | ★ 본인 환경 운영 메인. LSM-tree, incremental, 메모리 튜닝 |
| 05 | [`05-forst-poc-guide.md`](./05-forst-poc-guide.md) | 🧪 ForSt PoC. Experimental 명시, RocksDB 비교 |
| 06 | [`06-state-v2-async-api.md`](./06-state-v2-async-api.md) | FLIP-424 async state — ForSt 활용 전제 |
| 07 | [`07-changelog-state.md`](./07-changelog-state.md) | 짧은 interval 잡용 (본인 환경 일반 잡엔 X) |
