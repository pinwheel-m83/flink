# File Rolling & Compaction — 작은 파일 문제

> **요약**: streaming sink가 매 checkpoint마다 작은 파일 1개씩 만들면 시간이 지나면서 파일이 무수히 많아짐 → read 성능 저하. Rolling 정책 + 주기적 compaction으로 해결.

---

## 1. 문제: 작은 파일 누적

본인 환경 시나리오:
- checkpoint interval 30s
- Iceberg sink writer parallelism 8
- 1시간 = 120 checkpoint × 8 writer = **960 파일/시간**
- 1일 = 23,040 파일
- 1주 = 161,280 파일

각 파일 5~50MB → MinIO에 read 시 latency, Polaris의 manifest 관리 부담.

---

## 2. Rolling 정책 (per checkpoint)

writer 측에서 한 checkpoint 안에서도:

```
file size 도달 (예: 128MB) → 새 part file
또는
record count 도달 → 새 part file
또는
checkpoint barrier 도달 → 현재 part file finalize, 새 file 시작
```

너무 작은 file 생성 방지. Iceberg writer config:
```java
.with(WRITE_TARGET_FILE_SIZE_BYTES, 128 * 1024 * 1024)    // 128MB target
```

---

## 3. Compaction (post-write)

이미 만들어진 작은 파일들을 큰 파일로 합침. 두 가지 방식:

### 3.1 Iceberg 내장 compaction action (외부)

```sql
-- Spark 또는 Flink batch
CALL polaris.system.rewrite_data_files(table => 'my_db.events');
```

Iceberg 자체 procedure — Polaris 위에서 동작. 보통 cron 또는 Airflow로 주기적 실행 (1시간/1일).

### 3.2 Flink Sink V2 SupportsPostCommitTopology

FLIP-191의 PostCommitTopology를 활용하면 Sink writer 뒤에 별도 compactor operator를 chain — checkpoint complete 시 자동 compact. 본인 환경에선 외부 cron으로 처리하는 것이 더 단순.

---

## 4. Iceberg Snapshot Expiration

compact 후에도 옛 snapshot이 남아 있으면 옛 data file이 회수되지 않음. 별도 procedure:

```sql
CALL polaris.system.expire_snapshots(table => 'my_db.events', older_than => TIMESTAMP '2026-04-01 00:00:00');
```

snapshot 만료 + orphan file 정리:

```sql
CALL polaris.system.remove_orphan_files(table => 'my_db.events');
```

이 두 가지가 cron으로 주기 실행되면 storage 사용량 안정.

---

## 5. 권장 본인 환경 운영 패턴

```
[Streaming Flink 잡]
   매 30s checkpoint → 작은 Parquet 파일 생성 (target 128MB roll)
   ↓
[Polaris]
   매 commit이 새 snapshot 생성 (manifest 누적)
   ↓
[Daily cron job]
   1. rewrite_data_files (작은 파일 합침, target 512MB)
   2. expire_snapshots (오래된 snapshot 만료)
   3. remove_orphan_files (회수)
```

Operator의 `FlinkSessionJob` CR 또는 별도 batch 잡으로 cron 등록.

---

## 6. 08 카테고리 마무리

3개 문서:
- 01-parquet-bulk-writer.md
- 02-parquet-vectorized-reader.md
- 03-compaction-rolling.md (이 문서)

다음:
- [`../09-kubernetes-integration/`](../09-kubernetes-integration/) — flink-kubernetes 모듈, Operator 경계
