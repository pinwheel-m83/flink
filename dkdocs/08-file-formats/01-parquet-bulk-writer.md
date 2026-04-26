# Parquet BulkWriter — 본인 환경의 Iceberg 데이터 파일 형식

> **요약**: `flink-formats/flink-parquet`이 BulkWriter 인터페이스로 Parquet 파일을 만드는 메커니즘. Iceberg-Flink가 이 위에서 동작.
> **모듈**: `flink-formats/flink-parquet/`

---

## 1. TL;DR

`ParquetWriterFactory<T>`가 record stream을 받아 Parquet column-oriented 형식으로 파일을 만들고 `RecoverableWriter`(MinIO multipart) 위에 작성. row-group마다 column별 압축 + dictionary encoding 적용. Iceberg writer가 이 BulkWriter를 wrapping해 매 checkpoint마다 새 data file 1개 이상 만든다.

---

## 2. 핵심 클래스

| 역할 | 클래스 | 위치 |
|------|------|------|
| BulkWriter 추상 (Flink 공용) | `BulkWriter<T>` (Interface) | `flink-core/.../api/common/serialization/BulkWriter.java` |
| Parquet writer factory | `ParquetWriterFactory<T>` | `flink-formats/flink-parquet/.../ParquetWriterFactory.java` |
| Avro 통합 | `ParquetAvroWriters` | `flink-formats/flink-parquet/.../avro/ParquetAvroWriters.java` |
| RowData (Flink 내부 row) 통합 | `flink-formats/flink-parquet/.../row/` | `ParquetRowDataBuilder` 등 |
| Protobuf 통합 | `flink-formats/flink-parquet/.../protobuf/` | |

---

## 3. BulkWriter 인터페이스

```java
public interface BulkWriter<T> {
    void addElement(T element) throws IOException;
    void flush() throws IOException;
    void finish() throws IOException;
    
    interface Factory<T> extends Serializable {
        BulkWriter<T> create(FSDataOutputStream out) throws IOException;
    }
}
```

핵심: 단순 record-oriented writer 추상 — Parquet/Orc/Avro/CSV 등이 구현. `FSDataOutputStream`을 받아 그 위에 자기 형식으로 쓴다.

---

## 4. Parquet 동작 (간략)

1. `addElement(record)` 호출 → in-memory row buffer에 누적 (column 별로 분리)
2. row-group 크기(default 128MB) 도달 → row-group flush
   - 각 column에 압축 (snappy 등) + dictionary encoding (cardinality 작으면)
   - 각 column에 statistics (min/max/null count) — predicate pushdown 활용
3. `finish()` → footer 작성 (schema, row-group locations, column metadata)
4. `out.close()` → multipart upload complete → 최종 파일 publish

---

## 5. 본인 환경: Iceberg + Parquet + MinIO

```
[IcebergStreamWriter]
    record → Parquet RowData converter
    ↓
[ParquetWriterFactory]
    BulkWriter 생성
    ↓
[BulkWriter.addElement(record)] (반복)
    ↓ row-group 가득
[BulkWriter.flush()] → Parquet row-group → bytes
    ↓
[FSDataOutputStream (S3RecoverableFsDataOutputStream)]
    write(bytes) → multipart upload part
    ↓ checkpoint barrier
    persist() → S3Recoverable 직렬화 → IcebergCommittable 일부로 emit
```

매 checkpoint마다 새 Parquet 파일 1개 이상 생성. 작은 파일이 너무 많이 생기면 read 성능 저하 → compaction 필요 (다음 문서).

---

## 6. 권장 설정

```java
// Iceberg writer config (외부 레포 측)
.with(WRITE_TARGET_FILE_SIZE_BYTES, 128 * 1024 * 1024)   // 128MB target
.with(PARQUET_ROW_GROUP_SIZE_BYTES, 128 * 1024 * 1024)
.with(PARQUET_COMPRESSION, "zstd")                        // 또는 "snappy"
.with(PARQUET_DICT_SIZE_BYTES, 2 * 1024 * 1024)
```

128MB target file size = MinIO에서 read 효율성과 file 개수의 균형.

---

## 7. 다음

- Vectorized Parquet reader: [`./02-parquet-vectorized-reader.md`](./02-parquet-vectorized-reader.md)
- 작은 파일 compaction / rolling: [`./03-compaction-rolling.md`](./03-compaction-rolling.md)
