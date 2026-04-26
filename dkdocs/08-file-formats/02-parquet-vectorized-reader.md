# Parquet Vectorized Reader

> **요약**: Parquet 파일을 한 row씩 읽지 않고 column 단위로 batch read → 컬럼 연산 SIMD 가능 + heap 효율적. Iceberg/Flink Table 잡에서 사용.
> **모듈**: `flink-formats/flink-parquet/vector/`

---

## 1. TL;DR

`ParquetVectorizedInputFormat`(또는 그 sub) → 한 호출에 row N개의 데이터를 column oriented `ColumnarRowData`로 반환. column reader (`AbstractParquetVectorReader` 류)가 dictionary/RLE 디코딩 후 `WritableColumnVector`에 채움. row 단위 access는 lazy — 실제 컬럼이 사용될 때만 디코딩되도록 batch + projection pushdown으로 IO 절감.

---

## 2. 핵심 위치

```
flink-formats/flink-parquet/src/main/java/org/apache/flink/formats/parquet/vector/
├── ParquetColumnarRowSplitReader.java    ← Reader 본체
├── reader/                                ← 각 Parquet 타입별 column reader
│   ├── BytesColumnReader.java
│   ├── IntColumnReader.java
│   ├── LongColumnReader.java
│   ├── FloatColumnReader.java
│   └── ... (각 primitive + 복합 타입)
├── position/                              ← row position tracking
└── type/                                  ← Parquet ↔ Flink type 매핑
```

---

## 3. Vectorized vs Row-by-row

| | Row-by-row | Vectorized |
|---|----------|----------|
| 읽기 단위 | 1 row | N rows (batch, default 2048) |
| 디코딩 | 매 row마다 | batch 한 번에 |
| projection (일부 column만 read) | 모든 column 디코딩 | 필요 column만 디코딩 (IO + CPU 절감) |
| filter pushdown | 행 단위 평가 | column statistics + min/max 활용해 row group skip |
| memory | row 객체 GC 압박 | column vector 재사용 |

본인 환경의 SQL 잡(향후) 또는 Iceberg read는 vectorized 권장.

---

## 4. Predicate / Projection Pushdown

Iceberg가 plan 단계에서:
- WHERE 조건을 분석해 row group statistics(min/max)와 비교 → 무관한 row group skip
- SELECT 절의 column만 reader에 전달 → 다른 column 디코딩 안 함

이 두 가지가 잡 IO를 크게 줄임.

---

## 5. Streaming 잡에선?

본인 환경의 streaming 잡(Kafka → process → Iceberg)에선 sink만 사용 → vectorized reader는 직접 안 씀. 하지만 backfill 잡 (Iceberg에서 read해 재처리) 또는 batch 분석 잡에선 핵심.

---

## 6. 다음

- Compaction & rolling 정책: [`./03-compaction-rolling.md`](./03-compaction-rolling.md)
