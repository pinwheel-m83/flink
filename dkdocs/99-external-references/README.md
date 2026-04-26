# 99 — External References (외부 레포)

> **목적**: 본인 환경의 외부 레포(Apache 별도 프로젝트들)가 Flink 본 레포(이 dkdocs의 주된 분석 대상)와 어떻게 연결되는지의 진입점 매핑.
> **외부 레포 위치**: [`../../external/`](../../external/) — `external/clone.sh`로 관리

## 외부 레포 ↔ Flink 본 레포 매핑

| 외부 레포 | Flink 측 진입점 | 상세 매핑 |
|---------|---------------|----------|
| `apache/iceberg` (`iceberg-flink`) | Sink V2 + RecoverableWriter + Parquet | [`./01-iceberg-flink.md`](./01-iceberg-flink.md) |
| `apache/polaris` (REST Catalog) | Iceberg-Flink가 호출 (Flink 본 레포 직접 의존 X) | [`./02-polaris.md`](./02-polaris.md) |
| `apache/flink-kubernetes-operator` | flink-kubernetes 모듈을 띄움 + REST API 제어 | [`./03-flink-kubernetes-operator.md`](./03-flink-kubernetes-operator.md) |
| `apache/flink-connector-kafka` | Source V2 + Sink V2 SPI 위에 구현 | [`./04-flink-connector-kafka.md`](./04-flink-connector-kafka.md) |

## 인용 형식

dkdocs 본문에서 외부 레포 코드 인용:
```
external/<repo>/.../File.java:LINE
```

본 레포 코드와 명확히 구분.

## 외부 레포 업데이트

```bash
./external/clone.sh                      # 모든 레포 fetch + pull
./external/clone.sh polaris              # 특정 레포만
```
