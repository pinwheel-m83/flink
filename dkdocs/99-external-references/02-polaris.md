# Apache Polaris (REST Catalog)

> **외부 레포**: `external/polaris/`
> **Flink 측 진입점**: 직접 의존 X. Iceberg-Flink의 `RESTCatalog`가 Polaris와 통신 (HTTP).

---

## 1. 무엇인가

Apache Polaris = open Iceberg REST Catalog 구현체. Iceberg REST Catalog spec을 구현해 multi-engine(Spark, Flink, Trino, StarRocks 등) 호환. 본인 환경에서 HMS 대체로 도입 예정 (2주 내).

## 2. Flink와의 관계

Flink 본 레포와 직접 코드 의존 ❌. 흐름:
```
[Flink 잡]
  → Iceberg-Flink IcebergSink
    → Iceberg core RESTCatalog
      → HTTP request (Bearer 토큰 포함)
        → Polaris server (별도 K8s deployment)
          → metadata.json 갱신 + 응답
```

## 3. 인증 (본인 환경 결정사항)

**OAuth2 Client Credentials** grant — [`../00-overview/my-environment-map.md`](../00-overview/my-environment-map.md):

```
[잡 시작 시]
RESTCatalog → Polaris token endpoint (POST /v1/oauth/tokens)
  ↓ Bearer 토큰 (TTL ~1시간)
[매 commit]
RESTCatalog → Polaris commit endpoint (Authorization: Bearer ...)
  ↓ atomic snapshot 갱신
[TTL 만료 전]
auto refresh ('token-refresh-enabled' = 'true')
```

자세한 흐름은 [`../06-source-sink-spi/04-iceberg-kafka-mapping.md#33-polaris-oauth2-인증-흐름`](../06-source-sink-spi/04-iceberg-kafka-mapping.md).

## 4. K8s 측 secret 주입

```yaml
env:
  - name: POLARIS_CLIENT_ID
    valueFrom: { secretKeyRef: { name: polaris-oauth, key: client-id } }
  - name: POLARIS_CLIENT_SECRET
    valueFrom: { secretKeyRef: { name: polaris-oauth, key: client-secret } }
```

자세한 podTemplate은 [`../09-kubernetes-integration/06-pod-template-and-config.md`](../09-kubernetes-integration/06-pod-template-and-config.md).

## 5. 외부 레포 진입점

```
external/polaris/
├── api/                       # Iceberg REST API spec 구현
├── service/                   # 서버 측 로직
├── persistence/               # metadata 저장소 (PostgreSQL 등)
└── client/                    # Java 클라이언트 (Iceberg가 사용)
```

본인 환경 분석 시엔 client 측 (Iceberg가 호출하는 부분) 위주로 보면 충분. server 측은 Polaris 운영팀 영역.

## 6. 마이그레이션 시 주의 (HMS → Polaris)

- 기존 HMS 의 namespaces / tables → Polaris로 export
- 권한 모델 차이 (HMS는 Hadoop UGI, Polaris는 RBAC)
- Catalog 설정 변경 (`'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog'`, `'uri'`, `'credential'`)
- 같은 데이터 (S3/MinIO 위 Parquet)는 그대로 — 메타데이터 layer만 교체
