# V1 → V2 마이그레이션 고려사항

> **요약**: 본인 환경에서 V2 도입 시기와 방식. 운영 잡(V1, RocksDB sync)은 유지, 새 잡은 V2 검토. State schema 호환성과 connector 지원 매트릭스가 결정 요소.
> **선행**: [`./01-flip-409-overview.md`](./01-flip-409-overview.md)

---

## 1. 의사결정 핵심

| 시나리오 | V1 / V2 |
|---------|--------|
| 기존 운영 잡 (V1, savepoint 보존) | **V1 유지** — migration 비용 ≫ 이익 |
| 새 잡 + 단순 ProcessFunction 위주 | **V2 시도** |
| 새 잡 + 복잡한 window/CEP | **V1** (V2의 window/CEP 지원이 아직 V1만큼 안 풍부) |
| ForSt PoC 연동 | **V2 (async API와 자연 결합)** |
| Table/SQL 잡 | V1/V2 무관 (Table 영역은 별도 추상) |

---

## 2. State 호환성

V1 state → V2 state schema 자동 변환 ❌. savepoint 호환을 보장하려면:
- 같은 stateDescriptor name 사용
- 같은 TypeSerializer (또는 호환 가능한 schema evolution)
- V2의 새 state primitive(`StateFuture` 기반) 사용 시 schema 다를 수 있음

⇒ 운영 V1 잡을 V2로 옮기려면 **savepoint test 필수**.

---

## 3. Connector 호환

본인 환경 connector:
- **Kafka source/sink**: V2 SPI(Source V2, Sink V2) 위에 구현 → V1/V2 모두 호환
- **Iceberg sink**: 같음 — Sink V2 위
- **Polaris RESTCatalog**: connector-agnostic

⇒ connector 측은 양쪽 호환. 차이는 사용자 코드 측 API.

---

## 4. K8s + Operator 측

V2 잡도 같은 entrypoint(KubernetesApplicationClusterEntrypoint)에서 동작 — Operator 측 변경 불필요.

---

## 5. 권장 전환 순서

```
1. 새 잡 1개를 V2로 작성 (small scope, low criticality)
2. ForSt PoC 잡과 결합 (async state + V2 API)
3. 6개월 운영 후 안정성 평가
4. 결과에 따라 더 많은 새 잡에 V2 채택
5. 기존 V1 잡 마이그레이션은 별도 프로젝트로 (savepoint test 필수)
```

본인 환경 결정사항대로 V2는 "잠재적 미래" — 지금은 ForSt PoC와 함께 학습 트랙.

---

## 6. 13 카테고리 마무리 + 전체 dkdocs 종료

2개 문서로 V2 영역 cover:
- 01-flip-409-overview.md
- 02-migration-considerations.md (이 문서)

**Phase A + B + C 모두 완료**. 후속 작업:
- [`../99-external-references/`](../99-external-references/) — 외부 레포 진입점 매핑 stub
- [`../01-java-prerequisites/`](../01-java-prerequisites/) — Java 사전 지식 (인라인으로 충분 vs 별도 deep-dive 필요 시)
