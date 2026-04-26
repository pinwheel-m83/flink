# DefaultScheduler — Adaptive와의 대조

> **요약**: 고정 parallelism, region-based failover. AdaptiveScheduler 등장 전 표준. 여전히 batch 잡과 일부 operator 케이스에서 사용.

---

## 1. 차이 표 (이전 문서 정리)

| 항목 | DefaultScheduler | AdaptiveScheduler |
|------|------------------|-------------------|
| Parallelism | 고정 (잡 시작 시 결정) | 가변 (런타임 조정) |
| 슬롯 부족 시 | 잡 거부 또는 대기 | 가용한 만큼 줄여서 시작 |
| TM 손실 | region failover (영향 받는 부분만) | 전체 재시작 (체크포인트에서) |
| Failover 단위 | Pipelined region | 잡 전체 |
| 외부 자원 변경 | 어려움 | REST API (FLIP-291) |
| 적용 잡 | batch + streaming | streaming 전용 |

---

## 2. Region-based Failover (DefaultScheduler 강점)

`PipelinedRegion`: BLOCKING edges로 끊긴 vertex 묶음. 한 region이 fail하면 그 region만 재시작.

본인 환경의 streaming 잡은 보통 PIPELINED 만이라서 region 분리 안 됨 → region failover 효과 적음. AdaptiveScheduler가 더 적합.

---

## 3. 본인 환경엔 AdaptiveScheduler 권장

이유:
- Operator autoscaler 연동 필요 → AdaptiveScheduler만 가능
- streaming 잡 위주 → region failover 이점 적음
- DataStream API 위주 → adaptive batch scheduler 영역 아님

DefaultScheduler 비교용으로만 학습.

---

## 4. 다음

- Failover strategies (양쪽 공통): [`./03-failover-strategies.md`](./03-failover-strategies.md)
