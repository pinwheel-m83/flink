# 11 — Network Shuffle

> **다루는 영역**: TM 간 record 전송 메커니즘 — Netty, NetworkBuffer, credit-based flow control, backpressure.
> **본인 환경에서**: 직접 만질 일 적음. 주로 backpressure 디버깅 시 참조.

## 문서 목록

| # | 문서 | 한 줄 |
|---|------|------|
| 01 | [`01-overview.md`](./01-overview.md) | Netty 기반 데이터 채널 종합 (ResultPartition / InputGate / LocalBufferPool 개관) |
| 02 | [`02-result-partition-input-gate.md`](./02-result-partition-input-gate.md) | Producer/Consumer 측 자료구조 (Local vs Remote channel) |
| 03 | [`03-credit-based-flow.md`](./03-credit-based-flow.md) | Credit-based flow control + backpressure 디버깅 |
