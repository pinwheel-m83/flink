# Failover 전략

> **요약**: Task 실패 시 잡을 어떻게 재시작할 것인가의 정책. RestartStrategy + FailoverStrategy 두 축.

---

## 1. 두 종류

### 1.1 RestartStrategy (잡 전체 재시작 정책)

`flink-runtime/.../executiongraph/failover/RestartBackoffTimeStrategy`:
- `fixed-delay`: 매 N초 후 재시작, 최대 M회
- `exponential-delay`: 지수 backoff
- `failure-rate`: 일정 시간 내 실패 횟수 임계 초과 시만 fail
- `no-restart`: 즉시 fail
- `disable`: HA 없으면 fail, 있으면 처리 위임

본인 환경 권장:
```yaml
restart-strategy.type: exponential-delay
restart-strategy.exponential-delay.initial-backoff: 1s
restart-strategy.exponential-delay.max-backoff: 5min
restart-strategy.exponential-delay.backoff-multiplier: 2.0
restart-strategy.exponential-delay.reset-backoff-threshold: 1h
restart-strategy.exponential-delay.jitter-factor: 0.1
```

지수 backoff + 1시간 안정 후 reset — 일시적 장애와 영구 장애를 구분.

### 1.2 FailoverStrategy (어느 범위 재시작)

`flink-runtime/.../executiongraph/failover/`:
- `RestartAllFailoverStrategy`: 잡 전체 재시작 (AdaptiveScheduler 기본)
- `RestartPipelinedRegionFailoverStrategy`: 영향 받는 region만 (DefaultScheduler 기본, batch에 좋음)

본인 환경 (AdaptiveScheduler) 시 잡 전체 재시작이 자연스러움 — 마지막 checkpoint에서 빠르게 복구.

---

## 2. 흐름 (Task fail 시)

```
TM이 Task FAILED 상태 보고 → JM
   ↓
JobMaster.updateTaskExecutionState
   ↓
SchedulerNG.handleTaskFailure
   ↓ (AdaptiveScheduler)
state transition Executing → Restarting
   ↓
RestartBackoffTimeStrategy.canRestart?
  - true → 일정 backoff 후 재시작
  - false → 잡 fail
   ↓
새 ExecutionGraph 빌드 (또는 region restart)
   ↓
마지막 CompletedCheckpoint에서 state restore
   ↓
재실행
```

---

## 3. 본인 환경 디버깅

`JobManager 로그`에서:
```
grep -iE "Task .* (FAILED|SCHEDULED for restart)|Triggering recovery|restart-strategy" jm.log
```

자주 마주치는 경우:
- TM Pod OOM Kill → Task FAILED → restart → 새 TM Pod 요청 → resume
- 일시적 네트워크 장애 → backoff 짧게 두면 빠른 복구

---

## 4. 10 카테고리 마무리

3개 문서로 끝:
- 01-adaptive-scheduler.md (★ 메인)
- 02-default-scheduler-comparison.md
- 03-failover-strategies.md

다음:
- [`../11-network-shuffle/`](../11-network-shuffle/) — Netty stack
- [`../12-watermark-time/`](../12-watermark-time/) — EventTime
- [`../13-new-datastream-v2/`](../13-new-datastream-v2/) — FLIP-409
