# AdaptiveScheduler — 본인 환경 메인 스케줄러

> **요약**: 잡 실행 중 가용 슬롯 변동에 따라 parallelism을 자동 조정. K8s + Operator autoscaler와 결합 시 핵심.
> **모듈**: `flink-runtime/runtime/scheduler/adaptive/`

---

## 1. TL;DR

`AdaptiveScheduler`는 `SchedulerNG` 구현체로 잡 시작 시점 + 실행 중 모두 ResourceRequirements 기반으로 ExecutionGraph 빌드 — 슬롯이 부족하면 줄여서 시작 (degraded), 더 생기면 자동 scale-up. 기존 `DefaultScheduler`는 고정 parallelism으로 슬롯 부족 시 잡 시작 거부. 본인 환경의 Operator autoscaler가 REST로 ResourceRequirements를 변경하면 AdaptiveScheduler가 새 ExecutionGraph로 transition.

---

## 2. 핵심 클래스 / 진입점

| 역할 | 클래스 | 위치 |
|------|------|------|
| Adaptive scheduler | `AdaptiveScheduler` (`implements SchedulerNG`) | `flink-runtime/.../scheduler/adaptive/AdaptiveScheduler.java` |
| 상태 머신 | `State` (Interface), 구현체: `Created`, `WaitingForResources`, `CreatingExecutionGraph`, `Executing`, `Restarting`, `Failing`, `Finished` | 같은 패키지 |
| Default scheduler (대조) | `DefaultScheduler` | `flink-runtime/.../scheduler/DefaultScheduler.java` |
| Slot allocator | `SlotAllocator` (Interface) | `flink-runtime/.../scheduler/adaptive/allocator/` |
| Slot assignment | `SlotAssigner` | 같은 위치 |

---

## 3. State machine

```
[Created]
  ↓ start
[WaitingForResources]   — minimum required slots 대기
  ↓ enough slots
[CreatingExecutionGraph]
  ↓ EG 빌드 완료
[Executing]            — 실제 실행 중
  ↓ slot 변동 또는 ResourceRequirements 변경
[Restarting]            — 새 EG 빌드 후 transition
  ↓
[Executing]
  ↓ globally-terminal
[Finished]

[Failing] (어디서든 fatal 에러)
```

각 state가 자체적으로 행동을 결정 (Strategy 패턴). `WaitingForResources.onTrigger(...)`가 새 slot 생기면 `CreatingExecutionGraph`로 전이.

---

## 4. 활성화 설정

```yaml
jobmanager.scheduler: adaptive

# parallelism 범위
jobmanager.adaptive-scheduler.min-parallelism: 1
jobmanager.adaptive-scheduler.max-parallelism: 32

# transition 안정화
jobmanager.adaptive-scheduler.executing.resource-stabilization-timeout: 10s
jobmanager.adaptive-scheduler.executing.cooldown-after-rescaling: 30s
jobmanager.adaptive-scheduler.scaling-interval.min: 30s

# 시작 시 최소 자원 대기
jobmanager.adaptive-scheduler.resource-wait-timeout: 10min
```

`scaling-interval.min`이 너무 짧으면 잡이 자주 재시작. `cooldown-after-rescaling`은 한 번 rescale 후 일정 시간은 다시 안 함.

---

## 5. AdaptiveScheduler가 ExecutionGraph 재빌드하는 이유

ExecutionGraph는 immutable — parallelism 변경하려면 새 EG 빌드 → 기존 task graceful cancel → 마지막 checkpoint에서 새 EG로 restore.

```
[old EG, parallelism=8, RUNNING]
   ↓ ResourceRequirements 변경 또는 slot 변동
[Restarting]
   ↓ savepoint trigger (sync) — 마지막 안정 상태 보존
   ↓ task cancel
   ↓ new EG (parallelism=16) 빌드
   ↓ savepoint에서 restore
[Executing — new EG, parallelism=16]
```

`maxParallelism` (=key group 수)이 새 parallelism의 상한 — 첫 배포 시 충분히 크게 잡아둬야 (이전 문서에서 강조).

---

## 6. Operator autoscaler 연동 흐름

```
[Operator's autoscaler (외부 레포)]
  Flink REST `/jobs/<id>/metrics?get=...` 폴링 (busy time, backpressure 등)
  ↓
  새 parallelism 산출
  ↓
  REST `PATCH /jobs/<id>/resource-requirements` (FLIP-291)

[Flink JM AdaptiveScheduler]
  새 ResourceRequirements 받음 (per-vertex parallelism)
  ↓
  state transition Executing → Restarting → CreatingExecutionGraph → Executing
  ↓
  새 ExecutionGraph deploy
```

이 메커니즘 덕에 외부 (Operator)와 내부 (AdaptiveScheduler) 가 잘 분리됨.

---

## 7. 다음

- DefaultScheduler 비교: [`./02-default-scheduler-comparison.md`](./02-default-scheduler-comparison.md)
- Failover 전략: [`./03-failover-strategies.md`](./03-failover-strategies.md)
