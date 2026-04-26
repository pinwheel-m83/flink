# SourceCoordinator — JM 측 SplitEnumerator 호스트

> **요약**: `SourceCoordinator`는 `OperatorCoordinator`의 한 종류로, JM에서 `SplitEnumerator` 인스턴스를 호스팅하면서 reader-enumerator 간 OperatorEvent 라우팅, checkpoint 시 enumerator state 영속화, source reader registration/failover 처리 등을 담당.
> **모듈**: `flink-runtime/runtime/source/coordinator/`
> **선행**: [`./01-source-v2-overview.md`](./01-source-v2-overview.md)

---

## 1. TL;DR

`SourceCoordinator`는 `OperatorCoordinator` interface를 구현해 JM의 `OperatorCoordinatorHolder`(JobVertex당 1개)가 호스트. 안에 `SplitEnumerator`(예: `KafkaSourceEnumerator`)를 들고 있고, reader 측 `SourceOperator`가 보낸 `RequestSplitEvent` / `ReaderRegistrationEvent` / `SourceEventWrapper` 같은 OperatorEvent를 enumerator 메서드로 dispatch. 체크포인트 시 enumerator의 `snapshotState`를 호출해 직렬화 후 PendingCheckpoint에 acknowledger로 등록.

---

## 2. 핵심 클래스

| 역할 | 클래스 | 위치 |
|------|------|------|
| Coordinator 본체 | `SourceCoordinator<SplitT, EnumChkT>` | `flink-runtime/.../runtime/source/coordinator/SourceCoordinator.java` |
| 컨텍스트 (enumerator 측 API) | `SourceCoordinatorContext<SplitT>` | 같은 패키지 |
| Coordinator factory | `SourceCoordinatorProvider` | 같은 패키지 |
| OperatorCoordinator 추상 | `OperatorCoordinator` (Interface) | `flink-runtime/.../runtime/operators/coordination/` |
| Holder (JM 측 lifecycle) | `OperatorCoordinatorHolder` | 같은 위치 |
| Reader-enum events | `flink-runtime/.../runtime/source/event/` | `RequestSplitEvent`, `ReaderRegistrationEvent`, `AddSplitEvent`, `NoMoreSplitsEvent`, `SourceEventWrapper` |

---

## 3. 동작 (event 위주)

```
TM SourceOperator → OperatorEventGateway → JM SourceCoordinator → SplitEnumerator

이벤트 종류:
 - ReaderRegistrationEvent    : reader가 시작됨, address 등 알림
 - RequestSplitEvent          : reader가 새 split 요청
 - AddSplitEvent              : enumerator가 reader에게 split 할당
 - NoMoreSplitsEvent          : enumerator가 더 이상 split 없음 알림
 - SourceEventWrapper         : 사용자 정의 SourceEvent (Kafka offset commit 등에 활용)
```

각 이벤트는 RPC로 enumerator의 main thread executor로 dispatch — single-thread 모델.

---

## 4. Checkpoint 처리

`SourceCoordinator.checkpointCoordinator(checkpointId, resultFuture)`:

1. enumerator's main thread에서 `enumerator.snapshotState(checkpointId)` 호출
2. 결과 (EnumChkT)를 직렬화 (`Source.getEnumeratorCheckpointSerializer()`)
3. `resultFuture.complete(serializedBytes)` → `PendingCheckpoint`이 `notYetAcknowledgedOperatorCoordinators`에서 이 coordinator 제거

`notifyCheckpointComplete` 시 enumerator의 같은 메서드 호출 → Kafka offset commit 등 외부 commit 트리거 가능.

---

## 5. Reader Failover 처리

reader subtask가 죽으면:
1. JM이 `Execution.failed(...)` 처리
2. `SourceCoordinator.subtaskReset(subtask, checkpointId)` 호출
3. coordinator가 enumerator의 `addSplitsBack(assignedSplits, subtask)` 호출 → 미완료 split을 enumerator에 반환
4. 새 reader가 시작되면 `addReader(subtask)` → enumerator가 다시 split 분배

이 메커니즘 덕에 reader 실패 시 잡 전체 재시작 없이 부분 복구 가능.

---

## 6. 본인 환경 (Kafka)

`KafkaSourceEnumerator`(외부 레포):
- 시작 시 Kafka admin client로 partition 목록 fetch
- 새 partition 생기면 (topic 추가, partition 증가) 동적으로 reader에 할당 (`KafkaSourceEnumerator.discoveryNewPartitions()`)
- snapshot은 (assigned partitions, committed offsets, last discovery time)

본인 환경의 Operator autoscaler가 source parallelism을 늘리면:
1. AdaptiveScheduler가 새 ExecutionGraph (parallelism=N')
2. 새 reader subtask들 시작 → SourceCoordinator에 `ReaderRegistrationEvent`
3. enumerator가 partition을 새 reader들에게 재분배

---

## 7. 다음

- Sink V2 + 2PC commit: [`./03-sink-v2-overview.md`](./03-sink-v2-overview.md)
- Iceberg / Kafka 매핑 종합: [`./04-iceberg-kafka-mapping.md`](./04-iceberg-kafka-mapping.md)
