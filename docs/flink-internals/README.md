# Flink Internals — 코드 레벨 학습 가이드

> Apache Flink v2.2 기반, 코드 레벨에서 Flink 내부 구조를 이해하기 위한 학습 가이드

---

## 목차

### [Phase 1: Job 구조의 기본 이해](./phase1-job-structure-basics.md)
- Flink Job의 5단계 패턴 (Environment → Source → Transform → Sink → Execute)
- StreamExecutionEnvironment과 Lazy Evaluation
- `execute()` 내부 흐름 추적
- DataStream API의 Transformation 등록 메커니즘

### [Phase 2: Graph 변환 파이프라인](./phase2-graph-transformation-pipeline.md)
- StreamGraph: 논리적 DAG (StreamNode, StreamEdge, Virtual Node)
- StreamGraphGenerator: Transformation → StreamGraph 변환
- JobGraph: Operator Chaining 적용 (StreamingJobGraphGenerator)
- ExecutionGraph: 병렬도 확장 (ExecutionVertex, IntermediateResult)

### [Phase 3: 런타임 아키텍처](./phase3-runtime-architecture.md)
- RPC 통신 메커니즘 (RpcEndpoint, RpcGateway, FencedRpcEndpoint)
- Dispatcher: Job 제출 관문
- ResourceManager: 리소스(Slot) 관리
- JobMaster: Job 실행 조율
- TaskExecutor: Task 실행
- Task: 라이프사이클과 상태 전이
- StreamTask: 메일박스 이벤트 루프

### [Phase 4: State & Checkpointing](./phase4-state-and-checkpointing.md)
- CheckpointCoordinator: 체크포인트 오케스트레이션
- CheckpointBarrier: Chandy-Lamport 알고리즘 구현
- 배리어 정렬과 비정렬 체크포인트
- State Backend: HashMapStateBackend vs RocksDB
- Keyed State와 Operator State
- 체크포인트 복구 과정

### [Phase 5: Scheduling & Failover](./phase5-scheduling-and-failover.md)
- DefaultScheduler: 스케줄링 총괄
- PipelinedRegionSchedulingStrategy: 리전 기반 스케줄링
- Slot Pool과 Slot Sharing
- RestartPipelinedRegionFailoverStrategy: 리전 기반 장애 복구
- 재시작 전략 (고정/지수 딜레이)

### [Phase 6: Network Shuffle & 데이터 교환](./phase6-network-shuffle.md)
- RecordWriter와 ChannelSelector (파티셔닝)
- ResultPartition과 InputGate
- NetworkBuffer와 BufferPool (메모리 관리)
- Netty 기반 네트워크 통신
- Credit-based Flow Control (백프레셔)

### [Phase 7: Watermark & Time](./phase7-watermark-and-time.md)
- Event Time vs Processing Time
- WatermarkStrategy와 BoundedOutOfOrdernessWatermarks
- StatusWatermarkValve: 다중 입력 Watermark 정렬
- InternalTimerService: Event Time 타이머
- WindowOperator: 윈도우 연산 내부 동작
- Allowed Lateness와 Side Output

---

## 추천 학습 순서

1. **Phase 1 → 2**: Job의 전체 구조와 그래프 변환 이해 (기초)
2. **Phase 3**: 런타임 컴포넌트의 역할과 상호작용 (중급)
3. **Phase 4**: State & Checkpointing — Flink의 핵심 차별점 (중급~고급)
4. **Phase 5~7**: 주제별 심화 학습 (고급)

## 실습 병행 추천

- 예제 코드에 브레이크포인트를 걸고 디버깅으로 내부 흐름 추적
- `MiniCluster`로 로컬에서 전체 클러스터 동작 확인
- 각 모듈의 `src/test/` 테스트 코드를 읽고 동작 이해

---

## 주요 FLIP 참조 목록

각 Phase에서 참조하는 FLIP(Flink Improvement Proposal)의 전체 목록입니다.

| FLIP | 제목 | 관련 Phase | 상태 |
|------|------|-----------|------|
| [FLIP-6](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65147077) | Flink Deployment and Process Model | Phase 3, 5 | 완료 (1.5) |
| [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27:+Refactor+Source+Interface) | Refactor Source Interface | Phase 1 | 완료 (1.11) |
| [FLIP-76](https://cwiki.apache.org/confluence/display/FLINK/FLIP-76:+Unaligned+Checkpoints) | Unaligned Checkpoints | Phase 4 | 완료 (1.11) |
| [FLIP-92](https://cwiki.apache.org/confluence/display/FLINK/FLIP-92:+Add+N-Ary+Stream+Operator+in+Flink) | N-Ary Stream Operator | Phase 2 | 완료 (1.12) |
| [FLIP-151](https://cwiki.apache.org/confluence/display/FLINK/FLIP-151) | Incremental Snapshots for Heap Backend | Phase 4 | 완료 |
| [FLIP-160](https://cwiki.apache.org/confluence/display/FLINK/FLIP-160:+Adaptive+Scheduler) | Adaptive Scheduler | Phase 5 | 완료 |
| [FLIP-182](https://cwiki.apache.org/confluence/display/FLINK/FLIP-182) | Watermark Alignment (Sources) | Phase 7 | 완료 |
| [FLIP-187](https://cwiki.apache.org/confluence/display/FLINK/FLIP-187:+Adaptive+Batch+Scheduler) | Adaptive Batch Scheduler | Phase 5 | 완료 |
| [FLIP-217](https://cwiki.apache.org/confluence/display/FLINK/FLIP-217) | Watermark Alignment (Splits) | Phase 7 | 완료 (1.17) |
| [FLIP-408](https://cwiki.apache.org/confluence/display/FLINK/FLIP-408) | DataStream API V2 (Umbrella) | Phase 1 | 진행중 |
| [FLIP-411](https://cwiki.apache.org/confluence/display/FLINK/FLIP-411) | Chaining-agnostic Operator ID | Phase 2 | 완료 |
| [FLIP-423](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=293046855) | Disaggregated State (Umbrella) | Phase 4 | 진행중 |
| [FLIP-467](https://cwiki.apache.org/confluence/display/FLINK/FLIP-467) | Generalized Watermarks | Phase 7 | 진행중 |
| [FLINK-7282](https://issues.apache.org/jira/browse/FLINK-7282) | Credit-based Flow Control | Phase 6 | 완료 (1.5) |
| [FLINK-12477](https://issues.apache.org/jira/browse/FLINK-12477) | Mailbox Threading Model | Phase 3 | 완료 (1.9) |
