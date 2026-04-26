# Pekko 기반 Flink RPC

> **요약**: Flink의 모든 분산 컴포넌트(`Dispatcher`, `ResourceManager`, `JobMaster`, `TaskExecutor`)가 RPC로 통신하는 토대. Apache Pekko(Akka의 fork) actor를 백엔드로 쓰는 `RpcEndpoint`/`RpcGateway`/`RpcService` 추상.
> **모듈**: `flink-rpc/flink-rpc-core/`, `flink-rpc/flink-rpc-akka/`
> **기준 버전**: Flink `release-2.0`
> **운영 권장 여부**: ★Production

---

## 1. TL;DR (3문장)

`RpcEndpoint`는 분산 컴포넌트의 단일 thread "actor" — 모든 RPC 콜백이 endpoint의 main thread에서 직렬화 실행되어 internal mutable state에 lock 불필요. Caller는 `RpcGateway`(=Java interface)를 통해 typed method 호출 → Pekko가 내부적으로 actor message로 변환·전송 → endpoint 측 RpcServer가 수신해 main thread executor로 dispatch → method invoke → `CompletableFuture` return. **Fencing token**은 leader change 시 옛 leader의 stale RPC를 거부하기 위한 메커니즘 — `FencedRpcEndpoint<F>`가 자기 token을 가지고 incoming RPC의 token과 매칭.

---

## 2. 사전 지식

### 2.1 Actor 모델 (Erlang/Akka)

Actor = 메시지를 받아 처리하는 격리된 단위. 동시성을 메시지 전달로 처리(공유 메모리 X). Akka는 JVM에서 이 모델을 구현한 라이브러리. **Apache Pekko**는 Akka의 fork (Akka가 라이선스를 BSL로 변경하면서 ASF로 fork) — Flink는 2.x에서 Pekko로 전환.

### 2.2 Java dynamic proxy

`RpcGateway`는 인터페이스 — 실제 구현은 Pekko가 동적으로 생성한 proxy. 사용자는 `gateway.submitJob(plan, timeout)` 같은 typed call을 하지만 내부적으로 actor message로 변환되어 원격 endpoint에 전달.

### 2.3 `CompletableFuture` chaining + main thread

비동기 RPC 결과를 main thread에서 처리해야 할 때:

```java
gateway.someRemoteCall(arg)
    .thenAcceptAsync(result -> { /* main thread에서 실행 */ }, mainThreadExecutor);
```

이 패턴이 Flink runtime 코드 전반에 보임 — **IO는 다른 스레드, mutable state 접근은 main thread**.

---

## 3. 핵심 클래스 / 진입점

| 역할 | 클래스/인터페이스 | 위치 |
|------|------------------|------|
| Endpoint base (모든 컴포넌트 부모) | `RpcEndpoint` (Abstract Class) | `flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcEndpoint.java` |
| Gateway interface base | `RpcGateway` (Interface) | `flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcGateway.java` |
| Endpoint 호스팅 서비스 | `RpcService` (Interface) | `flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/RpcService.java` |
| Pekko 기반 service 구현 | `PekkoRpcService` (Class) | `flink-rpc/flink-rpc-akka/src/main/java/org/apache/flink/runtime/rpc/pekko/PekkoRpcService.java` |
| Fenced endpoint | `FencedRpcEndpoint<F>` (Abstract) | `flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/FencedRpcEndpoint.java` |
| Fenced gateway | `FencedRpcGateway<F>` (Interface) | `flink-rpc/flink-rpc-core/src/main/java/org/apache/flink/runtime/rpc/FencedRpcGateway.java` |
| Main thread executor (RpcEndpoint 안 inner class) | `RpcEndpoint.MainThreadExecutor` | 같은 파일 |
| Pekko actor | `PekkoRpcActor` (and `FencedPekkoRpcActor`) | `flink-rpc/flink-rpc-akka/src/main/java/org/apache/flink/runtime/rpc/pekko/` |

---

## 4. RPC 호출의 생명주기

```mermaid
sequenceDiagram
    participant Caller as "Caller (다른 endpoint 또는 client)"
    participant Gateway as "Gateway (Java proxy)"
    participant Net as Pekko remoting
    participant Actor as PekkoRpcActor
    participant MTE as MainThreadExecutor
    participant Endpoint as RpcEndpoint method handler

    Caller->>Gateway: gateway.submitJob(plan, timeout)
    Gateway->>Gateway: dynamic proxy: typed method → RemoteRpcInvocation message
    Gateway->>Net: 직렬화된 message + timeout
    Net->>Actor: actor receive (Pekko message dispatch)
    Actor->>Actor: fencing token 검증 (FencedRpcEndpoint면)
    Actor->>MTE: invoke method on main thread
    MTE->>Endpoint: handler 실행
    Endpoint-->>MTE: CompletableFuture&lt;Result&gt; 반환
    MTE-->>Actor: future complete 시 reply
    Actor->>Net: 결과 직렬화 → Pekko reply
    Net->>Gateway: future complete
    Gateway-->>Caller: CompletableFuture&lt;Result&gt;
```

---

## 5. 코드 워크스루

### 5.1 `RpcEndpoint` javadoc — 모델 정리

`flink-rpc/flink-rpc-core/.../RpcEndpoint.java:56-94`:

```java
/**
 * Base class for RPC endpoints. Distributed components which offer remote procedure calls have to
 * extend the RPC endpoint base class. An RPC endpoint is backed by an {@link RpcService}.
 *
 * <h1>Single Threaded Endpoint Execution</h1>
 *
 * <p>All RPC calls on the same endpoint are called by the same thread (referred to as the
 * endpoint's <i>main thread</i>). Thus, by executing all state changing operations within the main
 * thread, we don't have to reason about concurrent accesses, in the same way in the Actor Model of
 * Erlang or Akka.
 *
 * <p>The RPC endpoint provides {@link #runAsync(Runnable)}, {@link #callAsync(Callable, Duration)}
 * and the {@link #getMainThreadExecutor()} to execute code in the RPC endpoint's main thread.
 *
 * <h1>Lifecycle</h1>
 * - 생성: not running
 * - start(): onStart() 실행 후 running
 * - closeAsync(): onStop() 실행, 비동기 stop
 * - stop 완료: terminated
 */
public abstract class RpcEndpoint implements RpcGateway, AutoCloseableAsync {
    private final RpcService rpcService;
    private final String endpointId;
    protected final RpcServer rpcServer;
    final AtomicReference<Thread> currentMainThread = new AtomicReference<>(null);
    private final MainThreadExecutor mainThreadExecutor;
}
```

핵심 보장:
- **모든 RPC 콜백은 main thread**. 다른 스레드에서 호출 시도 시 `validateRunsInMainThread()`가 fail.
- main thread에서 다른 스레드 작업 결과를 처리하려면 `runAsync(Runnable)` / `callAsync(Callable, timeout)` / `mainThreadExecutor`.

### 5.2 `RpcEndpoint`의 핵심 헬퍼 메서드 (개념)

```java
// 1. 다른 스레드에서 main thread에 작업 던지기
endpoint.runAsync(() -> {
    // main thread에서 실행
    this.someState = newValue;
});

// 2. main thread에서 결과를 받아오는 패턴
CompletableFuture<Result> future = endpoint.callAsync(
    () -> { /* main thread에서 실행 후 결과 반환 */ return result; },
    Duration.ofSeconds(10));

// 3. 외부 future를 main thread에서 처리
externalFuture.thenAcceptAsync(
    result -> { /* main thread */ },
    endpoint.getMainThreadExecutor());

// 4. main thread 검증
endpoint.validateRunsInMainThread();  // 다른 스레드에서 호출 시 throw
```

이 패턴들이 `Dispatcher` / `JobMaster` 코드에 무수히 등장.

### 5.3 `RpcGateway` 인터페이스

`flink-rpc/flink-rpc-core/.../RpcGateway.java:21-37`:

```java
/** Rpc gateway interface which has to be implemented by Rpc gateways. */
public interface RpcGateway {

    /** Returns the fully qualified address under which the associated rpc endpoint is reachable. */
    String getAddress();

    /** Returns the fully qualified hostname under which the associated rpc endpoint is reachable. */
    String getHostname();
}
```

extension: `DispatcherGateway`, `JobMasterGateway`, `ResourceManagerGateway`, `TaskExecutorGateway` 모두 `RpcGateway` 또는 `FencedRpcGateway<...>`를 상속. 각 인터페이스의 메서드 = endpoint의 RPC 표면.

### 5.4 `RpcService` — endpoint 호스팅 + connect

`flink-rpc/flink-rpc-core/.../RpcService.java`:

```java
public interface RpcService {
    String getAddress();
    int getPort();

    <C extends RpcGateway> C getSelfGateway(Class<C> selfGatewayType, RpcServer rpcServer);

    /** 원격 endpoint에 연결 → typed gateway 반환 */
    <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz);

    /** 원격 fenced endpoint에 연결 (fencing token 함께 보냄) */
    <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz);

    /** 새 endpoint 호스팅 (RPC 서버 시작) */
    <C extends RpcEndpoint> RpcServer startServer(C rpcEndpoint, ...);

    // ...
}
```

JM이 RM에 연결하는 코드 (개념):

```java
CompletableFuture<ResourceManagerGateway> rmGatewayFuture = rpcService.connect(
    rmAddress, ResourceManagerGateway.class);

rmGatewayFuture.thenAcceptAsync(
    rmGateway -> {
        rmGateway.registerJobMaster(jobMasterId, ...);
    },
    getMainThreadExecutor());
```

### 5.5 `PekkoRpcService` — 실제 구현

`flink-rpc/flink-rpc-akka/.../PekkoRpcService.java`:

```java
public class PekkoRpcService implements RpcService {
    private final ActorSystem actorSystem;
    // ...
}
```

Pekko `ActorSystem`을 wrapping. `startServer(rpcEndpoint, ...)` 호출 시 새 `PekkoRpcActor`(또는 `FencedPekkoRpcActor`)를 actor system에 등록 → 그 actor가 endpoint 측 method를 main thread executor로 디스패치.

`connect(address, ...)` 호출 시 dynamic proxy를 만들어 반환 — 이 proxy가 Java method call → actor message 변환을 담당.

### 5.6 `FencedRpcEndpoint<F>` — Leader 보호

`flink-rpc/flink-rpc-core/.../FencedRpcEndpoint.java:34-65`:

```java
/**
 * ... rpc endpoint with fencing tokens. Furthermore, the rpc endpoint has its own fencing token
 * assigned. The rpc is then only executed if the attached fencing token equals the endpoint's own
 * token.
 */
public abstract class FencedRpcEndpoint<F extends Serializable> extends RpcEndpoint {

    private final F fencingToken;

    protected FencedRpcEndpoint(RpcService rpcService, String endpointId, F fencingToken,
                                Map<String, String> loggingContext) {
        super(rpcService, endpointId, loggingContext);
        Preconditions.checkNotNull(fencingToken, "The fence token should be null");
        Preconditions.checkNotNull(rpcServer, "The rpc server should be null");
        this.fencingToken = fencingToken;
    }

    public F getFencingToken() {
        return fencingToken;
    }
}
```

작동 원리:
1. Leader election → endpoint 인스턴스 생성 시 새 token 부여 (`DispatcherId`, `JobMasterId`, `ResourceManagerId` 등)
2. Caller는 `rpcService.connect(address, fencingToken, ...)`로 token과 함께 연결
3. Pekko actor가 incoming message의 token과 자기 token 비교 → 다르면 `FencingTokenException` reject
4. Leader 변경 시 새 token이 발급 → 옛 token으로 들어오는 RPC는 자동 거부

### 5.7 fencing의 실제 효과

JM failover 시나리오:
- 옛 JM이 자기 죽음을 모르고 있을 수 있음 (network partition)
- 새 JM이 leader가 됨 → 새 `JobMasterId` 발급
- 옛 JM이 RM이나 TM에게 RPC를 보냄 → 옛 token이라 거부됨
- 옛 JM은 fail해서 자살 → cluster에는 살아있는 JM이 새 것 1개

이 메커니즘 덕에 split-brain 시에도 데이터 정합성 유지.

---

## 6. 사용자 환경 매핑

### 6.1 본인 환경에서 흐르는 RPC

K8s 클러스터에서 일어나는 주요 RPC (대부분 같은 클러스터 내부 — Pod 간):

```
[Client / kubectl / Operator REST]
    ↓ HTTP REST
JM의 REST endpoint (DispatcherGateway → submitJob)
    ↓ Pekko RPC
JM Dispatcher → JobMaster (잡 spawn)
    ↓ Pekko RPC
JobMaster → ResourceManager (registerJobMaster, declareRequiredResources)
    ↓ Pekko RPC (or HTTP for K8s API)
ResourceManager → KubernetesResourceManagerDriver → fabric8 → K8s API (createPod)
    ↓
새 TM Pod 부팅
    ↓ Pekko RPC
TaskExecutor → ResourceManager (registerTaskExecutor)
    ↓ Pekko RPC
ResourceManager → TaskExecutor (requestSlot)
    ↓ Pekko RPC
TaskExecutor → JobMaster (offerSlots)
    ↓ Pekko RPC
JobMaster → TaskExecutor (submitTask)
    ↓
Task 실행 시작
    ↓ Pekko RPC (주기적)
TaskExecutor → JobMaster (heartbeatFromTaskManager + 상태 업데이트)
TaskExecutor → ResourceManager (heartbeatFromTaskManager)
```

### 6.2 RPC 설정 튜닝 (운영 시)

대표 옵션:
- `pekko.ask.timeout` — RPC ask timeout (기본 10초)
- `pekko.framesize` — 한 메시지의 최대 크기 (기본 10MB) — 큰 ExecutionPlan 보낼 때 늘려야 할 수 있음
- `pekko.tcp.timeout` — TCP 연결 timeout
- `taskmanager.network.request-backoff.initial` / `.max` — TM 간 네트워크 backoff (이건 RPC가 아닌 데이터 채널)

큰 잡(많은 vertex, 큰 사용자 jar)에서 `pekko.framesize` 늘려야 할 수 있음.

### 6.3 RPC 모니터링

- **JM 측**: 주요 RPC 메서드별 latency 메트릭 (rest endpoint).
- **Pekko log**: `log4j.logger.org.apache.pekko=INFO`로 actor system 로그 활성화.
- **잡 안 멈춤**: heartbeat timeout, RPC 거부 (fencing) 등을 로그에서 확인.

---

## 7. 관련 FLIP / JIRA

- [FLIP-6: Flink Deployment and Process Model](https://cwiki.apache.org/confluence/display/FLINK/FLIP-6+-+Flink+Deployment+and+Process+Model+-+ResourceManager%2C+JobManager%2C+TaskManager) — RPC 분리의 토대
- [FLINK-29281: Pekko 마이그레이션](https://issues.apache.org/jira/browse/FLINK-29281) — Akka → Pekko fork
- [FLIP-32: Restructure flink-table for future contributions](https://cwiki.apache.org/confluence/display/FLINK/FLIP-32%3A+Restructure+flink-table+for+future+contributions) — (간접) module 분리 흐름 참고
- [FLIP-185: Shorter Recovery Time](https://cwiki.apache.org/confluence/display/FLINK/FLIP-185%3A+Shorter+Recovery+Time+for+JobManager) — fencing + leader change 처리 개선

---

## 8. 디버깅 & 실험

### 8.1 로컬에서 RPC 호출 추적

`MiniCluster`에서 IDE 디버깅 시:
- `RpcEndpoint.runAsync` 또는 `callAsync` 브레이크
- `PekkoRpcActor.handleMessage` (안 보이면 source jar 확인)
- gateway proxy 호출 → actor message 변환 지점

### 8.2 RPC timeout / 거부 디버깅

```bash
# JM 로그에서
grep -iE "FencingTokenException|AskTimeout|RpcConnectionException|akka\\.pattern" jm.log
```

자주 마주치는 케이스:
- **AskTimeout**: 대상 endpoint가 main thread에서 다른 작업으로 막혀 있음 → thread dump
- **FencingTokenException**: 옛 leader의 RPC → 정상 (자동 회복)
- **RpcConnectionException**: Pod 재시작/네트워크 일시 단절 → 짧은 시간 내 자동 재연결 시도

### 8.3 큰 메시지 ack 실패

`framesize` 초과로 fail 시 로그에 명확히 표시. flink-conf.yaml 또는 K8s ConfigMap에서:
```yaml
pekko.framesize: 50mb
```

### 8.4 그래프 그래프 (MCP)

```
mcp__codebase-memory-mcp__search_graph(
  project="home-donamk-code-flink-flink-rpc",
  qn_pattern=".*PekkoRpcActor\\.(handleMessage|onReceive)$"
)
```

---

## 9. FAQ

**Q1. 왜 Akka가 아니라 Pekko?**
A. Akka가 BSL 라이선스로 변경 → ASF 호환 안 됨 → Apache Pekko로 fork (Akka 2.6.x 기반). API 거의 동일. Flink 1.17+에서 마이그레이션.

**Q2. RPC가 항상 single thread면 throughput 한계 아닌가?**
A. RPC handler는 main thread를 길게 점유하면 안 됨 (다른 RPC가 막힘). long-running 작업은 별도 executor로 던지고 결과를 future로 받음. main thread는 dispatch + light update에만 사용.

**Q3. data plane RPC도 Pekko인가?**
A. **아니**. 데이터 셔플(IntermediateResultPartition 데이터 송수신)은 별도 Netty 기반. RPC는 control plane (slot 협상, checkpoint trigger, heartbeat 등)만.

**Q4. RPC 직렬화는 어떻게?**
A. Java 직렬화 (`Serializable`) 또는 Kryo. RPC payload(arguments, return values)는 직렬화 가능해야 함. 그래서 `JobInformation`, `TaskInformation`, `SerializedValue<T>` 같은 wrapper 클래스가 많이 보임.

**Q5. fencing token 형태?**
A. 보통 `UUID` 기반 wrapper — `DispatcherId`, `JobMasterId`, `ResourceManagerId`. leader change 시마다 새 UUID. JM의 leader change는 `JobManagerRunner` 수준에서, RM은 `DefaultDispatcherRunner`/`ResourceManagerRunner` 수준에서.

**Q6. Cross-process RPC vs in-process RPC?**
A. 같은 JVM 안 endpoint끼리는 actor 메시지가 그냥 메모리 전달 (직렬화 skip 가능). 다른 JVM은 Pekko remoting (TCP). MiniCluster는 모두 한 JVM이라 in-process. K8s에선 Pod 간 cross-process.

---

## 10. 04 카테고리 마무리 및 다음

이 문서로 **Phase A의 04-runtime-architecture 6개 문서가 모두 완료**됨:

```
01-dispatcher.md        — 잡 접수처
02-resource-manager.md  — 슬롯/워커 관리
03-job-master.md        — 잡 1개의 매니저
04-task-executor.md     — slot 호스팅, Task 실행
05-stream-task-mailbox.md — subtask 메인 루프
06-rpc-pekko.md         — 모든 통신의 토대 (이 문서)
```

다음 큰 영역 — **Phase B (환경 직결)** 시작:
- [`../05-state-checkpoint/`](../05-state-checkpoint/) — Checkpoint, RocksDB, ForSt PoC
- [`../06-source-sink-spi/`](../06-source-sink-spi/) — Source V2, Sink V2, Iceberg/Kafka 매핑
- [`../07-filesystem-checkpoint-store/`](../07-filesystem-checkpoint-store/) — FileSystem, RecoverableWriter, S3/MinIO
- [`../08-file-formats/`](../08-file-formats/) — Parquet
- [`../09-kubernetes-integration/`](../09-kubernetes-integration/) — flink-kubernetes 모듈, K8s HA, Operator 경계
