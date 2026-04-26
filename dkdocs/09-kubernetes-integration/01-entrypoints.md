# K8s Entrypoints — Session vs Application Mode

> **요약**: `KubernetesSessionClusterEntrypoint` (다중 잡, long-running) vs `KubernetesApplicationClusterEntrypoint` (1 잡 = 1 cluster). Operator가 띄워주는 두 가지 cluster 시작점.
> **모듈**: `flink-kubernetes/entrypoint/`

---

## 1. TL;DR

K8s에서 JM Pod이 시작될 때 컨테이너 command가 둘 중 하나의 `main()`을 호출 — Session은 Dispatcher만 띄워 잡을 receive 대기, Application은 사용자 main을 같은 JVM에서 실행 → MiniDispatcher → 잡 종료 시 cluster shutdown까지. 두 entrypoint 모두 `ClusterEntrypoint` base를 상속, `KubernetesResourceManagerFactory`로 K8s-aware RM 구성.

---

## 2. 두 Entrypoint 비교

| 항목 | SessionMode | ApplicationMode |
|------|------------|----------------|
| 클래스 | `KubernetesSessionClusterEntrypoint` | `KubernetesApplicationClusterEntrypoint` |
| 잡 lifecycle | 클러스터 long-running, 잡 여러 번 submit/cancel | 1 잡 끝나면 cluster shutdown |
| Dispatcher | `StandaloneDispatcher` | `MiniDispatcher` |
| 사용자 main | client에서 실행 → REST submit | 같은 JM JVM에서 실행 (EmbeddedExecutor) |
| 권장 | Operator가 자유롭게 잡 관리 | Production 잡 1개 운영 시 |

---

## 3. ApplicationMode entrypoint 코드

`flink-kubernetes/.../entrypoint/KubernetesApplicationClusterEntrypoint.java:54-`:

```java
/** An {@link ApplicationClusterEntryPoint} for Kubernetes. */
@Internal
public final class KubernetesApplicationClusterEntrypoint extends ApplicationClusterEntryPoint {

    private KubernetesApplicationClusterEntrypoint(
            final Configuration configuration, final PackagedProgram program) {
        super(configuration, program, KubernetesResourceManagerFactory.getInstance());
    }

    public static void main(final String[] args) {
        EnvironmentInformation.logEnvironmentInfo(LOG, ..., args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        final Configuration dynamicParameters = ClusterEntrypointUtils.parseParametersOrExit(...);
        final Configuration configuration = KubernetesEntrypointUtils.loadConfiguration(dynamicParameters);

        PackagedProgram program = null;
        try {
            PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
            FileSystem.initialize(configuration, pluginManager);
            SecurityContext securityContext = installSecurityContext(configuration);
            program = securityContext.runSecured(() -> getPackagedProgram(configuration));
        } catch (Exception e) {
            LOG.error("Could not create application program.", e);
            System.exit(1);
        }
        // ... entrypoint 인스턴스 생성 + run
    }
}
```

핵심:
- **`KubernetesResourceManagerFactory.getInstance()`** — RM이 K8s-aware (TM Pod 요청)
- **`PluginManager` + `FileSystem.initialize`** — S3/MinIO plugin 로드 ([`../07-filesystem-checkpoint-store/04-plugin-classloading.md`](../07-filesystem-checkpoint-store/04-plugin-classloading.md))
- **`SecurityContext`** — Kerberos / Hadoop UserGroupInformation 설정 (HDFS 환경 시)
- **`PackagedProgram`** — 사용자 JAR + main class — Operator가 CR에 명시한 `jarURI`로부터 fetch

---

## 4. SessionMode entrypoint (앞서 04-runtime-architecture에서 다룸)

`KubernetesSessionClusterEntrypoint`는 더 단순 — `SessionClusterEntrypoint` 상속, `DispatcherResourceManagerComponent` 시작, REST endpoint listening. 자세한 내용은 [`../04-runtime-architecture/01-dispatcher.md`](../04-runtime-architecture/01-dispatcher.md) 참조.

---

## 5. K8s manifest 측 (Operator가 만들어줌)

### Application Mode pod
```yaml
spec:
  containers:
    - name: flink-main-container
      image: <flink-image-with-user-jar>
      command: ["bash", "-c", "kubernetes-application-entrypoint.sh"]
      args:
        - "--job-classname"
        - "com.example.MyApp"
        - ... (사용자 args)
```

### Session Mode pod
```yaml
command: ["jobmanager.sh", "start-foreground"]
# JobManager는 부팅하지만 잡은 별도 client/REST가 submit
```

Operator가 두 모드 자동으로 결정 (FlinkDeployment CR의 `mode` 또는 `job` 필드 유무).

---

## 6. 다음

- kubeclient + decorator 패턴: [`./02-kubeclient-decorators.md`](./02-kubeclient-decorators.md)
- K8s ResourceManagerDriver 자세히: [`./03-k8s-resource-manager-driver.md`](./03-k8s-resource-manager-driver.md)
- HA + leader election: [`./04-k8s-ha-leader-election.md`](./04-k8s-ha-leader-election.md)
- Operator-Flink 경계: [`./05-operator-flink-boundary.md`](./05-operator-flink-boundary.md)
