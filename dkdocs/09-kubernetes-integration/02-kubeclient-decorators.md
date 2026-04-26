# `FlinkKubeClient` & Decorator 패턴

> **요약**: `FlinkKubeClient` (fabric8 wrapper)가 K8s API를 호출. Pod spec은 Decorator 패턴으로 단계별 build — 사용자 podTemplate부터 시작해 conf mount, secret mount, init container 등을 차례로 적용.
> **모듈**: `flink-kubernetes/kubeclient/`

---

## 1. TL;DR

`FlinkKubeClient`가 fabric8 K8s 라이브러리를 wrap해 `createJobManagerComponent`, `createTaskManagerPod`, `stopPod` 등 high-level API 제공. Pod spec 생성은 `KubernetesStepDecorator` 인터페이스의 여러 구현체(`InitTaskManagerDecorator`, `FlinkConfMountDecorator`, `EnvSecretsDecorator` 등)를 chain으로 적용 — 사용자 podTemplate base 위에 Flink 측 필수 설정 덧붙임.

---

## 2. `FlinkKubeClient` 인터페이스

`flink-kubernetes/.../kubeclient/FlinkKubeClient.java:36-`:

```java
/**
 * The client to talk with kubernetes. The interfaces will be called both in Client and
 * ResourceManager. To avoid potentially blocking the execution of RpcEndpoint's main thread, these
 * interfaces createTaskManagerPod, stopPod should be implemented asynchronously.
 */
public interface FlinkKubeClient extends AutoCloseable {

    void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec);
    
    CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod);
    CompletableFuture<Void> stopPod(String podName);
    
    void stopAndCleanupCluster(String clusterId);
    
    Optional<KubernetesService> getRestService(String clusterId);
    KubernetesWatch watchPodsAndDoCallback(Map<String, String> labels, WatchCallbackHandler<KubernetesPod> podCallbackHandler);
    
    // ConfigMap (HA/leader election용)
    CompletableFuture<Boolean> checkAndUpdateConfigMap(String configMapName, Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> updateFunction);
    KubernetesConfigMapSharedWatcher createConfigMapSharedWatcher(Map<String, String> labels);
    
    // ...
}
```

핵심:
- `createTaskManagerPod` / `stopPod` — RM이 동적으로 호출 (Adaptive scaling)
- `watchPodsAndDoCallback` — Pod state 변화 (Running, Failed) watch
- `checkAndUpdateConfigMap` — atomic CAS update (HA leader election의 토대)

---

## 3. `KubernetesStepDecorator` (Pod spec 빌드)

decorators 패키지의 14개 구현체가 chain:

```
사용자 podTemplate (FlinkDeployment CR)
   ↓
InitTaskManagerDecorator    → 기본 container, image, resources 설정
   ↓
FlinkConfMountDecorator     → ConfigMap에 flink-conf.yaml mount
   ↓
HadoopConfMountDecorator    → Hadoop ConfigMap (HDFS 환경 시)
   ↓
EnvSecretsDecorator         → Secret을 환경변수로 주입 (예: AWS keys)
   ↓
MountSecretsDecorator       → Secret을 파일로 mount
   ↓
PodTemplateMountDecorator   → 사용자 추가 podTemplate 합성
   ↓
KerberosMountDecorator      → Kerberos keytab mount (Kerberos 환경)
   ↓
ExternalServiceDecorator    → REST endpoint를 외부에 노출하는 Service
   ↓
... (CmdTaskManagerDecorator로 마무리: command + args 설정)
   ↓
최종 Pod spec
```

각 Decorator는 `decorateFlinkPod(FlinkPod flinkPod) → FlinkPod` 메서드 — pure function 스타일.

---

## 4. 본인 환경에서의 의미

본인이 `FlinkDeployment` CR에 작성하는 podTemplate:
```yaml
spec:
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: MINIO_ACCESS_KEY
              valueFrom:
                secretKeyRef: { name: minio-creds, key: access-key }
          volumeMounts:
            - name: rocksdb-data
              mountPath: /flink/rocksdb
      volumes:
        - name: rocksdb-data
          emptyDir: { sizeLimit: 100Gi }
```

이게 `PodTemplateMountDecorator`에 의해 base spec과 합성. 그 결과 Flink 측 필수(ConfigMap mount, command args)와 사용자 측(volumes, env)이 통합된 최종 Pod spec이 K8s에 POST됨.

---

## 5. 다음

- ResourceManagerDriver 깊이: [`./03-k8s-resource-manager-driver.md`](./03-k8s-resource-manager-driver.md)
- HA & leader election (ConfigMap 기반): [`./04-k8s-ha-leader-election.md`](./04-k8s-ha-leader-election.md)
