# Pod Template & Config — 본인 환경 운영 패턴

> **요약**: 사용자 podTemplate + Operator + Flink decorator의 결합으로 만들어지는 최종 Pod spec의 구성. RocksDB PVC, MinIO secret, podTemplate yaml 권장 패턴.

---

## 1. 최종 Pod spec의 출처 (3-layer 합성)

```
Layer 1: 사용자 podTemplate (FlinkDeployment.spec.podTemplate, taskManager.podTemplate)
            → containers, env, volumes, resources, nodeSelector 등
Layer 2: Operator의 default + 사용자 spec.flinkConfiguration 반영
            → image, command, sidecar (필요시)
Layer 3: Flink decorator chain (앞 02 문서)
            → ConfigMap mount, secret env, command args, internal service
```

세 layer를 순차 합성한 결과가 K8s에 POST되는 Pod yaml.

---

## 2. 권장 TM podTemplate (전체)

```yaml
spec:
  taskManager:
    resource:
      memory: "8g"           # request==limit 권장
      cpu: 4
    podTemplate:
      spec:
        # === Pod-level ===
        priorityClassName: flink-job          # 우선순위 (선택)
        terminationGracePeriodSeconds: 60     # graceful shutdown
        # tolerations / nodeSelector / affinity 본인 K8s 정책 따라
        
        containers:
          - name: flink-main-container
            # === MinIO 인증 ===
            env:
              - name: AWS_ACCESS_KEY_ID
                valueFrom:
                  secretKeyRef:
                    name: minio-creds
                    key: access-key
              - name: AWS_SECRET_ACCESS_KEY
                valueFrom:
                  secretKeyRef:
                    name: minio-creds
                    key: secret-key
              # === Polaris OAuth2 ===
              - name: POLARIS_CLIENT_ID
                valueFrom:
                  secretKeyRef: { name: polaris-oauth, key: client-id }
              - name: POLARIS_CLIENT_SECRET
                valueFrom:
                  secretKeyRef: { name: polaris-oauth, key: client-secret }
            
            # === RocksDB local storage ===
            volumeMounts:
              - name: rocksdb-data
                mountPath: /flink/rocksdb
              - name: tmp-data
                mountPath: /tmp
            
            # === resources (request==limit 권장 — OOMKill 방지) ===
            resources:
              requests:
                memory: "8Gi"
                cpu: "4"
                ephemeral-storage: "20Gi"
              limits:
                memory: "8Gi"
                cpu: "4"
                ephemeral-storage: "20Gi"
        
        volumes:
          - name: rocksdb-data
            emptyDir:
              sizeLimit: 100Gi      # state size + compaction 여유
              # 또는 PVC for local recovery 보존:
              # persistentVolumeClaim:
              #   claimName: rocksdb-pvc-{POD_NAME}
          - name: tmp-data
            emptyDir:
              sizeLimit: 5Gi
```

---

## 3. JM podTemplate

```yaml
spec:
  jobManager:
    resource:
      memory: "2g"
      cpu: 1
    replicas: 1                    # HA: 2 권장
    podTemplate:
      spec:
        containers:
          - name: flink-main-container
            env:
              - name: AWS_ACCESS_KEY_ID
                valueFrom: { secretKeyRef: { name: minio-creds, key: access-key } }
              - name: AWS_SECRET_ACCESS_KEY
                valueFrom: { secretKeyRef: { name: minio-creds, key: secret-key } }
            resources:
              requests: { memory: "2Gi", cpu: "1" }
              limits: { memory: "2Gi", cpu: "1" }
```

JM HA를 위해 `replicas: 2` 권장 — 하나가 죽어도 다른 standby가 즉시 leader 획득.

---

## 4. flinkConfiguration 권장 종합

```yaml
spec:
  flinkConfiguration:
    # === HA ===
    high-availability.type: kubernetes
    high-availability.storageDir: s3://flink-ha/
    high-availability.cluster-id: my-streaming-job
    
    # === State backend ===
    state.backend.type: rocksdb
    state.backend.incremental: true
    state.backend.local-recovery: true
    state.backend.rocksdb.localdir: /flink/rocksdb
    state.backend.rocksdb.predefined-options: FLASH_SSD_OPTIMIZED
    state.backend.rocksdb.memory.managed: true
    state.backend.rocksdb.checkpoint.transfer-thread.num: 4
    
    # === Checkpoint storage ===
    state.checkpoint-storage: filesystem
    state.checkpoints.dir: s3://flink-checkpoints/
    state.savepoints.dir: s3://flink-savepoints/
    
    # === Checkpointing ===
    execution.checkpointing.interval: "30s"
    execution.checkpointing.mode: EXACTLY_ONCE
    execution.checkpointing.unaligned: "true"
    execution.checkpointing.aligned-checkpoint-timeout: "30s"
    execution.checkpointing.tolerable-failed-checkpoints: "3"
    execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
    state.checkpoints.num-retained: "3"
    
    # === MinIO ===
    s3.endpoint: https://minio.example.com
    s3.path.style.access: "true"
    s3.connection.maximum: "100"
    
    # === Scheduler ===
    jobmanager.scheduler: adaptive
    
    # === Memory ===
    taskmanager.memory.process.size: 8gb
    taskmanager.numberOfTaskSlots: "8"
    
    # === Plugins (built-in) ===
    "kubernetes.taskmanager.environment.ENABLE_BUILT_IN_PLUGINS": "flink-s3-fs-presto-2.0.0.jar"
```

---

## 5. ServiceAccount RBAC

Operator가 자동 생성하는 SA의 권한:
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
rules:
  - apiGroups: [""]
    resources: ["pods", "configmaps", "services"]
    verbs: ["create", "get", "list", "watch", "update", "patch", "delete"]
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["create", "get", "list", "watch", "update", "patch", "delete"]
```

이 권한이 없으면 RM이 TM Pod 못 만들고 HA가 ConfigMap 못 씀.

---

## 6. 09 카테고리 마무리 + Phase B 종료!

6개 문서로 K8s 통합 영역 완성:
- 01-entrypoints.md
- 02-kubeclient-decorators.md
- 03-k8s-resource-manager-driver.md
- 04-k8s-ha-leader-election.md
- 05-operator-flink-boundary.md
- 06-pod-template-and-config.md (이 문서)

**Phase B 전체 완료** — 본인 환경 직결 모든 영역 (state-checkpoint / source-sink-spi / filesystem / file-formats / kubernetes) 다룸.

다음 — **Phase C** (확장):
- [`../10-scheduling-failover/`](../10-scheduling-failover/) — AdaptiveScheduler 깊이, Failover 전략
- [`../11-network-shuffle/`](../11-network-shuffle/) — Netty stack, credit-based flow
- [`../12-watermark-time/`](../12-watermark-time/) — EventTime, Window
- [`../13-new-datastream-v2/`](../13-new-datastream-v2/) — FLIP-409
