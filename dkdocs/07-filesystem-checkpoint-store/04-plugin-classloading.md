# Plugin Classloading — S3/Hadoop jar 격리

> **요약**: Flink는 FileSystem 구현체(S3, HDFS, OSS 등)를 plugin으로 격리해 별도 child-first ClassLoader에서 로드. 사용자 JAR이 Hadoop의 다른 버전을 들고 와도 충돌 없음.
> **모듈**: `flink-core/core/plugin/`
> **선행**: [`./01-filesystem-abstraction.md`](./01-filesystem-abstraction.md)

---

## 1. TL;DR

각 plugin(예: `flink-s3-fs-presto`)은 `plugins/<name>/` 디렉토리에 jar로 배치 → Flink가 부팅 시 plugin manager가 각 plugin 디렉토리마다 별도 `URLClassLoader`(child-first 정책) 생성 → plugin이 의존하는 라이브러리(AWS SDK, Hadoop 등)가 plugin 안에서만 보임. 본인 환경에서 사용자 JAR이 Hadoop 의존성을 가져도 S3 plugin과 격리됨.

---

## 2. Plugin 디렉토리 구조 (Flink container 안)

```
/opt/flink/
├── lib/                           # Flink 본체 jars (모두에게 보임)
└── plugins/
    ├── s3-fs-presto/
    │   └── flink-s3-fs-presto-2.0.0.jar  # S3 PrestoS3FileSystem + 의존성 (shaded)
    ├── s3-fs-hadoop/
    │   └── flink-s3-fs-hadoop-2.0.0.jar
    └── metrics-prometheus/
        └── flink-metrics-prometheus-2.0.0.jar
```

각 plugin 디렉토리는 별도 ClassLoader. plugin끼리도 격리 (s3-presto와 s3-hadoop이 같은 객체를 다르게 들고 있어도 충돌 없음).

---

## 3. ClassLoader 계층

```
SystemClassLoader (JVM 기본)
   └─ AppClassLoader (Flink 본체 + lib/*.jar)
         ├─ Plugin ClassLoader: s3-fs-presto (child-first)
         │     └─ flink-s3-fs-presto-2.0.0.jar (AWS SDK shaded)
         ├─ Plugin ClassLoader: s3-fs-hadoop (child-first)
         │     └─ flink-s3-fs-hadoop-2.0.0.jar (Hadoop S3A shaded)
         └─ User Code ClassLoader (per-job, child-first)
               └─ user-app.jar (사용자 JAR)
```

**child-first 정책**: 자식 ClassLoader가 클래스 로드 요청 시 **부모에게 위임 X**, 자기 jar에서 먼저 찾음 → 부모(Flink lib)에 있는 같은 클래스의 다른 버전이 있어도 자기 것을 우선.

이게 plugin 격리의 핵심.

---

## 4. 핵심 클래스 (간단히)

| 역할 | 클래스 | 위치 |
|------|------|------|
| Plugin 로더 | `PluginManager` (Interface), `DefaultPluginManager` | `flink-core/.../core/plugin/` |
| Plugin 디스커버리 | `PluginUtils.createPluginManagerFromRootFolder(...)` | 같은 패키지 |
| Plugin classloader | `PluginLoader` | 같은 패키지 |
| Child-first | `ChildFirstClassLoader` | `flink-core/.../core/classloading/` |

---

## 5. 사용자 JAR ClassLoader는 별도

User code classloader는 **잡 단위**로 만들어짐 (TM에서 task 실행 시). default `child-first` (Flink lib과 사용자 JAR이 같은 라이브러리 다른 버전을 들고 있을 때 사용자 것 우선).

설정:
```yaml
classloader.resolve-order: child-first   # default (사용자 우선)
# 또는
classloader.resolve-order: parent-first  # Flink lib 우선 (호환성 안전)
```

본인 환경에서 사용자 JAR이 Iceberg 클라이언트를 가지고 있는데 Flink lib과 충돌하면 → child-first로 사용자 버전 우선.

---

## 6. 흔한 이슈

| 증상 | 원인 | 해결 |
|------|------|------|
| `ClassNotFoundException: org.apache.flink.fs.s3.common.S3FileSystem` | plugin 디렉토리에 jar 없음 | `/opt/flink/plugins/s3-fs-presto/` 안에 jar 배치 |
| `LinkageError` 또는 method not found | classloader 충돌 (사용자 JAR에 같은 클래스의 다른 버전) | classloader.resolve-order 점검, 의존성 shade |
| `s3 scheme not found` | plugin 로드 실패 또는 `META-INF/services/` 누락 | jar 안의 services 파일 확인 |

---

## 7. K8s Operator의 plugin 처리

`FlinkDeployment` CR에서:
```yaml
spec:
  flinkConfiguration:
    # plugin 활성화는 자동 — Flink 이미지에 이미 포함되거나
    # 사용자가 init container로 추가
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          env:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: "flink-s3-fs-presto-2.0.0.jar"
```

`ENABLE_BUILT_IN_PLUGINS` 환경변수가 Flink 부팅 스크립트가 인식 — `opt/flink/opt/<plugin>.jar` 를 `plugins/<name>/` 으로 복사.

---

## 8. 다음

- 체크포인트 storage on S3 종합: [`./05-checkpoint-storage-on-s3.md`](./05-checkpoint-storage-on-s3.md)
