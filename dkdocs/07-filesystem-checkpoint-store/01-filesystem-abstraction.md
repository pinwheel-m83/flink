# Flink `FileSystem` 추상 — Persistence Contract

> **요약**: Flink의 `FileSystem` 클래스는 다양한 storage(LocalFS, HDFS, S3/MinIO, GCS, Azure)를 통일된 인터페이스로 추상화 — checkpoint, savepoint, file sink가 모두 이 위에서 동작.
> **모듈**: `flink-core/core/fs/`, `flink-filesystems/*`
> **기준 버전**: Flink `release-2.0`
> **운영 권장 여부**: ★ Production (모든 잡의 토대)

---

## 1. TL;DR

`FileSystem`은 추상 클래스(JDK `java.nio.file`과 다른 Flink 자체 추상)로 `open(path)` → `FSDataInputStream`, `create(path)` → `FSDataOutputStream` 같은 기본 API와 **persistence contract**(visibility + durability)를 정의. 본인 환경의 MinIO는 `flink-s3-fs-presto` 또는 `flink-s3-fs-hadoop` 플러그인이 S3 API로 매핑. SPI(`META-INF/services/`)로 scheme(`s3://`, `hdfs://` 등)별 factory가 자동 등록.

---

## 2. 핵심 클래스

| 역할 | 클래스 | 위치 |
|------|------|------|
| 추상 base | `FileSystem` (Abstract Class) | `flink-core/.../core/fs/FileSystem.java` |
| Output stream 추상 | `FSDataOutputStream` | `flink-core/.../core/fs/FSDataOutputStream.java` |
| Input stream 추상 | `FSDataInputStream` | `flink-core/.../core/fs/FSDataInputStream.java` |
| Factory (SPI) | `FileSystemFactory` (Interface) | `flink-core/.../core/fs/FileSystemFactory.java` |
| 로컬 구현 | `LocalFileSystem` | 같은 패키지 |
| Plugin 로더 | `PluginManager`, `PluginLoader` | `flink-core/.../core/plugin/` |

---

## 3. Persistence Contract (FileSystem.java javadoc 인용)

`flink-core/.../core/fs/FileSystem.java:97-160`:

```
Data written to an output stream is considered persistent, if two requirements are met:

1. Visibility Requirement: All other processes / machines / containers that are able to access
   the file see the data consistently when given the absolute file path.
   (similar to close-to-open semantics, restricted to the file)
   
2. Durability Requirement: The file system's specific durability requirements must be met.
   - LocalFileSystem: no guarantees for HW/OS crash
   - HDFS: durability up to N concurrent failures (replication factor)
   - S3/MinIO: per-object durability via storage backend

Updates to parent directory listing are NOT required to be complete for data to be persistent.
This relaxation supports object stores with eventually-consistent listings.

The FSDataOutputStream guarantees data persistence for written bytes once close() returns.
```

핵심 시사점:
- **`close()` 반환 = persistence 보장** — 이게 contract의 핵심
- **Directory listing은 eventually consistent여도 OK** — 이게 S3/MinIO 같은 object store가 Flink와 호환되는 이유
- **Visibility는 absolute path 기준** — list는 안 봐도 path를 알면 read 가능해야

---

## 4. SPI 등록 메커니즘

각 scheme(`s3://`, `hdfs://`, `oss://`, `wasb://` 등) 마다 `FileSystemFactory` 구현체가 META-INF/services에 등록:

```
flink-s3-fs-presto/src/main/resources/META-INF/services/
  org.apache.flink.core.fs.FileSystemFactory  ← S3FileSystemFactory 등록
```

`FileSystem.get(uri)` 호출 시:
1. URI scheme 추출 (`s3://bucket/key` → `s3`)
2. 등록된 factory 중 scheme 일치하는 것 찾음
3. factory.create(uri) → 구현체 인스턴스 반환

---

## 5. 본인 환경 매핑

본인 환경에서 사용되는 FileSystem:

| Scheme | Factory (위치) | 용도 |
|--------|--------------|------|
| `file://` | `LocalFileSystem` (built-in) | 테스트, MiniCluster |
| `s3://` | `flink-s3-fs-presto` 또는 `flink-s3-fs-hadoop` 플러그인 | **MinIO 체크포인트, savepoint, sink** |

`flink-conf.yaml`:
```yaml
state.checkpoints.dir: s3://flink-checkpoints/
state.savepoints.dir: s3://flink-savepoints/
s3.endpoint: https://minio.example.com
s3.access-key: ${MINIO_ACCESS_KEY}
s3.secret-key: ${MINIO_SECRET_KEY}
s3.path.style.access: true     # MinIO는 path-style 권장 (virtual-host-style 미지원 케이스)
```

---

## 6. 선택: presto vs hadoop S3 커넥터

| 구분 | flink-s3-fs-presto | flink-s3-fs-hadoop |
|------|-------------------|-------------------|
| 기반 | Trino/Presto 의 S3 클라이언트 | Hadoop의 S3A |
| 의존성 | shaded 단독 | Hadoop 의존성 (큰 jar) |
| 권장 사용 | **체크포인트** (가벼움) | **batch 잡 / 큰 데이터 read/write** |
| 권장 | 본인 환경의 streaming 잡엔 presto | 둘 다 동시에 사용 가능 (다른 path) |

---

## 7. 다음

- RecoverableWriter (재개 가능한 stream): [`./02-recoverable-writer.md`](./02-recoverable-writer.md)
- S3 multipart upload 메커니즘: [`./03-s3-multipart-upload.md`](./03-s3-multipart-upload.md)
- Plugin classloading (jar 격리): [`./04-plugin-classloading.md`](./04-plugin-classloading.md)
