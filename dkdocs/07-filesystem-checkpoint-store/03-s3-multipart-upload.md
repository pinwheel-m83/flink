# S3 / MinIO Multipart Upload — 본인 환경 핵심

> **요약**: S3 (그리고 호환되는 MinIO)는 큰 객체를 업로드할 때 multipart upload — 한 객체를 여러 part로 분할해 병렬 업로드 후 합침. Flink의 RecoverableWriter가 multipart upload를 활용해 체크포인트 시점에 partial state를 영속화하고 재개 가능하게 함.
> **모듈**: `flink-filesystems/flink-s3-fs-base/`
> **선행**: [`./02-recoverable-writer.md`](./02-recoverable-writer.md)

---

## 1. S3 Multipart Upload 프로토콜 (간략)

```
1. CreateMultipartUpload(bucket, key)         → uploadId 반환
2. UploadPart(bucket, key, uploadId, partN)   → ETag (multiple parts in parallel)
3. CompleteMultipartUpload(bucket, key, uploadId, [{partN, ETag}, ...])
                                              → 최종 객체 visible
   또는
   AbortMultipartUpload(bucket, key, uploadId) → 중간 정리
```

특징:
- 각 part는 **5MB ≤ size ≤ 5GB** (마지막 part 제외)
- 객체 자체는 complete 호출 전까지 listing/GET 안 됨
- Parts는 별도 storage 영역 (incomplete multipart uploads)
- `CompleteMultipartUpload` 호출이 **atomic** — 부분 완성 상태 노출 X

---

## 2. Flink S3RecoverableWriter의 매핑

| Flink API | S3 API |
|-----------|--------|
| `writer.open(path)` | `CreateMultipartUpload` → uploadId |
| `out.write(buf)` (buffer 일정 크기 도달 시) | `UploadPart` |
| `out.persist()` | 현재까지의 part info를 ResumeRecoverable로 직렬화 (S3 호출 없음) |
| `out.closeForCommit()` | 마지막 part upload + Committer 객체 |
| `committer.commit()` | `CompleteMultipartUpload` |
| `writer.cleanupRecoverableState(...)` | `AbortMultipartUpload` |

핵심 코드 위치:
- `flink-filesystems/flink-s3-fs-base/.../writer/S3RecoverableWriter.java`
- `flink-filesystems/flink-s3-fs-base/.../writer/S3RecoverableMultipartUploadFactory.java`
- `flink-filesystems/flink-s3-fs-base/.../writer/S3Recoverable.java` (직렬화 형식)

---

## 3. ResumeRecoverable 안에 들어가는 것

`S3Recoverable` 클래스(개념):
```
{
  uploadId: "abc123",
  objectKey: "checkpoints/jobX/chk-100/_metadata",
  completedParts: [
    {partNumber: 1, ETag: "..."},
    {partNumber: 2, ETag: "..."},
    ...
  ],
  inProgressPartFile: <local temp file with current buffer>,
  inProgressPartFileSize: 1234,
  numBytesInParts: 10485760
}
```

- `completedParts` — 이미 S3에 업로드 끝난 part들 (재개 시 그대로 활용)
- `inProgressPartFile` — 마지막 partial part는 local에 임시 저장 (S3에 업로드된 part는 5MB 이상이어야 하므로)
- 직렬화되어 체크포인트에 들어감

---

## 4. 본인 환경 (MinIO) 설정

```yaml
# flink-conf.yaml
state.checkpoints.dir: s3://flink-checkpoints/
s3.endpoint: https://minio.example.com
s3.access-key: ${MINIO_ACCESS_KEY}
s3.secret-key: ${MINIO_SECRET_KEY}
s3.path.style.access: true
s3.connection.maximum: 100        # 동시 connection (체크포인트 transfer thread 수와 매칭)

# multipart 튜닝
s3.upload.min.part.size: 5242880          # 5MB (S3 최소)
s3.upload.max.concurrent.uploads: 10      # 동시 part 업로드
```

체크포인트 transfer thread (`state.backend.rocksdb.checkpoint.transfer-thread.num`)와 `s3.connection.maximum` 매칭 권장.

---

## 5. MinIO bucket lifecycle (필수)

incomplete multipart upload 자동 정리:

```bash
mc ilm rule add flink-checkpoints --abort-multipart-after 7d
# 또는 JSON으로
cat > lifecycle.json <<EOF
{
  "Rules": [{
    "ID": "abort-incomplete-mpu",
    "Status": "Enabled",
    "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}
  }]
}
EOF
mc ilm import flink-checkpoints < lifecycle.json
```

7일은 Flink 잡이 hard kill 후 며칠 만에 복구할 시간 여유 + storage 누적 방지의 균형.

---

## 6. 디버깅

```bash
# MinIO에 incomplete multipart 확인
mc ls --incomplete flink-checkpoints/

# 비정상 누적 시 manual abort
mc rm --incomplete --recursive --force flink-checkpoints/old-job/

# 한 객체의 multipart 상세
mc stat flink-checkpoints/jobX/chk-100/_metadata
```

---

## 7. 흔한 이슈

| 증상 | 원인 | 해결 |
|------|------|------|
| 체크포인트 storage 무한 증가 | incomplete MPU 누적 (cleanup 안 됨) | bucket lifecycle rule 적용 |
| 체크포인트 매우 느림 | part size 너무 작음 또는 concurrent uploads 너무 적음 | 튜닝 |
| Restore 실패 ("uploadId not found") | bucket lifecycle이 너무 짧아 multipart abort됨 | lifecycle DaysAfterInitiation 증가 |
| MinIO 503 SlowDown | 동시 part upload 너무 많음 | `s3.upload.max.concurrent.uploads` 감소 |
| `EntityTooSmall` | 마지막 외 part가 5MB 미만 | `s3.upload.min.part.size` 명시 (기본 5MB) |

---

## 8. 다음

- Plugin classloading (S3 jar 격리): [`./04-plugin-classloading.md`](./04-plugin-classloading.md)
- 체크포인트 storage on S3 종합: [`./05-checkpoint-storage-on-s3.md`](./05-checkpoint-storage-on-s3.md)
