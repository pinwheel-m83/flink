# `RecoverableWriter` — Resume-able Output Stream

> **요약**: 체크포인트 시점에 in-progress write를 재개 가능한 형태로 영속화하는 추상. 실패 후 재시작 시 마지막 체크포인트의 ResumeRecoverable로부터 이어쓰기. exactly-once 파일 sink와 checkpoint storage의 핵심.
> **모듈**: `flink-core/core/fs/`, `flink-filesystems/flink-s3-fs-base/`
> **선행**: [`./01-filesystem-abstraction.md`](./01-filesystem-abstraction.md)

---

## 1. TL;DR

`RecoverableWriter.open(path)` → `RecoverableFsDataOutputStream` 반환 → `out.write(...)` → 체크포인트 시 `out.persist()` → `ResumeRecoverable` 객체 (직렬화 가능, state에 저장 가능). 실패 후 `writer.recover(resumeRecoverable)`로 정확히 그 위치부터 이어쓰기. publish는 `out.closeForCommit().commit()` — atomic하게 최종 파일로 노출. S3에선 multipart upload, LocalFS에선 .inprogress 파일 + rename으로 구현.

---

## 2. 핵심 코드 (자체 javadoc이 가장 좋은 설명)

`flink-core/.../core/fs/RecoverableWriter.java:23-`:

```java
/**
 * The RecoverableWriter creates and recovers {@link RecoverableFsDataOutputStream}. It can be used
 * to write data to a file system in a way that the writing can be resumed consistently after a
 * failure and recovery without loss of data or possible duplication of bytes.
 *
 * <p>The streams do not make the files they write to immediately visible, but instead write to
 * temp files or other temporary storage. To publish the data atomically in the end, the stream
 * offers the {@link RecoverableFsDataOutputStream#closeForCommit()} method to create a committer
 * that publishes the result.
 *
 * <p>These writers are useful in the context of checkpointing. The example below illustrates how
 * to use them:
 *
 * <pre>{@code
 * // --------- initial run --------
 * RecoverableWriter writer = fileSystem.createRecoverableWriter();
 * RecoverableFsDataOutputStream out = writer.open(path);
 * out.write(...);
 *
 * // persist intermediate state
 * ResumeRecoverable intermediateState = out.persist();
 * storeInCheckpoint(intermediateState);
 *
 * // --------- recovery --------
 * ResumeRecoverable lastCheckpointState = ...; // get state from checkpoint
 * RecoverableWriter writer = fileSystem.createRecoverableWriter();
 * RecoverableFsDataOutputStream out = writer.recover(lastCheckpointState);
 *
 * out.write(...); // append more data
 * out.closeForCommit().commit(); // close stream and publish all the data
 *
 * // --------- recovery without appending --------
 * Committer committer = writer.recoverForCommit(lastCheckpointState);
 * committer.commit(); // publish the state as of the last checkpoint
 * }</pre>
 */
@PublicEvolving
public interface RecoverableWriter {
    RecoverableFsDataOutputStream open(Path path) throws IOException;
    RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) throws IOException;
    Committer recoverForCommit(CommitRecoverable resumable) throws IOException;
    boolean requiresCleanupOfRecoverableState();
    boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException;
    SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer();
    SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer();
    boolean supportsResume();
    
    interface ResumeRecoverable extends CommitRecoverable {}
    interface CommitRecoverable {}
}
```

핵심:
- **two-stage close**: `persist()` → 중간 상태 (체크포인트 가능), `closeForCommit().commit()` → 최종 publish
- **Recovery 두 가지 모드**: append 가능(`recover`) vs commit-only(`recoverForCommit`)
- **`SimpleVersionedSerializer`** — recoverable 객체를 직렬화 가능 (체크포인트에 들어감)
- **`supportsResume()`** — 모든 FS가 append를 지원하는 건 아님 (S3는 부분적 지원)

---

## 3. 사용처

| 컴포넌트 | RecoverableWriter 활용 |
|---------|---------------------|
| `FileSink` (Sink V2) | streaming 파일 sink — checkpoint 시 part file을 .inprogress → .pending → committed |
| `CheckpointStreamFactory` | state backend의 snapshot 영속화 (RocksDB SST 업로드 등) |
| Iceberg-Flink writer (외부) | data file 작성 (FileIO 추상의 한 구현으로 wrap 가능) |

---

## 4. RecoverableFsDataOutputStream 동작 패턴

```
open(path)
  ↓
write(data) → write(data) → ...
  ↓
persist() → ResumeRecoverable                    [체크포인트 N]
  ↓
write(data) → ...
  ↓
persist() → ResumeRecoverable                    [체크포인트 N+1]
  ↓
... (계속)
  ↓
closeForCommit() → Committer
  ↓
committer.commit()                                [최종 publish]
```

- 매 `persist()` 사이의 데이터는 commit 시 한꺼번에 atomic하게 노출
- 실패 시 `recover(latestResumeRecoverable)`로 그 시점부터 이어쓰기 가능

---

## 5. `requiresCleanupOfRecoverableState`

만약 `true` (S3 multipart upload 같은 경우)면, 잡 종료/실패 시 incomplete multipart upload를 명시적으로 cleanup해야 함 (안 하면 storage 비용 누적). Flink가 잡 종료 시 자동 cleanup 트리거.

---

## 6. 본인 환경 (S3/MinIO)에서

`S3RecoverableWriter` (`flink-s3-fs-base/.../writer/S3RecoverableWriter.java`)가:
- `open(path)` → S3 multipart upload 시작 + uploadId 발급
- `write(buf)` → 일정 크기 누적 후 `UploadPart` API 호출
- `persist()` → 누적된 part info를 직렬화 → `S3Recoverable`(`{uploadId, completedParts, currentPartBuf}`) 반환
- `recover(resumable)` → uploadId로 multipart upload 이어쓰기
- `commit()` → `CompleteMultipartUpload` API 호출 → 최종 객체 publish

자세한 multipart 동작은 [`./03-s3-multipart-upload.md`](./03-s3-multipart-upload.md).

---

## 7. FAQ

**Q1. checkpoint 사이에 write한 데이터는 어디 있나?**
A. S3의 incomplete multipart upload 영역 (객체 listing엔 안 보임). LocalFS면 .inprogress temp 파일.

**Q2. multipart upload timeout은?**
A. S3/MinIO 측 bucket policy로 설정. 보통 7일. **잡이 hard kill되어 며칠 후 수동 복구하려면** 충분히 길게 설정 필요 (FileSystem.java javadoc도 강조).

**Q3. cleanup이 안 되면?**
A. S3/MinIO에 incomplete multipart upload가 storage 차지 → 비용 누적. Lifecycle rule로 자동 abort 권장:

```
{
  "Rules": [{
    "ID": "abort-incomplete-mpu",
    "Status": "Enabled",
    "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}
  }]
}
```

**Q4. Flink Sink V2의 commit과 RecoverableWriter의 commit 관계?**
A. Sink V2의 `Committer`는 RecoverableWriter의 commit을 wrapping. checkpoint complete 시그널 받으면 `recoverForCommit(commitRecoverable).commit()` 호출 → 파일 publish.

---

## 8. 다음

- S3 multipart upload 상세: [`./03-s3-multipart-upload.md`](./03-s3-multipart-upload.md)
- Sink V2 Committer 와의 결합: [`../06-source-sink-spi/sink-committer.md`](../06-source-sink-spi/) (예정)
