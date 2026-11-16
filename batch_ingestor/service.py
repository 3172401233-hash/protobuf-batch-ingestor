"""High-level batch ingestion orchestration."""

from dataclasses import dataclass, field

from .batch import BatchReader
from .errors import (
    BatchError,
    DigestMismatch,
    DuplicateBatch,
    FrameTooLarge,
    FrameTruncated,
    FramingError,
    LengthOverflow,
    QuarantineError,
    QuarantineReason,
    RecordCountMismatch,
    UnknownMessageType,
)
from .framing import FrameBuffer
from .record import parse_record
from .schema import decode_payload


@dataclass
class QuarantineEvent:
    reason: str
    detail: str
    sequence: object = None
    raw: bytes = b""


@dataclass
class IngestResult:
    source: str
    frames_seen: int = 0
    batches_committed: int = 0
    duplicates: int = 0
    conflicts: int = 0
    records_staged: int = 0
    poisoned: bool = False
    events: list = field(default_factory=list)

    @property
    def quarantine_count(self):
        return len(self.events)


class Ingestor:
    """Frame-aware ingestor; one bad frame never drops later good frames."""

    def __init__(self, registry, store, source="default",
                 max_frame_size=64 * 1024 * 1024):
        self.registry = registry
        self.store = store
        self.source = source
        self.max_frame_size = max_frame_size

    def ingest_bytes(self, data):
        return self.ingest_stream([data])

    def ingest_file(self, path, chunk_size=64 * 1024):
        def chunks():
            with open(path, "rb") as handle:
                while True:
                    block = handle.read(chunk_size)
                    if not block:
                        return
                    yield block

        return self.ingest_stream(chunks())

    def ingest_stream(self, chunks):
        result = IngestResult(source=self.source)
        frame_buffer = FrameBuffer(max_frame_size=self.max_frame_size)
        poisoned = False

        for chunk in chunks:
            if poisoned:
                break
            try:
                frame_buffer.feed(chunk)
                for raw_frame in frame_buffer.frames():
                    result.frames_seen += 1
                    self._handle_frame(raw_frame, result)
            except (LengthOverflow, FrameTooLarge) as exc:
                self._quarantine(
                    result, QuarantineReason.LENGTH_OVERFLOW, str(exc), b""
                )
                poisoned = True

        if not poisoned:
            try:
                frame_buffer.end_of_stream()
            except FrameTruncated as exc:
                self._quarantine(
                    result,
                    QuarantineReason.TRUNCATED_FRAME,
                    str(exc),
                    bytes(frame_buffer._buf[frame_buffer._off:]),
                )
                poisoned = True
        result.poisoned = poisoned
        return result

    def _handle_frame(self, raw_frame, result):
        from .batch import parse_batch_body

        try:
            batch = parse_batch_body(raw_frame, raw_frame=raw_frame)
        except DigestMismatch as exc:
            self._quarantine(
                result, QuarantineReason.DIGEST_MISMATCH, str(exc), raw_frame
            )
            return
        except RecordCountMismatch as exc:
            self._quarantine(
                result, QuarantineReason.RECORD_COUNT_MISMATCH, str(exc),
                raw_frame,
            )
            return
        except QuarantineError as exc:
            self._quarantine(
                result, exc.reason, exc.detail or str(exc), exc.raw or raw_frame
            )
            return

        staged, quarantined = self._decode_records(batch, result)
        digest_hex = batch.digest.hex()
        try:
            status = self.store.commit_batch(
                self.source, batch.sequence, digest_hex, staged, quarantined
            )
        except DuplicateBatch:
            result.duplicates += 1
            return
        except BatchError as exc:
            result.conflicts += 1
            result.events.append(
                QuarantineEvent(
                    QuarantineReason.SEQUENCE_CONFLICT,
                    str(exc), batch.sequence, raw_frame,
                )
            )
            return
        if status == "committed":
            result.batches_committed += 1
            result.records_staged += len(staged)

    def _decode_records(self, batch, result):
        staged = []
        quarantined = []
        for index, record_frame in enumerate(batch.record_frames):
            try:
                record = parse_record(record_frame, raw_frame=record_frame)
                try:
                    spec = self.registry.get(
                        record.msg_type, record.schema_version
                    )
                except UnknownMessageType as exc:
                    raise QuarantineError(
                        QuarantineReason.UNKNOWN_MESSAGE_TYPE,
                        str(exc), record_frame,
                    ) from exc
                decoded = decode_payload(
                    spec, record.payload,
                    msg_type=record.msg_type,
                    schema_version=record.schema_version,
                )
                staged.append((index, record, decoded))
            except QuarantineError as exc:
                event = QuarantineEvent(
                    exc.reason, exc.detail or str(exc),
                    batch.sequence, exc.raw or record_frame,
                )
                result.events.append(event)
                quarantined.append(
                    {
                        "reason": event.reason,
                        "detail": event.detail,
                        "raw": event.raw,
                    }
                )
        return staged, quarantined

    def _quarantine(self, result, reason, detail, raw):
        result.events.append(QuarantineEvent(reason, detail, None, bytes(raw)))
        self.store.add_quarantine(self.source, None, reason, detail, raw)
