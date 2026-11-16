"""Batch envelope, digest and nested record framing.

A batch is itself one length-prefixed frame. Its body is a Protobuf message::

    field 1 (varint): sequence number
    field 2 (varint): record count
    field 3 (bytes) : SHA-256 digest of the serialized record region
    field 4 (bytes) : record region, itself a sequence of length-prefixed
                      record frames (see :mod:`batch_ingestor.record`)

The digest covers record content *and* record order, so a reordered
resubmission with the same sequence number is detected as a conflict.
"""

import hashlib
from dataclasses import dataclass

from .errors import (
    BatchError,
    DigestMismatch,
    QuarantineError,
    QuarantineReason,
    RecordCountMismatch,
    WireError,
)
from .framing import FrameBuffer, encode_frame
from .varint import MAX_UINT64
from .wire import (
    WIRE_LEN,
    WIRE_VARINT,
    encode_bytes_field,
    encode_varint_field,
    parse_fields,
)

TAG_SEQUENCE = 1
TAG_COUNT = 2
TAG_DIGEST = 3
TAG_RECORDS = 4


@dataclass
class Batch:
    sequence: int
    record_count: int
    digest: bytes
    record_frames: list


def compute_digest(record_region):
    return hashlib.sha256(record_region).digest()


def build_batch_body(sequence, record_bodies):
    region = b"".join(encode_frame(body) for body in record_bodies)
    digest = compute_digest(region)
    return (
        encode_varint_field(TAG_SEQUENCE, sequence)
        + encode_varint_field(TAG_COUNT, len(record_bodies))
        + encode_bytes_field(TAG_DIGEST, digest)
        + encode_bytes_field(TAG_RECORDS, region)
    ), digest, region


def parse_batch_body(body, raw_frame=b""):
    sequence = None
    record_count = None
    digest = None
    record_region = None
    try:
        fields = parse_fields(body)
    except WireError as exc:
        raise QuarantineError(
            QuarantineReason.ENVELOPE_ERROR, str(exc), raw_frame
        ) from exc
    for item in fields:
        if item.tag == TAG_SEQUENCE and item.wire_type == WIRE_VARINT:
            sequence = item.value
        elif item.tag == TAG_COUNT and item.wire_type == WIRE_VARINT:
            record_count = item.value
        elif item.tag == TAG_DIGEST and item.wire_type == WIRE_LEN:
            digest = item.value
        elif item.tag == TAG_RECORDS and item.wire_type == WIRE_LEN:
            record_region = item.value
    if sequence is None or record_count is None or digest is None:
        raise QuarantineError(
            QuarantineReason.ENVELOPE_ERROR,
            "batch envelope must contain sequence, count and digest",
            raw_frame,
        )
    record_region = record_region or b""
    actual_digest = compute_digest(record_region)
    if actual_digest != digest:
        raise DigestMismatch(
            f"digest mismatch: declared {digest.hex()}, "
            f"actual {actual_digest.hex()}"
        )
    record_frames = _split_record_region(record_region)
    if len(record_frames) != record_count:
        raise RecordCountMismatch(
            f"declared {record_count} records, region holds "
            f"{len(record_frames)}"
        )
    return Batch(sequence, record_count, digest, record_frames)


def _split_record_region(region):
    inner = FrameBuffer(max_frame_size=MAX_UINT64)
    inner.feed(region)
    frames = list(inner.frames())
    if inner.pending_bytes:
        raise QuarantineError(
            QuarantineReason.TRUNCATED_RECORD,
            "record region ends on a truncated record frame",
            region,
        )
    return frames


def split_chunks(data, chunk_size):
    """Test helper: cut a byte stream into fixed-size chunks."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


class BatchReader:
    """Yield complete outer batch frames from an arbitrary chunk stream."""

    def __init__(self, max_frame_size=64 * 1024 * 1024):
        self.frame_buffer = FrameBuffer(max_frame_size=max_frame_size)

    def feed(self, chunk):
        self.frame_buffer.feed(chunk)

    def batches(self):
        for raw_frame in self.frame_buffer.frames():
            yield parse_batch_body(raw_frame, raw_frame=raw_frame)

    def end_of_stream(self):
        return self.frame_buffer.end_of_stream()


__all__ = [
    "Batch",
    "BatchError",
    "BatchReader",
    "build_batch_body",
    "compute_digest",
    "parse_batch_body",
    "split_chunks",
]
