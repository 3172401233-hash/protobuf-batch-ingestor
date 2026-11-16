"""Self-contained length-prefixed Protobuf batch ingestion toolkit."""

from .batch import (
    Batch,
    BatchReader,
    build_batch_body,
    compute_digest,
    parse_batch_body,
    split_chunks,
)
from .errors import (
    BatchError,
    DigestMismatch,
    DuplicateBatch,
    FrameTooLarge,
    FrameTruncated,
    FramingError,
    IngestorError,
    LengthOverflow,
    QuarantineError,
    QuarantineReason,
    RecordCountMismatch,
    SequenceConflict,
    UnknownMessageType,
    VarintError,
    WireError,
)
from .framing import FrameBuffer, encode_frame
from .record import RecordFrame, encode_record, parse_record
from .schema import (
    DecodedField,
    DecodedRecord,
    SchemaRegistry,
    SchemaSpec,
    decode_payload,
)
from .service import IngestResult, Ingestor, QuarantineEvent
from .store import StagingStore
from .varint import decode_uvarint, encode_uvarint, read_uvarint
from .wire import parse_fields

__all__ = [
    "Batch",
    "BatchError",
    "BatchReader",
    "DecodedField",
    "DecodedRecord",
    "DigestMismatch",
    "DuplicateBatch",
    "FrameBuffer",
    "FrameTooLarge",
    "FrameTruncated",
    "FramingError",
    "IngestResult",
    "Ingestor",
    "IngestorError",
    "LengthOverflow",
    "QuarantineError",
    "QuarantineEvent",
    "QuarantineReason",
    "RecordCountMismatch",
    "RecordFrame",
    "SchemaRegistry",
    "SchemaSpec",
    "SequenceConflict",
    "StagingStore",
    "UnknownMessageType",
    "VarintError",
    "WireError",
    "build_batch_body",
    "compute_digest",
    "decode_payload",
    "decode_uvarint",
    "encode_frame",
    "encode_record",
    "encode_uvarint",
    "parse_batch_body",
    "parse_fields",
    "parse_record",
    "read_uvarint",
    "split_chunks",
]

__version__ = "1.0.0"
