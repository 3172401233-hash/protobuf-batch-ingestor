"""Exception hierarchy and quarantine reason codes."""


class QuarantineReason:
    TRUNCATED_FRAME = "TRUNCATED_FRAME"
    LENGTH_OVERFLOW = "LENGTH_OVERFLOW"
    ENVELOPE_ERROR = "ENVELOPE_ERROR"
    DIGEST_MISMATCH = "DIGEST_MISMATCH"
    RECORD_COUNT_MISMATCH = "RECORD_COUNT_MISMATCH"
    TRUNCATED_RECORD = "TRUNCATED_RECORD"
    UNKNOWN_MESSAGE_TYPE = "UNKNOWN_MESSAGE_TYPE"
    UNKNOWN_REQUIRED_MESSAGE = "UNKNOWN_REQUIRED_MESSAGE"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    WIRE_TYPE_MISMATCH = "WIRE_TYPE_MISMATCH"
    MALFORMED_WIRE = "MALFORMED_WIRE"
    NESTED_DECODE_ERROR = "NESTED_DECODE_ERROR"
    SEQUENCE_CONFLICT = "SEQUENCE_CONFLICT"


class IngestorError(Exception):
    """Base class for every error raised by the package."""


class VarintError(IngestorError):
    pass


class VarintTruncated(VarintError):
    """The byte stream ends in the middle of a varint."""


class VarintOverflow(VarintError):
    """A varint encodes more than 64 bits."""


class WireError(IngestorError):
    """A protobuf payload cannot be parsed on the wire level."""


class FramingError(IngestorError):
    """Base class for length-prefix / frame-boundary failures."""


class LengthOverflow(FramingError):
    """A frame-length varint itself exceeds 64 bits."""


class FrameTooLarge(FramingError):
    def __init__(self, declared, maximum):
        super().__init__(
            f"declared frame length {declared} exceeds maximum {maximum}"
        )
        self.declared = declared
        self.maximum = maximum


class FrameTruncated(FramingError):
    def __init__(self, received, expected=None):
        detail = f"got {received} bytes"
        if expected is not None:
            detail += f", expected {expected}"
        super().__init__(f"truncated frame: {detail}")
        self.received = received
        self.expected = expected


class UnknownMessageType(IngestorError):
    def __init__(self, msg_type):
        super().__init__(f"unknown message type {msg_type}")
        self.msg_type = msg_type


class QuarantineError(IngestorError):
    """A record/frame is valid on the wire but rejected by schema rules."""

    def __init__(self, reason, detail="", raw=b""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail
        self.raw = bytes(raw)


class BatchError(IngestorError):
    pass


class DigestMismatch(BatchError):
    pass


class RecordCountMismatch(BatchError):
    pass


class DuplicateBatch(BatchError):
    def __init__(self, source, sequence):
        super().__init__(f"duplicate batch {source}:{sequence}")
        self.source = source
        self.sequence = sequence


class SequenceConflict(BatchError):
    def __init__(self, source, sequence):
        super().__init__(f"sequence {sequence} reused with different digest")
        self.source = source
        self.sequence = sequence
