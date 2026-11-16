"""Record frame inside a batch.

Record body layout (Protobuf message)::

    field 1 (varint): message type
    field 2 (varint): schema version
    field 3 (bytes) : serialized record payload
"""

from dataclasses import dataclass

from .errors import QuarantineError, QuarantineReason, WireError
from .wire import (
    WIRE_LEN,
    WIRE_VARINT,
    encode_bytes_field,
    encode_varint_field,
    parse_fields,
)

TAG_MSG_TYPE = 1
TAG_SCHEMA_VERSION = 2
TAG_PAYLOAD = 3


@dataclass
class RecordFrame:
    msg_type: int
    schema_version: int
    payload: bytes
    raw_frame: bytes = b""


def encode_record(msg_type, schema_version, payload):
    return (
        encode_varint_field(TAG_MSG_TYPE, msg_type)
        + encode_varint_field(TAG_SCHEMA_VERSION, schema_version)
        + encode_bytes_field(TAG_PAYLOAD, payload)
    )


def parse_record(body, raw_frame=b""):
    msg_type = None
    schema_version = None
    payload = None
    try:
        fields = parse_fields(body)
    except WireError as exc:
        raise QuarantineError(
            QuarantineReason.MALFORMED_WIRE, str(exc), raw_frame
        ) from exc
    for item in fields:
        if item.tag == TAG_MSG_TYPE and item.wire_type == WIRE_VARINT:
            msg_type = item.value
        elif item.tag == TAG_SCHEMA_VERSION and item.wire_type == WIRE_VARINT:
            schema_version = item.value
        elif item.tag == TAG_PAYLOAD and item.wire_type == WIRE_LEN:
            payload = item.value
    if msg_type is None or schema_version is None or payload is None:
        raise QuarantineError(
            QuarantineReason.ENVELOPE_ERROR,
            "record frame must contain type, version and payload",
            raw_frame,
        )
    return RecordFrame(msg_type, schema_version, payload, raw_frame)
