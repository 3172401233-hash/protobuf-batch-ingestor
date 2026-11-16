"""Minimal self-contained Protobuf wire-format reader and writer.

Only the pieces needed by the ingestor are implemented: varint, fixed 32/64
bit, length-delimited fields. Length-delimited payloads are returned as raw
bytes so that unknown optional fields can be preserved verbatim.
"""

from dataclasses import dataclass

from .errors import VarintTruncated, WireError
from .varint import encode_uvarint, read_uvarint

WIRE_VARINT = 0
WIRE_FIXED64 = 1
WIRE_LEN = 2
WIRE_FIXED32 = 5


@dataclass
class RawField:
    tag: int
    wire_type: int
    value: object

    @property
    def as_bytes(self):
        return self.value


def encode_tag(field_number, wire_type):
    return encode_uvarint((field_number << 3) | wire_type)


def encode_varint_field(field_number, value):
    return encode_tag(field_number, WIRE_VARINT) + encode_uvarint(value)


def encode_bytes_field(field_number, data):
    data = bytes(data)
    return encode_tag(field_number, WIRE_LEN) + encode_uvarint(len(data)) + data


def encode_fixed32_field(field_number, value):
    return encode_tag(field_number, WIRE_FIXED32) + int(value).to_bytes(4, "little")


def encode_fixed64_field(field_number, value):
    return encode_tag(field_number, WIRE_FIXED64) + int(value).to_bytes(8, "little")


def _read_exact(buf, pos, size):
    if pos + size > len(buf):
        raise WireError(
            f"truncated fixed-size field: need {size} bytes at offset {pos}"
        )
    return bytes(buf[pos:pos + size]), pos + size


def read_field(buf, pos=0):
    """Read one field; return ``(RawField | None, new_pos)``.

    ``None`` marks a clean end of message.
    """
    if pos >= len(buf):
        return None, pos
    try:
        tag, pos = read_uvarint(buf, pos)
    except VarintTruncated as exc:
        raise WireError(str(exc)) from exc
    field_number = tag >> 3
    wire_type = tag & 0x07
    if field_number == 0:
        raise WireError("invalid field number 0")
    if wire_type == WIRE_VARINT:
        try:
            value, pos = read_uvarint(buf, pos)
        except VarintTruncated as exc:
            raise WireError(str(exc)) from exc
    elif wire_type == WIRE_FIXED64:
        value, pos = _read_exact(buf, pos, 8)
    elif wire_type == WIRE_LEN:
        try:
            length, pos = read_uvarint(buf, pos)
        except VarintTruncated as exc:
            raise WireError(str(exc)) from exc
        if length > len(buf) - pos:
            raise WireError(
                f"length-delimited field {field_number} runs past message end"
            )
        value = bytes(buf[pos:pos + length])
        pos += length
    elif wire_type == WIRE_FIXED32:
        value, pos = _read_exact(buf, pos, 4)
    else:
        raise WireError(f"unsupported wire type {wire_type} for field {field_number}")
    return RawField(field_number, wire_type, value), pos


def iter_fields(buf):
    pos = 0
    while pos < len(buf):
        field, pos = read_field(buf, pos)
        yield field


def parse_fields(buf):
    return list(iter_fields(buf))
