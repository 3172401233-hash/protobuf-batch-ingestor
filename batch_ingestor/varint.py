"""Unsigned base-128 varint codec (Protobuf LEB128)."""

from .errors import VarintOverflow, VarintTruncated

MAX_UINT64 = (1 << 64) - 1


def encode_uvarint(value):
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("varint value must be an int")
    if value < 0 or value > MAX_UINT64:
        raise ValueError("varint value out of uint64 range")
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value & 0x7F)
    return bytes(out)


def read_uvarint(buf, pos=0):
    """Return ``(value, new_pos)``; raise when the varint is incomplete.

    A varint longer than 10 bytes, or a 10th byte carrying bits beyond the
    uint64 range, is an overflow rather than a truncation.
    """
    result = 0
    shift = 0
    start = pos
    length = len(buf)
    while True:
        if pos >= length:
            raise VarintTruncated("stream ended inside varint")
        byte = buf[pos]
        pos += 1
        if shift == 63:
            if byte & 0x80 or byte > 0x01:
                raise VarintOverflow("varint exceeds uint64 range")
            result |= (byte & 0x01) << 63
            return result, pos
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, pos
        shift += 7
        if pos - start >= 10:
            raise VarintOverflow("varint exceeds uint64 range")


def decode_uvarint(buf):
    value, pos = read_uvarint(buf, 0)
    if pos != len(buf):
        raise ValueError("trailing bytes after varint")
    return value
