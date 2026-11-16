"""Streaming length-prefixed frame reader.

Wire layout of one frame::

    +-------------------+-----------------------------+
    | varint frame_len  | frame_len bytes of payload  |
    +-------------------+-----------------------------+

The reader is fed arbitrary byte chunks (socket reads, file blocks, ...)
and yields whole frame payloads without ever assuming a read boundary
coincides with a frame boundary.
"""

from .errors import (
    FrameTooLarge,
    FrameTruncated,
    LengthOverflow,
    VarintError,
    VarintOverflow,
    VarintTruncated,
)
from .varint import encode_uvarint, read_uvarint

_COMPACT_AT = 4096


def encode_frame(payload):
    payload = bytes(payload)
    return encode_uvarint(len(payload)) + payload


class FrameBuffer:
    def __init__(self, max_frame_size=8 * 1024 * 1024):
        self.max_frame_size = max_frame_size
        self._buf = bytearray()
        self._off = 0

    def feed(self, chunk):
        if self._off:
            del self._buf[:self._off]
            self._off = 0
        self._buf.extend(chunk)

    def _view(self):
        return memoryview(self._buf)[self._off:]

    def next_frame(self):
        """Return one frame payload, or ``None`` when more bytes are needed.

        Raises :class:`LengthOverflow` / :class:`FrameTooLarge` on framing
        violations that make the rest of the stream unresyncable.
        """
        view = self._view()
        try:
            length, varint_end = read_uvarint(view, 0)
        except VarintTruncated:
            self._maybe_compact()
            return None
        except VarintOverflow as exc:
            raise LengthOverflow(str(exc)) from exc
        if length > self.max_frame_size:
            raise FrameTooLarge(length, self.max_frame_size)
        end = varint_end + length
        if end > len(view):
            self._maybe_compact()
            return None
        payload = bytes(view[varint_end:end])
        self._off += end
        self._maybe_compact()
        return payload

    def frames(self):
        """Yield every complete frame currently buffered."""
        while True:
            frame = self.next_frame()
            if frame is None:
                return
            yield frame

    def end_of_stream(self):
        """Flush at close time; raise on a dangling (truncated) tail."""
        view = self._view()
        if not view:
            return
        try:
            length, varint_end = read_uvarint(view, 0)
        except VarintError as exc:
            raise FrameTruncated(len(view)) from exc
        raise FrameTruncated(
            received=max(0, len(view) - varint_end), expected=length
        )

    @property
    def pending_bytes(self):
        return len(self._buf) - self._off

    def _maybe_compact(self):
        if self._off >= _COMPACT_AT:
            del self._buf[:self._off]
            self._off = 0
