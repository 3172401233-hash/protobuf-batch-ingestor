import unittest

from batch_ingestor.errors import (
    FrameTooLarge,
    FrameTruncated,
    LengthOverflow,
)
from batch_ingestor.framing import FrameBuffer, encode_frame
from batch_ingestor.varint import encode_uvarint


class FrameBufferTests(unittest.TestCase):
    def test_multiple_frames_in_one_chunk(self):
        stream = encode_frame(b"a") + encode_frame(b"bb") + encode_frame(b"ccc")
        buffer = FrameBuffer()
        buffer.feed(stream)
        self.assertEqual(list(buffer.frames()), [b"a", b"bb", b"ccc"])
        buffer.end_of_stream()

    def test_cross_chunk_boundaries_at_every_offset(self):
        payloads = [b"", b"x", b"hello frame", bytes(range(200))]
        stream = b"".join(encode_frame(payload) for payload in payloads)
        for chunk_size in range(1, len(stream) + 1):
            buffer = FrameBuffer()
            received = []
            for start in range(0, len(stream), chunk_size):
                buffer.feed(stream[start:start + chunk_size])
                received.extend(buffer.frames())
            buffer.end_of_stream()
            self.assertEqual(received, payloads, f"chunk_size={chunk_size}")

    def test_varint_split_across_chunks(self):
        payload = b"z" * 300
        stream = encode_frame(payload)
        self.assertGreater(len(stream), 3)
        for split in range(1, 3):
            buffer = FrameBuffer()
            buffer.feed(stream[:split])
            self.assertIsNone(buffer.next_frame())
            buffer.feed(stream[split:])
            self.assertEqual(buffer.next_frame(), payload)
            buffer.end_of_stream()

    def test_length_overflow_varint(self):
        buffer = FrameBuffer()
        buffer.feed(b"\x80" * 10 + b"\x01")
        with self.assertRaises(LengthOverflow):
            buffer.next_frame()

    def test_declared_length_above_maximum(self):
        buffer = FrameBuffer(max_frame_size=10)
        buffer.feed(encode_uvarint(11) + b"partial")
        with self.assertRaises(FrameTooLarge):
            buffer.next_frame()

    def test_truncated_frame_at_eof(self):
        stream = encode_frame(b"complete") + encode_uvarint(5) + b"ab"
        buffer = FrameBuffer()
        buffer.feed(stream)
        self.assertEqual(buffer.next_frame(), b"complete")
        with self.assertRaises(FrameTruncated):
            buffer.end_of_stream()

    def test_pending_bytes_tracking(self):
        buffer = FrameBuffer()
        buffer.feed(encode_uvarint(8) + b"abc")
        self.assertEqual(buffer.pending_bytes, 4)
        buffer.feed(b"defgh")
        self.assertEqual(buffer.next_frame(), b"abcdefgh")
        self.assertEqual(buffer.pending_bytes, 0)


if __name__ == "__main__":
    unittest.main()
