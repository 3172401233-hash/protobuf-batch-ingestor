import unittest

from batch_ingestor.errors import VarintOverflow, VarintTruncated
from batch_ingestor.varint import (
    MAX_UINT64,
    decode_uvarint,
    encode_uvarint,
    read_uvarint,
)


class VarintTests(unittest.TestCase):
    def test_roundtrip_small_and_boundary_values(self):
        for value in [0, 1, 127, 128, 16383, 16384, 2**32 - 1,
                      2**32, MAX_UINT64]:
            encoded = encode_uvarint(value)
            decoded, pos = read_uvarint(encoded, 0)
            self.assertEqual(decoded, value)
            self.assertEqual(pos, len(encoded))
            self.assertEqual(decode_uvarint(encoded), value)

    def test_known_encoding(self):
        self.assertEqual(encode_uvarint(0), b"\x00")
        self.assertEqual(encode_uvarint(300), b"\xac\x02")
        self.assertEqual(encode_uvarint(127), b"\x7f")
        self.assertEqual(encode_uvarint(128), b"\x80\x01")

    def test_read_with_offset_and_trailing_bytes(self):
        data = encode_uvarint(150) + b"\x01\x02"
        value, pos = read_uvarint(data, 0)
        self.assertEqual(value, 150)
        self.assertEqual(data[pos:], b"\x01\x02")

    def test_truncated_varint(self):
        with self.assertRaises(VarintTruncated):
            read_uvarint(b"\x80", 0)
        with self.assertRaises(VarintTruncated):
            read_uvarint(b"\x80\x80", 0)

    def test_overflow_varint(self):
        eleven_bytes = b"\x80" * 10 + b"\x01"
        with self.assertRaises(VarintOverflow):
            read_uvarint(eleven_bytes, 0)
        bad_tenth = b"\xff" * 9 + b"\x02"
        with self.assertRaises(VarintOverflow):
            read_uvarint(bad_tenth, 0)

    def test_encode_rejects_out_of_range(self):
        with self.assertRaises(ValueError):
            encode_uvarint(-1)
        with self.assertRaises(ValueError):
            encode_uvarint(MAX_UINT64 + 1)


if __name__ == "__main__":
    unittest.main()
