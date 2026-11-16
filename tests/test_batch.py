import unittest

from batch_ingestor.batch import (
    build_batch_body,
    compute_digest,
    parse_batch_body,
    split_chunks,
)
from batch_ingestor.errors import (
    DigestMismatch,
    RecordCountMismatch,
)
from batch_ingestor.framing import encode_frame
from batch_ingestor.record import encode_record
from batch_ingestor.wire import encode_bytes_field, encode_varint_field


def record_body(identifier):
    return encode_record(
        100, 1, encode_varint_field(1, identifier)
        + encode_bytes_field(2, f"payload-{identifier}".encode())
    )


class BatchTests(unittest.TestCase):
    def test_build_and_parse_roundtrip(self):
        bodies = [record_body(1), record_body(2), record_body(3)]
        body, digest, region = build_batch_body(sequence=42,
                                                record_bodies=bodies)
        batch = parse_batch_body(body, raw_frame=body)
        self.assertEqual(batch.sequence, 42)
        self.assertEqual(batch.record_count, 3)
        self.assertEqual(batch.digest, digest)
        self.assertEqual(len(batch.record_frames), 3)
        self.assertEqual(batch.record_frames[0], bodies[0])

    def test_digest_covers_record_order(self):
        bodies = [record_body(1), record_body(2)]
        body_a, digest_a, region_a = build_batch_body(1, bodies)
        body_b, digest_b, region_b = build_batch_body(1, list(reversed(bodies)))
        self.assertNotEqual(digest_a, digest_b)
        self.assertEqual(compute_digest(region_a), digest_a)
        parse_batch_body(body_a)
        parse_batch_body(body_b)

    def test_tampered_digest_is_rejected(self):
        body, _, _ = build_batch_body(1, [record_body(1)])
        from batch_ingestor.batch import TAG_DIGEST

        fields_body = body
        declared_digest = parse_batch_body(fields_body).digest
        tampered = fields_body.replace(
            encode_bytes_field(TAG_DIGEST, declared_digest),
            encode_bytes_field(TAG_DIGEST, b"\x00" * 32),
            1,
        )
        with self.assertRaises(DigestMismatch):
            parse_batch_body(tampered)

    def test_record_count_mismatch(self):
        body, _, region = build_batch_body(1, [record_body(1), record_body(2)])
        lying = body.replace(encode_varint_field(2, 2),
                             encode_varint_field(2, 5), 1)
        with self.assertRaises(RecordCountMismatch):
            parse_batch_body(lying)

    def test_truncated_inner_record_frame(self):
        from batch_ingestor.batch import _split_record_region
        from batch_ingestor.errors import (
            DigestMismatch,
            QuarantineError,
            QuarantineReason,
        )

        body, _, region = build_batch_body(1, [record_body(1)])
        cut_region = region[:-2]
        with self.assertRaises(DigestMismatch):
            parse_batch_body(
                body.replace(
                    encode_bytes_field(4, region),
                    encode_bytes_field(4, cut_region),
                    1,
                )
            )
        with self.assertRaises(QuarantineError) as ctx:
            _split_record_region(cut_region)
        self.assertEqual(ctx.exception.reason,
                         QuarantineReason.TRUNCATED_RECORD)

    def test_split_chunks_helper(self):
        data = encode_frame(b"abc")
        self.assertEqual(split_chunks(data, 2), [data[:2], data[2:]])
        with self.assertRaises(ValueError):
            split_chunks(data, 0)


if __name__ == "__main__":
    unittest.main()
