import unittest

from batch_ingestor.batch import build_batch_body, split_chunks
from batch_ingestor.errors import QuarantineReason
from batch_ingestor.framing import encode_frame
from batch_ingestor.record import encode_record
from batch_ingestor.schema import SchemaRegistry, SchemaSpec
from batch_ingestor.service import Ingestor
from batch_ingestor.store import StagingStore
from batch_ingestor.varint import encode_uvarint
from batch_ingestor.wire import (
    encode_bytes_field,
    encode_varint_field,
)


def event_payload(identifier, extra_fields=b"", version=1):
    fields = encode_varint_field(1, identifier)
    if version >= 2:
        fields += encode_bytes_field(2, f"name-{identifier}".encode())
    if version >= 3:
        fields += encode_varint_field(3, identifier * 10)
    return fields + extra_fields


def make_registry():
    registry = SchemaRegistry()
    registry.register(
        SchemaSpec(100, 1, {1: 0}, required_tags=frozenset({1}))
    )
    registry.register(
        SchemaSpec(
            100, 2, {1: 0, 2: 2}, required_tags=frozenset({1}),
            unknown_required_tags=frozenset({9}),
        )
    )
    registry.register(
        SchemaSpec(
            100, 3, {1: 0, 2: 2, 3: 0},
            required_tags=frozenset({1}),
            unknown_required_tags=frozenset({9}),
        )
    )
    registry.register(
        SchemaSpec(200, 1, {1: 0}, required_tags=frozenset({1}))
    )
    return registry


def build_stream(sequence, records):
    bodies = [
        encode_record(msg_type, version, payload)
        for msg_type, version, payload in records
    ]
    body, _, _ = build_batch_body(sequence, bodies)
    return encode_frame(body)


class IngestorIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.registry = make_registry()
        self.store = StagingStore(":memory:")
        self.ingestor = Ingestor(self.registry, self.store, source="svc-a")

    def test_cross_chunk_frames_at_every_chunk_size(self):
        stream = build_stream(
            1,
            [
                (100, 1, event_payload(1)),
                (100, 2, event_payload(2, version=2)),
                (100, 3, event_payload(3, version=3)),
                (200, 1, encode_varint_field(1, 77)),
            ],
        )
        for chunk_size in [1, 2, 3, 5, 7, 13, 64, 4096]:
            store = StagingStore(":memory:")
            ingestor = Ingestor(self.registry, store, source=f"cs-{chunk_size}")
            result = ingestor.ingest_stream(split_chunks(stream, chunk_size))
            self.assertEqual(result.frames_seen, 1)
            self.assertEqual(result.batches_committed, 1)
            self.assertEqual(result.records_staged, 4)
            self.assertEqual(result.poisoned, False)
            self.assertEqual(store.quarantine_count(), 0)

    def test_length_overflow_quarantines_and_poisons_stream(self):
        good = build_stream(1, [(100, 1, event_payload(1))])
        overflow = b"\x80" * 10 + b"\x01" + b"junk"
        result = self.ingestor.ingest_stream([good, overflow])
        self.assertEqual(result.batches_committed, 1)
        self.assertTrue(result.poisoned)
        self.assertEqual(result.events[0].reason,
                         QuarantineReason.LENGTH_OVERFLOW)
        self.assertEqual(self.store.quarantine_count(), 1)

    def test_declared_frame_too_large_is_quarantined(self):
        ingestor = Ingestor(self.registry, self.store, source="svc-a",
                            max_frame_size=16)
        result = ingestor.ingest_stream(
            [encode_uvarint(1024) + b"ab"]
        )
        self.assertTrue(result.poisoned)
        self.assertEqual(result.events[0].reason,
                         QuarantineReason.LENGTH_OVERFLOW)

    def test_unknown_optional_fields_preserved_unknown_required_quarantined(self):
        optional_unknown = encode_varint_field(5, 4242)
        stream = build_stream(
            1,
            [
                (100, 3, event_payload(1, optional_unknown, version=3)),
                (100, 3, event_payload(
                    2, encode_bytes_field(9, b"required-msg"), version=3)),
            ],
        )
        result = self.ingestor.ingest_bytes(stream)
        self.assertEqual(result.records_staged, 1)
        self.assertEqual(result.quarantine_count, 1)
        self.assertEqual(result.events[0].reason,
                         QuarantineReason.UNKNOWN_REQUIRED_MESSAGE)

        rows = self.store.staging_rows()
        self.assertEqual(len(rows), 1)
        self.assertIn('"unknown": true', rows[0][6])
        self.assertIn("4242", rows[0][6])

    def test_mixed_schema_versions_inside_one_batch(self):
        stream = build_stream(
            1,
            [
                (100, 1, event_payload(1)),
                (100, 2, event_payload(2, version=2)),
                (100, 3, event_payload(3, version=3)),
                (200, 1, encode_varint_field(1, 9)),
            ],
        )
        result = self.ingestor.ingest_bytes(stream)
        self.assertEqual(result.records_staged, 4)
        rows = self.store.staging_rows()
        self.assertEqual([row[4] for row in rows], [1, 2, 3, 1])
        self.assertEqual([row[3] for row in rows], [100, 100, 100, 200])

    def test_bad_frame_does_not_stop_later_frames(self):
        first = build_stream(
            1,
            [
                (100, 1, event_payload(1)),
                (100, 1, b""),
                (100, 1, event_payload(3)),
            ],
        )
        second = build_stream(
            2,
            [
                (100, 2, event_payload(4, version=2)),
                (999, 1, encode_varint_field(1, 1)),
                (100, 3, event_payload(6, version=3)),
            ],
        )
        third = build_stream(3, [(200, 1, encode_varint_field(1, 55))])

        result = self.ingestor.ingest_stream([first, second, third])
        self.assertEqual(result.batches_committed, 3)
        self.assertEqual(result.records_staged, 5)
        reasons = sorted(event.reason for event in result.events)
        self.assertEqual(
            reasons,
            [
                QuarantineReason.MISSING_REQUIRED_FIELD,
                QuarantineReason.UNKNOWN_MESSAGE_TYPE,
            ],
        )
        self.assertFalse(result.poisoned)
        rows = self.store.staging_rows()
        self.assertEqual(
            [(row[1], row[2]) for row in rows],
            [(1, 0), (1, 2), (2, 0), (2, 2), (3, 0)],
        )

    def test_truncated_outer_frame_at_eof_is_quarantined(self):
        good = build_stream(1, [(100, 1, event_payload(1))])
        result = self.ingestor.ingest_stream([good, good[:4]])
        self.assertEqual(result.batches_committed, 1)
        self.assertTrue(result.poisoned)
        self.assertEqual(result.events[0].reason,
                         QuarantineReason.TRUNCATED_FRAME)

    def test_duplicate_submission_does_not_restage(self):
        stream = build_stream(1, [(100, 1, event_payload(1))])
        first = self.ingestor.ingest_bytes(stream)
        second = self.ingestor.ingest_bytes(stream)
        third = self.ingestor.ingest_bytes(stream)
        self.assertEqual(first.batches_committed, 1)
        self.assertEqual(second.duplicates, 1)
        self.assertEqual(third.duplicates, 1)
        self.assertEqual(self.store.staging_count(), 1)
        self.assertEqual(len(self.store.ledger_rows()), 1)

    def test_data_order_change_changes_digest_and_conflicts(self):
        records = [
            (100, 1, event_payload(1)),
            (100, 1, event_payload(2)),
        ]
        original = build_stream(1, records)
        reordered = build_stream(1, list(reversed(records)))
        first = self.ingestor.ingest_bytes(original)
        second = self.ingestor.ingest_bytes(reordered)
        self.assertEqual(first.batches_committed, 1)
        self.assertEqual(second.conflicts, 1)
        self.assertEqual(second.batches_committed, 0)
        self.assertEqual(self.store.staging_count(), 2)
        self.assertEqual(second.events[0].reason,
                         QuarantineReason.SEQUENCE_CONFLICT)

    def test_digest_tampered_batch_is_quarantined(self):
        from batch_ingestor.batch import TAG_DIGEST
        from batch_ingestor.wire import encode_bytes_field

        body, digest, _ = build_batch_body(
            1, [encode_record(100, 1, event_payload(1))]
        )
        bad_body = body.replace(
            encode_bytes_field(TAG_DIGEST, digest),
            encode_bytes_field(TAG_DIGEST, b"\x00" * 32),
            1,
        )
        result = self.ingestor.ingest_bytes(encode_frame(bad_body))
        self.assertEqual(result.batches_committed, 0)
        self.assertEqual(result.events[0].reason,
                         QuarantineReason.DIGEST_MISMATCH)
        self.assertEqual(self.store.staging_count(), 0)

    def test_ingest_file_roundtrip(self):
        import tempfile
        from pathlib import Path

        stream = build_stream(
            1,
            [(100, 1, event_payload(1)), (100, 2, event_payload(2, version=2))],
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "batch.bin"
            path.write_bytes(stream)
            result = self.ingestor.ingest_file(str(path), chunk_size=7)
        self.assertEqual(result.records_staged, 2)
        self.assertFalse(result.poisoned)


if __name__ == "__main__":
    unittest.main()
