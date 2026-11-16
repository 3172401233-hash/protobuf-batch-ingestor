import tempfile
import unittest
from pathlib import Path

from batch_ingestor.errors import DuplicateBatch, SequenceConflict
from batch_ingestor.record import encode_record, parse_record
from batch_ingestor.schema import (
    DecodedRecord,
    SchemaRegistry,
    SchemaSpec,
    decode_payload,
)
from batch_ingestor.store import StagingStore
from batch_ingestor.wire import encode_varint_field


def make_staged(identifier):
    payload = encode_varint_field(1, identifier)
    record = parse_record(encode_record(100, 1, payload))
    registry = SchemaRegistry()
    registry.register(SchemaSpec(100, 1, {1: 0}, frozenset({1})))
    decoded = decode_payload(registry.get(100, 1), payload, 100, 1)
    return record, decoded


class StagingStoreTests(unittest.TestCase):
    def test_commit_and_query(self):
        store = StagingStore(":memory:")
        record, decoded = make_staged(1)
        store.commit_batch("src", 1, "a" * 64,
                           [(0, record, decoded)], [])
        rows = store.staging_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][:5], ("src", 1, 0, 100, 1))
        self.assertEqual(store.staging_count(), 1)
        ledger = store.ledger_rows()
        self.assertEqual(ledger[0][:2], ("src", 1))

    def test_duplicate_batch_is_rejected(self):
        store = StagingStore(":memory:")
        record, decoded = make_staged(1)
        digest = "b" * 64
        store.commit_batch("src", 7, digest, [(0, record, decoded)], [])
        with self.assertRaises(DuplicateBatch):
            store.commit_batch("src", 7, digest, [(0, record, decoded)], [])
        self.assertEqual(store.staging_count(), 1)

    def test_same_sequence_different_digest_is_conflict(self):
        store = StagingStore(":memory:")
        record, decoded = make_staged(1)
        store.commit_batch("src", 7, "a" * 64,
                           [(0, record, decoded)], [])
        with self.assertRaises(SequenceConflict):
            store.commit_batch("src", 7, "c" * 64,
                               [(0, record, decoded)], [])
        self.assertEqual(store.staging_count(), 1)

    def test_quarantine_persisted_alongside_partial_batch(self):
        store = StagingStore(":memory:")
        record, decoded = make_staged(2)
        store.commit_batch(
            "src", 9, "d" * 64,
            [(0, record, decoded)],
            [{"reason": "MALFORMED_WIRE", "detail": "boom",
              "raw": b"\x01\x02"}],
        )
        quarantined = store.quarantine_rows()
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0][:4],
                         ("src", 9, "MALFORMED_WIRE", "boom"))
        self.assertEqual(bytes(quarantined[0][4]), b"\x01\x02")

    def test_persistence_to_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "staging.db"
            store = StagingStore(str(path))
            record, decoded = make_staged(5)
            store.commit_batch("src", 1, "e" * 64,
                               [(0, record, decoded)], [])
            store.close()

            reopened = StagingStore(str(path))
            self.assertEqual(reopened.staging_count(), 1)
            self.assertEqual(len(reopened.ledger_rows()), 1)
            reopened.close()


if __name__ == "__main__":
    unittest.main()
