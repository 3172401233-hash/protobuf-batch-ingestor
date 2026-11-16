"""SQLite-backed staging, quarantine and idempotency ledger."""

import json
import sqlite3

from .errors import DuplicateBatch, SequenceConflict


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS staging (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    sequence_num INTEGER NOT NULL,
    frame_index INTEGER NOT NULL,
    msg_type INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    payload BLOB NOT NULL,
    decoded TEXT NOT NULL,
    raw_frame BLOB NOT NULL,
    staged_at REAL NOT NULL DEFAULT (julianday('now')),
    UNIQUE(source, sequence_num, frame_index)
);

CREATE TABLE IF NOT EXISTS batch_ledger (
    source TEXT NOT NULL,
    sequence_num INTEGER NOT NULL,
    digest_hex TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    PRIMARY KEY (source, sequence_num)
);

CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    sequence_num,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    raw BLOB NOT NULL,
    created_at REAL NOT NULL DEFAULT (julianday('now'))
);
"""


class StagingStore:
    def __init__(self, path=":memory:"):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def batch_status(self, source, sequence, digest_hex):
        row = self.conn.execute(
            "SELECT digest_hex FROM batch_ledger "
            "WHERE source = ? AND sequence_num = ?",
            (source, sequence),
        ).fetchone()
        if row is None:
            return None
        if row[0] == digest_hex:
            return "duplicate"
        return "conflict"

    def commit_batch(self, source, sequence, digest_hex, staged, quarantined):
        """Commit ledger + staging + quarantine rows in one transaction."""
        digest_hex = digest_hex.lower()
        status = self.batch_status(source, sequence, digest_hex)
        if status == "duplicate":
            raise DuplicateBatch(source, sequence)
        if status == "conflict":
            raise SequenceConflict(source, sequence)
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO batch_ledger "
                    "(source, sequence_num, digest_hex, record_count) "
                    "VALUES (?, ?, ?, ?)",
                    (source, sequence, digest_hex, len(staged)),
                )
                for index, record, decoded in staged:
                    self.conn.execute(
                        "INSERT INTO staging "
                        "(source, sequence_num, frame_index, msg_type, "
                        "schema_version, payload, decoded, raw_frame) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            source,
                            sequence,
                            index,
                            record.msg_type,
                            record.schema_version,
                            sqlite3.Binary(record.payload),
                            json.dumps(
                                _decoded_to_json(decoded), sort_keys=True
                            ),
                            sqlite3.Binary(record.raw_frame),
                        ),
                    )
                for entry in quarantined:
                    self.add_quarantine(
                        source, sequence,
                        entry["reason"], entry.get("detail", ""),
                        entry.get("raw", b""),
                        transactional=True,
                    )
        except Exception:
            raise
        return "committed"

    def add_quarantine(self, source, sequence, reason, detail, raw,
                       transactional=False):
        sql = (
            "INSERT INTO quarantine "
            "(source, sequence_num, reason, detail, raw) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        params = (source, sequence, reason, detail, sqlite3.Binary(bytes(raw)))
        if transactional:
            self.conn.execute(sql, params)
        else:
            with self.conn:
                self.conn.execute(sql, params)

    def staging_rows(self, source=None):
        sql = (
            "SELECT source, sequence_num, frame_index, msg_type, "
            "schema_version, payload, decoded, raw_frame "
            "FROM staging ORDER BY id"
        )
        params = ()
        if source is not None:
            sql = (
                "SELECT source, sequence_num, frame_index, msg_type, "
                "schema_version, payload, decoded, raw_frame "
                "FROM staging WHERE source = ? ORDER BY id"
            )
            params = (source,)
        return self.conn.execute(sql, params).fetchall()

    def staging_count(self):
        return self.conn.execute("SELECT COUNT(*) FROM staging").fetchone()[0]

    def quarantine_rows(self, source=None):
        sql = (
            "SELECT source, sequence_num, reason, detail, raw "
            "FROM quarantine ORDER BY id"
        )
        params = ()
        if source is not None:
            sql = (
                "SELECT source, sequence_num, reason, detail, raw "
                "FROM quarantine WHERE source = ? ORDER BY id"
            )
            params = (source,)
        return self.conn.execute(sql, params).fetchall()

    def quarantine_count(self):
        return self.conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]

    def ledger_rows(self):
        return self.conn.execute(
            "SELECT source, sequence_num, digest_hex, record_count "
            "FROM batch_ledger ORDER BY source, sequence_num"
        ).fetchall()


def _decoded_to_json(decoded):
    from .schema import DecodedField, DecodedRecord

    def convert(value):
        if isinstance(value, DecodedRecord):
            return {
                "msg_type": value.msg_type,
                "schema_version": value.schema_version,
                "fields": [convert(field) for field in value.fields],
                "raw": value.raw.hex(),
            }
        if isinstance(value, DecodedField):
            return {
                "tag": value.tag,
                "wire_type": value.wire_type,
                "unknown": value.unknown,
                "value": convert(value.value),
            }
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {"bytes_hex": bytes(value).hex()}
        return value

    return convert(decoded)
