import unittest

from batch_ingestor.errors import (
    QuarantineError,
    QuarantineReason,
    UnknownMessageType,
)
from batch_ingestor.schema import (
    DecodedRecord,
    SchemaRegistry,
    SchemaSpec,
    decode_payload,
)
from batch_ingestor.wire import (
    encode_bytes_field,
    encode_fixed64_field,
    encode_varint_field,
)


def build_registry():
    registry = SchemaRegistry()
    registry.register(
        SchemaSpec(
            msg_type=100,
            version=1,
            field_wire_types={1: 0, 2: 2},
            required_tags=frozenset({1}),
        )
    )
    registry.register(
        SchemaSpec(
            msg_type=100,
            version=2,
            field_wire_types={1: 0, 2: 2, 3: 0},
            required_tags=frozenset({1}),
            unknown_required_tags=frozenset({9}),
            nested_schemas={
                7: SchemaSpec(
                    msg_type=101,
                    version=1,
                    field_wire_types={1: 0},
                    required_tags=frozenset({1}),
                    unknown_required_tags=frozenset({8}),
                )
            },
        )
    )
    return registry


class SchemaDecodeTests(unittest.TestCase):
    def setUp(self):
        self.registry = build_registry()

    def test_unknown_optional_fields_keep_raw_bytes(self):
        spec = self.registry.get(100, 2)
        raw_optional = b"\xde\xad\xbe\xef"
        payload = (
            encode_varint_field(1, 7)
            + encode_varint_field(5, 12345)
            + encode_bytes_field(6, raw_optional)
        )
        decoded = decode_payload(spec, payload, 100, 2)
        unknown = decoded.unknown_fields()
        self.assertEqual({field.tag for field in unknown}, {5, 6})
        by_tag = {field.tag: field for field in unknown}
        self.assertEqual(by_tag[6].value, raw_optional)
        self.assertEqual(by_tag[5].value, 12345)
        self.assertTrue(all(field.unknown for field in unknown))

    def test_unknown_required_message_segment_is_quarantined(self):
        spec = self.registry.get(100, 2)
        payload = (
            encode_varint_field(1, 7)
            + encode_bytes_field(9, b"required-new-message")
        )
        with self.assertRaises(QuarantineError) as ctx:
            decode_payload(spec, payload, 100, 2)
        self.assertEqual(ctx.exception.reason,
                         QuarantineReason.UNKNOWN_REQUIRED_MESSAGE)
        self.assertEqual(ctx.exception.raw, payload)

    def test_missing_required_field_is_quarantined(self):
        spec = self.registry.get(100, 1)
        with self.assertRaises(QuarantineError) as ctx:
            decode_payload(spec, encode_bytes_field(2, b"no id"), 100, 1)
        self.assertEqual(ctx.exception.reason,
                         QuarantineReason.MISSING_REQUIRED_FIELD)

    def test_wire_type_mismatch_is_quarantined(self):
        spec = self.registry.get(100, 1)
        payload = encode_varint_field(1, 1) + encode_fixed64_field(2, 9)
        with self.assertRaises(QuarantineError) as ctx:
            decode_payload(spec, payload, 100, 1)
        self.assertEqual(ctx.exception.reason,
                         QuarantineReason.WIRE_TYPE_MISMATCH)

    def test_mixed_schema_versions_in_one_batch_style(self):
        v1_payload = encode_varint_field(1, 11) + encode_bytes_field(2, b"v1")
        v2_payload = (
            encode_varint_field(1, 12)
            + encode_bytes_field(2, b"v2")
            + encode_varint_field(3, 99)
        )
        first = decode_payload(self.registry.get(100, 1), v1_payload, 100, 1)
        second = decode_payload(self.registry.get(100, 2), v2_payload, 100, 2)
        self.assertEqual(first.schema_version, 1)
        self.assertEqual(second.schema_version, 2)
        self.assertEqual(second.known_value(3), 99)

    def test_unknown_schema_version_falls_back_to_latest(self):
        spec = self.registry.get(100, 99)
        self.assertEqual(spec.version, 99)
        self.assertEqual(spec.field_wire_types, {1: 0, 2: 2, 3: 0})
        payload = encode_varint_field(1, 1) + encode_varint_field(42, 5)
        decoded = decode_payload(spec, payload, 100, 99)
        self.assertEqual(decoded.schema_version, 99)
        self.assertEqual(decoded.unknown_fields()[0].tag, 42)

    def test_unknown_message_type(self):
        with self.assertRaises(UnknownMessageType):
            self.registry.get(999, 1)

    def test_nested_message_decode_and_inner_quarantine(self):
        spec = self.registry.get(100, 2)
        good_nested = encode_varint_field(1, 3)
        payload = (
            encode_varint_field(1, 1)
            + encode_bytes_field(7, good_nested)
        )
        decoded = decode_payload(spec, payload, 100, 2)
        nested = decoded.known_value(7)
        self.assertIsInstance(nested, DecodedRecord)
        self.assertEqual(nested.known_value(1), 3)

        bad_nested = encode_bytes_field(8, b"unknown required inner")
        with self.assertRaises(QuarantineError) as ctx:
            decode_payload(
                spec,
                encode_varint_field(1, 1)
                + encode_bytes_field(7, bad_nested),
                100, 2,
            )
        self.assertEqual(ctx.exception.reason,
                         QuarantineReason.NESTED_DECODE_ERROR)


if __name__ == "__main__":
    unittest.main()
