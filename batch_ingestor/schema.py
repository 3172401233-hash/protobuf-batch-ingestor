"""Schema registry and record decoding.

The registry is intentionally schema-framework agnostic: a :class:`SchemaSpec`
declares the wire type of every known field, which fields are required and
which unknown field numbers are *required* message segments introduced by
newer producers. Everything else unknown is optional and kept as raw bytes.
"""

from dataclasses import dataclass, field

from .errors import (
    QuarantineError,
    QuarantineReason,
    UnknownMessageType,
    WireError,
)
from .wire import (
    WIRE_LEN,
    WIRE_VARINT,
    parse_fields,
)


@dataclass
class DecodedField:
    tag: int
    wire_type: int
    value: object
    unknown: bool = False


@dataclass
class DecodedRecord:
    msg_type: int
    schema_version: int
    fields: list
    raw: bytes

    def known_value(self, tag, default=None):
        for item in self.fields:
            if item.tag == tag:
                return item.value
        return default

    def unknown_fields(self):
        return [item for item in self.fields if item.unknown]


@dataclass
class SchemaSpec:
    msg_type: int
    version: int
    field_wire_types: dict = field(default_factory=dict)
    required_tags: frozenset = frozenset()
    unknown_required_tags: frozenset = frozenset()
    nested_schemas: dict = field(default_factory=dict)

    def wire_type_of(self, tag):
        return self.field_wire_types.get(tag)


class SchemaRegistry:
    def __init__(self):
        self._schemas = {}

    def register(self, spec):
        self._schemas[(spec.msg_type, spec.version)] = spec
        return spec

    def get(self, msg_type, version):
        key = (msg_type, version)
        if key in self._schemas:
            return self._schemas[key]
        known_versions = sorted(
            version for (mtype, version) in self._schemas if mtype == msg_type
        )
        if not known_versions:
            raise UnknownMessageType(msg_type)
        fallback = self._schemas[(msg_type, known_versions[-1])]
        return SchemaSpec(
            msg_type=msg_type,
            version=version,
            field_wire_types=dict(fallback.field_wire_types),
            required_tags=frozenset(fallback.required_tags),
            unknown_required_tags=frozenset(fallback.unknown_required_tags),
            nested_schemas=dict(fallback.nested_schemas),
        )

    def known_versions(self, msg_type):
        return sorted(
            version
            for (mtype, version) in self._schemas
            if mtype == msg_type
        )


def decode_payload(spec, payload, msg_type, schema_version):
    try:
        raw_fields = parse_fields(payload)
    except WireError as exc:
        raise QuarantineError(
            QuarantineReason.MALFORMED_WIRE, str(exc), payload
        ) from exc

    decoded = []
    seen = set()
    for raw_field in raw_fields:
        expected_wire = spec.wire_type_of(raw_field.tag)
        nested_spec = spec.nested_schemas.get(raw_field.tag)
        if expected_wire is None and nested_spec is None:
            if raw_field.tag in spec.unknown_required_tags:
                raise QuarantineError(
                    QuarantineReason.UNKNOWN_REQUIRED_MESSAGE,
                    f"unknown required field {raw_field.tag} on "
                    f"type {msg_type} v{schema_version}",
                    payload,
                )
            decoded.append(
                DecodedField(
                    raw_field.tag, raw_field.wire_type, raw_field.value, unknown=True
                )
            )
            continue
        if expected_wire is not None and expected_wire != raw_field.wire_type:
            raise QuarantineError(
                QuarantineReason.WIRE_TYPE_MISMATCH,
                f"field {raw_field.tag} expects wire type {expected_wire}, "
                f"got {raw_field.wire_type}",
                payload,
            )
        if nested_spec is not None:
            if raw_field.wire_type != WIRE_LEN:
                raise QuarantineError(
                    QuarantineReason.WIRE_TYPE_MISMATCH,
                    f"nested field {raw_field.tag} must be length-delimited",
                    payload,
                )
            try:
                nested = decode_payload(
                    nested_spec, raw_field.value,
                    msg_type=nested_spec.msg_type,
                    schema_version=nested_spec.version,
                )
            except QuarantineError as exc:
                raise QuarantineError(
                    QuarantineReason.NESTED_DECODE_ERROR,
                    f"field {raw_field.tag}: {exc.detail or exc.reason}",
                    payload,
                ) from exc
            decoded.append(
                DecodedField(raw_field.tag, raw_field.wire_type, nested)
            )
        else:
            decoded.append(
                DecodedField(raw_field.tag, raw_field.wire_type, raw_field.value)
            )
        seen.add(raw_field.tag)

    missing = spec.required_tags - seen
    if missing:
        raise QuarantineError(
            QuarantineReason.MISSING_REQUIRED_FIELD,
            f"missing required tags {sorted(missing)}",
            payload,
        )
    return DecodedRecord(msg_type, schema_version, decoded, bytes(payload))
