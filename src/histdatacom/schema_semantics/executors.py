"""Closed, reviewed byte-format implementations; no schema upgrade claims."""

import json

from .canonical import MAX_INPUT_BYTES, canonical_json, load_json

CANONICAL_EXECUTOR = "schema-semantics.canonical-json.v1"
INDENTED_EXECUTOR = "schema-semantics.indented-json.v1"


def execute_representation(implementation_id: str, source_json: str) -> str:
    value = load_json(source_json)
    if implementation_id == CANONICAL_EXECUTOR:
        return canonical_json(value, maximum=MAX_INPUT_BYTES)
    if implementation_id == INDENTED_EXECUTOR:
        result = json.dumps(
            value, ensure_ascii=True, sort_keys=True, indent=2, allow_nan=False
        )
        if len(result) > MAX_INPUT_BYTES:
            raise ValueError("representation output byte bound")
        return result
    raise ValueError("migration implementation is not admitted")
