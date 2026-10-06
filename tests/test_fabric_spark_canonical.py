from __future__ import annotations

import base64
import hashlib
import json
import math
import uuid
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from people_counter.fabric_spark_canonical import (
    SparkCanonicalizationError,
    canonical_spark_json_bytes,
    normalize_spark_value,
    spark_sha256,
)


class Row:
    __module__ = "pyspark.sql.types"

    def __init__(self, **values: object) -> None:
        self.values = values

    def __len__(self) -> int:
        return len(self.values)

    def asDict(self, recursive: bool = False) -> dict[str, object]:
        assert recursive is False
        return dict(self.values)


def test_actual_capture_date_path_and_type_have_redacted_diagnostics() -> None:
    secret = "abfss://private@storage/secret?token=credential"

    with pytest.raises(SparkCanonicalizationError) as captured:
        normalize_spark_value(
            Row(capture_date=object(), source_uri=secret),
            stage="spark-row-normalization:people_counter_video_work",
        )

    metadata = captured.value.safe_metadata()
    assert metadata == {
        "json_path": '$["capture_date"]',
        "python_type": "builtins.object",
        "reason": "unsupported Spark-returned type",
        "stage": "spark-row-normalization:people_counter_video_work",
        "structural_summary": {"kind": "object"},
    }
    diagnostic = str(captured.value)
    assert secret not in diagnostic
    assert "credential" not in diagnostic


def test_normalizes_expected_spark_values_losslessly_and_canonically() -> None:
    identifier = uuid.UUID("45db0b76-27d8-40ed-93c0-b88ff0480618")
    binary = bytearray(b"\x00\xff")
    value = Row(
        nested={
            "aware": datetime(
                2026,
                10,
                5,
                8,
                30,
                tzinfo=timezone(timedelta(hours=2)),
            ),
            "naive": datetime(2026, 10, 5, 6, 30),
            "date": date(2026, 10, 5),
            "decimal": Decimal("1000.2300"),
            "binary": binary,
            "uuid": identifier,
            "tuple": (Row(count=1), [True, None, 2.5]),
        }
    )

    normalized = normalize_spark_value(value, stage="test")

    assert normalized == {
        "nested": {
            "aware": "2026-10-05T06:30:00+00:00",
            "naive": "2026-10-05T06:30:00+00:00",
            "date": "2026-10-05",
            "decimal": {"$spark_decimal": "1000.2300"},
            "binary": {
                "$spark_binary_base64": base64.b64encode(binary).decode("ascii")
            },
            "uuid": str(identifier),
            "tuple": [{"count": 1}, [True, None, 2.5]],
        }
    }
    encoded = canonical_spark_json_bytes(value, stage="test")
    assert encoded == json.dumps(
        normalized,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assert spark_sha256(value, stage="test") == spark_sha256(
        normalized, stage="test"
    )


def test_canonical_order_and_hash_are_mapping_order_independent() -> None:
    left = {"z": 1, "a": {"y": 2, "b": 3}, "unicode": "\u00e9"}
    right = {"unicode": "\u00e9", "a": {"b": 3, "y": 2}, "z": 1}

    assert canonical_spark_json_bytes(left, stage="left") == (
        b'{"a":{"b":3,"y":2},"unicode":"\\u00e9","z":1}'
    )
    assert spark_sha256(left, stage="left") == spark_sha256(
        right, stage="right"
    )
    assert spark_sha256(left, stage="left") == (
        hashlib.sha256(
            canonical_spark_json_bytes(left, stage="left")
        ).hexdigest()
    )


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_rejects_nonfinite_float(value: float) -> None:
    with pytest.raises(
        SparkCanonicalizationError, match="non-finite float"
    ) as captured:
        normalize_spark_value({"nested": [value]}, stage="float-hash")
    assert captured.value.safe_metadata() == {
        "json_path": '$["nested"][0]',
        "python_type": "builtins.float",
        "reason": "non-finite float is forbidden",
        "stage": "float-hash",
        "structural_summary": {"finite": False, "kind": "float"},
    }


@pytest.mark.parametrize(
    "value",
    [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")],
)
def test_rejects_nonfinite_decimal(value: Decimal) -> None:
    with pytest.raises(
        SparkCanonicalizationError, match="non-finite Decimal"
    ) as captured:
        normalize_spark_value(value, stage="decimal-hash")
    assert captured.value.safe_metadata() == {
        "json_path": "$",
        "python_type": "decimal.Decimal",
        "reason": "non-finite Decimal is forbidden",
        "stage": "decimal-hash",
        "structural_summary": {
            "digits": len(value.as_tuple().digits),
            "exponent_type": "str",
            "finite": False,
            "kind": "decimal",
        },
    }


def test_rejects_unknown_object_without_repr_or_value() -> None:
    class SecretObject:
        def __repr__(self) -> str:
            return "SecretObject(token=do-not-leak)"

    with pytest.raises(SparkCanonicalizationError) as captured:
        normalize_spark_value(SecretObject(), stage="unknown")

    diagnostic = str(captured.value)
    assert "do-not-leak" not in diagnostic
    assert "SecretObject" in diagnostic


def test_rejects_non_string_and_duplicate_normalized_keys() -> None:
    with pytest.raises(
        SparkCanonicalizationError, match="mapping key is not a string"
    ) as non_string:
        normalize_spark_value({1: "value"}, stage="key")
    assert non_string.value.safe_metadata() == {
        "json_path": "$[<key>]",
        "python_type": "builtins.int",
        "reason": "mapping key is not a string",
        "stage": "key",
        "structural_summary": {"kind": "int"},
    }

    with pytest.raises(
        SparkCanonicalizationError, match="duplicate normalized mapping key"
    ) as captured:
        normalize_spark_value(
            {"e\u0301": 1, "\u00e9": 2},
            stage="key",
        )
    assert captured.value.safe_metadata() == {
        "json_path": '$["\\u00e9"]',
        "python_type": "builtins.str",
        "reason": "duplicate normalized mapping key",
        "stage": "key",
        "structural_summary": {"kind": "string", "length": 1},
    }


def test_rejects_row_like_unknown_object() -> None:
    class Pretender:
        def asDict(self, recursive: bool = True) -> dict[str, object]:
            return {"accepted": recursive}

    with pytest.raises(
        SparkCanonicalizationError, match="unsupported Spark-returned type"
    ):
        normalize_spark_value(Pretender(), stage="row")


def test_row_boundary_rejects_invalid_asdict_with_safe_metadata() -> None:
    def broken_as_dict(self: object, recursive: bool = False) -> object:
        assert recursive is False
        return object()

    BrokenRow = type(
        "Row",
        (),
        {
            "__module__": "pyspark.sql.types",
            "asDict": broken_as_dict,
        },
    )

    with pytest.raises(SparkCanonicalizationError) as captured:
        canonical_spark_json_bytes(
            BrokenRow(),
            stage="broken-row",
            path="$[7]",
        )
    assert captured.value.safe_metadata() == {
        "json_path": "$[7]",
        "python_type": "pyspark.sql.types.Row",
        "reason": "Spark Row asDict did not return a mapping",
        "stage": "broken-row",
        "structural_summary": {"kind": "row", "length": None},
    }


def test_hash_propagates_custom_path_and_stage_to_diagnostics() -> None:
    with pytest.raises(SparkCanonicalizationError) as captured:
        spark_sha256(
            {"nested": [object()]},
            stage="source-row-hash",
            path="$[3]",
        )

    assert captured.value.safe_metadata() == {
        "json_path": '$[3]["nested"][0]',
        "python_type": "builtins.object",
        "reason": "unsupported Spark-returned type",
        "stage": "source-row-hash",
        "structural_summary": {"kind": "object"},
    }


@pytest.mark.parametrize(
    "boundary",
    [canonical_spark_json_bytes, spark_sha256],
)
def test_canonical_boundaries_default_to_root_json_path(
    boundary: Callable[..., object],
) -> None:
    with pytest.raises(SparkCanonicalizationError) as captured:
        boundary(object(), stage="root")

    assert captured.value.path == "$"
