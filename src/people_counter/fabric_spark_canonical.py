"""Lossless conversion of expected Spark values to canonical JSON values.

This module is intentionally separate from the strict external JSON contract.
Only values returned by Spark cross this boundary; signed requests and other
external inputs must already be canonical JSON.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import unicodedata
import uuid
from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any


_SPARK_ROW_TYPE = "pyspark.sql.types.Row"


def _qualified_type(value: object) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _summary(value: object) -> dict[str, object]:
    """Describe structure without including a value or object representation."""

    if isinstance(value, Mapping):
        return {"kind": "mapping", "length": len(value)}
    if isinstance(value, (list, tuple)):
        return {"kind": "sequence", "length": len(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"kind": "binary", "length": len(value)}
    if isinstance(value, str):
        return {"kind": "string", "length": len(value)}
    if isinstance(value, datetime):
        return {
            "kind": "datetime",
            "timezone_aware": value.tzinfo is not None,
        }
    if isinstance(value, date):
        return {"kind": "date"}
    if isinstance(value, Decimal):
        decimal_tuple = value.as_tuple()
        return {
            "kind": "decimal",
            "digits": len(decimal_tuple.digits),
            "exponent_type": type(decimal_tuple.exponent).__name__,
            "finite": value.is_finite(),
        }
    if isinstance(value, float):
        return {"kind": "float", "finite": math.isfinite(value)}
    if value is None:
        return {"kind": "null"}
    if isinstance(value, (bool, int)):
        return {"kind": type(value).__name__}
    if isinstance(value, uuid.UUID):
        return {"kind": "uuid", "version": value.version}
    if _qualified_type(value) == _SPARK_ROW_TYPE:
        try:
            length = len(value)  # type: ignore[arg-type]
        except TypeError:
            length = None
        return {"kind": "row", "length": length}
    return {"kind": "object"}


class SparkCanonicalizationError(ValueError):
    """A Spark-returned value cannot cross the canonical JSON boundary."""

    def __init__(
        self,
        *,
        path: str,
        value: object,
        stage: str,
        reason: str,
    ) -> None:
        self.path = path
        self.python_type = _qualified_type(value)
        self.structural_summary = _summary(value)
        self.stage = stage
        self.reason = reason
        super().__init__(
            "Spark canonicalization failed: "
            + json.dumps(
                self.safe_metadata(),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
        )

    def safe_metadata(self) -> dict[str, object]:
        """Return only redaction-safe failure metadata."""

        return {
            "json_path": self.path,
            "python_type": self.python_type,
            "reason": self.reason,
            "stage": self.stage,
            "structural_summary": self.structural_summary,
        }


def _error(
    value: object,
    *,
    path: str,
    stage: str,
    reason: str,
) -> SparkCanonicalizationError:
    return SparkCanonicalizationError(
        path=path,
        value=value,
        stage=stage,
        reason=reason,
    )


def _member_path(path: str, key: str) -> str:
    return f"{path}[{json.dumps(key, ensure_ascii=True)}]"


def _spark_row_mapping(
    value: object,
    *,
    path: str,
    stage: str,
) -> Mapping[str, object] | None:
    if _qualified_type(value) != _SPARK_ROW_TYPE:
        return None
    as_dict = getattr(value, "asDict", None)
    if not callable(as_dict):
        raise _error(
            value,
            path=path,
            stage=stage,
            reason="Spark Row has no callable asDict",
        )
    converted = as_dict(recursive=False)
    if not isinstance(converted, Mapping):
        raise _error(
            value,
            path=path,
            stage=stage,
            reason="Spark Row asDict did not return a mapping",
        )
    return converted


def normalize_spark_value(
    value: object,
    *,
    stage: str,
    path: str = "$",
) -> object:
    """Convert only expected Spark return types without lossy coercion."""

    row_mapping = _spark_row_mapping(value, path=path, stage=stage)
    if row_mapping is not None:
        value = row_mapping

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _error(
                value,
                path=path,
                stage=stage,
                reason="non-finite float is forbidden",
            )
        return value
    if isinstance(value, datetime):
        observed = value
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        return observed.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise _error(
                value,
                path=path,
                stage=stage,
                reason="non-finite Decimal is forbidden",
            )
        return {"$spark_decimal": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "$spark_binary_base64": base64.b64encode(bytes(value)).decode(
                "ascii"
            )
        }
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise _error(
                    key,
                    path=f"{path}[<key>]",
                    stage=stage,
                    reason="mapping key is not a string",
                )
            normalized_key = unicodedata.normalize("NFC", key)
            if normalized_key in normalized:
                raise _error(
                    key,
                    path=_member_path(path, normalized_key),
                    stage=stage,
                    reason="duplicate normalized mapping key",
                )
            normalized[normalized_key] = normalize_spark_value(
                item,
                stage=stage,
                path=_member_path(path, normalized_key),
            )
        return normalized
    if isinstance(value, (list, tuple)):
        return [
            normalize_spark_value(
                item,
                stage=stage,
                path=f"{path}[{index}]",
            )
            for index, item in enumerate(value)
        ]
    raise _error(
        value,
        path=path,
        stage=stage,
        reason="unsupported Spark-returned type",
    )


def canonical_spark_json_bytes(
    value: object,
    *,
    stage: str,
    path: str = "$",
) -> bytes:
    """Return canonical bytes after strict Spark-boundary normalization."""

    normalized = normalize_spark_value(value, stage=stage, path=path)
    return json.dumps(
        normalized,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def spark_sha256(
    value: object,
    *,
    stage: str,
    path: str = "$",
) -> str:
    """Hash one Spark-returned value with the shared lossless normalizer."""

    return hashlib.sha256(
        canonical_spark_json_bytes(value, stage=stage, path=path)
    ).hexdigest()
