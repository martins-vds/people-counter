"""Validated Fabric artifact bindings, independent of any deployed instance."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeVar


_Target = TypeVar("_Target", bound="FabricTarget")


def validate_artifact_id(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a canonical UUID")
    try:
        identity = uuid.UUID(value)
    except ValueError as error:
        raise ValueError(f"{label} must be a canonical UUID") from error
    if str(identity) != value or identity.int == 0:
        raise ValueError(f"{label} must be a canonical UUID")
    return value


@dataclass(frozen=True, slots=True)
class FabricTarget:
    workspace_id: str
    lakehouse_id: str
    environment_id: str

    def __post_init__(self) -> None:
        for name in ("workspace_id", "lakehouse_id", "environment_id"):
            validate_artifact_id(getattr(self, name), name)

    @classmethod
    def from_runtime_context(cls: type[_Target], context: Mapping[str, object]) -> _Target:
        if not isinstance(context, Mapping):
            raise ValueError("Fabric runtime context must be a mapping")
        values = (
            context.get("defaultLakehouseWorkspaceId"),
            context.get("defaultLakehouseId"),
            context.get("environmentId"),
        )
        if not all(isinstance(value, str) for value in values):
            raise ValueError("Fabric runtime context must identify its Lakehouse and Environment")
        workspace, lakehouse, environment = values
        config = cls(
            workspace_id=validate_artifact_id(workspace, "runtime workspace_id"),
            lakehouse_id=validate_artifact_id(lakehouse, "runtime lakehouse_id"),
            environment_id=validate_artifact_id(environment, "runtime environment_id"),
        )
        config.validate_runtime_context(context)
        return config

    @classmethod
    def current(cls: type[_Target]) -> _Target:
        try:
            import notebookutils
        except ModuleNotFoundError as error:
            if error.name != "notebookutils":
                raise
            raise RuntimeError(
                "Fabric runtime context is unavailable; supply an explicit Fabric target"
            ) from error
        context = getattr(getattr(notebookutils, "runtime", None), "context", None)
        if not isinstance(context, Mapping):
            raise ValueError("Fabric runtime context must be a mapping")
        return cls.from_runtime_context(context)

    def validate_runtime_context(self, context: Mapping[str, object]) -> None:
        if not isinstance(context, Mapping):
            raise ValueError("Fabric runtime context must be a mapping")
        for key, expected in (
            ("currentWorkspaceId", self.workspace_id),
            ("defaultLakehouseId", self.lakehouse_id),
            ("defaultLakehouseWorkspaceId", self.workspace_id),
            ("environmentId", self.environment_id),
            ("environmentWorkspaceId", self.workspace_id),
        ):
            if context.get(key) != expected:
                raise ValueError(
                    f"Fabric runtime context {key} must match {expected}; "
                    f"observed {context.get(key)!r}"
                )

    def command_line_arguments(self) -> list[str]:
        return [
            "--artifacts", self.workspace_id, self.lakehouse_id, self.environment_id,
        ]
