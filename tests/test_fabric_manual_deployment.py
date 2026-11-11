from __future__ import annotations

import sys
import json
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import people_counter.fabric_sjd as stable
import people_counter.fabric_sjd_jobs as jobs
import people_counter.fabric_sjd_runtime as runtime
from people_counter.fabric_input_resolver import InputBackend
from people_counter.fabric_source_cache import LocalizedSourceCache


WORKSPACE = "11111111-1111-4111-8111-111111111111"
LAKEHOUSE = "22222222-2222-4222-8222-222222222222"
ENVIRONMENT = "33333333-3333-4333-8333-333333333333"
MANUAL_ARGS = ["--manual-artifacts", WORKSPACE, LAKEHOUSE, ENVIRONMENT]
CONTEXT = {
    "currentWorkspaceId": WORKSPACE,
    "defaultLakehouseId": LAKEHOUSE,
    "defaultLakehouseWorkspaceId": WORKSPACE,
    "environmentId": ENVIRONMENT,
    "environmentWorkspaceId": WORKSPACE,
}


def manual_config(**overrides):
    return stable.ManualFabricSjdConfig(
        **{
            "workspace_id": WORKSPACE,
            "lakehouse_id": LAKEHOUSE,
            "environment_id": ENVIRONMENT,
            **overrides,
        }
    )


def test_manual_config_is_explicit_immutable_and_keeps_stable_namespace() -> None:
    config = manual_config()
    assert isinstance(config, stable.FabricSjdConfig)
    assert config.workspace_id == WORKSPACE
    assert config.lakehouse_id == LAKEHOUSE
    assert config.environment_id == ENVIRONMENT
    assert config.table("work") == "people_counter_sjd_work"
    assert config.file_path("attempts/batch=a") == (
        "Files/people-counter/sjd/v1/attempts/batch=a"
    )
    assert config.abfss_path(config.file_path("control")) == (
        f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE}/Files/people-counter/sjd/v1/control"
    )
    config.require_write_enabled()
    with pytest.raises(FrozenInstanceError):
        config.workspace_id = LAKEHOUSE


def test_runtime_binding_can_be_derived_from_attached_deployment() -> None:
    assert stable.ManualFabricSjdConfig.from_runtime_context(CONTEXT) == manual_config()


@pytest.mark.parametrize("context", [None, [], "not a runtime context"])
def test_derived_binding_rejects_nonmapping_context(context) -> None:
    with pytest.raises(ValueError, match="runtime context"):
        stable.ManualFabricSjdConfig.from_runtime_context(context)


@pytest.mark.parametrize("key", list(CONTEXT))
def test_derived_runtime_binding_requires_complete_matching_context(key: str) -> None:
    context = {name: value for name, value in CONTEXT.items() if name != key}
    with pytest.raises(ValueError, match="runtime context"):
        stable.ManualFabricSjdConfig.from_runtime_context(context)


@pytest.mark.parametrize("missing", ["workspace_id", "lakehouse_id", "environment_id"])
def test_manual_config_requires_every_artifact_id(missing: str) -> None:
    arguments = dict(
        workspace_id=WORKSPACE, lakehouse_id=LAKEHOUSE, environment_id=ENVIRONMENT
    )
    del arguments[missing]
    with pytest.raises(TypeError):
        stable.ManualFabricSjdConfig(**arguments)


@pytest.mark.parametrize("field", ["workspace_id", "lakehouse_id", "environment_id"])
@pytest.mark.parametrize(
    "invalid",
    [
        "",
        "not-a-guid",
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "0" * 32,
        "00000000-0000-0000-0000-000000000000",
        None,
    ],
)
def test_manual_config_rejects_invalid_artifact_ids(field: str, invalid) -> None:
    with pytest.raises(ValueError, match="canonical UUID"):
        manual_config(**{field: invalid})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("table_prefix", "other_"),
        ("runtime", "1.3"),
        ("stable_production", False),
        ("mode", "CANARY"),
    ],
)
def test_manual_config_cannot_weaken_namespace_guards(field: str, value) -> None:
    with pytest.raises(ValueError, match="configuration is fixed"):
        manual_config(**{field: value})


@pytest.mark.parametrize("context", [None, [], "not a runtime context"])
def test_manual_context_must_be_a_mapping(context) -> None:
    with pytest.raises(ValueError, match="runtime context"):
        manual_config().validate_runtime_context(context)


def test_stable_config_accepts_the_same_explicit_target_as_the_manual_alias() -> None:
    config = stable.FabricSjdConfig(
        workspace_id=WORKSPACE, lakehouse_id=LAKEHOUSE, environment_id=ENVIRONMENT,
    )
    assert config.workspace_id == WORKSPACE
    assert config.lakehouse_id == LAKEHOUSE
    assert config.environment_id == ENVIRONMENT


@pytest.mark.parametrize(
    ("entrypoint", "target", "arguments", "forwarded"),
    [
        (jobs.control_main, "control_main", ["bootstrap"], ["bootstrap"]),
        (jobs.dispatcher_main, "dispatcher_main", [], []),
        (jobs.process_main, "process_main", ["--batch-id", "b"], ["--batch-id", "b"]),
        (jobs.gold_main, "gold_main", [], []),
        (
            jobs.refresh_main,
            "refresh_main",
            ["--workspace-id", WORKSPACE, "--semantic-model-id", LAKEHOUSE],
            ["--workspace-id", WORKSPACE, "--semantic-model-id", LAKEHOUSE],
        ),
        (jobs.reconciliation_main, "control_main", [], ["reconcile"]),
    ],
)
def test_manual_entrypoints_forward_only_checked_binding(
    monkeypatch, entrypoint, target, arguments, forwarded
) -> None:
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(runtime=SimpleNamespace(context=CONTEXT))
    )
    with patch.object(runtime, target, return_value=7) as run:
        assert entrypoint([*MANUAL_ARGS, *arguments]) == 7
    assert run.call_args.args == (forwarded,)
    config = run.call_args.kwargs["config"]
    assert config == manual_config()
    if target in {"dispatcher_main", "process_main"}:
        assert run.call_args.kwargs["route_mode"] == "PRODUCTION"


def test_manual_binding_is_read_from_actual_job_arguments(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["main.py", *MANUAL_ARGS, "bootstrap"])
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(runtime=SimpleNamespace(context=CONTEXT))
    )
    with patch.object(runtime, "control_main", return_value=0) as run:
        assert jobs.control_main() == 0
    assert run.call_args.args == (["bootstrap"],)
    assert run.call_args.kwargs["config"] == manual_config()


@pytest.mark.parametrize("key", list(CONTEXT))
@pytest.mark.parametrize("missing", [False, True])
def test_manual_binding_rejects_wrong_or_missing_live_context(
    monkeypatch, key: str, missing: bool
) -> None:
    context = dict(CONTEXT)
    if missing:
        del context[key]
    else:
        context[key] = "44444444-4444-4444-8444-444444444444"
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(runtime=SimpleNamespace(context=context))
    )
    with patch.object(runtime, "control_main") as run:
        with pytest.raises(ValueError, match="runtime context") as error:
            jobs.control_main([*MANUAL_ARGS, "bootstrap"])
    assert key in str(error.value)
    assert f"observed {context.get(key)!r}" in str(error.value)
    run.assert_not_called()


def test_partial_manual_arguments_do_not_fall_back_to_production() -> None:
    with patch.object(runtime, "control_main") as run:
        with pytest.raises(SystemExit) as error:
            jobs.control_main(["--manual-artifacts", WORKSPACE, LAKEHOUSE])
    assert error.value.code == 2
    run.assert_not_called()


@pytest.mark.parametrize("arguments", [["bootstrap"], ["reconcile", "extra"], ["recover"]])
def test_manual_reconciliation_rejects_non_reconcile_commands(monkeypatch, arguments) -> None:
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(runtime=SimpleNamespace(context=CONTEXT))
    )
    with patch.object(runtime, "control_main") as run:
        with pytest.raises(ValueError, match="only the reconcile command"):
            jobs.reconciliation_main([*MANUAL_ARGS, *arguments])
    run.assert_not_called()


def test_empty_reconciliation_arguments_use_the_checked_runtime_target(monkeypatch) -> None:
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(runtime=SimpleNamespace(context=CONTEXT))
    )
    with patch.object(runtime, "control_main", return_value=0) as run:
        assert jobs.reconciliation_main([]) == 0
    assert run.call_args.args == (["reconcile"],)
    assert run.call_args.kwargs["config"].workspace_id == WORKSPACE


@pytest.mark.parametrize(
    "arguments",
    [["--help"], ["--manual-art", WORKSPACE, LAKEHOUSE, ENVIRONMENT, "bootstrap"]],
)
def test_common_binding_parser_does_not_consume_help_or_abbreviations(monkeypatch, arguments) -> None:
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(runtime=SimpleNamespace(context=CONTEXT))
    )
    with patch.object(runtime, "control_main", return_value=7) as run:
        assert jobs.control_main(arguments) == 7
    assert run.call_args.args == (arguments,)
    if "--help" in arguments:
        assert run.call_args.kwargs["config"] is None
    else:
        assert type(run.call_args.kwargs["config"]) is stable.FabricSjdConfig


def test_manual_refresh_rejects_a_different_reporting_workspace(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace())
    with patch.object(runtime, "_spark") as spark:
        with pytest.raises(ValueError, match="reporting workspace"):
            runtime.refresh_main(
                ["--workspace-id", LAKEHOUSE, "--semantic-model-id", LAKEHOUSE],
                config=manual_config(),
            )
    spark.assert_not_called()


def test_manual_refresh_idle_is_explicit_and_never_requests_credentials(monkeypatch, capsys) -> None:
    config = manual_config()
    session = object()
    state = Mock()
    state.pending_refreshes.return_value = []
    credentials = Mock()
    monkeypatch.setitem(sys.modules, "notebookutils", SimpleNamespace(credentials=credentials))
    with (
        patch.object(runtime, "_spark", return_value=session),
        patch("people_counter.fabric_gold.FabricGoldState", return_value=state) as create_state,
        patch.object(runtime, "_refresh_semantic_model") as refresh,
    ):
        assert runtime.refresh_main(
            ["--workspace-id", WORKSPACE, "--semantic-model-id", LAKEHOUSE], config=config
        ) == 0
    create_state.assert_called_once_with(session, config=config)
    assert json.loads(capsys.readouterr().out) == {"status": "IDLE"}
    credentials.getToken.assert_not_called()
    refresh.assert_not_called()
    state.acknowledge_refresh.assert_not_called()


@pytest.mark.parametrize("model_count", [1, 2])
def test_manual_refresh_targets_only_selected_workspace_and_acknowledges_after_refresh(
    monkeypatch, capsys, model_count
) -> None:
    config = manual_config()
    session = object()
    state = Mock()
    state.pending_refreshes.return_value = [
        {"outbox_id": 41, "dedupe_key": "first"},
        {"outbox_id": 42, "dedupe_key": "second"},
    ]
    state.acknowledge_refresh.return_value = True
    credentials = Mock()
    credentials.getToken.return_value = "test-token"
    monkeypatch.setitem(
        sys.modules, "notebookutils", SimpleNamespace(credentials=credentials)
    )
    models = [LAKEHOUSE, ENVIRONMENT][:model_count]
    arguments = ["--workspace-id", WORKSPACE, "--poll-seconds", "1", "--timeout-seconds", "2"]
    for model in models:
        arguments.extend(["--semantic-model-id", model])
    with (
        patch.object(runtime, "_spark", return_value=session),
        patch("people_counter.fabric_gold.FabricGoldState", return_value=state) as create_state,
        patch.object(runtime, "_refresh_semantic_model", return_value={"status": "Completed"}) as refresh,
    ):
        assert runtime.refresh_main(arguments, config=config) == 0
    create_state.assert_called_once_with(session, config=config)
    credentials.getToken.assert_called_once_with("pbi")
    assert [call.args for call in refresh.call_args_list] == [(WORKSPACE, model) for model in models]
    for call in refresh.call_args_list:
        assert call.kwargs == {"token": "test-token", "poll_seconds": 1.0, "timeout_seconds": 2.0}
    assert [call.args for call in state.acknowledge_refresh.call_args_list] == [(41,), (42,)]
    assert [call.kwargs for call in state.acknowledge_refresh.call_args_list] == [
        {"actor": "pc-production-refresh-sjd", "expected_dedupe_key": "first"},
        {"actor": "pc-production-refresh-sjd", "expected_dedupe_key": "second"},
    ]
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "REFRESHED"
    assert result["acknowledged_outbox_ids"] == [41, 42]
    expected = [{"status": "Completed", "semantic_model_id": model} for model in models]
    if model_count == 1:
        assert result["refresh"] == expected[0]
        assert "refreshes" not in result
    else:
        assert result["refreshes"] == expected
        assert "refresh" not in result


def test_manual_video_fallback_never_uses_legacy_artifact_ids(monkeypatch, tmp_path) -> None:
    fallback = Mock(return_value=("localized-video", "a" * 64))
    monkeypatch.setattr(runtime, "_fallback_localize", fallback)
    runtime._resolve_localized_video(
        object(),
        {
            "source_video": "/lakehouse/default/Files/footage/video.mp4",
            "source_sha256": "a" * 64,
        },
        backend=InputBackend.FABRIC_FALLBACK,
        mount_root=None,
        cache=LocalizedSourceCache(tmp_path / "cache", max_entries=10, max_total_bytes=1024),
        staged_sources={},
        broadcast_digests=set(),
        workspace_id=WORKSPACE,
        lakehouse_id=LAKEHOUSE,
        validation_error=ValueError,
    )
    assert fallback.call_args.kwargs["abfss_uri"] == (
        f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE}/Files/footage/video.mp4"
    )
    assert fallback.call_args.kwargs["original_basename"] == "video.mp4"
    assert fallback.call_args.kwargs["expected_sha256"] == "a" * 64


def test_manual_model_fallback_never_uses_legacy_artifact_ids(monkeypatch, tmp_path) -> None:
    fallback = Mock(return_value=("localized-model", "b" * 64))
    monkeypatch.setattr(runtime, "_fallback_localize", fallback)
    runtime._resolve_localized_models(
        object(),
        {"model_artifact_sha256": {"model/model.onnx": "b" * 64}},
        ("model/model.onnx",),
        backend=InputBackend.FABRIC_FALLBACK,
        mount_root=None,
        cache=LocalizedSourceCache(tmp_path / "cache", max_entries=10, max_total_bytes=1024),
        staged_sources={},
        broadcast_digests=set(),
        workspace_id=WORKSPACE,
        lakehouse_id=LAKEHOUSE,
        validation_error=ValueError,
    )
    assert fallback.call_args.kwargs["abfss_uri"] == (
        f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE}/Files/models/model/model.onnx"
    )
    assert fallback.call_args.kwargs["original_basename"] == "model.onnx"
    assert fallback.call_args.kwargs["expected_sha256"] == "b" * 64


def test_manual_localizer_forwards_binding_to_both_resolvers(monkeypatch) -> None:
    config = manual_config()
    store = SimpleNamespace(
        config=config,
        load_claim_envelope_with_digest=lambda _: (
            {"items": [{"work_id": "w", "payload": {}}]}, "digest"
        ),
    )
    monkeypatch.setattr(
        runtime, "select_input_backend",
        lambda *args, **kwargs: (InputBackend.FABRIC_DIRECT, {"mount_root": "/lakehouse/default"}),
    )
    video = Mock(return_value=("Files/footage/video.mp4", "a" * 64, None))
    models = Mock(return_value={"model.onnx": {"sha256": "b" * 64}})
    monkeypatch.setattr(runtime, "_resolve_localized_video", video)
    monkeypatch.setattr(runtime, "_model_artifact_paths", lambda *args, **kwargs: ("model.onnx",))
    monkeypatch.setattr(runtime, "_resolve_localized_models", models)
    runtime._localize_process_inputs(object(), store, "batch")
    for resolver in (video, models):
        assert resolver.call_args.kwargs["workspace_id"] == WORKSPACE
        assert resolver.call_args.kwargs["lakehouse_id"] == LAKEHOUSE
        assert resolver.call_args.kwargs["distributed_alias_root_uri"] == (
            f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/"
            f"{LAKEHOUSE}/{config.file_path('distributed-cache')}"
        )
