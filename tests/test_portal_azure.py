import base64
import json
import subprocess
from types import SimpleNamespace

import pytest

from people_counter.portal_azure import PortalAzureError, PortalAzureSession


def token(user="user-1", tenant="tenant-1"):
    payload = base64.urlsafe_b64encode(
        json.dumps({"oid": user, "tid": tenant, "upn": "alice@example.com", "exp": 4102444800}).encode()
    ).decode().rstrip("=")
    return f"header.{payload}.signature"


class Cli:
    def __init__(self):
        self.calls = []
        self.account = {
            "id": "sub-1", "name": "Subscription", "tenantId": "tenant-1",
            "user": {"name": "alice@example.com", "type": "user"},
        }
        self.user = "user-1"
        self.errors = {}
        self.responses = {
            ("ad", "signed-in-user", "show"): {"id": "user-1", "displayName": "Alice Example"},
            ("account", "list"): [self.account],
            ("storage", "account", "list"): [],
        }

    def __call__(self, args, **kwargs):
        self.calls.append((args, kwargs))
        key = tuple(args[1:4]) if args[1] in {"ad", "storage"} else tuple(args[1:3])
        error = self.errors.get(key)
        if isinstance(error, Exception):
            raise error
        if error:
            return SimpleNamespace(returncode=1, stdout="", stderr=error)
        if key == ("account", "show"):
            result = self.account
        elif key == ("account", "get-access-token"):
            result = {"accessToken": token(self.user)}
        else:
            result = self.responses[key]
        return SimpleNamespace(returncode=0, stdout=json.dumps(result), stderr="")


@pytest.fixture
def cli(monkeypatch, tmp_path):
    monkeypatch.setenv("AZURE_CONFIG_DIR", str(tmp_path / "azure"))
    return Cli()


def test_status_verifies_token_and_caches_but_refresh_invalidates(cli):
    now = [0]
    session = PortalAzureSession(run=cli, clock=lambda: now[0])
    expected = {
        "signed_in": True, "name": "Alice Example", "initials": "AE",
        "account": "alice@example.com", "tenant_id": "tenant-1", "error": "",
    }
    assert session.status() == expected
    session.status()["name"] = "tampered"
    assert len(cli.calls) == 3
    assert session.status() == expected
    now[0] = 6
    assert session.status() == expected
    assert len(cli.calls) == 6
    session.refresh()
    assert session.status() == expected
    assert len(cli.calls) == 9
    for args, kwargs in cli.calls:
        assert isinstance(args, list)
        assert args[-3:] == ["--output", "json", "--only-show-errors"]
        assert kwargs == {"capture_output": True, "text": True, "timeout": 10, "check": False}
        assert not any(command in args for command in ("login", "logout", "set"))


def test_cached_status_never_runs_cli_even_when_display_cache_is_cold_or_expired(cli):
    now = [0]
    session = PortalAzureSession(run=cli, clock=lambda: now[0])
    initial = session.cached_status()
    assert "Checking" in initial["name"]
    assert initial["signed_in"] is False
    assert cli.calls == []
    current = session.status()
    cli.calls.clear()
    now[0] = 100
    assert session.cached_status() == current
    assert cli.calls == []


@pytest.mark.parametrize("failure", [False, True])
def test_lookup_in_progress_cannot_undo_an_explicit_identity_refresh(cli, failure):
    holder = []
    def run(args, **kwargs):
        if failure and args[1:3] == ["account", "show"]:
            holder[0].refresh()
            return SimpleNamespace(returncode=1, stdout="", stderr="Please run az login")
        if not failure and args[1:3] == ["ad", "signed-in-user"]:
            holder[0].refresh()
        return cli(args, **kwargs)
    session = PortalAzureSession(run=run)
    holder.append(session)
    session.status()
    assert "Checking" in session.cached_status()["name"]


@pytest.mark.parametrize("error,code,label", [
    (FileNotFoundError("secret"), "cli_missing", "not installed"),
    ("Please run 'az login'", "signed_out", "signed out"),
    ("AADSTS700082 token expired SECRET", "expired", "expired"),
    (subprocess.TimeoutExpired(["az"], 10), "cli_timeout", "timed out"),
    ("permission denied SECRET", "permission_denied", "Permission"),
    ("unknown SECRET", "azure_error", "Azure CLI"),
])
def test_signed_out_status_and_safe_errors(cli, error, code, label, caplog):
    cli.errors[("account", "show")] = error
    session = PortalAzureSession(run=cli)
    status = session.status()
    assert status["signed_in"] is False
    assert label in status["error"]
    assert "SECRET" not in repr(status) + caplog.text
    with pytest.raises(PortalAzureError) as exc:
        session.require_identity()
    assert exc.value.code == code


def test_service_principal_is_rejected_before_token_or_graph(cli):
    cli.account["user"]["type"] = "servicePrincipal"
    session = PortalAzureSession(run=cli)
    assert "user" in session.status()["error"]
    with pytest.raises(PortalAzureError, match="user"):
        session.require_identity()
    assert all(args[1:3] == ["account", "show"] for args, _ in cli.calls)


def test_stale_account_cache_is_not_sign_in(cli):
    cli.errors[("account", "get-access-token")] = "Token expired SECRET"
    assert PortalAzureSession(run=cli).status()["signed_in"] is False


def test_graph_failure_has_visible_logged_fallback_but_identity_usable(cli, caplog):
    cli.errors[("ad", "signed-in-user", "show")] = "Graph permission denied SECRET"
    session = PortalAzureSession(run=cli)
    status = session.status()
    assert status["signed_in"]
    assert status["name"] == "alice@example.com (display name unavailable)"
    assert status["initials"] == "A"
    assert "display name unavailable" in status["error"]
    assert "display name unavailable" in caplog.text
    assert "SECRET" not in caplog.text
    assert session.require_identity() == {
        "account": "alice@example.com", "tenant_id": "tenant-1", "user_id": "user-1",
    }


def test_graph_user_mismatch_is_not_accepted_as_display_name(cli):
    cli.responses[("ad", "signed-in-user", "show")]["id"] = "someone-else"
    assert "unavailable" in PortalAzureSession(run=cli).status()["name"]


@pytest.mark.parametrize("value", ["not-json", "[]", '{"accessToken":"opaque"}'])
def test_malformed_cli_responses_are_safe(cli, value):
    def run(args, **kwargs):
        if args[1:3] == ["account", "get-access-token"]:
            return SimpleNamespace(returncode=0, stdout=value, stderr="")
        return cli(args, **kwargs)
    assert not PortalAzureSession(run=run).status()["signed_in"]


def test_credential_is_explicit_and_checks_user_on_every_token(cli):
    calls = []
    def acquire(*args, **kwargs):
        assert args == ("https://storage.azure.com/.default",)
        assert kwargs == {"claims": "requested-claims"}
        return SimpleNamespace(token=token(), expires_on=999)
    raw = SimpleNamespace(get_token=acquire)
    def factory(**kwargs):
        calls.append(kwargs)
        return raw
    session = PortalAzureSession(run=cli, credential_factory=factory)
    snapshot = session.require_identity()
    credential = session.credential(snapshot, subscription="sub-1")
    assert calls == [{"tenant_id": "tenant-1", "subscription": "sub-1", "process_timeout": 10}]
    assert credential.get_token("https://storage.azure.com/.default", claims="requested-claims").token == token()
    cli.user = "different"
    with pytest.raises(PortalAzureError, match="changed"):
        credential.get_token("https://storage.azure.com/.default")
    assert not session.status()["signed_in"]
    assert snapshot["user_id"] == "user-1"


def test_token_returned_by_sdk_cannot_cross_principals(cli):
    session = PortalAzureSession(
        run=cli, credential_factory=lambda **kw: SimpleNamespace(
            get_token=lambda *a, **kw: SimpleNamespace(token=token("other"), expires_on=999),
        ),
    )
    with pytest.raises(PortalAzureError, match="changed"):
        session.credential().get_token("scope")


def test_sdk_failure_clears_cached_success_without_leaking_errors(cli):
    def fail(*args, **kwargs):
        raise RuntimeError("SECRET accessToken")
    session = PortalAzureSession(
        run=cli, credential_factory=lambda **kw: SimpleNamespace(get_token=fail),
    )
    assert session.status()["signed_in"]
    with pytest.raises(PortalAzureError) as exc:
        session.credential().get_token("scope")
    assert exc.value.code == "azure_error"
    assert "SECRET" not in str(exc.value)
    assert not session.status()["signed_in"]


def test_subscriptions_and_hns_accounts_are_scoped_without_account_set(cli):
    good = dict(cli.account)
    other_tenant = {**good, "id": "sub-2", "tenantId": "other"}
    other_user = {**good, "id": "sub-3", "user": {"name": "other", "type": "user"}}
    cli.responses[("account", "list")] = [good, other_tenant, other_user]
    cli.responses[("storage", "account", "list")] = [
        {"id": "storage-id", "name": "lake", "resourceGroup": "group", "isHnsEnabled": True},
        {"id": "flat-id", "name": "flat", "resourceGroup": "group", "isHnsEnabled": False},
    ]
    session = PortalAzureSession(run=cli)
    assert session.subscriptions() == [{"id": "sub-1", "name": "Subscription"}]
    assert session.accounts("sub-1") == [{"id": "storage-id", "name": "lake", "resourceGroup": "group"}]
    args = next(args for args, _ in cli.calls if args[1:4] == ["storage", "account", "list"])
    assert args[4:6] == ["--subscription", "sub-1"]
    with pytest.raises(PortalAzureError, match="subscription"):
        session.accounts("sub-2")


def test_filesystems_use_guarded_credential_and_close_client(cli):
    cli.responses[("account", "list")] = [cli.account]
    cli.responses[("storage", "account", "list")] = [
        {"id": "storage-id", "name": "lake", "resourceGroup": "group", "isHnsEnabled": True},
    ]
    calls = []
    class Client:
        def __init__(self, **kwargs):
            calls.append(kwargs)
            self.policy = kwargs["_additional_pipeline_policies"][0]
        def list_file_systems(self):
            self.policy.on_request(SimpleNamespace())
            return [{"name": "footage"}]
        def close(self):
            calls.append("closed")
    session = PortalAzureSession(
        run=cli, credential_factory=lambda **kw: SimpleNamespace(),
        service_client_factory=Client,
    )
    assert session.filesystems("lake", "sub-1") == [{"name": "footage"}]
    assert calls[0]["account_url"] == "https://lake.dfs.core.windows.net"
    assert hasattr(calls[0]["credential"], "get_token")
    assert calls[-1] == "closed"
    with pytest.raises(PortalAzureError, match="account"):
        session.filesystems("evil.example", "sub-1")


def test_snapshot_tenant_or_account_change_requires_fresh_review(cli):
    session = PortalAzureSession(run=cli, credential_factory=lambda **kw: SimpleNamespace())
    snapshot = session.require_identity()
    cli.account["user"]["name"] = "bob@example.com"
    with pytest.raises(PortalAzureError, match="changed"):
        session.credential(snapshot)


@pytest.mark.parametrize("key", ["tenant_id", "user_id", "account"])
def test_reviewed_snapshot_cannot_change_any_identity_key(cli, key):
    session = PortalAzureSession(run=cli, credential_factory=lambda **kw: SimpleNamespace())
    snapshot = session.require_identity()
    snapshot[key] = "different"
    with pytest.raises(PortalAzureError, match="changed"):
        session.credential(snapshot)


def test_default_sdk_credential_uses_explicit_current_subscription(cli, monkeypatch):
    import azure.identity
    calls = []
    monkeypatch.setattr(azure.identity, "AzureCliCredential", lambda **kw: calls.append(kw) or SimpleNamespace())
    PortalAzureSession(run=cli).credential()
    assert calls == [{"tenant_id": "tenant-1", "subscription": "sub-1", "process_timeout": 10}]


def test_explicit_other_subscription_never_uses_current_default(cli):
    cli.responses[("account", "list")].append({**cli.account, "id": "sub-2"})
    calls = []
    session = PortalAzureSession(
        run=cli, credential_factory=lambda **kw: calls.append(kw) or SimpleNamespace(),
    )
    session.credential(subscription="sub-2")
    assert calls[0]["subscription"] == "sub-2"
    with pytest.raises(PortalAzureError, match="subscription"):
        session.credential(subscription="unreviewed")


@pytest.mark.parametrize("claims,code", [
    ({"oid": "user-1", "tid": "tenant-1", "exp": 0}, "expired"),
    ({"oid": "user-1", "tid": "other", "exp": 4102444800}, "identity_changed"),
    ({"oid": "user-1", "tid": "tenant-1", "exp": 4102444800, "idtyp": "app"}, "service_principal"),
    ({"oid": "user-1", "tid": "tenant-1"}, "invalid_response"),
    ({"oid": 42, "tid": "tenant-1", "exp": 4102444800}, "invalid_response"),
])
def test_fresh_storage_token_must_be_live_user_in_current_tenant(cli, claims, code):
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    def run(args, **kwargs):
        if args[1:3] == ["account", "get-access-token"]:
            return SimpleNamespace(returncode=0, stdout=json.dumps({"accessToken": f"x.{payload}.x"}))
        return cli(args, **kwargs)
    with pytest.raises(PortalAzureError) as exc:
        PortalAzureSession(run=run).require_identity()
    assert exc.value.code == code


def test_context_change_during_sdk_token_acquisition_is_detected(cli):
    def acquire(*args, **kwargs):
        cli.user = "other"
        return SimpleNamespace(token=token(), expires_on=4102444800)
    session = PortalAzureSession(run=cli, credential_factory=lambda **kw: SimpleNamespace(get_token=acquire))
    with pytest.raises(PortalAzureError, match="changed"):
        session.credential().get_token("scope")


def test_token_tenant_override_is_rejected_without_sdk_call(cli):
    def forbidden(*args, **kwargs):
        pytest.fail("Do not obtain a token for a different tenant")
    session = PortalAzureSession(
        run=cli, credential_factory=lambda **kw: SimpleNamespace(get_token=forbidden),
    )
    with pytest.raises(PortalAzureError, match="changed"):
        session.credential().get_token("scope", tenant_id="other")


def test_discovery_failure_invalidates_status(cli):
    session = PortalAzureSession(run=cli)
    assert session.status()["signed_in"]
    cli.errors[("storage", "account", "list")] = "AuthorizationFailed SECRET"
    with pytest.raises(PortalAzureError) as exc:
        session.accounts("sub-1")
    assert exc.value.code == "permission_denied"
    assert not session.status()["signed_in"]


def test_unexpected_identity_failure_is_sanitized(cli):
    cli.account["user"] = []
    session = PortalAzureSession(run=cli)
    assert not session.status()["signed_in"]


def test_sdk_permission_denied_is_distinct_and_token_free(cli):
    def fail(*args, **kwargs):
        error = RuntimeError("SECRET")
        error.status_code = 403
        raise error
    session = PortalAzureSession(
        run=cli, credential_factory=lambda **kw: SimpleNamespace(get_token=fail),
    )
    with pytest.raises(PortalAzureError) as exc:
        session.credential().get_token("scope")
    assert exc.value.code == "permission_denied"
    assert "SECRET" not in session.status()["error"]


@pytest.mark.parametrize("retry", [False, True])
def test_request_policy_stops_cached_bearer_token_before_second_http_request(cli, retry):
    from azure.core.credentials import AccessToken
    from azure.core.pipeline import Pipeline
    from azure.core.pipeline.policies import BearerTokenCredentialPolicy, RetryPolicy, SansIOHTTPPolicy
    from azure.core.pipeline.transport import HttpRequest, HttpResponse, HttpTransport

    token_calls = []
    network_calls = []

    class Credential:
        def get_token(self, *scopes, **kwargs):
            token_calls.append(scopes)
            return AccessToken(token(), 4102444800)

    class Transport(HttpTransport):
        def open(self):
            pass
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.close()
        def send(self, request, **kwargs):
            network_calls.append(request)
            response = HttpResponse(request, None)
            response.status_code = 429 if retry else 200
            if retry:
                cli.user = "other"
                response.headers["Retry-After"] = "0"
            return response

    session = PortalAzureSession(run=cli)
    snapshot = session.require_identity()
    policy = session.policy(snapshot)
    assert isinstance(policy, SansIOHTTPPolicy)
    pipeline = Pipeline(
        transport=Transport(),
        policies=[RetryPolicy(retry_total=1), BearerTokenCredentialPolicy(Credential(), "scope"), policy],
    )
    if retry:
        with pytest.raises(PortalAzureError, match="changed"):
            pipeline.run(HttpRequest("GET", "https://lake.dfs.core.windows.net/"))
    else:
        pipeline.run(HttpRequest("GET", "https://lake.dfs.core.windows.net/"))
        assert len(network_calls) == len(token_calls) == 1
        cli.user = "other"
        with pytest.raises(PortalAzureError, match="changed"):
            pipeline.run(HttpRequest("GET", "https://lake.dfs.core.windows.net/next"))
    assert len(network_calls) == len(token_calls) == 1
    assert not session.status()["signed_in"]


def test_policy_freezes_snapshot_and_rejects_stale_review_before_construction(cli):
    session = PortalAzureSession(run=cli)
    snapshot = session.require_identity()
    policy = session.policy(snapshot)
    snapshot["user_id"] = "other"
    policy.on_request(SimpleNamespace())
    with pytest.raises(PortalAzureError, match="changed"):
        session.policy(snapshot)


def test_policy_without_snapshot_checks_identity_on_each_request(cli):
    session = PortalAzureSession(run=cli)
    policy = session.policy()
    policy.on_request(SimpleNamespace())
    cli.errors[("account", "get-access-token")] = "Token expired SECRET"
    with pytest.raises(PortalAzureError) as exc:
        policy.on_request(SimpleNamespace())
    assert exc.value.code == "expired"
    assert not session.status()["signed_in"]


@pytest.mark.parametrize("adapter", ["publication", "onelake", "source"])
@pytest.mark.parametrize("retry", [False, True])
def test_storage_clients_install_identity_guard_on_cached_requests_and_retries(cli, monkeypatch, adapter, retry):
    import azure.storage.filedatalake
    from azure.core.credentials import AccessToken
    from azure.core.pipeline.transport import HttpResponse, HttpTransport
    from azure.storage.filedatalake import DataLakeServiceClient, ExponentialRetry
    from people_counter.adls import AzureDataLakeStorage
    from people_counter.backfill_operator import OneLakeFiles
    from people_counter.portal_sources import AdlsFootageSource

    sent = []
    acquired = []
    class Credential:
        def get_token(self, *scopes, **kwargs):
            acquired.append(scopes)
            return AccessToken(token(), 4102444800)
    class Response(HttpResponse):
        def body(self):
            return b""
    class Transport(HttpTransport):
        def open(self):
            pass
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.close()
        def send(self, request, **kwargs):
            sent.append(request)
            response = Response(request, None)
            response.status_code = 503 if retry else 200
            response.headers = {
                "Content-Length": "8", "ETag": '"version"',
                "Last-Modified": "Fri, 13 Nov 2026 12:00:00 GMT",
            }
            if retry:
                cli.user = "other"
            return response
    def factory(*args, **kwargs):
        return DataLakeServiceClient(
            *args, **kwargs, transport=Transport(),
            retry_policy=ExponentialRetry(initial_backoff=0, increment_base=0, random_jitter_range=0, retry_total=1),
        )
    monkeypatch.setattr(azure.storage.filedatalake, "DataLakeServiceClient", factory)
    session = PortalAzureSession(run=cli, credential_factory=lambda **kwargs: Credential(), cli_state=lambda: cli.user)
    snapshot = session.require_identity()
    if adapter == "publication":
        storage = AzureDataLakeStorage("lake", "clips", chunk_size=8, credential=session.credential(snapshot), identity_policy=session.policy(snapshot))
        file = storage._file_client("clip.mp4")
    elif adapter == "onelake":
        files = OneLakeFiles(SimpleNamespace(workspace_id="workspace", lakehouse_id="lakehouse"), credential=session.credential(snapshot), identity_policy=session.policy(snapshot))
        file = files._file("clip.mp4")
    else:
        source = AdlsFootageSource({"account": "lake", "filesystem": "clips", "subscription": "sub-1"}, session)
        file = source.file_client("clip.mp4")
    if not retry:
        file.get_file_properties()
        assert len(sent) == len(acquired) == 1
        cli.calls.clear()
        for _ in range(100):
            file.get_file_properties()
        assert cli.calls == []
        cli.user = "other"
    with pytest.raises(PortalAzureError, match="changed"):
        file.get_file_properties()
    assert len(sent) == (1 if retry else 101)
    assert len(acquired) == 1


@pytest.mark.parametrize("cache_name", ["msal_token_cache.json", "msal_token_cache.bin"])
def test_cli_state_fingerprint_tracks_profile_cache_and_configuration_changes(tmp_path, monkeypatch, cache_name):
    import os
    from people_counter.portal_azure import _cli_state

    monkeypatch.setenv("AZURE_CONFIG_DIR", str(tmp_path))
    assert _cli_state() is None
    profile = tmp_path / "azureProfile.json"
    profile.write_text("{}")
    initial = _cli_state()
    assert initial is not None and _cli_state() == initial
    metadata = profile.stat()
    os.utime(profile, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1000000))
    changed = _cli_state()
    assert changed != initial
    cache = tmp_path / cache_name
    cache.write_text("{}")
    assert _cli_state() != changed
    cache.unlink()
    assert _cli_state() == changed
    monkeypatch.setenv("AZURE_CONFIG_DIR", str(tmp_path / "other"))
    assert _cli_state() is None


def test_cli_state_monitors_the_default_azure_directory(tmp_path, monkeypatch):
    from pathlib import Path
    from people_counter.portal_azure import _cli_state

    monkeypatch.delenv("AZURE_CONFIG_DIR", raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    root = tmp_path / ".azure"
    root.mkdir()
    (root / "azureProfile.json").write_text("{}")
    initial = _cli_state()
    assert initial is not None
    (root / "msal_token_cache.bin").write_bytes(b"cache")
    assert _cli_state() != initial


def test_unchanged_request_guards_do_not_spawn_cli_but_state_changes_revalidate(cli):
    state = ["initial"]
    session = PortalAzureSession(run=cli, cli_state=lambda: tuple(state))
    snapshot = session.require_identity()
    policy = session.policy(snapshot)
    cli.calls.clear()
    for _ in range(100):
        policy.on_request(SimpleNamespace())
        session.check_identity(snapshot)
    assert cli.calls == []
    cli.user = "other"
    state[0] = "changed"
    with pytest.raises(PortalAzureError, match="changed"):
        policy.on_request(SimpleNamespace())
    assert len(cli.calls) == 2


def test_unknown_cli_state_disables_guard_cache_and_expiry_or_refresh_revalidates(cli, monkeypatch):
    import people_counter.portal_azure as module
    state = ["known"]
    session = PortalAzureSession(run=cli, cli_state=lambda: state[0])
    snapshot = session.require_identity()
    cli.calls.clear()
    session.check_identity(snapshot)
    assert cli.calls == []
    state[0] = None
    session.check_identity(snapshot)
    session.check_identity(snapshot)
    assert len(cli.calls) == 4
    state[0] = "known"
    session.require_identity()
    cli.calls.clear()
    monkeypatch.setattr(module.time, "time", lambda: 4102444801)
    with pytest.raises(PortalAzureError, match="expired"):
        session.check_identity(snapshot)
    assert len(cli.calls) == 2
    monkeypatch.undo()
    session.require_identity()
    session.refresh()
    cli.calls.clear()
    session.check_identity(snapshot)
    assert len(cli.calls) == 2
