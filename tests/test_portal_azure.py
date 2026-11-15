import base64
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any, cast

import pytest
from azure.core.credentials import AccessToken
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from azure.core.pipeline import Pipeline, PipelineContext, PipelineRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.core.pipeline.transport import HttpRequest, HttpResponse, HttpTransport
from azure.identity import EnvironmentCredential as SdkEnvironmentCredential
from azure.identity import DefaultAzureCredential as SdkDefaultAzureCredential
from azure.identity import WorkloadIdentityCredential as SdkWorkloadIdentityCredential

from people_counter.portal_azure import PortalAzureError, PortalAzureSession
from people_counter.portal_azure import _runtime_credential

SCOPE = "https://storage.azure.com/.default"


def token(**overrides):
    claims = {
        "oid": "principal-1", "tid": "tenant-1", "upn": "alice@example.com",
        "name": "Alice Example", "exp": 4102444800,
    }
    claims.update(overrides)
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return AccessToken(f"header.{payload}.signature", 4102444800)


class Credential:
    def __init__(self):
        self.result = token()
        self.error = None
        self.calls = []
        self.callback = None
        self.closed = False
        self.close_calls = 0

    def get_token(self, *scopes, **kwargs):
        self.calls.append((scopes, kwargs))
        if self.callback:
            self.callback()
        if self.error:
            raise self.error
        return self.result

    def close(self):
        self.closed = True
        self.close_calls += 1


@pytest.fixture
def sdk():
    return Credential()


@pytest.fixture(autouse=True)
def no_live_credentials(monkeypatch):
    def unavailable(*args, **kwargs):
        pytest.fail("Tests must inject a fake SDK credential")

    for name in (
        "WEBSITE_HOSTNAME", "AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET",
        "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_CLIENT_CERTIFICATE_PASSWORD",
        "AZURE_CLIENT_SEND_CERTIFICATE_CHAIN", "AZURE_FEDERATED_TOKEN_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("azure.identity.DefaultAzureCredential", unavailable)
    monkeypatch.setattr("azure.identity.ManagedIdentityCredential", unavailable)
    monkeypatch.setattr("azure.identity.EnvironmentCredential", unavailable)
    monkeypatch.setattr("azure.identity.WorkloadIdentityCredential", unavailable)
    monkeypatch.setattr("azure.identity.CertificateCredential", unavailable)


def test_compose_empty_defaults_allow_native_local_credential_construction(monkeypatch):
    for name in (
        "AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET",
        "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_CLIENT_CERTIFICATE_PASSWORD",
        "AZURE_CLIENT_SEND_CERTIFICATE_CHAIN", "AZURE_FEDERATED_TOKEN_FILE",
    ):
        monkeypatch.setenv(name, "")
    monkeypatch.setattr("azure.identity.DefaultAzureCredential", SdkDefaultAzureCredential)
    credential = _runtime_credential()
    assert isinstance(credential, SdkDefaultAzureCredential)
    credential.close()


@pytest.mark.parametrize("password,chain,expected_password,expected_chain", [
    ("", "", None, False), ("test-only-password", "True", "test-only-password", True),
    ("", "False", None, False),
])
def test_compose_empty_secret_keeps_certificate_authentication(monkeypatch, sdk, password, chain, expected_password, expected_chain):
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-1")
    monkeypatch.setenv("AZURE_CLIENT_ID", "application-client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "")
    monkeypatch.setenv("AZURE_CLIENT_CERTIFICATE_PATH", "/run/secrets/client.pem")
    monkeypatch.setenv("AZURE_CLIENT_CERTIFICATE_PASSWORD", password)
    monkeypatch.setenv("AZURE_CLIENT_SEND_CERTIFICATE_CHAIN", chain)
    constructors = []
    def certificate(**kwargs):
        constructors.append(kwargs)
        return sdk
    monkeypatch.setattr("azure.identity.CertificateCredential", certificate)
    assert PortalAzureSession().require_identity()["user_id"] == "principal-1"
    assert constructors == [{
        "tenant_id": "tenant-1", "client_id": "application-client",
        "certificate_path": "/run/secrets/client.pem", "password": expected_password,
        "send_certificate_chain": expected_chain,
    }]


@pytest.mark.parametrize("missing", ["AZURE_TENANT_ID", "AZURE_CLIENT_ID"])
def test_certificate_configuration_without_required_identity_fields_is_rejected(monkeypatch, sdk, missing):
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-1")
    monkeypatch.setenv("AZURE_CLIENT_ID", "application-client")
    monkeypatch.delenv(missing)
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "")
    monkeypatch.setenv("AZURE_CLIENT_CERTIFICATE_PATH", "/run/secrets/client.pem")
    calls = []
    monkeypatch.setattr("azure.identity.CertificateCredential", lambda **kwargs: calls.append(kwargs) or sdk)
    with pytest.raises(PortalAzureError) as error:
        PortalAzureSession().require_identity()
    assert error.value.code == "azure_error"
    assert calls == []


@pytest.mark.parametrize("hostname", [None, "portal.azurewebsites.net"])
@pytest.mark.parametrize("configuration,expected", [
    ({"AZURE_CLIENT_SECRET": "test-only-secret"}, "environment"),
    ({"AZURE_CLIENT_CERTIFICATE_PATH": "/run/secrets/azure/client.pem"}, "environment"),
    ({"AZURE_FEDERATED_TOKEN_FILE": "/run/secrets/azure/token"}, "workload"),
    ({"AZURE_CLIENT_SECRET": "test-only-secret", "AZURE_FEDERATED_TOKEN_FILE": "/run/secrets/azure/token"}, "environment"),
    ({"AZURE_CLIENT_CERTIFICATE_PATH": "/run/secrets/azure/client.pem",
      "AZURE_FEDERATED_TOKEN_FILE": "/run/secrets/azure/token"}, "environment"),
])
def test_explicit_environment_identity_precedes_host_credentials(monkeypatch, sdk, hostname, configuration, expected):
    if hostname:
        monkeypatch.setenv("WEBSITE_HOSTNAME", hostname)
    monkeypatch.setenv("AZURE_CLIENT_ID", "application-client")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-1")
    for name, value in configuration.items():
        monkeypatch.setenv(name, value)
    constructors = []
    def environment():
        constructors.append("environment")
        return sdk
    def workload():
        constructors.append("workload")
        return sdk
    monkeypatch.setattr("azure.identity.EnvironmentCredential", environment)
    monkeypatch.setattr("azure.identity.WorkloadIdentityCredential", workload)
    sdk.result = token(upn=None, idtyp="app", appid="application-client")
    session = PortalAzureSession()
    assert constructors == []
    identity = session.require_identity()
    assert identity["principal_type"] == "app"
    assert identity["account"] == "application-client"
    assert constructors == [expected]
    assert session.credential(identity).get_token(SCOPE) == sdk.result
    session.close()
    assert constructors == [expected]
    assert sdk.close_calls == 1


@pytest.mark.parametrize("setting,credential_name", [
    ("AZURE_CLIENT_SECRET", "EnvironmentCredential"),
    ("AZURE_CLIENT_CERTIFICATE_PATH", "EnvironmentCredential"),
    ("AZURE_FEDERATED_TOKEN_FILE", "WorkloadIdentityCredential"),
])
def test_explicit_environment_authentication_failure_never_changes_identity(monkeypatch, sdk, setting, credential_name):
    monkeypatch.setenv("WEBSITE_HOSTNAME", "portal.azurewebsites.net")
    monkeypatch.setenv(setting, "test-only-setting")
    sdk.error = ClientAuthenticationError("Test credential rejected")
    monkeypatch.setattr(f"azure.identity.{credential_name}", lambda: sdk)
    session = PortalAzureSession()
    with pytest.raises(PortalAzureError) as error:
        session.require_identity()
    assert error.value.code == "azure_error"
    assert not session.cached_status()["signed_in"]


@pytest.mark.parametrize("credential_name,setting,native_credential", [
    ("EnvironmentCredential", "AZURE_CLIENT_SECRET", SdkEnvironmentCredential),
    ("WorkloadIdentityCredential", "AZURE_FEDERATED_TOKEN_FILE", SdkWorkloadIdentityCredential),
])
def test_explicit_incomplete_environment_configuration_surfaces_sdk_error(monkeypatch, credential_name, setting, native_credential):
    monkeypatch.setenv(setting, "test-only-setting")
    monkeypatch.setattr(f"azure.identity.{credential_name}", native_credential)
    with pytest.raises(PortalAzureError) as error:
        PortalAzureSession().require_identity()
    assert error.value.code == "azure_error"


def test_empty_environment_selectors_preserve_managed_identity_selection(monkeypatch, sdk):
    monkeypatch.setenv("WEBSITE_HOSTNAME", "portal.azurewebsites.net")
    for name in ("AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_FEDERATED_TOKEN_FILE"):
        monkeypatch.setenv(name, "")
    monkeypatch.setattr("azure.identity.ManagedIdentityCredential", lambda: sdk)
    assert PortalAzureSession().require_identity()["user_id"] == "principal-1"


@pytest.mark.parametrize("hostname,client_id,kind,options", [
    ("portal.azurewebsites.net", "user-assigned-client", "managed", {"client_id": "user-assigned-client"}),
    ("portal.azurewebsites.net", None, "managed", {}),
    ("portal.azurewebsites.net", "", "managed", {}),
    (None, "local-app-client", "default", {}),
    (None, None, "default", {}),
    ("", "local-app-client", "default", {}),
])
def test_runtime_selects_managed_identity_only_when_azure_hosted(monkeypatch, sdk, hostname, client_id, kind, options):
    if hostname is not None:
        monkeypatch.setenv("WEBSITE_HOSTNAME", hostname)
    if client_id is not None:
        monkeypatch.setenv("AZURE_CLIENT_ID", client_id)
    constructors = []
    def managed(**kwargs):
        constructors.append(("managed", kwargs))
        return sdk
    def default(**kwargs):
        constructors.append(("default", kwargs))
        return sdk
    monkeypatch.setattr("azure.identity.ManagedIdentityCredential", managed)
    monkeypatch.setattr("azure.identity.DefaultAzureCredential", default)
    session = PortalAzureSession()
    assert constructors == []
    identity = session.require_identity()
    if kind == "default":
        options = {
            "exclude_environment_credential": True, "exclude_workload_identity_credential": True,
            "managed_identity_client_id": client_id or None,
        }
    assert constructors == [(kind, options)]
    session.credential(identity).get_token(SCOPE)
    assert constructors == [(kind, options)]
    session.close()
    assert sdk.close_calls == 1


def test_azure_managed_identity_failure_never_falls_back_to_local_credentials(monkeypatch, sdk):
    monkeypatch.setenv("WEBSITE_HOSTNAME", "portal.azurewebsites.net")
    monkeypatch.setenv("AZURE_CLIENT_ID", "user-assigned-client")
    sdk.error = ClientAuthenticationError("Managed identity endpoint unavailable")
    monkeypatch.setattr("azure.identity.ManagedIdentityCredential", lambda **kwargs: sdk)
    session = PortalAzureSession()
    with pytest.raises(PortalAzureError) as error:
        session.require_identity()
    assert error.value.code == "azure_error"
    assert not session.cached_status()["signed_in"]


def test_explicit_credential_and_factory_override_runtime_detection(monkeypatch, sdk):
    monkeypatch.setenv("WEBSITE_HOSTNAME", "portal.azurewebsites.net")
    monkeypatch.setenv("AZURE_CLIENT_ID", "user-assigned-client")
    injected = PortalAzureSession(credential=sdk)
    assert injected.require_identity()["user_id"] == "principal-1"
    factory_calls = []
    configured = PortalAzureSession(credential_factory=lambda: factory_calls.append(True) or sdk)
    assert configured.require_identity()["user_id"] == "principal-1"
    assert factory_calls == [True]


def test_default_credential_is_single_native_owner(monkeypatch, sdk):
    factories = []
    monkeypatch.setattr(
        "azure.identity.DefaultAzureCredential",
        lambda **kwargs: factories.append(True) or sdk,
    )
    session = PortalAzureSession()
    assert factories == []
    assert not session.cached_status()["signed_in"]
    reviewed = session.require_identity()
    first = session.credential(reviewed, subscription="legacy-subscription")
    second = session.credential(reviewed)
    assert factories == [True]
    assert first.get_token(SCOPE) == sdk.result
    assert second.get_token(SCOPE) == sdk.result
    cast(Any, first).close()
    assert not sdk.closed
    assert all(scopes == (SCOPE,) for scopes, _ in sdk.calls)
    assert not hasattr(session, "device_login")
    assert not any(hasattr(session, name) for name in ("subscriptions", "accounts", "filesystems"))


def test_session_close_is_lazy_and_closes_only_the_shared_owner_once(sdk):
    factories = []
    cold = PortalAzureSession(credential_factory=lambda: factories.append(True) or sdk)
    cold.close()
    assert factories == []
    assert not sdk.closed
    assert not cold.cached_status()["signed_in"]
    session = PortalAzureSession(credential=sdk)
    borrowed = session.credential()
    cast(Any, borrowed).close()
    assert sdk.close_calls == 0
    session.close()
    session.close()
    assert sdk.close_calls == 1
    with pytest.raises(PortalAzureError, match="credentials"):
        session.require_identity()


def test_close_invalidates_in_flight_sdk_tokens(sdk):
    session = PortalAzureSession(credential=sdk)
    borrowed = session.credential()
    sdk.callback = session.close
    with pytest.raises(PortalAzureError, match="changed"):
        borrowed.get_token(SCOPE)
    assert not session.cached_status()["signed_in"]


def test_session_close_error_does_not_leak_sdk_details():
    class BrokenCredential(Credential):
        def close(self):
            raise RuntimeError("SECRET token")

    session = PortalAzureSession(credential=BrokenCredential())
    assert session.status()["signed_in"]
    with pytest.raises(PortalAzureError, match="credentials") as exc:
        session.close()
    assert "SECRET" not in str(exc.value)
    assert not session.cached_status()["signed_in"]
    session.close()


def test_sdk_user_snapshot_and_display_cache(sdk):
    now = [0]
    session = PortalAzureSession(credential=sdk, clock=lambda: now[0])
    assert sdk.calls == []
    initial = session.cached_status()
    assert "Checking" in initial["name"]
    status = session.status()
    assert status == {
        "signed_in": True, "name": "Alice Example", "initials": "AE",
        "account": "alice@example.com", "tenant_id": "tenant-1",
        "user_id": "principal-1", "principal_type": "user", "error": "",
    }
    assert sdk.calls == [((SCOPE,), {})]
    status["name"] = "tampered"
    assert session.status()["name"] == "Alice Example"
    now[0] = 5
    assert session.cached_status()["name"] == "Alice Example"
    assert len(sdk.calls) == 1
    assert session.status()["signed_in"]
    assert len(sdk.calls) == 2
    snapshot = session.require_identity()
    assert snapshot == {
        "account": "alice@example.com", "tenant_id": "tenant-1",
        "user_id": "principal-1", "principal_type": "user",
    }
    assert "token" not in repr(snapshot) + repr(session.cached_status())
    session.refresh()
    assert not session.cached_status()["signed_in"]
    assert session.status()["signed_in"]
    assert len(sdk.calls) == 4


@pytest.mark.parametrize("claims,account,kind", [
    ({"idtyp": "app", "appid": "app-1", "upn": None, "name": None}, "app-1", "app"),
    ({"idtyp": "app", "azp": "app-2", "upn": None, "name": None}, "app-2", "app"),
    ({"idtyp": "app", "upn": None, "name": None}, "principal-1", "app"),
    ({"upn": None, "preferred_username": "bob@example.com", "name": None}, "bob@example.com", "user"),
])
def test_user_and_workload_identity_do_not_need_graph(sdk, claims, account, kind):
    sdk.result = token(**claims)
    session = PortalAzureSession(credential=sdk)
    identity = session.require_identity()
    assert identity["account"] == account
    assert identity["principal_type"] == kind
    status = session.status()
    assert status["signed_in"]
    assert status["name"] == account
    assert status["initials"] == account[0].upper()
    assert status["error"] == ""
    assert session.credential(identity).get_token(SCOPE) == sdk.result
    assert all(scopes == (SCOPE,) for scopes, _ in sdk.calls)


@pytest.mark.parametrize("error,code", [
    (ClientAuthenticationError("SECRET token"), "azure_error"),
    (RuntimeError("SECRET token"), "azure_error"),
    (HttpResponseError(message="SECRET", response=None), "azure_error"),
])
def test_authentication_failure_is_safe_and_invalidates_success(sdk, error, code, caplog):
    session = PortalAzureSession(credential=sdk)
    assert session.status()["signed_in"]
    sdk.error = error
    session.refresh()
    status = session.status()
    assert not status["signed_in"]
    assert session.cached_status() == status
    assert "credentials" in status["error"]
    assert "SECRET" not in repr(status) + caplog.text
    assert "az login" not in status["error"]
    with pytest.raises(PortalAzureError) as exc:
        session.require_identity()
    assert exc.value.code == code
    assert not session.cached_status()["signed_in"]


@pytest.mark.parametrize("claims,code", [
    ({"oid": ""}, "invalid_response"),
    ({"tid": 1}, "invalid_response"),
    ({"exp": "future"}, "invalid_response"),
    ({"exp": float("nan")}, "invalid_response"),
    ({"exp": float("inf")}, "invalid_response"),
    ({"exp": 0}, "expired"),
])
def test_token_metadata_must_be_valid_and_current(sdk, claims, code):
    sdk.result = token(**claims)
    session = PortalAzureSession(credential=sdk)
    with pytest.raises(PortalAzureError) as exc:
        session.require_identity()
    assert exc.value.code == code
    assert not session.cached_status()["signed_in"]


@pytest.mark.parametrize("expiry", [True, float("nan"), float("inf")])
def test_sdk_token_expiry_is_not_fabricated_from_claims(sdk, expiry):
    sdk.result = AccessToken(token().token, expiry)
    with pytest.raises(PortalAzureError, match="identity metadata"):
        PortalAzureSession(credential=sdk).require_identity()


def test_sdk_token_expired_at_exact_boundary_is_rejected(sdk):
    sdk.result = AccessToken(token().token, 100)
    with pytest.raises(PortalAzureError, match="expired"):
        PortalAzureSession(credential=sdk, wall_clock=lambda: 100).require_identity()


def test_storage_permission_failure_is_generic(sdk, caplog):
    error = HttpResponseError("SECRET")
    error.status_code = 403
    sdk.error = error
    session = PortalAzureSession(credential=sdk)
    with pytest.raises(PortalAzureError) as exc:
        session.require_identity()
    assert exc.value.code == "permission_denied"
    assert "SECRET" not in str(exc.value) + caplog.text


def test_lazy_credential_owner_is_shared_by_concurrent_status_requests(sdk):
    entered = Event()
    release = Event()
    factories = []

    def factory():
        factories.append(True)
        entered.set()
        assert release.wait(5)
        return sdk

    session = PortalAzureSession(credential_factory=factory)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(session.require_identity)
        assert entered.wait(5)
        second = executor.submit(session.require_identity)
        release.set()
        assert first.result() == second.result()
    assert factories == [True]


@pytest.mark.parametrize("value", ["opaque", "a.%%%.b", "a.W10.b"])
def test_opaque_or_malformed_token_cannot_supply_review_identity(sdk, value):
    sdk.result = AccessToken(value, 4102444800)
    session = PortalAzureSession(credential=sdk)
    with pytest.raises(PortalAzureError, match="identity metadata"):
        session.require_identity()


@pytest.mark.parametrize("changes", [{"oid": "other"}, {"tid": "other"}])
def test_reviewed_token_refresh_rejects_principal_and_tenant_drift(sdk, changes):
    session = PortalAzureSession(credential=sdk)
    snapshot = session.require_identity()
    guarded = session.credential(snapshot)
    assert guarded.get_token(SCOPE, claims="challenge") == sdk.result
    assert sdk.calls[-1] == ((SCOPE,), {"claims": "challenge"})
    sdk.result = token(**changes)
    with pytest.raises(PortalAzureError, match="changed"):
        guarded.get_token(SCOPE)
    assert not session.cached_status()["signed_in"]
    assert snapshot["user_id"] == "principal-1"
    with pytest.raises(PortalAzureError, match="changed"):
        session.credential(snapshot)


def test_sdk_auth_failure_during_guarded_refresh_is_not_cached_authorization(sdk):
    session = PortalAzureSession(credential=sdk)
    assert session.status()["signed_in"]
    snapshot = session.require_identity()
    guarded = session.credential(snapshot)
    sdk.error = ClientAuthenticationError("SECRET")
    with pytest.raises(PortalAzureError, match="credentials"):
        guarded.get_token(SCOPE)
    assert not session.cached_status()["signed_in"]


def test_display_account_changes_do_not_change_principal_identity(sdk):
    session = PortalAzureSession(credential=sdk)
    snapshot = session.require_identity()
    sdk.result = token(upn="renamed@example.com")
    assert session.credential(snapshot).get_token(SCOPE) == sdk.result


def test_principal_identifiers_and_explicit_tenant_match_case_insensitively(sdk):
    session = PortalAzureSession(credential=sdk)
    reviewed = session.require_identity()
    reviewed["tenant_id"] = reviewed["tenant_id"].upper()
    reviewed["user_id"] = reviewed["user_id"].upper()
    guarded = session.credential(reviewed)
    assert guarded.get_token(SCOPE, tenant_id="TENANT-1") == sdk.result
    session.check_identity(reviewed)


def test_request_guard_failure_invalidates_previously_cached_display(sdk):
    session = PortalAzureSession(credential=sdk)
    assert session.status()["signed_in"]
    reviewed = session.require_identity()
    reviewed["user_id"] = "other"
    with pytest.raises(PortalAzureError, match="changed"):
        session.check_identity(reviewed)
    assert not session.cached_status()["signed_in"]


@pytest.mark.parametrize("claims,kind,account", [
    ({"upn": None, "name": None}, "user", "principal-1"),
    ({"upn": None, "appid": "app", "scp": "read"}, "user", "principal-1"),
    ({"upn": None, "appid": "app"}, "app", "app"),
    ({"idtyp": "app", "appid": 3, "name": None}, "app", "principal-1"),
])
def test_optional_display_claims_and_legacy_workload_tokens(sdk, claims, kind, account):
    sdk.result = token(**claims)
    session = PortalAzureSession(credential=sdk)
    reviewed = session.require_identity()
    assert reviewed["account"] == account
    assert reviewed["principal_type"] == kind
    assert session.status()["signed_in"]


def test_tenant_override_rejected_before_sdk_acquisition(sdk):
    session = PortalAzureSession(credential=sdk)
    guarded = session.credential()
    calls = len(sdk.calls)
    with pytest.raises(PortalAzureError, match="changed"):
        guarded.get_token(SCOPE, tenant_id="other")
    assert len(sdk.calls) == calls


def test_per_request_policy_reuses_sdk_identity_until_token_expiry(sdk):
    wall = [100]
    sdk.result = AccessToken(token().token, 200)
    session = PortalAzureSession(credential=sdk, wall_clock=lambda: wall[0])
    snapshot = session.require_identity()
    policy = session.policy(snapshot)
    request = PipelineRequest(HttpRequest("GET", "https://account.dfs.core.windows.net"), PipelineContext(None))
    for _ in range(20):
        session.check_identity(snapshot)
        policy.on_request(request)
    assert len(sdk.calls) == 1
    wall[0] = 170
    sdk.result = token(oid="other")
    with pytest.raises(PortalAzureError, match="changed"):
        policy.on_request(request)
    assert len(sdk.calls) == 2


def test_policy_without_snapshot_acquires_sdk_identity(sdk):
    session = PortalAzureSession(credential=sdk)
    policy = session.policy()
    policy.on_request(PipelineRequest(HttpRequest("GET", "https://account.dfs.core.windows.net"), PipelineContext(None)))
    assert sdk.calls == [((SCOPE,), {})]


@pytest.mark.parametrize("kind", ["user", "app"])
def test_native_sdk_pipeline_checks_cached_bearer_requests_and_leaves_authorization_to_azure(sdk, kind):
    sdk.result = token(idtyp=kind)
    session = PortalAzureSession(credential=sdk)
    reviewed = session.require_identity()
    borrowed = session.credential(reviewed)
    requests = []

    class ForbiddenTransport(HttpTransport):
        def open(self):
            pass

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

        def send(self, request, **kwargs):
            requests.append(request)
            assert request.headers["Authorization"] == f"Bearer {sdk.result.token}"
            response = HttpResponse(request, None)
            response.status_code = 403
            return response

    with Pipeline(
        transport=ForbiddenTransport(),
        policies=[session.policy(reviewed), BearerTokenCredentialPolicy(borrowed, SCOPE)],
    ) as pipeline:
        response = pipeline.run(HttpRequest("GET", "https://account.dfs.core.windows.net"))
        assert response.http_response.status_code == 403
        calls = len(sdk.calls)
        pipeline.run(HttpRequest("GET", "https://account.dfs.core.windows.net"))
        assert len(sdk.calls) == calls
        assert len(requests) == 2
        session.refresh()
        sdk.result = token(oid="other", idtyp=kind)
        with pytest.raises(PortalAzureError, match="changed"):
            pipeline.run(HttpRequest("GET", "https://account.dfs.core.windows.net"))
        assert len(requests) == 2
    assert not sdk.closed


@pytest.mark.parametrize("failure", [False, True])
def test_refresh_generation_rejects_in_flight_status_results(sdk, failure):
    session = PortalAzureSession(credential=sdk)
    sdk.callback = session.refresh
    if failure:
        sdk.error = RuntimeError("SECRET")
    assert not session.status()["signed_in"]
    assert "Checking" in session.cached_status()["name"]
    sdk.callback = None
    sdk.error = None
    assert session.status()["signed_in"]


def test_refresh_invalidates_request_guard_and_in_flight_token(sdk):
    session = PortalAzureSession(credential=sdk)
    reviewed = session.require_identity()
    guarded = session.credential(reviewed)
    session.refresh()
    sdk.result = token(oid="other")
    with pytest.raises(PortalAzureError, match="changed"):
        session.check_identity(reviewed)
    sdk.result = token()
    sdk.callback = session.refresh
    with pytest.raises(PortalAzureError, match="changed"):
        guarded.get_token(SCOPE)
    assert not session.cached_status()["signed_in"]


def test_invalid_cache_configuration(sdk):
    with pytest.raises(ValueError):
        PortalAzureSession(credential=sdk, cache_seconds=-1)
