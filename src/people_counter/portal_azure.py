"""User-only Azure CLI identity for the host-run portal; never stores tokens."""

from __future__ import annotations

import base64
import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
    from azure.core.pipeline.policies import SansIOHTTPPolicy


_LOG = logging.getLogger(__name__)
_STORAGE_RESOURCE = "https://storage.azure.com/"
_ERRORS = {
    "cli_missing": "Azure CLI is not installed. Install it and sign in externally.",
    "signed_out": "Azure CLI is signed out. Run az login externally, then refresh.",
    "expired": "Azure CLI session expired. Sign in externally, then refresh.",
    "cli_timeout": "Azure CLI timed out. Retry after checking the CLI session.",
    "service_principal": "This workflow requires an Azure CLI user, not a service principal.",
    "permission_denied": "Permission denied by Azure. Check the user's access, then refresh.",
    "identity_changed": "Azure CLI user or tenant changed. Refresh and review the job again.",
    "invalid_response": "Azure CLI identity response is invalid. Refresh the CLI session.",
    "azure_error": "Azure CLI authentication or request failed. Check the CLI externally, then refresh.",
    "subscription": "Selected subscription is not available to the current tenant and user.",
    "account": "Selected account is not an HNS account in the selected subscription.",
}


class PortalAzureError(RuntimeError):
    """Safe, token-free error for display by portal endpoints."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(_ERRORS[code])


def _safe_error(exc: Exception) -> PortalAzureError:
    if isinstance(exc, PortalAzureError):
        return exc
    return PortalAzureError(
        "permission_denied" if getattr(exc, "status_code", None) == 403 else "azure_error",
    )


def _failure_code(message: str) -> str:
    lowered = message.lower()
    if "expired" in lowered or "aadsts700082" in lowered or "aadsts70043" in lowered:
        return "expired"
    if "az login" in lowered or "not logged" in lowered or "no accounts" in lowered:
        return "signed_out"
    if "permission" in lowered or "forbidden" in lowered or "authorizationfailed" in lowered:
        return "permission_denied"
    return "azure_error"


def _claims(access_token: str) -> dict:
    try:
        payload = access_token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        if not isinstance(claims, dict):
            raise ValueError
        if not all(isinstance(claims.get(key), str) and claims[key] for key in ("oid", "tid")):
            raise ValueError
        if not isinstance(claims.get("exp"), (int, float)):
            raise ValueError
        if claims["exp"] <= time.time():
            raise PortalAzureError("expired")
        if claims.get("idtyp") == "app":
            raise PortalAzureError("service_principal")
        return claims
    except (ValueError, TypeError, IndexError, AttributeError):
        raise PortalAzureError("invalid_response") from None


def _same_identity(expected: dict, actual: dict) -> None:
    if any(str(expected.get(key, "")).casefold() != str(actual.get(key, "")).casefold()
           for key in ("account", "tenant_id", "user_id")):
        raise PortalAzureError("identity_changed")


def _cli_state() -> tuple | None:
    try:
        root = Path(os.environ.get("AZURE_CONFIG_DIR", str(Path.home() / ".azure"))).resolve()
        profile = (root / "azureProfile.json").stat()
        files: list[tuple[int, int, int, int] | None] = [
            (profile.st_ino, profile.st_size, profile.st_mtime_ns, profile.st_ctime_ns),
        ]
        for name in ("msal_token_cache.json", "msal_token_cache.bin"):
            try:
                value = (root / name).stat()
                files.append((value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns))
            except FileNotFoundError:
                files.append(None)
        return str(root), tuple(files)
    except (OSError, RuntimeError, ValueError):
        _LOG.debug("CLI state monitoring unavailable; using fresh CLI identity checks")
        return None


class _GuardedCredential:
    def __init__(self, session: PortalAzureSession, snapshot: dict, credential: Any):
        self._session = session
        self._snapshot = dict(snapshot)
        self._credential = credential

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        try:
            _same_identity(self._snapshot, self._session.require_identity())
            if kwargs.get("tenant_id") not in (None, self._snapshot["tenant_id"]):
                raise PortalAzureError("identity_changed")
            result = self._credential.get_token(*scopes, **kwargs)
            claims = _claims(result.token)
            if (claims["oid"] != self._snapshot["user_id"]
                    or claims["tid"] != self._snapshot["tenant_id"]):
                raise PortalAzureError("identity_changed")
            _same_identity(self._snapshot, self._session.require_identity())
            return result
        except Exception as exc:
            error = _safe_error(exc)
            self._session._failed(error)
            raise error from None

    def close(self) -> None:
        close = getattr(self._credential, "close", None)
        if close:
            close()


class PortalAzureSession:
    """Inject ``run`` and SDK factories in tests; all CLI calls have deadlines.

    ``require_identity`` returns a token-free job snapshot. Pass that snapshot
    to ``credential`` before execution/resume; token refresh checks it again.
    Storage clients install ``policy(snapshot)`` using their additional-policies
    hook. Request guards reuse a verified identity only while CLI state files
    and the token lifetime remain unchanged; approval and token checks stay fresh.
    ``status`` is display-only and may be cached for ``cache_seconds``.
    """

    def __init__(
        self, *, run: Callable = subprocess.run, credential_factory: Callable | None = None,
        service_client_factory: Callable | None = None, clock: Callable = time.monotonic,
        cache_seconds: float = 5, timeout: int = 10,
        cli_state: Callable[[], object] = _cli_state,
    ):
        if timeout <= 0 or cache_seconds < 0:
            raise ValueError("timeout must be positive and cache_seconds nonnegative")
        self._run = run
        self._credential_factory = credential_factory
        self._service_client_factory = service_client_factory
        self._clock = clock
        self._cache_seconds = cache_seconds
        self._timeout = timeout
        self._cached: dict | None = None
        self._cached_at = 0.0
        self._display_revision = 0
        self._subscription: str | None = None
        self._cli_state = cli_state
        self._verified: tuple[dict, float, object] | None = None

    def _json(self, *arguments: str) -> Any:
        try:
            result = self._run(
                ["az", *arguments, "--output", "json", "--only-show-errors"],
                capture_output=True, text=True, timeout=self._timeout, check=False,
            )
        except FileNotFoundError:
            raise PortalAzureError("cli_missing") from None
        except subprocess.TimeoutExpired:
            raise PortalAzureError("cli_timeout") from None
        except OSError:
            raise PortalAzureError("azure_error") from None
        if result.returncode:
            raise PortalAzureError(_failure_code(result.stderr)) from None
        try:
            return json.loads(result.stdout)
        except (ValueError, TypeError):
            raise PortalAzureError("invalid_response") from None

    def _identity(self) -> dict:
        context = self._json("account", "show")
        try:
            account = context["user"]["name"]
            tenant = context["tenantId"]
            subscription = context["id"]
            user_type = context["user"]["type"]
        except (KeyError, TypeError):
            raise PortalAzureError("invalid_response") from None
        if user_type != "user":
            raise PortalAzureError("service_principal")
        if not all(isinstance(value, str) and value for value in (account, tenant, subscription)):
            raise PortalAzureError("invalid_response")
        response = self._json(
            "account", "get-access-token", "--resource", _STORAGE_RESOURCE, "--tenant", tenant,
        )
        if not isinstance(response, dict):
            raise PortalAzureError("invalid_response")
        access_token = response.get("accessToken")
        if not isinstance(access_token, str):
            raise PortalAzureError("invalid_response")
        # The fresh CLI token, not the account cache or Graph availability, proves session usability.
        claims = _claims(access_token)
        if claims["tid"] != tenant:
            raise PortalAzureError("identity_changed")
        username = claims.get("upn") or claims.get("preferred_username")
        if username and username.casefold() != account.casefold():
            raise PortalAzureError("identity_changed")
        self._subscription = subscription
        identity = {"account": account, "tenant_id": tenant, "user_id": claims["oid"]}
        self._verified = (dict(identity), float(claims["exp"]) - 30, self._cli_state())
        return identity

    def _remember(self, value: dict, revision: int | None = None) -> None:
        if revision is not None and revision != self._display_revision:
            return
        self._cached = dict(value)
        self._cached_at = self._clock()

    def _failed(self, error: PortalAzureError) -> dict:
        self._subscription = None
        self._verified = None
        context = {
            "signed_in": False, "name": "Azure CLI unavailable", "initials": "?",
            "account": "", "tenant_id": "", "error": str(error),
        }
        self._remember(context)
        _LOG.warning("Portal Azure request failed (%s)", error.code)
        return context

    def refresh(self) -> None:
        self._display_revision += 1
        self._cached = None
        self._subscription = None
        self._verified = None

    def require_identity(self) -> dict:
        try:
            return self._identity()
        except Exception as exc:
            error = _safe_error(exc)
            self._failed(error)
            raise error from None

    def check_identity(self, snapshot: dict) -> None:
        """Check CLI-state changes without spawning a process for each request."""
        try:
            state = self._cli_state()
            verified = self._verified
            if state is not None and verified is not None and state == verified[2] and time.time() < verified[1]:
                actual = verified[0]
            else:
                actual = self.require_identity()
            _same_identity(snapshot, actual)
        except PortalAzureError as error:
            self._failed(error)
            raise

    def status(self) -> dict:
        if self._cached is not None and self._clock() - self._cached_at < self._cache_seconds:
            return dict(self._cached)
        revision = self._display_revision
        try:
            identity = self._identity()
        except Exception as exc:
            if revision != self._display_revision:
                return self.cached_status()
            return self._failed(_safe_error(exc))
        display_name = ""
        try:
            graph = self._json("ad", "signed-in-user", "show")
            if isinstance(graph, dict) and graph.get("id") == identity["user_id"]:
                display_name = graph.get("displayName") or ""
            if not isinstance(display_name, str):
                display_name = ""
        except PortalAzureError:
            pass
        warning = "" if display_name else "Azure CLI display name unavailable."
        if warning:
            _LOG.warning(warning)
        context = {
            "signed_in": True,
            "name": display_name or f'{identity["account"]} (display name unavailable)',
            "initials": "".join(word[0] for word in display_name.split()[:2]).upper()
            if display_name else identity["account"][0].upper(),
            "account": identity["account"], "tenant_id": identity["tenant_id"], "error": warning,
        }
        self._remember(context, revision)
        return dict(context)

    def cached_status(self) -> dict:
        """Render immediately; background status requests perform CLI lookup."""
        if self._cached is not None:
            return dict(self._cached)
        return {
            "signed_in": False, "name": "Checking Azure CLI identity", "initials": "?",
            "account": "", "tenant_id": "", "error": "",
        }

    def credential(
        self, snapshot: dict | None = None, subscription: str | None = None,
    ) -> TokenCredential:
        identity = self.require_identity()
        reviewed = dict(snapshot) if snapshot is not None else identity
        try:
            _same_identity(reviewed, identity)
            selected = subscription or self._subscription
            if subscription and subscription not in {s["id"] for s in self._subscriptions(identity)}:
                raise PortalAzureError("subscription")
            factory = self._credential_factory
            if factory is None:
                from azure.identity import AzureCliCredential
                factory = AzureCliCredential
            return _GuardedCredential(self, reviewed, factory(
                tenant_id=identity["tenant_id"], subscription=selected, process_timeout=self._timeout,
            ))
        except Exception as exc:
            error = _safe_error(exc)
            self._failed(error)
            raise error from None

    def policy(self, snapshot: dict | None = None) -> SansIOHTTPPolicy:
        """Reject changed CLI identity before every physical SDK request."""
        from azure.core.pipeline.policies import SansIOHTTPPolicy

        reviewed = dict(snapshot) if snapshot is not None else self.require_identity()
        self.check_identity(reviewed)
        session = self

        class IdentityPolicy(SansIOHTTPPolicy):
            def on_request(self, request: Any) -> None:
                try:
                    session.check_identity(reviewed)
                except PortalAzureError as error:
                    session._failed(error)
                    raise

        return IdentityPolicy()

    def _subscriptions(self, identity: dict) -> list[dict]:
        rows = self._json("account", "list", "--refresh")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise PortalAzureError("invalid_response")
        return [
            {"id": row["id"], "name": row["name"]}
            for row in rows
            if row.get("tenantId") == identity["tenant_id"]
            and row.get("user", {}).get("type") == "user"
            and row.get("user", {}).get("name", "").casefold() == identity["account"].casefold()
            and row.get("id") and row.get("name")
        ]

    def subscriptions(self) -> list[dict]:
        identity = self.require_identity()
        try:
            rows = self._subscriptions(identity)
            _same_identity(identity, self.require_identity())
            return rows
        except Exception as exc:
            error = _safe_error(exc)
            self._failed(error)
            raise error from None

    def accounts(self, subscription: str) -> list[dict]:
        identity = self.require_identity()
        try:
            if subscription not in {row["id"] for row in self._subscriptions(identity)}:
                raise PortalAzureError("subscription")
            rows = self._json("storage", "account", "list", "--subscription", subscription)
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise PortalAzureError("invalid_response")
            result = [
                {key: row[key] for key in ("name", "id", "resourceGroup")}
                for row in rows if row.get("isHnsEnabled") is True
                and all(row.get(key) for key in ("name", "id", "resourceGroup"))
            ]
            _same_identity(identity, self.require_identity())
            return result
        except Exception as exc:
            error = _safe_error(exc)
            self._failed(error)
            raise error from None

    def filesystems(self, account: str, subscription: str) -> list[dict]:
        identity = self.require_identity()
        try:
            if account not in {row["name"] for row in self.accounts(subscription)}:
                raise PortalAzureError("account")
            credential = self.credential(identity, subscription)
            factory = self._service_client_factory
            if factory is None:
                from azure.storage.filedatalake import DataLakeServiceClient
                factory = DataLakeServiceClient
            client = factory(
                account_url=f"https://{account}.dfs.core.windows.net", credential=credential,
                _additional_pipeline_policies=[self.policy(identity)],
            )
            try:
                result = [{"name": filesystem["name"]} for filesystem in client.list_file_systems()]
                _same_identity(identity, self.require_identity())
                return result
            finally:
                client.close()
                close = getattr(credential, "close", None)
                if callable(close):
                    close()
        except Exception as exc:
            error = _safe_error(exc)
            self._failed(error)
            raise error from None
