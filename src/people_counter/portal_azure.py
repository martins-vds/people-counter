"""SDK-owned Azure identity with token-free portal display and review snapshots."""

from __future__ import annotations

import base64
import json
import logging
import math
import time
from collections.abc import Callable
from threading import Lock
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from azure.core.credentials import AccessToken, TokenCredential
    from azure.core.pipeline.policies import SansIOHTTPPolicy


_LOG = logging.getLogger(__name__)
_STORAGE_SCOPE = "https://storage.azure.com/.default"
_ERRORS = {
    "expired": "Azure runtime credentials have expired. Refresh and review again.",
    "permission_denied": "Permission denied by Azure. Check the principal's access.",
    "identity_changed": "Azure principal or tenant changed. Refresh and review the job again.",
    "invalid_response": "Azure runtime credentials returned invalid identity metadata.",
    "azure_error": "Azure runtime credentials are unavailable or the request failed. Refresh and retry.",
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


def _claims(access_token: str, now: float) -> dict:
    # SDK-acquired claims are consistency/display evidence, not Azure authorization.
    try:
        payload = access_token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        if not isinstance(claims, dict):
            raise TypeError
        if not all(isinstance(claims.get(key), str) and claims[key] for key in ("oid", "tid")):
            raise ValueError
        expiry = claims.get("exp")
        if isinstance(expiry, bool) or not isinstance(expiry, (int, float)) or not math.isfinite(expiry):
            raise ValueError
        if expiry <= now:
            raise PortalAzureError("expired")
        return claims
    except (ValueError, TypeError, IndexError, AttributeError):
        raise PortalAzureError("invalid_response") from None


def _same_identity(expected: dict, actual: dict) -> None:
    if any(str(expected.get(key, "")).casefold() != str(actual.get(key, "")).casefold()
           for key in ("tenant_id", "user_id")):
        raise PortalAzureError("identity_changed")


class _GuardedCredential:
    def __init__(self, session: PortalAzureSession, snapshot: dict):
        self._session = session
        self._snapshot = dict(snapshot)

    def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        session = self._session
        revision = session._display_revision
        try:
            tenant = kwargs.get("tenant_id")
            if tenant is not None and str(tenant).casefold() != self._snapshot["tenant_id"].casefold():
                raise PortalAzureError("identity_changed")
            result = session._owner().get_token(*scopes, **kwargs)
            session._accept_token(result, revision, self._snapshot)
            return result
        except Exception as exc:  # noqa: BLE001 -- Never expose SDK credential details.
            error = _safe_error(exc)
            session._failed(error, revision)
            raise error from None

    def close(self) -> None:
        """Borrowed SDK clients must not close the session's shared owner."""


class PortalAzureSession:
    """One DefaultAzureCredential (or injected TokenCredential) owns all tokens.

    ``user_id`` is the legacy snapshot field for the principal object ID, for
    both users and workloads. Subscription is accepted by ``credential`` only
    for caller compatibility; ADLS authorization belongs to the native SDK.
    Request policies compare token-free evidence until token refresh is needed.
    Every acquired token is checked for reviewed principal/tenant drift.
    ``cached_status`` is immediate display only, never authorization.
    """

    def __init__(
        self, *, credential: TokenCredential | None = None,
        credential_factory: Callable[[], TokenCredential] | None = None,
        clock: Callable[[], float] = time.monotonic, cache_seconds: float = 5,
        wall_clock: Callable[[], float] = time.time,
    ):
        if cache_seconds < 0:
            raise ValueError("cache_seconds must be nonnegative")
        self._credential = credential
        self._owner_lock = Lock()
        self._closed = False
        self._credential_factory = credential_factory
        self._clock = clock
        self._wall_clock = wall_clock
        self._cache_seconds = cache_seconds
        self._cached: dict | None = None
        self._cached_at = 0.0
        self._display_revision = 0
        self._verified: tuple[dict, float] | None = None

    def _owner(self) -> TokenCredential:
        with self._owner_lock:
            if self._closed:
                raise PortalAzureError("azure_error")
            if self._credential is None:
                factory = self._credential_factory
                if factory is None:
                    from azure.identity import DefaultAzureCredential
                    factory = DefaultAzureCredential
                self._credential = factory()
            return self._credential

    def close(self) -> None:
        """Close the shared owner once; a cold session creates no SDK resources."""
        with self._owner_lock:
            if self._closed:
                return
            self._closed = True
            owner = self._credential
            self._credential = None
            self.refresh()
        close = getattr(owner, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:  # noqa: BLE001 -- Never expose SDK credential details.
                raise _safe_error(exc) from None

    def _accept_token(
        self, result: AccessToken, revision: int, expected: dict | None = None,
    ) -> tuple[dict, str]:
        now = self._wall_clock()
        claims = _claims(result.token, now)
        if isinstance(result.expires_on, bool) or not isinstance(result.expires_on, (int, float)) \
                or not math.isfinite(result.expires_on):
            raise PortalAzureError("invalid_response")
        if result.expires_on <= now:
            raise PortalAzureError("expired")
        username = claims.get("upn") or claims.get("preferred_username")
        kind = "app" if claims.get("idtyp") == "app" or (
            not username and (claims.get("appid") or claims.get("azp")) and not claims.get("scp")
        ) else "user"
        account = (claims.get("appid") or claims.get("azp")) if kind == "app" else username
        if not isinstance(account, str) or not account:
            account = claims["oid"]
        identity = {
            "account": account, "tenant_id": claims["tid"], "user_id": claims["oid"],
            "principal_type": kind,
        }
        if expected is not None:
            _same_identity(expected, identity)
        if revision != self._display_revision:
            raise PortalAzureError("identity_changed")
        self._verified = (dict(identity), min(result.expires_on, claims["exp"]) - 30)
        name = claims.get("name")
        return identity, name if isinstance(name, str) and name.strip() else account

    def _identity(self) -> tuple[dict, str]:
        revision = self._display_revision
        result = self._owner().get_token(_STORAGE_SCOPE)
        return self._accept_token(result, revision)

    def _remember(self, value: dict, revision: int) -> None:
        if revision != self._display_revision:
            return
        self._cached = dict(value)
        self._cached_at = self._clock()

    def _failed(self, error: PortalAzureError, revision: int) -> dict:
        context = {
            "signed_in": False, "name": "Azure identity unavailable", "initials": "?",
            "account": "", "tenant_id": "", "error": str(error),
        }
        if revision == self._display_revision:
            self._verified = None
            self._remember(context, revision)
        _LOG.warning("Portal Azure request failed (%s)", error.code)
        return context

    def refresh(self) -> None:
        self._display_revision += 1
        self._cached = None
        self._verified = None

    def require_identity(self) -> dict:
        revision = self._display_revision
        try:
            return self._identity()[0]
        except Exception as exc:  # noqa: BLE001 -- Never expose SDK credential details.
            error = _safe_error(exc)
            self._failed(error, revision)
            raise error from None

    def check_identity(self, snapshot: dict) -> None:
        """No credential call per range while SDK token evidence remains current."""
        revision = self._display_revision
        try:
            verified = self._verified
            actual = verified[0] if verified is not None and self._wall_clock() < verified[1] \
                else self.require_identity()
            _same_identity(snapshot, actual)
        except PortalAzureError as exc:
            error = _safe_error(exc)
            self._failed(error, revision)
            raise error from None

    def status(self) -> dict:
        if self._cached is not None and self._clock() - self._cached_at < self._cache_seconds:
            return dict(self._cached)
        revision = self._display_revision
        try:
            identity, name = self._identity()
        except Exception as exc:  # noqa: BLE001 -- Never expose SDK credential details.
            if revision != self._display_revision:
                return self.cached_status()
            return self._failed(_safe_error(exc), revision)
        context = {
            "signed_in": True, "name": name,
            "initials": "".join(word[0] for word in name.split()[:2]).upper(),
            **identity, "error": "",
        }
        self._remember(context, revision)
        return dict(context) if revision == self._display_revision else self.cached_status()

    def cached_status(self) -> dict:
        """Render immediately; background status requests acquire SDK identity."""
        if self._cached is not None:
            return dict(self._cached)
        return {
            "signed_in": False, "name": "Checking Azure identity", "initials": "?",
            "account": "", "tenant_id": "", "error": "",
        }

    def credential(
        self, snapshot: dict | None = None, subscription: str | None = None,
    ) -> TokenCredential:
        revision = self._display_revision
        identity = self.require_identity()
        reviewed = dict(snapshot) if snapshot is not None else identity
        try:
            _same_identity(reviewed, identity)
            return _GuardedCredential(self, reviewed)
        except PortalAzureError as exc:
            error = _safe_error(exc)
            self._failed(error, revision)
            raise error from None

    def policy(self, snapshot: dict | None = None) -> SansIOHTTPPolicy:
        """Reject principal/tenant drift before physical SDK requests."""
        from azure.core.pipeline.policies import SansIOHTTPPolicy

        reviewed = dict(snapshot) if snapshot is not None else self.require_identity()
        self.check_identity(reviewed)
        session = self

        class IdentityPolicy(SansIOHTTPPolicy):
            def on_request(self, request: Any) -> None:
                session.check_identity(reviewed)

        return IdentityPolicy()
