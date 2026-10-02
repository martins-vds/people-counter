"""Fenced local queue-to-Spark batch coordination."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from people_counter.local_queue import (
    ClaimedMessage,
    LeaseLostError,
    SerializedQueueActor,
)
from people_counter.local_spark import (
    BatchStaging,
    MANIFEST_VERSION,
    StagingValidationError,
    attempt_staging_path,
)
from people_counter.local_storage import ContentAddressedStore, StoredObject


_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class ControlRun:
    batch_id: str
    correlation_id: str
    manifest: StoredObject | None
    staging: BatchStaging | None
    claimed_count: int


SparkBatchRunner = Callable[[Path, str, str], BatchStaging]


class BatchHeartbeat:
    """Renew all claim tokens through the serialized control actor."""

    def __init__(
        self,
        actor: SerializedQueueActor,
        claims: list[ClaimedMessage],
        *,
        lock_seconds: float,
        interval_seconds: float,
    ) -> None:
        if interval_seconds <= 0 or interval_seconds >= lock_seconds:
            raise ValueError("heartbeat interval must be positive and below lock")
        self._actor = actor
        self._tokens = [claim.lock_token for claim in claims]
        self._lock_seconds = lock_seconds
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._lost: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="local-queue-heartbeat",
            daemon=True,
        )

    def __enter__(self) -> BatchHeartbeat:
        self._thread.start()
        return self

    def __exit__(self, error_type, error, traceback) -> None:
        self._stop.set()
        self._thread.join(
            timeout=self._actor.call_timeout_seconds + self._interval_seconds
        )
        if self._thread.is_alive():
            raise TimeoutError("batch heartbeat shutdown timed out")

    def require_live(self) -> None:
        if self._lost is not None:
            raise LeaseLostError(
                f"batch heartbeat lost its queue fence: {self._lost}"
            ) from self._lost

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self._actor.call(
                    lambda queue: queue.renew_many(
                        self._tokens,
                        lock_seconds=self._lock_seconds,
                    )
                )
            except BaseException as error:
                self._lost = error
                self._stop.set()


def run_claimed_batch(
    actor: SerializedQueueActor,
    store: ContentAddressedStore,
    runner: SparkBatchRunner,
    *,
    owner: str,
    max_items: int,
    minimum_items: int,
    lock_seconds: float,
    heartbeat_seconds: float,
    staging_root: Path,
) -> ControlRun:
    """Claim, materialize, run, validate, and atomically settle one batch."""
    claims = actor.call(
        lambda queue: queue.claim(
            max_items,
            owner=owner,
            lock_seconds=lock_seconds,
            minimum_count=minimum_items,
        )
    )
    batch_id = str(uuid4())
    correlation_id = str(uuid4())
    if not claims:
        return ControlRun(batch_id, correlation_id, None, None, 0)

    batch_attempt_id = str(uuid4())
    expected_staging_path = attempt_staging_path(
        staging_root,
        batch_id,
        batch_attempt_id,
    )
    manifest = store.put_json(
        {
            "schema_version": MANIFEST_VERSION,
            "batch_id": batch_id,
            "batch_attempt_id": batch_attempt_id,
            "correlation_id": correlation_id,
            "expected_staging_path": str(expected_staging_path),
            "items": [_manifest_item(claim) for claim in claims],
        }
    )
    heartbeat = BatchHeartbeat(
        actor,
        claims,
        lock_seconds=lock_seconds,
        interval_seconds=heartbeat_seconds,
    )
    try:
        with heartbeat:
            staging = runner(
                manifest.path,
                manifest.sha256,
                correlation_id,
            )
            heartbeat.require_live()
            _verify_staging_fence(
                staging,
                batch_id=batch_id,
                batch_attempt_id=batch_attempt_id,
                manifest_sha256=manifest.sha256,
                expected_path=expected_staging_path,
            )
            failed = set(staging.failed_work_ids)
            outcomes = [
                (
                    claim.lock_token,
                    "abandon" if claim.message_id in failed else "complete",
                    "Spark staging contains a failed terminal record"
                    if claim.message_id in failed
                    else None,
                )
                for claim in claims
            ]
            actor.call(lambda queue: queue.finalize(outcomes))
    except BaseException:
        _abandon_live_claims(actor, claims)
        raise
    _LOG.info(
        "local_batch_committed",
        extra={
            "event": "local_batch_committed",
            "batch_id": batch_id,
            "correlation_id": correlation_id,
            "manifest_sha256": manifest.sha256,
            "record_count": staging.record_count,
        },
    )
    return ControlRun(
        batch_id,
        correlation_id,
        manifest,
        staging,
        len(claims),
    )


def _manifest_item(claim: ClaimedMessage) -> dict[str, Any]:
    payload = dict(claim.payload)
    payload["work_id"] = claim.message_id
    payload["attempt_id"] = claim.lock_token
    return payload


def _verify_staging_fence(
    staging: BatchStaging,
    *,
    batch_id: str,
    batch_attempt_id: str,
    manifest_sha256: str,
    expected_path: Path,
) -> None:
    mismatches = []
    if staging.batch_id != batch_id:
        mismatches.append("batch_id")
    if staging.batch_attempt_id != batch_attempt_id:
        mismatches.append("batch_attempt_id")
    if staging.manifest_sha256 != manifest_sha256:
        mismatches.append("manifest_sha256")
    if staging.path.expanduser().resolve() != expected_path:
        mismatches.append("path")
    if mismatches:
        raise StagingValidationError(
            f"staging fence mismatch for {', '.join(mismatches)}"
        )


def _abandon_live_claims(
    actor: SerializedQueueActor,
    claims: list[ClaimedMessage],
) -> None:
    try:
        actor.call(
            lambda queue: queue.finalize(
                [
                    (claim.lock_token, "abandon", "batch execution failed")
                    for claim in claims
                ]
            )
        )
    except LeaseLostError:
        _LOG.warning(
            "local_batch_abandon_lost_fence",
            extra={"event": "local_batch_abandon_lost_fence"},
        )
