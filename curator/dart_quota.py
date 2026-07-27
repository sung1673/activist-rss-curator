from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import httpx


KST = ZoneInfo("Asia/Seoul")
DART_DAILY_LIMIT = 40_000
_REVISION_RE = re.compile(r"^[0-9a-f]{7,40}$")
_BACKEND_BINDING_RE = re.compile(r"^[0-9a-f]{64}$")
_CREDENTIAL_ID_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_COMPONENT_RE = re.compile(r"[^0-9A-Za-z_.-]+")
_SAFE_ERROR_CODES = frozenset(
    {
        "backend_binding_mismatch",
        "backend_binding_required",
        "dart_credential_blocked",
        "dart_credential_disabled",
        "dart_quota_exhausted",
        "dart_quota_idempotency_conflict",
        "dart_quota_persistence_failed",
        "invalid_request",
        "quota_date_mismatch",
    }
)
_SAFE_INVALID_REQUEST_DETAILS = frozenset(
    {
        "attempt_or_revision",
        "consumed_attempt_required",
        "credential_id",
        "exact_fields_required",
        "operation",
        "reason",
    }
)
_SAFE_PERSISTENCE_DETAILS = frozenset(
    {
        "transaction_commit_failed",
        "transaction_readback_attempt_failed",
        "transaction_readback_binding_failed",
        "transaction_readback_connection_failed",
        "transaction_readback_credential_failed",
        "transaction_readback_day_failed",
        "transaction_state_invalid",
    }
)
_DNS_HOST_RE = re.compile(
    r"(?=.{1,253}\Z)"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?"
)


class DartQuotaLedgerError(RuntimeError):
    """The authoritative daily DART quota ledger could not safely acknowledge a call."""


class DartQuotaLedgerRejectedError(DartQuotaLedgerError):
    """The authoritative ledger rejected a call because the day is blocked or exhausted."""


class DartGlobalQuotaExceededError(DartQuotaLedgerRejectedError):
    """The authoritative ledger exhausted the shared KST-day request pool."""


class DartCredentialUnavailableError(DartQuotaLedgerRejectedError):
    """One credential is unavailable while other pool credentials may still work."""

    def __init__(self, *, reason: str, credential_id: str) -> None:
        if reason not in {"blocked_020", "disabled_901"}:
            raise ValueError("unsupported DART credential-unavailable reason")
        if _CREDENTIAL_ID_RE.fullmatch(credential_id) is None:
            raise ValueError("invalid DART credential identifier")
        self.reason = reason
        self.credential_id = credential_id
        super().__init__(f"DART credential is unavailable: {reason}")


@dataclass(frozen=True)
class DartQuotaPermit:
    attempt_id: str
    quota_day: str
    credential_id: str
    used_count: int
    remaining_count: int
    credential_used_count: int
    credential_remaining_count: int
    duplicate: bool


def _validated_api_base_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if (
        any(ord(character) <= 32 or ord(character) == 127 for character in value)
        or "\\" in value
        or "%" in value
    ):
        raise DartQuotaLedgerError(
            "DART quota API base URL contains an unsafe URL character"
        )
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise DartQuotaLedgerError("DART quota API base URL is invalid") from exc
    hostname = (parsed.hostname or "").casefold()
    if (
        parsed.scheme.casefold() != "https"
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or _DNS_HOST_RE.fullmatch(hostname) is None
        or port not in (None, 443)
    ):
        raise DartQuotaLedgerError(
            "DART quota API base URL must use credential-free canonical HTTPS"
        )
    path = parsed.path.rstrip("/")
    segments = path.split("/")
    if (
        not path.startswith("/")
        or not path.endswith("/api/v1")
        or "//" in path
        or any(segment in {".", ".."} for segment in segments)
    ):
        raise DartQuotaLedgerError("DART quota API base URL must end with /api/v1")
    return urlunsplit(("https", hostname, path, "", ""))


def dart_quota_api_base_url() -> str:
    return _validated_api_base_url(
        os.environ.get("BSIDE_API_BASE_URL", "").strip()
        or os.environ.get("GOVERNANCE_API_BASE_URL", "").strip()
    )


def dart_quota_api_token() -> str:
    return os.environ.get("BSIDE_OPS_TOKEN", "").strip()


def dart_quota_backend_binding_id() -> str:
    return os.environ.get("BSIDE_BACKEND_BINDING_ID", "").strip()


def dart_credential_id() -> str:
    return os.environ.get("DART_CREDENTIAL_ID", "").strip()


def durable_dart_quota_required() -> bool:
    explicit = os.environ.get("CURATOR_REQUIRE_DURABLE_DART_QUOTA", "").strip().casefold() in {
        "1",
        "true",
        "yes",
        "on",
    }
    github_actions = os.environ.get("GITHUB_ACTIONS", "").strip().casefold() == "true"
    return explicit or github_actions


def durable_dart_quota_configured() -> bool:
    """Treat even partial production configuration as fail-closed intent."""

    return bool(
        os.environ.get("BSIDE_API_BASE_URL", "").strip()
        or os.environ.get("GOVERNANCE_API_BASE_URL", "").strip()
        or os.environ.get("BSIDE_OPS_TOKEN", "").strip()
        or os.environ.get("BSIDE_BACKEND_BINDING_ID", "").strip()
    )


def _clean_component(value: str, *, fallback: str) -> str:
    cleaned = _ID_COMPONENT_RE.sub("-", value.strip()).strip("-._")
    return (cleaned or fallback)[:48]


def _default_run_prefix(phase: str) -> str:
    run_id = os.environ.get("GITHUB_RUN_ID", "").strip()
    if run_id:
        run_attempt = _clean_component(
            os.environ.get("GITHUB_RUN_ATTEMPT", "1"), fallback="1"
        )
        job = _clean_component(os.environ.get("GITHUB_JOB", "job"), fallback="job")
        raw = "gha-{}-{}-{}-{}-{}".format(
            _clean_component(run_id, fallback="run"),
            run_attempt,
            job,
            _clean_component(phase, fallback="dart"),
            secrets.token_hex(6),
        )
    else:
        raw = "local-{}-{}".format(
            secrets.token_hex(12), _clean_component(phase, fallback="dart")
        )
    # The server stores attempt IDs in VARCHAR(96); reserve nine characters
    # for the hyphen plus eight-digit local counter while retaining a stable
    # digest of any unusually long job/phase prefix.
    if len(raw) > 87:
        raw = f"{raw[:70]}-{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]}"
    return raw


def _code_revision() -> str:
    revision = (
        os.environ.get("GITHUB_SHA", "")
        or os.environ.get("CURATOR_CODE_REVISION", "")
    ).strip().casefold()
    if not _REVISION_RE.fullmatch(revision):
        raise DartQuotaLedgerError(
            "durable DART quota requires GITHUB_SHA or CURATOR_CODE_REVISION (7-40 lowercase hex)"
        )
    return revision


class DartQuotaClient:
    """Consume one durable quota unit immediately before each physical DART request.

    A generated attempt ID is retained across quota-API retries, so a lost ACK
    cannot double charge the quota. The caller invokes ``consume`` again for a
    physical DART retry, which generates a new ID and consumes a new unit.
    """

    limit = DART_DAILY_LIMIT

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        backend_binding_id: str | None = None,
        credential_id: str | None = None,
        code_revision: str | None = None,
        phase: str = "official-ingest",
        timeout: float = 10.0,
        max_ack_retries: int = 2,
        backoff_seconds: float = 0.25,
        transport: httpx.BaseTransport | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        now_provider: Callable[[], datetime] = lambda: datetime.now(KST),
        sleeper: Callable[[float], None] = time.sleep,
        run_prefix: str | None = None,
    ) -> None:
        self.base_url = _validated_api_base_url(
            base_url if base_url is not None else dart_quota_api_base_url()
        ).rstrip("/")
        self.token = (token if token is not None else dart_quota_api_token()).strip()
        raw_credential_id = (
            credential_id if credential_id is not None else dart_credential_id()
        )
        configured_credential_id = raw_credential_id.strip()
        if configured_credential_id and (
            raw_credential_id != configured_credential_id
            or _CREDENTIAL_ID_RE.fullmatch(configured_credential_id) is None
        ):
            raise DartQuotaLedgerError(
                "DART credential_id must be the full lowercase SHA-256 of key bytes"
            )
        self.credential_id = configured_credential_id
        binding_id = (
            backend_binding_id
            if backend_binding_id is not None
            else dart_quota_backend_binding_id()
        ).strip()
        revision = (
            code_revision if code_revision is not None else _code_revision()
        ).strip().casefold()
        if not self.base_url or not self.token or not binding_id:
            raise DartQuotaLedgerError(
                "durable DART quota requires BSIDE_API_BASE_URL, BSIDE_OPS_TOKEN, "
                "and BSIDE_BACKEND_BINDING_ID"
            )
        if not _BACKEND_BINDING_RE.fullmatch(binding_id):
            raise DartQuotaLedgerError(
                "BSIDE_BACKEND_BINDING_ID must be 64 lowercase hexadecimal characters"
            )
        if not _REVISION_RE.fullmatch(revision):
            raise DartQuotaLedgerError("DART quota code_revision must be 7-40 lowercase hex")
        if timeout <= 0 or max_ack_retries < 0 or backoff_seconds < 0:
            raise ValueError("invalid DART quota retry configuration")
        self.code_revision = revision
        self.backend_binding_id = binding_id
        self.phase = _clean_component(phase, fallback="dart")
        self.timeout = timeout
        self.max_ack_retries = max_ack_retries
        self.backoff_seconds = backoff_seconds
        self.transport = transport
        self.client_factory = client_factory
        self.now_provider = now_provider
        self.sleeper = sleeper
        self.run_prefix = run_prefix or _default_run_prefix(self.phase)
        if (
            len(self.run_prefix) > 87
            or re.fullmatch(r"[0-9A-Za-z_.:-]+", self.run_prefix) is None
        ):
            raise DartQuotaLedgerError(
                "DART quota run_prefix must be a credential-free entity ID of at most 87 characters"
            )
        self.used = 0
        self._counter = 0
        # One lock protects the exact ACK/replay pair, local counters, and
        # client shutdown. This keeps a shared client safe if callers invoke a
        # quota object from multiple worker threads and prevents close() from
        # racing an in-flight durable acknowledgment.
        self._lock = threading.RLock()
        self._close_requested = False
        self._closed = False
        self._client = self.client_factory(
            timeout=self.timeout,
            transport=self.transport,
            follow_redirects=False,
        )

    @property
    def endpoint(self) -> str:
        return f"{self.base_url}/ops/dart-quota"

    def _ensure_open(self) -> None:
        if self._close_requested:
            raise DartQuotaLedgerError(
                "DART quota client is closed; DART request was not sent"
            )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._close_requested = True
            self._client.close()
            self._closed = True

    def __enter__(self) -> DartQuotaClient:
        with self._lock:
            self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def _next_attempt_id(self) -> str:
        with self._lock:
            self._counter += 1
            counter = self._counter
        return f"{self.run_prefix}-{counter:08d}"

    def _current_quota_day(self) -> str:
        current = self.now_provider()
        if current.tzinfo is None:
            raise DartQuotaLedgerError("DART quota clock must return a timezone-aware datetime")
        return current.astimezone(KST).date().isoformat()

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json; charset=utf-8",
        }

    @staticmethod
    def _response_object(response: httpx.Response) -> dict[str, object]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise DartQuotaLedgerError(
                f"DART quota API returned invalid JSON (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict):
            raise DartQuotaLedgerError("DART quota API response must be a JSON object")
        return payload

    @staticmethod
    def _error_code(payload: dict[str, object]) -> str:
        error = payload.get("error")
        if isinstance(error, dict):
            code = error.get("code")
            return (
                code
                if isinstance(code, str)
                and code in _SAFE_ERROR_CODES
                else "unknown_error"
            )
        if isinstance(error, str) and error in _SAFE_ERROR_CODES:
            return error
        code = payload.get("code")
        return (
            code
            if isinstance(code, str)
            and code in _SAFE_ERROR_CODES
            else "unknown_error"
        )

    @classmethod
    def _safe_error_detail(cls, payload: dict[str, object]) -> str:
        error = payload.get("error")
        if not isinstance(error, dict):
            return ""
        detail = error.get("detail")
        if not isinstance(detail, str):
            return ""
        error_code = cls._error_code(payload)
        allowed = (
            _SAFE_INVALID_REQUEST_DETAILS
            if error_code == "invalid_request"
            else (
                _SAFE_PERSISTENCE_DETAILS
                if error_code == "dart_quota_persistence_failed"
                else frozenset()
            )
        )
        if detail in allowed:
            return detail
        return ""

    @classmethod
    def _safe_error_context(cls, payload: dict[str, object]) -> str:
        error_code = cls._error_code(payload)
        detail = cls._safe_error_detail(payload)
        return f"{error_code}; detail={detail}" if detail else error_code

    def _post_with_idempotent_retry(self, body: dict[str, object]) -> dict[str, object]:
        encoded = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
        last_error: Exception | None = None
        for retry in range(self.max_ack_retries + 1):
            try:
                # Short-lived clients previously isolated every ACK, including
                # the independent replay. Reuse sockets without allowing a
                # server cookie to change the headers of the next exact POST.
                self._client.cookies.clear()
                response = self._client.post(
                    self.endpoint,
                    content=encoded,
                    headers=self._headers(),
                )
            except httpx.TransportError as exc:
                last_error = exc
                if retry < self.max_ack_retries:
                    self.sleeper(self.backoff_seconds * (2**retry))
                    continue
                break
            if 500 <= response.status_code <= 599 and retry < self.max_ack_retries:
                self.sleeper(self.backoff_seconds * (2**retry))
                continue
            payload = self._response_object(response)
            if response.status_code != 200:
                error_code = self._error_code(payload)
                error_context = self._safe_error_context(payload)
                if 500 <= response.status_code <= 599:
                    raise DartQuotaLedgerError(
                        "DART quota API did not acknowledge "
                        f"{body['action']} (HTTP {response.status_code}: {error_context}); "
                        "DART request was not sent"
                    )
                if error_code in {
                    "dart_credential_blocked",
                    "dart_credential_disabled",
                }:
                    credential_id = body.get("credential_id")
                    if (
                        not isinstance(credential_id, str)
                        or _CREDENTIAL_ID_RE.fullmatch(credential_id) is None
                    ):
                        raise DartQuotaLedgerError(
                            "DART quota credential rejection omitted its identifier"
                        )
                    raise DartCredentialUnavailableError(
                        reason=(
                            "blocked_020"
                            if error_code == "dart_credential_blocked"
                            else "disabled_901"
                        ),
                        credential_id=credential_id,
                    )
                if (
                    response.status_code == 429
                    and error_code == "dart_quota_exhausted"
                ):
                    raise DartGlobalQuotaExceededError(
                        "DART global KST-day quota is exhausted"
                    )
                error_type = (
                    DartQuotaLedgerRejectedError
                    if response.status_code in {409, 429}
                    else DartQuotaLedgerError
                )
                raise error_type(
                    f"DART quota API rejected {body['action']} "
                    f"(HTTP {response.status_code}): {error_context}"
                )
            return payload
        raise DartQuotaLedgerError(
            f"DART quota API did not acknowledge {body['action']}; DART request was not sent"
        ) from last_error

    @staticmethod
    def _validate_ack(
        payload: dict[str, object],
        *,
        action: str,
        attempt_id: str,
        quota_day: str,
        credential_id: str,
        backend_binding_id: str,
        require_blocked_until: bool,
        require_duplicate: bool | None = None,
    ) -> DartQuotaPermit:
        if payload.get("ok") is not True or payload.get("accepted") != 1:
            raise DartQuotaLedgerError("DART quota API omitted the exact accepted=1 acknowledgment")
        if payload.get("action") != action:
            raise DartQuotaLedgerError("DART quota API acknowledged a different action")
        if payload.get("attempt_id") != attempt_id:
            raise DartQuotaLedgerError("DART quota API acknowledged a different attempt_id")
        if payload.get("quota_day") != quota_day:
            raise DartQuotaLedgerError("DART quota API acknowledged a different quota_day")
        if payload.get("credential_id") != credential_id:
            raise DartQuotaLedgerError(
                "DART quota API acknowledged a different credential_id"
            )
        acknowledged_binding_id = payload.get("backend_binding_id")
        if (
            not isinstance(acknowledged_binding_id, str)
            or _BACKEND_BINDING_RE.fullmatch(acknowledged_binding_id) is None
            or not hmac.compare_digest(acknowledged_binding_id, backend_binding_id)
        ):
            raise DartQuotaLedgerError(
                "DART quota API backend binding acknowledgment does not match"
            )
        limit = payload.get("limit_count")
        used = payload.get("used_count")
        remaining = payload.get("remaining_count")
        credential_limit = payload.get("credential_limit_count")
        credential_used = payload.get("credential_used_count")
        credential_remaining = payload.get("credential_remaining_count")
        duplicate = payload.get("duplicate")
        if (
            type(limit) is not int
            or limit != DART_DAILY_LIMIT
            or type(used) is not int
            or type(remaining) is not int
            or type(duplicate) is not bool
            or not 0 <= used <= limit
            or remaining != limit - used
            or type(credential_limit) is not int
            or credential_limit != DART_DAILY_LIMIT
            or type(credential_used) is not int
            or type(credential_remaining) is not int
            or not 0 <= credential_used <= credential_limit
            or credential_remaining != credential_limit - credential_used
        ):
            raise DartQuotaLedgerError("DART quota API returned inconsistent quota counters")
        if require_duplicate is not None and duplicate is not require_duplicate:
            raise DartQuotaLedgerError(
                "DART quota API explicit replay was not acknowledged as duplicate"
            )
        credential_status = payload.get("credential_status")
        credential_blocked_until = payload.get("credential_blocked_until")
        blocked_until = payload.get("blocked_until")
        if action == "consume":
            if credential_status != "active":
                raise DartQuotaLedgerError(
                    "DART quota consume ACK reports an unavailable credential"
                )
            if blocked_until is not None or credential_blocked_until is not None:
                raise DartQuotaLedgerError(
                    "DART quota consume ACK unexpectedly reports a block"
                )
        elif action == "block_020":
            if credential_status not in {"active", "disabled_901"}:
                raise DartQuotaLedgerError(
                    "DART quota block ACK reports an invalid credential status"
                )
            if (
                not isinstance(blocked_until, str)
                or not isinstance(credential_blocked_until, str)
                or blocked_until != credential_blocked_until
            ):
                raise DartQuotaLedgerError(
                    "DART quota block ACK omitted matching block timestamps"
                )
            try:
                parsed_blocked_until = datetime.fromisoformat(
                    blocked_until.replace("Z", "+00:00")
                )
            except ValueError as exc:
                raise DartQuotaLedgerError(
                    "DART quota block ACK returned an invalid block timestamp"
                ) from exc
            if (
                not blocked_until.strip()
                or parsed_blocked_until.tzinfo is None
                or parsed_blocked_until.utcoffset() is None
            ):
                raise DartQuotaLedgerError(
                    "DART quota block ACK returned an invalid block timestamp"
                )
        elif action == "disable_901":
            if credential_status != "disabled_901":
                raise DartQuotaLedgerError(
                    "DART quota disable ACK did not confirm disabled_901"
                )
        elif require_blocked_until:
            raise DartQuotaLedgerError("unsupported DART quota block acknowledgment")
        return DartQuotaPermit(
            attempt_id=attempt_id,
            quota_day=quota_day,
            credential_id=credential_id,
            used_count=used,
            remaining_count=remaining,
            credential_used_count=credential_used,
            credential_remaining_count=credential_remaining,
            duplicate=duplicate,
        )

    def _post_with_durable_replay(
        self,
        body: dict[str, object],
        *,
        action: str,
        attempt_id: str,
        quota_day: str,
        credential_id: str,
        require_blocked_until: bool,
    ) -> DartQuotaPermit:
        """Require a separate exact replay after the first valid HTTP 200 ACK.

        Transport retries within the first POST may already return
        ``duplicate=true`` after a lost response, so the first ACK may be
        either new or duplicate. The independent second POST must always be a
        duplicate. That proves the attempt and its mutation survived beyond
        the first response before the caller sends or retries a physical DART
        request.
        """

        first_payload = self._post_with_idempotent_retry(body)
        first_permit = self._validate_ack(
            first_payload,
            action=action,
            attempt_id=attempt_id,
            quota_day=quota_day,
            credential_id=credential_id,
            backend_binding_id=self.backend_binding_id,
            require_blocked_until=require_blocked_until,
        )
        replay_payload = self._post_with_idempotent_retry(body)
        replay_permit = self._validate_ack(
            replay_payload,
            action=action,
            attempt_id=attempt_id,
            quota_day=quota_day,
            credential_id=credential_id,
            backend_binding_id=self.backend_binding_id,
            require_blocked_until=require_blocked_until,
            require_duplicate=True,
        )
        # Other processes may consume from the same global or credential pool
        # between these calls, but a durable ledger must never move backwards.
        if (
            replay_permit.used_count < first_permit.used_count
            or replay_permit.credential_used_count
            < first_permit.credential_used_count
        ):
            raise DartQuotaLedgerError(
                "DART quota API counters regressed during explicit replay"
            )
        return first_permit

    def consume(
        self,
        *,
        operation: str = "list",
        credential_id: str | None = None,
    ) -> DartQuotaPermit:
        with self._lock:
            self._ensure_open()
            if operation not in {"list", "corp_code"}:
                raise ValueError("unsupported DART quota operation")
            raw_credential_id = (
                credential_id if credential_id is not None else self.credential_id
            )
            selected_credential_id = raw_credential_id.strip()
            if (
                raw_credential_id != selected_credential_id
                or _CREDENTIAL_ID_RE.fullmatch(selected_credential_id) is None
            ):
                raise DartQuotaLedgerError(
                    "DART physical attempt requires a full lowercase SHA-256 credential_id"
                )
            attempt_id = self._next_attempt_id()
            quota_day = self._current_quota_day()
            body: dict[str, object] = {
                "action": "consume",
                "attempt_id": attempt_id,
                "quota_day": quota_day,
                "credential_id": selected_credential_id,
                "operation": operation,
                "code_revision": self.code_revision,
                "expected_backend_binding_id": self.backend_binding_id,
            }
            permit = self._post_with_durable_replay(
                body,
                action="consume",
                attempt_id=attempt_id,
                quota_day=quota_day,
                credential_id=selected_credential_id,
                require_blocked_until=False,
            )
            # The explicit replay proves the same durable attempt and must not
            # consume another local physical-request unit.
            self.used += 1
            return permit

    def block_020(self, permit: object) -> None:
        with self._lock:
            self._ensure_open()
            if not isinstance(permit, DartQuotaPermit):
                raise DartQuotaLedgerError(
                    "DART status 020 cannot be blocked without its quota permit"
                )
            body: dict[str, object] = {
                "action": "block_020",
                "attempt_id": permit.attempt_id,
                "quota_day": permit.quota_day,
                "credential_id": permit.credential_id,
                "reason": "opendart_status_020",
                "code_revision": self.code_revision,
                "expected_backend_binding_id": self.backend_binding_id,
            }
            self._post_with_durable_replay(
                body,
                action="block_020",
                attempt_id=permit.attempt_id,
                quota_day=permit.quota_day,
                credential_id=permit.credential_id,
                require_blocked_until=True,
            )

    def disable_901(self, permit: object) -> None:
        with self._lock:
            self._ensure_open()
            if not isinstance(permit, DartQuotaPermit):
                raise DartQuotaLedgerError(
                    "DART status 901 cannot disable a credential without its quota permit"
                )
            body: dict[str, object] = {
                "action": "disable_901",
                "attempt_id": permit.attempt_id,
                "quota_day": permit.quota_day,
                "credential_id": permit.credential_id,
                "reason": "opendart_status_901",
                "code_revision": self.code_revision,
                "expected_backend_binding_id": self.backend_binding_id,
            }
            self._post_with_durable_replay(
                body,
                action="disable_901",
                attempt_id=permit.attempt_id,
                quota_day=permit.quota_day,
                credential_id=permit.credential_id,
                require_blocked_until=False,
            )


def durable_dart_quota_client(*, phase: str) -> DartQuotaClient:
    return DartQuotaClient(phase=phase)
