from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Mapping
from datetime import date
from typing import NoReturn, Protocol, SupportsIndex


_DART_KEY_RE = re.compile(r"^[0-9a-f]{40}$")
_CREDENTIAL_ID_RE = re.compile(r"^[0-9a-f]{64}$")


class OpenDartCredentialConfigurationError(ValueError):
    """A credential configuration cannot be used without risking ambiguity."""


class OpenDartCredential:
    """An in-memory OpenDART credential with a non-secret stable identifier.

    ``__slots__`` prevents generic ``__dict__`` serializers from copying the
    secret, while the representation deliberately contains only the SHA-256
    identifier.  Callers must use ``credential_id`` in logs and persistence.
    """

    __slots__ = ("__key", "credential_id")

    def __init__(self, key: str, *, validate: bool = True) -> None:
        if validate and _DART_KEY_RE.fullmatch(key) is None:
            raise OpenDartCredentialConfigurationError(
                "OpenDART credentials must each be exactly 40 lowercase hexadecimal characters"
            )
        if not key:
            raise OpenDartCredentialConfigurationError("OpenDART credential is empty")
        self.__key = key
        self.credential_id = hashlib.sha256(key.encode("ascii")).hexdigest()

    @property
    def key(self) -> str:
        """Return the provider credential for request construction only."""

        return self.__key

    def __repr__(self) -> str:
        return f"OpenDartCredential(credential_id={self.credential_id!r})"

    def __str__(self) -> str:
        return self.credential_id

    def __reduce_ex__(self, protocol: SupportsIndex) -> NoReturn:
        del protocol
        raise TypeError("OpenDART credentials cannot be serialized")

    def __getstate__(self) -> object:
        raise TypeError("OpenDART credentials cannot be serialized")


def _split_pool(raw: str) -> tuple[str, ...]:
    # Environment and GitHub secrets may use LF, CRLF, or a comma-separated
    # single line. Empty separator-only fragments are ignored, but any
    # non-empty malformed value fails the complete pool.
    return tuple(part.strip() for part in re.split(r"[\r\n,]+", raw) if part.strip())


def load_opendart_credentials(
    environment: Mapping[str, str] | None = None,
) -> tuple[OpenDartCredential, ...]:
    """Load a strict OpenDART key pool, with an unambiguous legacy fallback.

    ``OPENDART_API_KEYS`` takes the pool form. ``DART_API_KEY`` is supported
    only when the pool is absent. Supplying both is rejected even when they
    happen to contain the same key, preventing rollout mistakes from silently
    choosing one configuration.
    """

    source = os.environ if environment is None else environment
    raw_pool = str(source.get("OPENDART_API_KEYS", ""))
    raw_legacy = str(source.get("DART_API_KEY", ""))
    pool_configured = bool(raw_pool.strip())
    legacy_configured = bool(raw_legacy.strip())
    if pool_configured and legacy_configured:
        raise OpenDartCredentialConfigurationError(
            "OPENDART_API_KEYS and DART_API_KEY cannot both be configured"
        )

    values = (
        _split_pool(raw_pool)
        if pool_configured
        else ((raw_legacy.strip(),) if legacy_configured else ())
    )
    if pool_configured and not values:
        raise OpenDartCredentialConfigurationError(
            "OpenDART credential pool does not contain a credential"
        )
    if not values:
        return ()
    if any(_DART_KEY_RE.fullmatch(value) is None for value in values):
        raise OpenDartCredentialConfigurationError(
            "OpenDART credentials must each be exactly 40 lowercase hexadecimal characters"
        )
    if len({value.casefold() for value in values}) != len(values):
        raise OpenDartCredentialConfigurationError(
            "OpenDART credential pool contains a duplicate credential"
        )
    credentials = tuple(OpenDartCredential(value) for value in values)
    if len({credential.credential_id for credential in credentials}) != len(credentials):
        # SHA-256 collision is operationally indistinguishable from duplicate
        # identity and must not be resolved by falling back to secret values.
        raise OpenDartCredentialConfigurationError(
            "OpenDART credential pool contains a duplicate credential identity"
        )
    return credentials


class DartCredentialAvailability(Protocol):
    """Optional durable availability view for pre-existing credential state."""

    def is_available(self, *, credential_id: str, quota_day: date) -> bool: ...


class OpenDartCredentialPool:
    """Round-robin selector with process-local fail-closed state overlays."""

    def __init__(
        self,
        credentials: tuple[OpenDartCredential, ...],
        *,
        availability: DartCredentialAvailability | None = None,
    ) -> None:
        if not credentials:
            raise OpenDartCredentialConfigurationError(
                "at least one OpenDART credential is required"
            )
        ids = tuple(credential.credential_id for credential in credentials)
        if any(_CREDENTIAL_ID_RE.fullmatch(value) is None for value in ids):
            raise OpenDartCredentialConfigurationError(
                "OpenDART credential identity is invalid"
            )
        if len(set(ids)) != len(ids):
            raise OpenDartCredentialConfigurationError(
                "OpenDART credential pool contains a duplicate credential identity"
            )
        self._credentials = credentials
        self._availability = availability
        self._cursor = 0
        self._blocked_days: set[tuple[str, date]] = set()
        self._disabled: set[str] = set()

    @property
    def size(self) -> int:
        return len(self._credentials)

    @property
    def credential_ids(self) -> tuple[str, ...]:
        return tuple(credential.credential_id for credential in self._credentials)

    def next(self, quota_day: date) -> OpenDartCredential | None:
        for offset in range(len(self._credentials)):
            index = (self._cursor + offset) % len(self._credentials)
            credential = self._credentials[index]
            credential_id = credential.credential_id
            if credential_id in self._disabled:
                continue
            if (credential_id, quota_day) in self._blocked_days:
                continue
            if self._availability is not None and not self._availability.is_available(
                credential_id=credential_id,
                quota_day=quota_day,
            ):
                continue
            self._cursor = (index + 1) % len(self._credentials)
            return credential
        return None

    def block_for_day(self, credential_id: str, quota_day: date) -> None:
        self._require_member(credential_id)
        self._blocked_days.add((credential_id, quota_day))

    def disable(self, credential_id: str) -> None:
        self._require_member(credential_id)
        self._disabled.add(credential_id)

    def unavailable_reason(self, quota_day: date) -> str:
        usable_after_day = any(
            credential.credential_id not in self._disabled
            for credential in self._credentials
        )
        blocked_today = any(
            (credential.credential_id, quota_day) in self._blocked_days
            for credential in self._credentials
        )
        return "blocked_020" if usable_after_day and blocked_today else "disabled_901"

    def _require_member(self, credential_id: str) -> None:
        if credential_id not in self.credential_ids:
            raise ValueError("unknown OpenDART credential identity")
