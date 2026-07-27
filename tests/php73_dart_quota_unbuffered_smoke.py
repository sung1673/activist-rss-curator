#!/usr/bin/env python3
"""Exercise durable DART quota ACKs with unbuffered native PDO MySQL queries."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any


ADMIN_TOKEN = "php73-ci-admin-token-00000000000000000000"
KST = timezone(timedelta(hours=9))
TABLES = (
    "ci_dart_quota_attempts",
    "ci_dart_quota_credential_days",
    "ci_dart_quota_credentials",
    "ci_dart_quota_days",
)


class SmokeFailure(RuntimeError):
    """Raised when the unbuffered durable-ACK contract is not satisfied."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


def mysql_execute(container_id: str, sql: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "exec",
            container_id,
            "mysql",
            "--user=root",
            "--password=activist_ci_root_password",
            "--batch",
            "--skip-column-names",
            "activist_ci",
            f"--execute={sql}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def request_json(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    expected_status: int = 200,
) -> dict[str, Any]:
    body = None
    headers = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/{path.lstrip('/')}",
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            status = response.status
            raw = response.read()
    except urllib.error.HTTPError as error:
        status = error.code
        raw = error.read()
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SmokeFailure(
            f"{path} returned non-JSON HTTP {status}: {raw[:300]!r}"
        ) from error
    require(
        status == expected_status,
        f"{path}: expected HTTP {expected_status}, got {status}: {decoded!r}",
    )
    require(isinstance(decoded, dict), f"{path}: response must be an object")
    return decoded


def backend_binding_id(container_id: str) -> str:
    server_uuid, database_name = mysql_execute(
        container_id,
        "SELECT LOWER(@@server_uuid),DATABASE()",
    ).split("\t")
    return hashlib.sha256(
        f"mysql8\n{server_uuid}\n{database_name}\nci_".encode()
    ).hexdigest()


def assert_innodb_tables(container_id: str) -> None:
    names = ",".join(f"'{name}'" for name in TABLES)
    rows = mysql_execute(
        container_id,
        "SELECT TABLE_NAME,ENGINE FROM information_schema.TABLES "
        f"WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ({names}) "
        "ORDER BY TABLE_NAME",
    ).splitlines()
    observed = {
        name: engine
        for name, engine in (row.split("\t", 1) for row in rows if row)
    }
    require(set(observed) == set(TABLES), f"quota tables missing: {observed!r}")
    require(
        all(engine.casefold() == "innodb" for engine in observed.values()),
        f"quota tables must all use InnoDB: {observed!r}",
    )


def durable_snapshot(
    container_id: str,
    *,
    quota_day: str,
    credential_id: str,
    attempt_id: str,
) -> tuple[str, ...]:
    quoted_day = quota_day.replace("'", "''")
    quoted_credential = credential_id.replace("'", "''")
    quoted_attempt = attempt_id.replace("'", "''")
    sql = (
        "SELECT "
        "(SELECT COUNT(*) FROM ci_dart_quota_days),"
        "(SELECT COUNT(*) FROM ci_dart_quota_credentials),"
        "(SELECT COUNT(*) FROM ci_dart_quota_credential_days),"
        "(SELECT COUNT(*) FROM ci_dart_quota_attempts),"
        "(SELECT CONCAT(limit_count,':',used_count,':',blocked) "
        "FROM ci_dart_quota_days "
        f"WHERE quota_day='{quoted_day}'),"
        "(SELECT status FROM ci_dart_quota_credentials "
        f"WHERE credential_id='{quoted_credential}'),"
        "(SELECT CONCAT(limit_count,':',used_count,':',blocked) "
        "FROM ci_dart_quota_credential_days "
        f"WHERE quota_day='{quoted_day}' "
        f"AND credential_id='{quoted_credential}'),"
        "(SELECT CONCAT(status,':',consumed_units,':',operation,':',"
        "CHAR_LENGTH(consume_request_sha256),':',CHAR_LENGTH(code_revision)) "
        "FROM ci_dart_quota_attempts "
        f"WHERE attempt_id='{quoted_attempt}')"
    )
    result = tuple(mysql_execute(container_id, sql).split("\t"))
    require(
        result
        == (
            "1",
            "1",
            "1",
            "1",
            "40000:1:0",
            "active",
            "40000:1:0",
            "consumed:1:list:64:40",
        ),
        f"unexpected durable quota state: {result!r}",
    )
    return result


def run(base_url: str, container_id: str) -> None:
    require(
        not os.environ.get("OPENDART_API_KEY")
        and not os.environ.get("OPENDART_API_KEYS"),
        "unbuffered ledger fixture must not receive OpenDART credentials",
    )
    assert_innodb_tables(container_id)
    empty_counts = mysql_execute(
        container_id,
        "SELECT "
        "(SELECT COUNT(*) FROM ci_dart_quota_days),"
        "(SELECT COUNT(*) FROM ci_dart_quota_credentials),"
        "(SELECT COUNT(*) FROM ci_dart_quota_credential_days),"
        "(SELECT COUNT(*) FROM ci_dart_quota_attempts)",
    )
    require(empty_counts == "0\t0\t0\t0", f"quota fixture not empty: {empty_counts}")

    quota_day = datetime.now(KST).date().isoformat()
    binding_id = backend_binding_id(container_id)
    status = request_json(
        base_url,
        f"api.php/api/v1/ops/dart-quota?quota_day={quota_day}",
    )
    require(
        status.get("used_count") == 0
        and status.get("remaining_count") == 40000
        and status.get("backend_binding_id") == binding_id,
        repr(status),
    )

    credential_id = "7" * 64
    attempt_id = "dart-unbuffered-native-smoke-0001"
    payload = {
        "action": "consume",
        "attempt_id": attempt_id,
        "quota_day": quota_day,
        "credential_id": credential_id,
        "operation": "list",
        "code_revision": "7" * 40,
        "expected_backend_binding_id": binding_id,
    }
    consumed = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        payload=payload,
    )
    require(
        consumed.get("action") == "consume"
        and consumed.get("attempt_id") == attempt_id
        and consumed.get("credential_id") == credential_id
        and consumed.get("accepted") == 1
        and consumed.get("duplicate") is False
        and consumed.get("used_count") == 1
        and consumed.get("credential_used_count") == 1
        and consumed.get("backend_binding_id") == binding_id,
        repr(consumed),
    )
    first_snapshot = durable_snapshot(
        container_id,
        quota_day=quota_day,
        credential_id=credential_id,
        attempt_id=attempt_id,
    )

    replay = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        payload=payload,
    )
    require(
        replay.get("action") == "consume"
        and replay.get("attempt_id") == attempt_id
        and replay.get("credential_id") == credential_id
        and replay.get("accepted") == 1
        and replay.get("duplicate") is True
        and replay.get("used_count") == 1
        and replay.get("credential_used_count") == 1
        and replay.get("backend_binding_id") == binding_id,
        repr(replay),
    )
    replay_snapshot = durable_snapshot(
        container_id,
        quota_day=quota_day,
        credential_id=credential_id,
        attempt_id=attempt_id,
    )
    require(
        replay_snapshot == first_snapshot,
        "exact replay changed durable quota state",
    )
    print(
        "PHP 7.3 unbuffered native-PDO DART quota smoke passed "
        "(durable consume and exact duplicate replay).",
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8788")
    parser.add_argument("--mysql-container-id", required=True)
    args = parser.parse_args()
    run(args.base_url, args.mysql_container_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
