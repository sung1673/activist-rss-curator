#!/usr/bin/env bash
set -euo pipefail

: "${BSIDE_ADMIN_TOKEN:?BSIDE_ADMIN_TOKEN is required}"
: "${GOVERNANCE_API_BASE_URL:?GOVERNANCE_API_BASE_URL is required}"
: "${ROLLBACK_REASON:?ROLLBACK_REASON is required}"
: "${ROLLBACK_REQUEST_PREFIX:?ROLLBACK_REQUEST_PREFIX is required}"

v1_api="${GOVERNANCE_API_BASE_URL%/}"
[[ "$v1_api" == "https://alignpe.gabia.io/activist/api.php/api/v1" ]] || {
  echo "::error::GOVERNANCE_API_BASE_URL must match the pinned production v1 endpoint."
  exit 1
}
v2_api="${v1_api%/api/v1}/api/v2"
readonly -a read_curl_args=(
  --connect-timeout 5
  --max-time 15
  --retry 2
  --retry-all-errors
  --retry-max-time 30
)

close_state() {
  local lane="$1"
  local endpoint="$2"
  local state_filter="$3"
  local version_filter="$4"
  local response_filter="$5"
  local attempt before response state version payload http_status curl_status

  for attempt in 1 2 3 4 5; do
    before="${RUNNER_TEMP:-.}/rollback-${lane}-before-${attempt}.json"
    response="${RUNNER_TEMP:-.}/rollback-${lane}-closed-${attempt}.json"
    curl "${read_curl_args[@]}" --fail-with-body --silent --show-error \
      -H "Authorization: Bearer $BSIDE_ADMIN_TOKEN" \
      "$endpoint" > "$before"
    state="$(jq -er "$state_filter" "$before")"
    version="$(jq -er "$version_filter | select(type == \"number\")" "$before")"
    if [[ "$state" == "closed" ]]; then
      return 0
    fi

    payload="$(jq -nc \
      --argjson version "$version" \
      --arg reason "$ROLLBACK_REASON" \
      '{release_state:"closed",expected_version:$version,reason:$reason}')"
    set +e
    http_status="$(
      curl --silent --show-error \
        --connect-timeout 5 \
        --max-time 20 \
        --output "$response" \
        --write-out '%{http_code}' \
        -X POST \
        -H "Authorization: Bearer $BSIDE_ADMIN_TOKEN" \
        -H "Content-Type: application/json" \
        -H "X-Request-ID: ${ROLLBACK_REQUEST_PREFIX}-${lane}-${attempt}" \
        --data "$payload" \
        "$endpoint"
    )"
    curl_status=$?
    set -e
    if [[ "$curl_status" -ne 0 ]]; then
      # The server may have committed after the client lost the response.
      # Never repeat the POST blindly; the next optimistic loop starts with GET.
      continue
    fi
    if [[ "$http_status" =~ ^2[0-9][0-9]$ ]]; then
      jq -e "$response_filter" "$response" > /dev/null
      return 0
    fi
    if [[ "$http_status" == "408" ||
          "$http_status" == "409" ||
          "$http_status" == "425" ||
          "$http_status" == "429" ||
          "$http_status" =~ ^5[0-9][0-9]$ ]]; then
      # These responses can be transient or ambiguous. Re-enter through GET so
      # a committed close is observed before any new optimistic POST is built.
      continue
    fi
    echo "::error::Rollback close for $lane failed with HTTP $http_status."
    return 1
  done

  echo "::error::Rollback close for $lane lost five optimistic-lock races."
  return 1
}

close_state \
  "v2" \
  "$v2_api/admin/release-state" \
  ".data.release_state" \
  ".data.state_version" \
  '.data.release_state == "closed"'
close_state \
  "v1" \
  "$v1_api/admin/release-state" \
  ".release_state" \
  ".state_version" \
  '.release_state == "closed"'

curl "${read_curl_args[@]}" --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $BSIDE_ADMIN_TOKEN" \
  "$v2_api/admin/release-state" | jq -e '.data.release_state == "closed"' > /dev/null
curl "${read_curl_args[@]}" --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $BSIDE_ADMIN_TOKEN" \
  "$v1_api/admin/release-state" | jq -e '.release_state == "closed"' > /dev/null
