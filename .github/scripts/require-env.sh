#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -eq 0 ]]; then
  echo "Usage: require-env.sh NAME [NAME ...]" >&2
  exit 2
fi

missing=()
for name in "$@"; do
  if [[ -z "${!name:-}" ]]; then
    missing+=("$name")
  fi
done

if (( ${#missing[@]} > 0 )); then
  printf '::error::Missing required operational configuration: %s\n' "$(IFS=', '; echo "${missing[*]}")" >&2
  exit 1
fi

echo "Required operational configuration is present."
