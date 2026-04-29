from __future__ import annotations

import os
import re
from typing import Any

import httpx


def ai_config(config: dict[str, object]) -> dict[str, Any]:
    value = config.get("ai", {})
    return value if isinstance(value, dict) else {}


def github_models_token() -> str:
    return (
        os.environ.get("GITHUB_MODELS_TOKEN")
        or os.environ.get("GITHUB_TOKEN")
        or os.environ.get("GH_TOKEN")
        or ""
    ).strip()


def call_github_models(
    system_prompt: str,
    user_prompt: str,
    *,
    model: str,
    max_tokens: int,
    config: dict[str, object],
    client: httpx.Client | None = None,
) -> str | None:
    settings = ai_config(config)
    if not settings.get("enabled", True):
        return None
    token = github_models_token()
    if not token:
        return None

    endpoint = str(settings.get("endpoint") or "https://models.github.ai/inference/chat/completions")
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.1,
        "max_tokens": max_tokens,
    }
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    timeout = float(settings.get("timeout_seconds", 25))

    try:
        if client is None:
            with httpx.Client(timeout=timeout) as local_client:
                response = local_client.post(endpoint, headers=headers, json=payload)
        else:
            response = client.post(endpoint, headers=headers, json=payload)
        if response.status_code >= 400:
            return None
        data = response.json()
    except (httpx.HTTPError, ValueError, TypeError):
        return None

    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None
    if not isinstance(content, str):
        return None
    return re.sub(r"\n{3,}", "\n\n", content).strip()
