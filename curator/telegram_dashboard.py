from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html import escape
from pathlib import Path

from .dates import datetime_to_iso, parse_datetime
from .telegram_sources import ensure_telegram_state, is_collectable_public_channel, message_key, telegram_issue_signals


TELEGRAM_DASHBOARD_RELATIVE_PATH = Path("public") / "feed" / "telegram-admin.html"
TOKEN_STOPWORDS = {
    "그리고",
    "관련",
    "기사",
    "뉴스",
    "시장",
    "오늘",
    "이번",
    "지난",
    "있는",
    "없는",
    "으로",
    "에서",
    "한다",
    "했다",
    "합니다",
    "보도",
    "공유",
}


def _dt(value: object, timezone_name: str) -> datetime | None:
    return parse_datetime(value, timezone_name)


def _date_key(value: object, timezone_name: str) -> str:
    parsed = _dt(value, timezone_name)
    return parsed.strftime("%Y-%m-%d") if parsed else "날짜 미상"


def _compact(value: object, max_chars: int = 90) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)].rstrip() + "…"


def _tokens(text: str) -> list[str]:
    tokens = [token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", text)]
    return [token for token in tokens if token not in TOKEN_STOPWORDS and len(token) >= 2]


def _message_type(message: dict[str, object]) -> str:
    text = str(message.get("normalized_text") or message.get("text") or "").casefold()
    if any(keyword in text for keyword in ("공시", "불성실공시", "거래정지", "상장폐지", "정정신고서")):
        return "공시·규제"
    if any(keyword in text for keyword in ("실적", "매출", "영업이익", "컨센서스", "가이던스")):
        return "실적"
    if any(keyword in text for keyword in ("주주", "행동주의", "경영권", "위임장", "공개매수", "이사회")):
        return "주주·지배구조"
    if any(keyword in text for keyword in ("밸류업", "벨류업", "배당", "자사주", "주주환원")):
        return "밸류업·환원"
    if any(keyword in text for keyword in ("환율", "채권", "금리", "fed", "미국", "중국", "일본")):
        return "매크로·해외"
    return "기타"


def telegram_dashboard_model(state: dict[str, object], config: dict[str, object], now: datetime) -> dict[str, object]:
    ensure_telegram_state(state)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    channels = [channel for channel in state.get("telegram_source_channels", []) if isinstance(channel, dict)]
    collectable_channels = [channel for channel in channels if is_collectable_public_channel(channel)]
    enabled_channels = [channel for channel in collectable_channels if bool(channel.get("enabled", True))]
    messages = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict) and not message.get("deleted_at")
    ]
    matches = [match for match in state.get("telegram_article_matches", []) if isinstance(match, dict)]
    candidates = [candidate for candidate in state.get("telegram_channel_candidates", []) if isinstance(candidate, dict)]
    since_24h = now - timedelta(hours=24)
    since_14d = now - timedelta(days=14)
    recent_24h = [message for message in messages if (_dt(message.get("posted_at"), timezone_name) or now) >= since_24h]
    recent_14d = [message for message in messages if (_dt(message.get("posted_at"), timezone_name) or now) >= since_14d]

    messages_by_channel: dict[str, list[dict[str, object]]] = defaultdict(list)
    type_counter: Counter[str] = Counter()
    day_counter: Counter[str] = Counter()
    keyword_counter: Counter[str] = Counter()
    for message in messages:
        handle = str(message.get("handle") or message.get("channel_title") or "unknown")
        messages_by_channel[handle].append(message)
        type_counter[_message_type(message)] += 1
        day_counter[_date_key(message.get("posted_at"), timezone_name)] += 1
    for message in recent_14d:
        keyword_counter.update(_tokens(str(message.get("normalized_text") or message.get("text") or ""))[:18])

    channel_rows: list[dict[str, object]] = []
    for channel in enabled_channels:
        handle = str(channel.get("handle") or "")
        channel_messages = messages_by_channel.get(handle, [])
        latest_at = max((str(message.get("posted_at") or "") for message in channel_messages), default="")
        channel_rows.append(
            {
                "handle": handle,
                "title": channel.get("title") or handle,
                "quality_score": int(channel.get("quality_score") or 0),
                "messages": len(channel_messages),
                "latest_at": latest_at,
                "last_error": channel.get("last_error") or "",
            }
        )
    channel_rows.sort(key=lambda row: (int(row.get("messages") or 0), str(row.get("latest_at") or "")), reverse=True)

    sample = recent_14d[-500:] if len(recent_14d) > 500 else recent_14d
    avg_bytes = 0
    if sample:
        avg_bytes = max(1, round(len(json.dumps(sample, ensure_ascii=False, sort_keys=True).encode("utf-8")) / len(sample)))
    daily_messages = len(recent_14d) / 14 if recent_14d else 0

    signals = telegram_issue_signals(state, limit=12)
    return {
        "generated_at": datetime_to_iso(now),
        "channels_total": len(channels),
        "channels_collectable": len(collectable_channels),
        "channels_enabled": len(enabled_channels),
        "channels_failed": len([channel for channel in enabled_channels if channel.get("last_error")]),
        "messages_total": len(messages),
        "messages_24h": len(recent_24h),
        "messages_14d": len(recent_14d),
        "matches_total": len(matches),
        "candidates_total": len(candidates),
        "candidate_pending": len([candidate for candidate in candidates if candidate.get("status") == "pending"]),
        "top_channels": channel_rows[:24],
        "type_counts": type_counter.most_common(),
        "day_counts": sorted(day_counter.items())[-21:],
        "top_keywords": keyword_counter.most_common(30),
        "signals": signals,
        "growth": {
            "avg_message_bytes": avg_bytes,
            "daily_messages": round(daily_messages, 1),
            "monthly_messages": round(daily_messages * 30),
            "yearly_messages": round(daily_messages * 365),
            "monthly_mb": round(daily_messages * 30 * avg_bytes / 1024 / 1024, 2) if avg_bytes else 0,
            "yearly_mb": round(daily_messages * 365 * avg_bytes / 1024 / 1024, 2) if avg_bytes else 0,
        },
    }


def _stat_card(label: str, value: object, note: str = "") -> str:
    note_html = f"<span>{escape(str(note))}</span>" if note else ""
    return f'<article class="stat"><strong>{escape(str(value))}</strong><p>{escape(label)}</p>{note_html}</article>'


def write_telegram_dashboard(project_root: Path, state: dict[str, object], config: dict[str, object], now: datetime) -> Path:
    model = telegram_dashboard_model(state, config, now)
    output_path = project_root / TELEGRAM_DASHBOARD_RELATIVE_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stats = "\n".join(
        [
            _stat_card("수집 가능 공개 채널", model["channels_collectable"], f"enabled {model['channels_enabled']}"),
            _stat_card("최근 24시간 메시지", model["messages_24h"]),
            _stat_card("최근 14일 메시지", model["messages_14d"]),
            _stat_card("기사 매칭", model["matches_total"]),
            _stat_card("추천 후보", model["candidates_total"], f"pending {model['candidate_pending']}"),
            _stat_card("월간 예상", f"{model['growth']['monthly_messages']}건", f"{model['growth']['monthly_mb']} MB"),
        ]
    )
    channel_rows = "\n".join(
        "<tr>"
        f"<td>@{escape(str(row.get('handle') or ''))}</td>"
        f"<td>{escape(_compact(row.get('title'), 42))}</td>"
        f"<td>{escape(str(row.get('quality_score') or 0))}</td>"
        f"<td>{escape(str(row.get('messages') or 0))}</td>"
        f"<td>{escape(str(row.get('latest_at') or ''))}</td>"
        f"<td>{escape(str(row.get('last_error') or ''))}</td>"
        "</tr>"
        for row in model["top_channels"]
    )
    type_rows = "\n".join(
        f"<li><b>{escape(str(label))}</b><span>{count}건</span></li>"
        for label, count in model["type_counts"]
    )
    keyword_rows = "\n".join(
        f"<span>{escape(str(keyword))} <b>{count}</b></span>"
        for keyword, count in model["top_keywords"][:24]
    )
    signal_rows = "\n".join(
        "<tr>"
        f"<td>{escape(str(signal.get('article_id') or ''))}</td>"
        f"<td>{escape(str(signal.get('related_telegram_count') or 0))}</td>"
        f"<td>{escape(str(signal.get('related_telegram_channels_count') or 0))}</td>"
        f"<td>{escape(', '.join(str(keyword) for keyword in signal.get('top_keywords', [])[:5]))}</td>"
        f"<td>{escape(', '.join(str(flag) for flag in signal.get('risk_flags', [])[:5]))}</td>"
        "</tr>"
        for signal in model["signals"]
    )
    day_rows = "\n".join(
        f"<div><span>{escape(str(day))}</span><b style=\"width:{min(100, count * 100 / max(1, max((c for _d, c in model['day_counts']), default=1))):.1f}%\"></b><em>{count}</em></div>"
        for day, count in model["day_counts"]
    )
    html = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Telegram 수집 운영 대시보드 | BSIDE Daily News</title>
  <style>
    :root {{ --ink:#171321; --muted:#6d6478; --accent:#6f35e8; --line:#ded7ec; --soft:#f6f1ff; --paper:#fff; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Arial, "Noto Sans KR", sans-serif; color:var(--ink); background:#fbf9ff; }}
    main {{ max-width:1120px; margin:0 auto; padding:28px 22px 56px; }}
    header {{ display:flex; justify-content:space-between; gap:20px; align-items:flex-start; border-bottom:2px solid var(--ink); padding-bottom:18px; }}
    h1 {{ margin:20px 0 8px; font-family:Georgia, "Times New Roman", serif; font-size:42px; letter-spacing:0; }}
    h2 {{ margin:30px 0 12px; font-size:20px; }}
    p {{ color:var(--muted); line-height:1.6; }}
    a {{ color:var(--accent); }}
    .brand {{ color:var(--accent); font-size:34px; font-weight:800; letter-spacing:-1px; text-decoration:none; }}
    .brand span {{ font-size:12px; letter-spacing:2px; margin-left:8px; }}
    .stats {{ display:grid; grid-template-columns:repeat(6,1fr); gap:10px; margin:22px 0; }}
    .stat {{ border:1px solid var(--line); background:var(--paper); padding:14px; min-height:96px; }}
    .stat strong {{ display:block; font-size:25px; color:var(--accent); }}
    .stat p {{ margin:8px 0 0; color:var(--ink); font-weight:700; }}
    .stat span {{ color:var(--muted); font-size:12px; }}
    .grid {{ display:grid; grid-template-columns:1.1fr .9fr; gap:20px; }}
    table {{ width:100%; border-collapse:collapse; background:var(--paper); border:1px solid var(--line); }}
    th, td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:left; font-size:13px; vertical-align:top; }}
    th {{ color:var(--accent); background:var(--soft); }}
    .chips {{ display:flex; flex-wrap:wrap; gap:8px; }}
    .chips span {{ border:1px solid var(--line); border-radius:999px; padding:7px 10px; background:var(--paper); font-size:13px; }}
    .types {{ list-style:none; padding:0; margin:0; border:1px solid var(--line); background:var(--paper); }}
    .types li {{ display:flex; justify-content:space-between; padding:10px 12px; border-bottom:1px solid var(--line); }}
    .bars {{ border:1px solid var(--line); background:var(--paper); padding:12px; }}
    .bars div {{ display:grid; grid-template-columns:88px minmax(20px,1fr) 44px; gap:8px; align-items:center; margin:6px 0; font-size:12px; }}
    .bars b {{ display:block; height:8px; border-radius:99px; background:var(--accent); }}
    .note {{ border-left:4px solid var(--accent); background:var(--soft); padding:12px 14px; }}
    @media (max-width:900px) {{ .stats {{ grid-template-columns:repeat(2,1fr); }} .grid {{ grid-template-columns:1fr; }} h1 {{ font-size:32px; }} }}
  </style>
</head>
<body>
<main>
  <header>
    <a class="brand" href="https://bside.ai">bside<span>DAILY NEWS</span></a>
    <p>{escape(str(model["generated_at"]))}</p>
  </header>
  <h1>Telegram 수집 운영 대시보드</h1>
  <p>공개 broadcast 채널만 대상으로 수집 상태, 메시지 유형, 기사 매칭, 후보 채널과 저장량 추정치를 확인합니다. 개인 대화, 저장한 메시지, 그룹 대화는 수집 대상에서 제외됩니다.</p>
  <section class="stats">{stats}</section>
  <section class="grid">
    <div>
      <h2>채널별 수집 상태</h2>
      <table>
        <thead><tr><th>Handle</th><th>Title</th><th>Quality</th><th>Messages</th><th>Latest</th><th>Error</th></tr></thead>
        <tbody>{channel_rows or '<tr><td colspan="6">수집 대상 채널이 아직 없습니다.</td></tr>'}</tbody>
      </table>
    </div>
    <div>
      <h2>메시지 유형</h2>
      <ul class="types">{type_rows or '<li><b>데이터 없음</b><span>0건</span></li>'}</ul>
      <h2>최근 14일 키워드</h2>
      <div class="chips">{keyword_rows or '<span>키워드 없음</span>'}</div>
    </div>
  </section>
  <section class="grid">
    <div>
      <h2>일별 수집량</h2>
      <div class="bars">{day_rows or '<p>아직 표시할 수집량이 없습니다.</p>'}</div>
    </div>
    <div>
      <h2>분석 제안</h2>
      <div class="note">
        <p>URL 직접 공유는 기사 반응도, 키워드 반복 언급은 시장 관심도, 여러 채널 동시 언급은 이슈 확산 신호로 해석할 수 있습니다.</p>
        <p>추천 후보는 바로 가입하지 않고 pending 상태로 유지한 뒤, 운영자가 품질 점수와 제목을 보고 승인하는 방식이 안전합니다.</p>
      </div>
    </div>
  </section>
  <section>
    <h2>기사-텔레그램 이슈 신호</h2>
    <table>
      <thead><tr><th>Article</th><th>Messages</th><th>Channels</th><th>Keywords</th><th>Risk flags</th></tr></thead>
      <tbody>{signal_rows or '<tr><td colspan="5">아직 기사와 연결된 Telegram 신호가 없습니다.</td></tr>'}</tbody>
    </table>
  </section>
</main>
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")
    return output_path


def main() -> None:
    from .config import load_config
    from .dates import now_in_timezone
    from .state import load_state

    project_root = Path.cwd()
    config = load_config(project_root / "config.yaml")
    state = load_state(project_root / "data" / "state.json")
    now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
    path = write_telegram_dashboard(project_root, state, config, now)
    print(path)


if __name__ == "__main__":
    main()
