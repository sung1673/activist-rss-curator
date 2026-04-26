from __future__ import annotations

import re
from datetime import datetime, timedelta

from rapidfuzz import fuzz

from .dates import datetime_to_iso, hours_between, parse_datetime
from .normalize import stable_hash
from .relevance import topic_keywords_for_article


KNOWN_COMPANIES = [
    "신한금융",
    "신한지주",
    "KB금융",
    "KB금융지주",
    "삼성물산",
    "고려아연",
    "KT&G",
    "KT",
    "SK스퀘어",
    "SK이노베이션",
    "LG화학",
    "LG전자",
    "현대차",
    "현대모비스",
    "한화오션",
    "두산밥캣",
    "DB하이텍",
    "SM엔터",
    "카카오",
    "네이버",
    "셀트리온",
    "포스코홀딩스",
]

COMPANY_SUFFIX_PATTERN = re.compile(
    r"([가-힣A-Za-z0-9&]{2,}(?:금융|지주|전자|물산|제약|화학|바이오|엔터|건설|증권|은행|보험|투자|홀딩스|그룹|산업|상사|에너지|중공업|해운|통신))"
)
COMPANY_STOPWORDS = {"행동주의", "주주행동", "기업지배구조", "거버넌스", "이사회", "주주총회"}
RELEVANCE_RANK = {"low": 0, "medium": 1, "high": 2}
SENSITIVE_ARTICLE_KEYS = {"source_feed_url", "feed_url"}


def extract_company_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    for company in KNOWN_COMPANIES:
        if company in text:
            candidates.append(company)

    for match in COMPANY_SUFFIX_PATTERN.finditer(text):
        value = match.group(1).strip()
        if value not in COMPANY_STOPWORDS and value not in candidates:
            candidates.append(value)
    return candidates[:5]


def enrich_article_for_clustering(article: dict[str, object]) -> dict[str, object]:
    enriched = dict(article)
    for key in SENSITIVE_ARTICLE_KEYS:
        enriched.pop(key, None)
    text = f"{article.get('clean_title') or article.get('title') or ''} {article.get('summary') or ''}"
    enriched["company_candidates"] = list(article.get("company_candidates") or extract_company_candidates(text))
    enriched["topic_keywords"] = list(article.get("topic_keywords") or topic_keywords_for_article(article))
    return enriched


def article_datetime(article: dict[str, object], now: datetime, timezone_name: str = "Asia/Seoul") -> datetime:
    return (
        parse_datetime(str(article.get("published_at") or ""), timezone_name)
        or parse_datetime(str(article.get("article_published_at") or ""), timezone_name)
        or parse_datetime(str(article.get("feed_published_at") or ""), timezone_name)
        or now
    )


def cluster_base_string(article: dict[str, object]) -> str:
    companies = list(article.get("company_candidates") or [])
    keywords = list(article.get("topic_keywords") or [])
    title_seed = str(article.get("normalized_title") or article.get("clean_title") or "untitled")
    if companies:
        return "|".join([str(item) for item in companies[:2] + keywords[:2]])
    if keywords:
        return "|".join([str(item) for item in keywords[:2] + [title_seed[:90]]])
    return title_seed


def make_cluster_key(article: dict[str, object]) -> str:
    return stable_hash(cluster_base_string(article), length=16)


def max_relevance(left: str, right: str) -> str:
    return left if RELEVANCE_RANK.get(left, 0) >= RELEVANCE_RANK.get(right, 0) else right


def next_sequence_for_key(state: dict[str, object], cluster_key: str) -> int:
    sequence = 1
    for cluster in list(state.get("pending_clusters", [])) + list(state.get("published_clusters", [])):
        if cluster.get("cluster_key") == cluster_key:
            sequence = max(sequence, int(cluster.get("sequence") or 1) + 1)
    return sequence


def create_cluster(
    article: dict[str, object],
    now: datetime,
    state: dict[str, object],
    *,
    cluster_key: str | None = None,
    is_followup: bool = False,
) -> dict[str, object]:
    enriched = enrich_article_for_clustering(article)
    key = cluster_key or make_cluster_key(enriched)
    article_dt = article_datetime(enriched, now)
    return {
        "cluster_key": key,
        "sequence": next_sequence_for_key(state, key),
        "status": "pending",
        "is_followup": is_followup,
        "relevance_level": enriched.get("relevance_level") or "medium",
        "keywords": list(enriched.get("topic_keywords") or []),
        "companies": list(enriched.get("company_candidates") or []),
        "representative_title": enriched.get("clean_title") or enriched.get("title") or "",
        "representative_title_normalized": enriched.get("normalized_title") or "",
        "representative_url": enriched.get("canonical_url") or enriched.get("link") or "",
        "created_at": datetime_to_iso(now),
        "last_article_seen_at": datetime_to_iso(now),
        "last_article_at": datetime_to_iso(article_dt),
        "published_at": None,
        "guid": None,
        "articles": [enriched],
        "article_count": 1,
    }


def article_already_in_cluster(article: dict[str, object], cluster: dict[str, object]) -> bool:
    url_hash = str(article.get("canonical_url_hash") or "")
    title_hash = str(article.get("title_hash") or "")
    for existing in cluster.get("articles", []):
        if url_hash and existing.get("canonical_url_hash") == url_hash:
            return True
        if title_hash and existing.get("title_hash") == title_hash:
            return True
    return False


def add_article_to_cluster(article: dict[str, object], cluster: dict[str, object], now: datetime) -> None:
    enriched = enrich_article_for_clustering(article)
    if article_already_in_cluster(enriched, cluster):
        return

    cluster["articles"].append(enriched)  # type: ignore[index, union-attr]
    cluster["article_count"] = len(cluster.get("articles", []))
    cluster["last_article_seen_at"] = datetime_to_iso(now)
    cluster["last_article_at"] = datetime_to_iso(article_datetime(enriched, now))
    cluster["relevance_level"] = max_relevance(
        str(cluster.get("relevance_level") or "medium"),
        str(enriched.get("relevance_level") or "medium"),
    )
    cluster["keywords"] = sorted(set(cluster.get("keywords", [])) | set(enriched.get("topic_keywords", [])))
    cluster["companies"] = sorted(set(cluster.get("companies", [])) | set(enriched.get("company_candidates", [])))


def same_company_and_keyword(article: dict[str, object], cluster: dict[str, object]) -> bool:
    companies = set(article.get("company_candidates") or [])
    keywords = set(article.get("topic_keywords") or [])
    return bool(companies & set(cluster.get("companies", []))) and bool(keywords & set(cluster.get("keywords", [])))


def within_cluster_window(
    article: dict[str, object],
    cluster: dict[str, object],
    now: datetime,
    window_hours: int,
    timezone_name: str,
) -> bool:
    article_dt = article_datetime(article, now, timezone_name)
    cluster_dt = parse_datetime(str(cluster.get("last_article_at") or ""), timezone_name) or now
    return hours_between(article_dt, cluster_dt) <= window_hours


def can_join_cluster(
    article: dict[str, object],
    cluster: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> bool:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    cluster_config = config.get("cluster", {})
    dedupe_config = config.get("dedupe", {})
    window_hours = int(cluster_config.get("cluster_window_hours", 48))  # type: ignore[union-attr]
    if not within_cluster_window(article, cluster, now, window_hours, timezone_name):
        return False

    title_score = fuzz.token_set_ratio(
        str(article.get("normalized_title") or ""),
        str(cluster.get("representative_title_normalized") or cluster.get("representative_title") or ""),
    )
    if title_score >= int(dedupe_config.get("title_cluster_threshold", 80)):  # type: ignore[union-attr]
        return True

    if same_company_and_keyword(article, cluster):
        return True

    representative = (cluster.get("articles") or [{}])[0]
    summary_score = fuzz.token_set_ratio(str(article.get("summary") or ""), str(representative.get("summary") or ""))
    return title_score >= 80 and summary_score >= int(dedupe_config.get("summary_cluster_threshold", 85))  # type: ignore[union-attr]


def find_matching_cluster(
    article: dict[str, object],
    clusters: list[dict[str, object]],
    config: dict[str, object],
    now: datetime,
) -> dict[str, object] | None:
    for cluster in clusters:
        if can_join_cluster(article, cluster, config, now):
            return cluster
    return None


def buffer_minutes_for_cluster(cluster: dict[str, object], config: dict[str, object]) -> int:
    cluster_config = config.get("cluster", {})
    if cluster.get("relevance_level") == "high":
        return int(cluster_config.get("buffer_minutes_high", 20))  # type: ignore[union-attr]
    return int(cluster_config.get("buffer_minutes_default", 45))  # type: ignore[union-attr]


def cluster_guid(cluster: dict[str, object], timezone_name: str = "Asia/Seoul") -> str:
    published_at = parse_datetime(str(cluster.get("published_at") or ""), timezone_name)
    published_date = published_at.strftime("%Y%m%d") if published_at else "unknown"
    return f"cluster:{cluster.get('cluster_key')}:{published_date}:{cluster.get('sequence') or 1}"


def ready_to_publish(cluster: dict[str, object], config: dict[str, object], now: datetime) -> bool:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    created_at = parse_datetime(str(cluster.get("created_at") or ""), timezone_name) or now
    last_seen = parse_datetime(str(cluster.get("last_article_seen_at") or ""), timezone_name) or created_at
    cluster_config = config.get("cluster", {})
    if now - created_at >= timedelta(hours=int(cluster_config.get("max_pending_hours", 3))):  # type: ignore[union-attr]
        return True
    return now - last_seen >= timedelta(minutes=buffer_minutes_for_cluster(cluster, config))


def publish_ready_clusters(state: dict[str, object], config: dict[str, object], now: datetime) -> list[dict[str, object]]:
    pending = list(state.get("pending_clusters", []))
    still_pending: list[dict[str, object]] = []
    newly_published: list[dict[str, object]] = []

    for cluster in pending:
        if ready_to_publish(cluster, config, now):
            cluster["status"] = "published"
            cluster["published_at"] = datetime_to_iso(now)
            cluster["guid"] = cluster.get("guid") or cluster_guid(cluster, str(config.get("timezone") or "Asia/Seoul"))
            newly_published.append(cluster)
        else:
            still_pending.append(cluster)

    state["pending_clusters"] = still_pending
    state["published_clusters"] = list(state.get("published_clusters", [])) + newly_published
    return newly_published


def cluster_articles(
    articles: list[dict[str, object]],
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> list[dict[str, object]]:
    state.setdefault("pending_clusters", [])
    state.setdefault("published_clusters", [])

    for article in articles:
        enriched = enrich_article_for_clustering(article)
        pending = state["pending_clusters"]  # type: ignore[assignment]
        matched_pending = find_matching_cluster(enriched, pending, config, now)  # type: ignore[arg-type]
        if matched_pending:
            add_article_to_cluster(enriched, matched_pending, now)
            continue

        matched_published = find_matching_cluster(
            enriched,
            list(state.get("published_clusters", [])),
            config,
            now,
        )
        if matched_published:
            followup = create_cluster(
                enriched,
                now,
                state,
                cluster_key=str(matched_published.get("cluster_key")),
                is_followup=True,
            )
            pending.append(followup)  # type: ignore[union-attr]
            continue

        pending.append(create_cluster(enriched, now, state))  # type: ignore[union-attr]

    return publish_ready_clusters(state, config, now)
