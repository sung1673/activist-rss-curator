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
    "풍산",
    "한국앤컴퍼니",
    "KT&G",
    "KT",
    "SK스퀘어",
    "SK이노베이션",
    "LG화학",
    "LG전자",
    "한화",
    "한화솔루션",
    "현대차",
    "현대모비스",
    "HD현대",
    "HD현대일렉트릭",
    "HD현대로보틱스",
    "한화오션",
    "두산밥캣",
    "DB하이텍",
    "SM엔터",
    "카카오",
    "네이버",
    "셀트리온",
    "포스코홀딩스",
    "우리금융",
    "우리금융지주",
    "일진홀딩스",
    "슈프리마에이치큐",
    "보령",
    "코웨이",
    "쿠팡",
    "아이로보틱스",
    "인크레더블버즈",
    "Shinhan Financial Group",
    "KB Financial Group",
    "Samsung C&T",
    "Korea Zinc",
    "LG Chem",
    "Hyundai Motor",
    "Hyundai Mobis",
    "NPS",
    "Elliott Management",
    "Starboard Value",
    "Third Point",
    "Trian Partners",
    "D.E. Shaw",
    "ValueAct",
    "Sachem Head",
    "Saba Capital",
    "Browning West",
]

COMPANY_SUFFIX_PATTERN = re.compile(
    r"([가-힣A-Za-z0-9&]{2,}(?:금융|지주|전자|물산|제약|화학|바이오|엔터|건설|증권|은행|보험|투자|홀딩스|그룹|산업|상사|에너지|중공업|해운|통신))"
)
COMPANY_STOPWORDS = {"행동주의", "주주행동", "기업지배구조", "거버넌스", "이사회", "주주총회"}
RELEVANCE_RANK = {"low": 0, "medium": 1, "high": 2}
SENSITIVE_ARTICLE_KEYS = {"source_feed_url", "feed_url"}
THEME_GROUPS = [
    (
        "shareholder_proposal",
        "주주제안·공개서한",
        ["주주제안", "공개서한", "권고적 주주제안", "국민연금", "shareholder proposal", "open letter"],
    ),
    (
        "minority_shareholder",
        "소액주주·주주연대 분쟁",
        ["소액주주", "소액주주연대", "주주연대", "주주행동", "minority shareholder", "minority shareholders", "shareholder rights"],
    ),
    (
        "activism_trend",
        "행동주의 펀드·주주행동 트렌드",
        [
            "행동주의",
            "행동주의 주주",
            "얼라인",
            "KCGI",
            "트러스톤",
            "플래쉬라이트",
            "엘리엇",
            "shareholder activism",
            "activist investor",
            "activist campaign",
            "proxy fight",
            "proxy contest",
            "universal proxy",
            "Elliott Management",
            "Starboard Value",
            "Third Point",
            "Trian Partners",
            "D.E. Shaw",
            "ValueAct",
            "Sachem Head",
            "Saba Capital",
        ],
    ),
    (
        "control_dispute",
        "경영권 분쟁 관련",
        ["경영권 분쟁"],
    ),
    (
        "board_audit",
        "이사회 재편·임시주총·감사 선임",
        ["이사회 교체", "감사 선임", "이사회", "감사위원", "사외이사", "임시주총", "임시 주총", "board seat", "board seats"],
    ),
    (
        "voting_disclosure",
        "주총·의결권·표결 공시",
        ["의결권", "의안별 표결", "의결정족수", "이사 보수한도", "의결권 자문", "proxy voting"],
    ),
    (
        "capital_market_policy",
        "정책·자본시장 제도",
        [
            "중복상장",
            "물적분할",
            "인적분할",
            "모회사 주주 보호",
            "주식매수청구권",
            "자본시장법",
            "상법",
            "집단소송법",
            "상장폐지",
            "공모주",
            "IPO",
            "WGBI",
            "대기업집단",
            "기업집단",
            "공정위",
            "금감원",
            "금융위",
            "ESG 공시",
            "5%룰",
            "경영권 영향 목적",
            "capital market reform",
            "Commercial Act",
            "fiduciary duty",
        ],
    ),
    (
        "capital_raise_disclosure",
        "자본조달·공시",
        ["유상증자", "정정신고서", "불성실공시", "불성실공시법인", "PRS", "현물출자", "영업양도"],
    ),
    (
        "ownership_succession",
        "지배구조·승계·대주주",
        [
            "경영권 승계",
            "승계",
            "상속세",
            "오너일가",
            "총수일가",
            "총수",
            "대주주",
            "주식담보",
            "우호지분",
            "일감 몰아주기",
            "주주대표소송",
            "터널링",
            "chaebol",
            "controlling shareholder",
            "controlling shareholders",
            "tunneling",
        ],
    ),
    (
        "valueup_return",
        "밸류업·주주환원·지배구조",
        [
            "밸류업",
            "주주환원",
            "자사주 매입",
            "자사주 소각",
            "지배구조",
            "거버넌스",
            "스튜어드십",
            "corporate governance",
            "shareholder return",
            "shareholder returns",
            "shareholder value",
            "share buyback",
            "stock buyback",
            "treasury shares",
            "Korea discount",
            "Value-up Program",
            "stewardship code",
            "National Pension Service",
            "NPS",
        ],
    ),
]
THEME_LABELS = {theme_id: label for theme_id, label, _ in THEME_GROUPS}
COMPANY_STRICT_THEME_GROUPS = {
    "shareholder_proposal",
    "minority_shareholder",
    "control_dispute",
    "capital_raise_disclosure",
    "ownership_succession",
}


def extract_company_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    folded_text = text.casefold()
    for company in KNOWN_COMPANIES:
        if company in text or company.casefold() in folded_text:
            candidates.append(company)

    for match in COMPANY_SUFFIX_PATTERN.finditer(text):
        value = match.group(1).strip()
        if value not in COMPANY_STOPWORDS and value not in candidates:
            candidates.append(value)
    return candidates[:5]


def extract_theme_groups(text: str, keywords: list[object] | None = None) -> list[str]:
    haystack = f"{text or ''} {' '.join(str(keyword) for keyword in keywords or [])}".casefold()
    groups: list[str] = []
    for theme_id, _label, needles in THEME_GROUPS:
        if any(needle.casefold() in haystack for needle in needles):
            groups.append(theme_id)
    activism_priority_terms = ["shareholder activism", "activist investor", "activist campaign", "proxy fight", "proxy contest"]
    if "activism_trend" in groups and any(term in haystack for term in activism_priority_terms):
        groups = ["activism_trend"] + [group for group in groups if group != "activism_trend"]
    explicit_board_terms = ["감사위원", "감사 선임", "사외이사", "이사회 재편", "이사회 교체", "임시주총", "임시 주총"]
    higher_priority_groups = {"shareholder_proposal", "minority_shareholder", "activism_trend", "control_dispute"}
    if (
        "valueup_return" in groups
        and not any(group in groups for group in higher_priority_groups)
        and not any(term.casefold() in haystack for term in explicit_board_terms)
    ):
        groups = ["valueup_return"] + [group for group in groups if group != "valueup_return"]
    return groups


def enrich_article_for_clustering(article: dict[str, object]) -> dict[str, object]:
    enriched = dict(article)
    for key in SENSITIVE_ARTICLE_KEYS:
        enriched.pop(key, None)
    text = f"{article.get('clean_title') or article.get('title') or ''} {article.get('summary') or ''}"
    enriched["company_candidates"] = list(article.get("company_candidates") or extract_company_candidates(text))
    enriched["topic_keywords"] = list(article.get("topic_keywords") or topic_keywords_for_article(article))
    enriched["theme_groups"] = extract_theme_groups(text, list(enriched["topic_keywords"]))
    enriched["theme_group"] = primary_theme_group(enriched)
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
    theme_groups = list(article.get("theme_groups") or [])
    title_seed = str(article.get("normalized_title") or article.get("clean_title") or "untitled")
    if theme_groups:
        return f"theme:{theme_groups[0]}"
    if companies:
        return "|".join([str(item) for item in companies[:2] + keywords[:2]])
    if keywords:
        return "|".join([str(item) for item in keywords[:2] + [title_seed[:90]]])
    return title_seed


def title_for_theme_group(theme_group: str, companies: list[object] | None = None) -> str:
    label = THEME_LABELS.get(theme_group, "주주·거버넌스 뉴스")
    company_names = [str(company) for company in companies or [] if str(company)]
    if theme_group in {"shareholder_proposal", "minority_shareholder"} and company_names:
        return f"{company_names[0]} {label}"
    return label


def primary_theme_group(value: dict[str, object]) -> str:
    if value.get("theme_group"):
        return str(value.get("theme_group"))
    theme_groups = list(value.get("theme_groups") or [])
    return str(theme_groups[0]) if theme_groups else ""


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
        "theme_groups": list(enriched.get("theme_groups") or []),
        "theme_group": primary_theme_group(enriched),
        "theme_grouped": False,
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
    cluster["theme_groups"] = sorted(set(cluster.get("theme_groups", [])) | set(enriched.get("theme_groups", [])))
    cluster["theme_group"] = cluster.get("theme_group") or primary_theme_group(enriched)
    if cluster.get("theme_grouped") and cluster.get("theme_groups"):
        cluster["representative_title"] = title_for_theme_group(
            primary_theme_group(cluster),
            list(cluster.get("companies", [])),
        )


def same_company_and_keyword(article: dict[str, object], cluster: dict[str, object]) -> bool:
    companies = set(article.get("company_candidates") or [])
    keywords = set(article.get("topic_keywords") or [])
    return bool(companies & set(cluster.get("companies", []))) and bool(keywords & set(cluster.get("keywords", [])))


def same_theme_group(article: dict[str, object], cluster: dict[str, object]) -> bool:
    article_theme = primary_theme_group(article)
    cluster_theme = primary_theme_group(cluster)
    return bool(article_theme and article_theme == cluster_theme)


def can_join_by_theme_group(article: dict[str, object], cluster: dict[str, object], title_score: float, threshold: int) -> bool:
    theme_group = primary_theme_group(article)
    if not theme_group or theme_group != primary_theme_group(cluster):
        return False
    if theme_group not in COMPANY_STRICT_THEME_GROUPS:
        return True

    article_companies = set(article.get("company_candidates") or [])
    cluster_companies = set(cluster.get("companies") or [])
    if article_companies and cluster_companies:
        return bool(article_companies & cluster_companies)
    if article_companies or cluster_companies:
        return False
    return title_score >= threshold


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


def within_theme_window(
    article: dict[str, object],
    cluster: dict[str, object],
    now: datetime,
    config: dict[str, object],
    timezone_name: str,
) -> bool:
    cluster_config = config.get("cluster", {})
    theme_window_hours = int(cluster_config.get("theme_group_window_hours", 168))  # type: ignore[union-attr]
    article_dt = article_datetime(article, now, timezone_name)
    cluster_dt = parse_datetime(str(cluster.get("last_article_at") or ""), timezone_name) or now
    return hours_between(article_dt, cluster_dt) <= theme_window_hours


def can_join_cluster(
    article: dict[str, object],
    cluster: dict[str, object],
    config: dict[str, object],
    now: datetime,
    *,
    allow_theme_group: bool = True,
) -> bool:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    cluster_config = config.get("cluster", {})
    dedupe_config = config.get("dedupe", {})
    window_hours = int(cluster_config.get("cluster_window_hours", 48))  # type: ignore[union-attr]
    title_threshold = int(dedupe_config.get("title_cluster_threshold", 80))  # type: ignore[union-attr]
    title_score = fuzz.token_set_ratio(
        str(article.get("normalized_title") or ""),
        str(cluster.get("representative_title_normalized") or cluster.get("representative_title") or ""),
    )
    theme_window_match = (
        allow_theme_group
        and same_theme_group(article, cluster)
        and within_theme_window(article, cluster, now, config, timezone_name)
        and can_join_by_theme_group(article, cluster, title_score, title_threshold)
    )
    if not within_cluster_window(article, cluster, now, window_hours, timezone_name):
        if theme_window_match:
            cluster["theme_grouped"] = True
            return True
        return False

    if title_score >= title_threshold:
        return True

    if same_company_and_keyword(article, cluster):
        return True

    if theme_window_match:
        cluster["theme_grouped"] = True
        return True

    representative = (cluster.get("articles") or [{}])[0]
    summary_score = fuzz.token_set_ratio(str(article.get("summary") or ""), str(representative.get("summary") or ""))
    return title_score >= 80 and summary_score >= int(dedupe_config.get("summary_cluster_threshold", 85))  # type: ignore[union-attr]


def find_matching_cluster(
    article: dict[str, object],
    clusters: list[dict[str, object]],
    config: dict[str, object],
    now: datetime,
    *,
    allow_theme_group: bool = True,
) -> dict[str, object] | None:
    for cluster in clusters:
        if can_join_cluster(article, cluster, config, now, allow_theme_group=allow_theme_group):
            return cluster
    return None


def merge_cluster(source: dict[str, object], target: dict[str, object], now: datetime) -> None:
    last_seen_values = [
        str(value)
        for value in (target.get("last_article_seen_at"), source.get("last_article_seen_at"))
        if value
    ]
    last_article_values = [
        str(value)
        for value in (target.get("last_article_at"), source.get("last_article_at"))
        if value
    ]
    for article in list(source.get("articles", [])):
        add_article_to_cluster(article, target, now)
    if last_seen_values:
        target["last_article_seen_at"] = max(last_seen_values)
    if last_article_values:
        target["last_article_at"] = max(last_article_values)
    target["theme_grouped"] = bool(target.get("theme_grouped") or source.get("theme_grouped") or same_theme_group(source, target))
    target["keywords"] = sorted(set(target.get("keywords", [])) | set(source.get("keywords", [])))
    target["companies"] = sorted(set(target.get("companies", [])) | set(source.get("companies", [])))
    target["theme_groups"] = sorted(set(target.get("theme_groups", [])) | set(source.get("theme_groups", [])))
    target["theme_group"] = target.get("theme_group") or primary_theme_group(source)
    target["relevance_level"] = max_relevance(
        str(target.get("relevance_level") or "medium"),
        str(source.get("relevance_level") or "medium"),
    )
    if target.get("theme_grouped") and target.get("theme_groups"):
        target["representative_title"] = title_for_theme_group(
            primary_theme_group(target),
            list(target.get("companies", [])),
        )


def reconcile_pending_clusters(state: dict[str, object], config: dict[str, object], now: datetime) -> None:
    reconciled: list[dict[str, object]] = []
    for cluster in list(state.get("pending_clusters", [])):
        articles = [enrich_article_for_clustering(article) for article in list(cluster.get("articles", []))]
        cluster["articles"] = articles
        cluster["article_count"] = len(articles)
        cluster["keywords"] = sorted(set(cluster.get("keywords", [])) | {keyword for article in articles for keyword in article.get("topic_keywords", [])})
        cluster["companies"] = sorted(set(cluster.get("companies", [])) | {company for article in articles for company in article.get("company_candidates", [])})
        cluster["theme_groups"] = sorted(set(cluster.get("theme_groups", [])) | {theme for article in articles for theme in article.get("theme_groups", [])})
        cluster["theme_group"] = primary_theme_group(articles[0]) if articles else primary_theme_group(cluster)
        representative_article = articles[0] if articles else {}
        match = find_matching_cluster(representative_article, reconciled, config, now, allow_theme_group=True)
        if match:
            cluster["theme_grouped"] = bool(cluster.get("theme_grouped") or same_theme_group(representative_article, match))
            merge_cluster(cluster, match, now)
        else:
            if cluster.get("theme_grouped") and cluster.get("theme_groups"):
                cluster["representative_title"] = title_for_theme_group(
                    primary_theme_group(cluster),
                    list(cluster.get("companies", [])),
                )
            reconciled.append(cluster)
    state["pending_clusters"] = reconciled


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
            allow_theme_group=False,
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

    reconcile_pending_clusters(state, config, now)
    return publish_ready_clusters(state, config, now)
