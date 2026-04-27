from __future__ import annotations

HIGH_KEYWORDS = [
    "행동주의",
    "행동주의 주주",
    "주주제안",
    "위임장",
    "공개서한",
    "경영권 분쟁",
    "이사회 교체",
    "감사 선임",
    "소액주주",
    "소액주주연대",
    "주주행동",
    "주주권",
    "얼라인",
    "KCGI",
    "트러스톤",
    "플래쉬라이트",
    "엘리엇",
]

MEDIUM_KEYWORDS = [
    "밸류업",
    "기업 밸류업",
    "주주환원",
    "배당 확대",
    "자사주 매입",
    "자사주 소각",
    "지배구조",
    "거버넌스",
    "스튜어드십",
    "이사회",
    "주총",
    "주주총회",
]

LOW_PATTERNS = [
    "리포트 브리핑",
    "브리핑",
    "목표가",
    "목표주가",
    "투자의견",
    "단순 실적",
    "장중",
    "특징주",
    "증시",
    "증시요약",
    "마감 시황",
    "오늘의 증시",
    "뉴욕증시",
    "주간추천",
    "추천주",
    "52주 신고가",
    "신고가",
    "상한가",
    "하한가",
    "연례 주주총회 개최",
    "주총서 이사",
    "감사인 선임 승인",
    "이사 14명",
]


def _contains(text: str, needle: str) -> bool:
    return needle.casefold() in text.casefold()


def find_matches(text: str, keywords: list[str]) -> list[str]:
    return [keyword for keyword in keywords if _contains(text, keyword)]


def classify_relevance(title: str, summary: str = "") -> str:
    text = f"{title or ''} {summary or ''}"
    if find_matches(text, HIGH_KEYWORDS):
        return "high"
    if find_matches(text, LOW_PATTERNS):
        return "low"
    if find_matches(text, MEDIUM_KEYWORDS):
        return "medium"
    return "low"


def relevance_details(title: str, summary: str = "") -> dict[str, object]:
    text = f"{title or ''} {summary or ''}"
    high = find_matches(text, HIGH_KEYWORDS)
    low = find_matches(text, LOW_PATTERNS)
    medium = find_matches(text, MEDIUM_KEYWORDS)

    level = "low"
    matches: list[str] = []
    if high:
        level = "high"
        matches = high
    elif low:
        level = "low"
        matches = low
    elif medium:
        level = "medium"
        matches = medium

    return {
        "level": level,
        "matched_keywords": matches,
        "high_keywords": high,
        "medium_keywords": medium,
        "low_patterns": low,
    }


def topic_keywords_for_article(article: dict[str, object]) -> list[str]:
    text = f"{article.get('clean_title') or article.get('title') or ''} {article.get('summary') or ''}"
    return find_matches(text, HIGH_KEYWORDS + MEDIUM_KEYWORDS)
