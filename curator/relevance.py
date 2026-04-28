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
    "주주대표소송",
    "일감 몰아주기",
    "터널링",
    "의결권",
    "의안별 표결",
    "의결정족수",
    "이사 보수한도",
    "중복상장",
    "물적분할",
    "주식매수청구권",
    "모회사 주주 보호",
    "경영권 승계",
    "오너일가",
    "총수일가",
    "주식담보",
    "공정위 신고",
    "5%룰",
    "경영권 영향 목적",
    "shareholder activism",
    "activist investor",
    "activist campaign",
    "shareholder proposal",
    "proxy fight",
    "proxy contest",
    "universal proxy",
    "open letter",
    "board seats",
    "board seat",
    "minority shareholder",
    "minority shareholders",
    "shareholder rights",
    "proxy voting",
    "Elliott Management",
    "Starboard Value",
    "Third Point",
    "Trian Partners",
    "D.E. Shaw",
    "Carl Icahn",
    "ValueAct",
    "Sachem Head",
    "Saba Capital",
    "Browning West",
]

MEDIUM_KEYWORDS = [
    "밸류업",
    "벨류업",
    "기업 밸류업",
    "기업 벨류업",
    "주주환원",
    "배당 확대",
    "자사주 매입",
    "자사주 소각",
    "지배구조",
    "거버넌스",
    "스튜어드십",
    "이사회",
    "사외이사",
    "경영진 견제",
    "주총",
    "주주총회",
    "유상증자",
    "정정신고서",
    "불성실공시",
    "불성실공시법인",
    "상장폐지",
    "상장적격성 실질심사",
    "거래정지",
    "개선기간",
    "의무공개매수",
    "일반주주",
    "공모주",
    "IPO",
    "코너스톤 투자자",
    "코너스톤 제도",
    "대기업집단",
    "기업집단",
    "총수",
    "공정위",
    "금감원",
    "금융위",
    "자본시장법",
    "자본시장 정책",
    "상법",
    "집단소송법",
    "임원보수",
    "임원 보수",
    "성과보상",
    "성과급",
    "금융투자회사 성과급",
    "주식보상",
    "주식 보상",
    "RSU",
    "사업보고서",
    "기업공시서식",
    "주주권익 보호",
    "ETF 시장",
    "운용사 의결권",
    "주총 영향력",
    "거수기 탈피",
    "해외부동산펀드",
    "핵심위험 설명서",
    "주니어 ISA",
    "배당소득 분리과세",
    "분리과세",
    "저PBR",
    "PBR",
    "지주회사",
    "비과세배당",
    "비과세 배당",
    "현물출자",
    "영업양도",
    "PRS",
    "ESG 공시",
    "국민연금",
    "뉴프레임워크",
    "WGBI",
    "corporate governance",
    "shareholder return",
    "shareholder returns",
    "shareholder value",
    "share buyback",
    "stock buyback",
    "buyback cancellation",
    "treasury shares",
    "Korea discount",
    "Value-up Program",
    "capital market reform",
    "fiduciary duty",
    "Commercial Act",
    "chaebol",
    "controlling shareholder",
    "controlling shareholders",
    "tunneling",
    "stewardship code",
    "stewardship responsibilities",
    "institutional investor",
    "institutional investors",
    "National Pension Service",
    "NPS",
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
    "earnings report",
    "price target",
    "analyst rating",
    "market close",
    "stock market close",
]

LOW_OVERRIDE_PATTERNS = [
    "대표변호사",
    "변호사 선출",
    "기업 자문 M&A",
    "실적 발표 앞두고",
    "보합권",
    "혼조세",
    "섹터 강세",
    "반등 성공",
]


def _contains(text: str, needle: str) -> bool:
    return needle.casefold() in text.casefold()


def find_matches(text: str, keywords: list[str]) -> list[str]:
    return [keyword for keyword in keywords if _contains(text, keyword)]


def classify_relevance(title: str, summary: str = "") -> str:
    text = f"{title or ''} {summary or ''}"
    if find_matches(text, LOW_OVERRIDE_PATTERNS):
        return "low"
    if find_matches(text, HIGH_KEYWORDS):
        return "high"
    if find_matches(text, LOW_PATTERNS):
        return "low"
    if find_matches(text, MEDIUM_KEYWORDS):
        return "medium"
    return "low"


def relevance_details(title: str, summary: str = "") -> dict[str, object]:
    text = f"{title or ''} {summary or ''}"
    low_override = find_matches(text, LOW_OVERRIDE_PATTERNS)
    high = find_matches(text, HIGH_KEYWORDS)
    low = find_matches(text, LOW_PATTERNS)
    medium = find_matches(text, MEDIUM_KEYWORDS)

    level = "low"
    matches: list[str] = []
    if low_override:
        level = "low"
        matches = low_override
    elif high:
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
        "low_patterns": low_override + low,
        "low_override_patterns": low_override,
    }


def topic_keywords_for_article(article: dict[str, object]) -> list[str]:
    text = f"{article.get('clean_title') or article.get('title') or ''} {article.get('summary') or ''}"
    return find_matches(text, HIGH_KEYWORDS + MEDIUM_KEYWORDS)
