from __future__ import annotations

import hashlib
import html
import base64
import re
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit


TRACKING_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "igshid",
    "ref",
}

SOURCE_SUFFIXES = [
    "Daum",
    "다음",
    "네이트 뉴스",
    "뉴스핌",
    "중앙일보",
    "한국경제",
    "매일경제",
    "연합뉴스",
    "머니투데이",
    "조선비즈",
    "이데일리",
    "서울경제",
    "파이낸셜뉴스",
    "아시아경제",
    "뉴시스",
    "헤럴드경제",
    "뉴스1",
    "한겨레",
    "경향신문",
    "조선일보",
    "서울경제신문",
    "동아일보",
    "비즈니스포스트",
    "매일일보",
    "더벨(thebell)",
    "딜사이트",
    "연합인포맥스",
    "글로벌이코노믹",
    "아주경제",
    "지구인사이드",
    "한국경제TV",
    "데일리안",
    "블로터",
    "한국금융신문",
    "전자신문",
    "시사위크",
    "인베스트조선",
    "팍스넷뉴스",
    "뉴스토마토",
    "뉴스웨이",
    "법률신문",
    "넘버스",
    "마켓인",
    "톱데일리",
    "인포스탁데일리",
    "디지털타임스",
    "뉴스톱",
    "아이뉴스24",
    "이코노미스트",
    "시사저널",
    "세계일보",
    "쿠키뉴스",
    "한국일보",
    "조세일보",
    "비즈니스워치",
    "아시아타임즈",
    "데일리임팩트",
    "이코노믹리뷰",
    "매일경제TV",
    "서울파이낸스",
    "에너지경제신문",
    "지디넷코리아",
    "뉴데일리경제",
    "서울신문",
    "SBS Biz",
    "중앙일보",
    "Reuters",
    "Bloomberg",
    "Investing.com",
    "YouTube",
    "ZUM 뉴스",
    "MSN",
    "임팩트온",
]

PREFIX_TAG_PATTERN = re.compile(r"^\s*[\[【(](단독|종합|속보|긴급|인터뷰|분석)[\]】)]\s*")
SOURCE_SUFFIX_PATTERN = re.compile(
    r"\s*(?:(?:-|–|—|\|)\s*)+(" + "|".join(re.escape(source) for source in SOURCE_SUFFIXES) + r")\s*$",
    re.IGNORECASE,
)
GENERIC_MEDIA_SUFFIX_PATTERN = re.compile(
    r"\s*(?:(?:-|–|—|\|)\s*)+([^|–—-]{2,45}(?:뉴스|신문|경제|일보|투데이|데일리|비즈|타임스|저널|미디어|TV|닷컴|\\.com|\\.co\\.kr))\s*$",
    re.IGNORECASE,
)
WHITESPACE_PATTERN = re.compile(r"\s+")
QUOTE_PATTERN = re.compile(r"[\"'“”‘’`´]")
NOISY_PUNCT_PATTERN = re.compile(r"[<>「」『』《》〈〉]")
HTML_TAG_PATTERN = re.compile(r"</?[^>]+>")
URL_IN_BYTES_PATTERN = re.compile(rb"https?://[^\x00-\x20\x80-\xff]+")


def stable_hash(value: str, length: int = 24) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def decode_google_redirect_url(url: str) -> str:
    """Unwrap common Google Alerts / Google URL redirect forms."""
    if not url:
        return ""

    stripped = html.unescape(url.strip())
    parsed = urlsplit(stripped)
    hostname = (parsed.hostname or "").lower()

    if hostname.endswith("google.com") and parsed.path in {"/url", "/alerts/redirect"}:
        params = dict(parse_qsl(parsed.query, keep_blank_values=True))
        for key in ("url", "q", "u"):
            target = params.get(key)
            if target:
                return unquote(target)

    if hostname == "news.google.com":
        decoded = decode_google_news_url(parsed.path)
        if decoded:
            return decoded

    return stripped


def decode_google_news_url(path: str) -> str | None:
    """Extract the embedded article URL from Google News RSS article URLs."""
    if not path:
        return None
    article_id = path.rstrip("/").split("/")[-1]
    if not article_id or article_id in {"articles", "read"}:
        return None
    try:
        payload = base64.urlsafe_b64decode(article_id + "=" * ((4 - len(article_id) % 4) % 4))
    except (ValueError, base64.binascii.Error):
        return None
    matches = URL_IN_BYTES_PATTERN.findall(payload)
    if not matches:
        return None
    return matches[0].decode("utf-8", errors="ignore")


def normalize_url(url: str) -> str:
    unwrapped = decode_google_redirect_url(url)
    if not unwrapped:
        return ""

    parsed = urlsplit(unwrapped)
    if not parsed.scheme or not parsed.netloc:
        return unwrapped.strip()

    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    port = parsed.port
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{hostname}:{port}"
    else:
        netloc = hostname

    path = parsed.path or ""
    if path == "/":
        path = ""
    elif path.endswith("/"):
        path = path.rstrip("/")

    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in TRACKING_QUERY_PARAMS
    ]
    query = urlencode(sorted(query_items), doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def canonical_url_hash(url: str) -> str:
    return stable_hash(normalize_url(url))


def hostname_from_url(url: str) -> str:
    parsed = urlsplit(url)
    return (parsed.hostname or "").lower()


def extract_title_prefixes(title: str) -> tuple[list[str], str]:
    prefixes: list[str] = []
    remaining = title
    while True:
        match = PREFIX_TAG_PATTERN.match(remaining)
        if not match:
            break
        prefixes.append(match.group(1))
        remaining = remaining[match.end() :]
    return prefixes, remaining.strip()


def strip_media_suffix(title: str) -> tuple[str, str | None]:
    remaining = title.strip()
    source_suffix: str | None = None
    while True:
        match = SOURCE_SUFFIX_PATTERN.search(remaining) or GENERIC_MEDIA_SUFFIX_PATTERN.search(remaining)
        if not match:
            break
        source_suffix = match.group(1).strip()
        remaining = remaining[: match.start()].strip()
    return remaining, source_suffix


def clean_title_text(title: str) -> str:
    decoded = html.unescape(title or "")
    decoded = HTML_TAG_PATTERN.sub("", decoded)
    decoded = decoded.replace("\xa0", " ")
    decoded = QUOTE_PATTERN.sub("", decoded)
    decoded = NOISY_PUNCT_PATTERN.sub(" ", decoded)
    decoded = WHITESPACE_PATTERN.sub(" ", decoded)
    return decoded.strip(" \t\r\n-–—|")


def normalize_title_parts(raw_title: str) -> dict[str, object]:
    decoded = html.unescape(raw_title or "")
    prefixes, without_prefix = extract_title_prefixes(decoded)
    without_suffix, source_suffix = strip_media_suffix(without_prefix)
    cleaned = clean_title_text(without_suffix)
    normalized = cleaned.casefold()
    normalized = re.sub(r"[\[\]{}()!?,.:;·ㆍ/\\|]+", " ", normalized)
    normalized = WHITESPACE_PATTERN.sub(" ", normalized).strip()
    return {
        "raw_title": raw_title or "",
        "prefixes": prefixes,
        "source_suffix": source_suffix,
        "clean_title": cleaned,
        "normalized_title": normalized,
        "title_hash": stable_hash(normalized),
    }


def normalize_title(raw_title: str) -> str:
    return str(normalize_title_parts(raw_title)["normalized_title"])
