from __future__ import annotations

import hashlib
import hmac
import io
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Iterable, Iterator, Mapping, Protocol
from urllib.parse import urlencode, urlparse
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import httpx

from .dart_quota import (
    DartCredentialUnavailableError,
    DartGlobalQuotaExceededError,
)
from .event_identity import (
    EventIdentity,
    EventIdentityMatch,
    EventIdentityStatus,
    compare_event_identities,
    event_identity_from_mapping,
    normalize_identity_text,
)
from .governance import GovernanceEventType, stable_id
from .opendart_credentials import (
    DartCredentialAvailability,
    OpenDartCredential,
    OpenDartCredentialPool,
)


DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DART_CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
DART_VIEWER_URL = "https://dart.fss.or.kr/dsaf001/main.do"
KIND_VIEWER_URL = "https://kind.krx.co.kr/common/disclsviewer.do"


class OfficialSourceError(RuntimeError):
    pass


class DartQuotaExceededError(OfficialSourceError):
    """OpenDART status 020; the caller must resume in a later quota period."""


class DartRequestBudgetError(OfficialSourceError):
    """The bounded per-process OpenDART request budget was exhausted."""


class DartRequestQuota(Protocol):
    limit: int
    used: int

    def consume(
        self,
        *,
        operation: str = "list",
        credential_id: str,
    ) -> object: ...

    def block_020(self, permit: object) -> None: ...

    def disable_901(self, permit: object) -> None: ...


def validate_kind_endpoint(endpoint: str) -> str:
    """Return a credential-safe production KIND adapter URL.

    Authentication is sent in an Authorization header, so plaintext HTTP,
    URL credentials, and query/fragment secrets are rejected before a client
    can make a request. Tests use an HTTPS origin with ``MockTransport``.
    """

    if not endpoint or endpoint != endpoint.strip() or any(ord(char) < 32 for char in endpoint):
        raise ValueError("KIND endpoint must be a clean absolute HTTPS URL")
    try:
        parsed = urlparse(endpoint)
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("KIND endpoint must be a valid absolute HTTPS URL") from exc
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or bool(parsed.params)
        or bool(parsed.query)
        or bool(parsed.fragment)
    ):
        raise ValueError(
            "KIND endpoint must use HTTPS without URL credentials, parameters, query, or fragment"
        )
    return endpoint


@dataclass
class DartRequestBudget:
    """Per-invocation safety budget, distinct from the durable 40k KST-day pool."""

    limit: int = 10_000
    used: int = 0

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("DART request budget must be at least 1")
        if self.used < 0 or self.used > self.limit:
            raise ValueError("DART request budget usage is invalid")

    def consume(
        self,
        *,
        operation: str = "list",
        credential_id: str = "",
    ) -> None:
        if operation not in {"list", "corp_code"}:
            raise ValueError("unsupported DART quota operation")
        if credential_id and re.fullmatch(r"[0-9a-f]{64}", credential_id) is None:
            raise ValueError("invalid DART credential identity")
        if self.used >= self.limit:
            raise DartRequestBudgetError(
                f"OpenDART request budget exhausted ({self.used}/{self.limit})"
            )
        self.used += 1

    def block_020(self, permit: object) -> None:
        # The local budget remains useful for unit tests and explicitly local
        # development. Production workflows require the durable MySQL-backed
        # implementation, which overrides this hook and records the day block.
        del permit

    def disable_901(self, permit: object) -> None:
        del permit


class DartInvocationQuota:
    """Apply a smaller invocation cap in front of a durable daily ledger."""

    def __init__(self, delegate: DartRequestQuota, *, limit: int) -> None:
        if limit < 1:
            raise ValueError("DART invocation request budget must be at least 1")
        self._delegate = delegate
        self.limit = limit
        self.used = 0

    def consume(
        self,
        *,
        operation: str = "list",
        credential_id: str,
    ) -> object:
        if self.used >= self.limit:
            raise DartRequestBudgetError(
                f"OpenDART request budget exhausted ({self.used}/{self.limit})"
            )
        permit = self._delegate.consume(
            operation=operation,
            credential_id=credential_id,
        )
        # Count only a durably acknowledged physical-request permit. A
        # pre-blocked credential rejection therefore does not consume the
        # invocation safety budget.
        self.used += 1
        return permit

    def block_020(self, permit: object) -> None:
        self._delegate.block_020(permit)

    def disable_901(self, permit: object) -> None:
        self._delegate.disable_901(permit)


EVENT_PATTERNS: tuple[tuple[GovernanceEventType, tuple[str, ...]], ...] = (
    (
        GovernanceEventType.FIVE_PERCENT_HOLDING,
        (
            "주식등의대량보유상황보고서",
            "주식등의 대량보유상황보고서",
            "대량보유 상황보고",
            "대량보유상황보고",
            "report of significant holding",
        ),
    ),
    (GovernanceEventType.TENDER_OFFER, ("공개매수", "tender offer")),
    (GovernanceEventType.SHAREHOLDER_PROPOSAL, ("주주제안", "주주 제안", "의안상정", "shareholder proposal")),
    (GovernanceEventType.GENERAL_MEETING, ("주주총회", "소집공고", "소집결의", "의결권대리행사", "proxy solicitation")),
    (
        GovernanceEventType.EXECUTIVE_COMPENSATION,
        (
            "임원보수",
            "이사보수",
            "이사 보수",
            "보수한도",
            "주식매수선택권부여",
            "주식매수선택권 부여",
            "executive compensation",
            "director compensation",
        ),
    ),
    (GovernanceEventType.TREASURY_SHARES, ("자기주식", "자사주", "treasury share", "share buyback")),
    (GovernanceEventType.DIVIDEND, ("배당", "현금ㆍ현물배당", "dividend")),
    (GovernanceEventType.DUPLICATE_LISTING, ("중복상장", "자회사 상장", "동시상장", "duplicate listing")),
    (GovernanceEventType.MERGER, ("합병", "주식교환", "주식이전", "merger")),
    (GovernanceEventType.SPLIT, ("분할", "물적분할", "인적분할", "spin-off", "split-off")),
    (GovernanceEventType.RIGHTS_ISSUE, ("유상증자", "신주발행", "rights issue")),
    (GovernanceEventType.CONVERTIBLE_BOND, ("전환사채", "cb 발행", "convertible bond")),
    (GovernanceEventType.BOND_WITH_WARRANT, ("신주인수권부사채", "bw 발행", "bond with warrant")),
    (GovernanceEventType.EXCHANGEABLE_BOND, ("교환사채", "eb 발행", "exchangeable bond")),
    (GovernanceEventType.TRADING_SUSPENSION, ("거래정지", "매매거래정지", "trading suspension")),
    (GovernanceEventType.DELISTING, ("상장폐지", "상장적격성", "상장심사", "delisting")),
    (GovernanceEventType.VALUE_UP, ("기업가치 제고", "밸류업", "value-up", "value up")),
    (
        GovernanceEventType.BOARD,
        (
            "사외이사",
            "감사위원회위원",
            "감사위원 선임",
            "감사위원 해임",
            "대표이사변경",
            "대표이사 변경",
            "이사회구성",
            "이사회 구성",
            "director appointment",
            "outside director",
            "audit committee member",
            "board composition",
        ),
    ),
)


# OpenDART disclosure detail types documented for list.json filters. The
# standard list response omits these fields, but detail-filtered/enriched rows
# and configured KIND adapters may retain them. Only unambiguous types are
# mapped; C004/E003 and broad periodic/market categories still need title/body
# evidence.
DETAIL_TYPE_EVENTS: dict[str, GovernanceEventType] = {
    "D001": GovernanceEventType.FIVE_PERCENT_HOLDING,
    "D003": GovernanceEventType.GENERAL_MEETING,
    "D004": GovernanceEventType.TENDER_OFFER,
    "E001": GovernanceEventType.TREASURY_SHARES,
    "E002": GovernanceEventType.TREASURY_SHARES,
    "E004": GovernanceEventType.EXECUTIVE_COMPENSATION,
    "E005": GovernanceEventType.BOARD,
    "E006": GovernanceEventType.GENERAL_MEETING,
}
DETAIL_TYPE_EXCLUSIONS = {"D002", "D005"}
DART_GOVERNANCE_DETAIL_CODES = tuple(DETAIL_TYPE_EVENTS)

ENDPOINT_TYPE_EVENTS: dict[str, GovernanceEventType] = {
    "majorstock": GovernanceEventType.FIVE_PERCENT_HOLDING,
    "exctvsttus": GovernanceEventType.BOARD,
    "hmvauditallsttus": GovernanceEventType.EXECUTIVE_COMPENSATION,
    "hmvauditindvdlbysttus": GovernanceEventType.EXECUTIVE_COMPENSATION,
    "hmvauditindvdlbysttusv2": GovernanceEventType.EXECUTIVE_COMPENSATION,
    "unrstexctvmendngsttus": GovernanceEventType.EXECUTIVE_COMPENSATION,
}
ENDPOINT_TYPE_EXCLUSIONS = {"elestock"}

METADATA_DETAIL_FIELDS = (
    "pblntf_detail_ty",
    "disclosure_detail_type",
    "disclosure_detail_code",
    "detail_type_code",
    "detail_code",
)
METADATA_EVENT_FIELDS = ("governance_event_type", "event_type", "document_type")
METADATA_ENDPOINT_FIELDS = ("api_endpoint", "endpoint", "api_name", "source_endpoint")
METADATA_LABEL_FIELDS = (
    "disclosure_type_name",
    "disclosure_detail_name",
    "detail_type_name",
    "report_type_name",
    "category_name",
)

METADATA_EVENT_ALIASES: dict[str, GovernanceEventType] = {
    "large_shareholding": GovernanceEventType.FIVE_PERCENT_HOLDING,
    "significant_holding": GovernanceEventType.FIVE_PERCENT_HOLDING,
    "majorstock": GovernanceEventType.FIVE_PERCENT_HOLDING,
    "proxy_solicitation": GovernanceEventType.GENERAL_MEETING,
    "outside_director": GovernanceEventType.BOARD,
    "director_appointment": GovernanceEventType.BOARD,
    "board_change": GovernanceEventType.BOARD,
    "director_compensation": GovernanceEventType.EXECUTIVE_COMPENSATION,
    "stock_option_grant": GovernanceEventType.EXECUTIVE_COMPENSATION,
}
METADATA_EVENT_EXCLUSIONS = {
    "executive_ownership",
    "insider_ownership",
    "executive_trading_plan",
    "major_shareholder_ownership",
}


REVISION_PATTERN = re.compile(
    r"^\s*(?:\[(?:기재정정|첨부정정|첨부추가|변경등록|연장결정|발행조건확정|정정|철회|취소)\]|(?:기재|첨부)?정정(?:\s*[:：-]\s*|\s+|$)|철회(?:\s*[:：-]\s*|\s+|$)|취소(?:\s*[:：-]\s*|\s+|$)|(?:correction|amend(?:ed|ment)?|withdraw(?:n|al)?|cancel(?:led|lation)?)(?:\s*[:\-]\s*|\s+|$))\s*",
    re.IGNORECASE,
)
CANCEL_PATTERN = re.compile(r"철회|취소|withdraw|cancel", re.IGNORECASE)
CORRECTION_TITLE_PATTERN = re.compile(
    r"^\s*(?:\[(?:기재정정|첨부정정|첨부추가|변경등록|연장결정|발행조건확정|정정)\]|(?:기재|첨부)?정정(?:\s*[:：-]\s*|\s+|$)|correction(?:\s*[:\-]\s*|\s+|$)|amend(?:ed|ment)?(?:\s*[:\-]\s*|\s+|$))",
    re.IGNORECASE,
)
DART_REMARK_FLAGS_PATTERN = re.compile(r"^[유코채넥공연정철]+$")


def _dart_remark_has_flag(remarks: str, flag: str) -> bool:
    compact = re.sub(r"\s+", "", remarks)
    if DART_REMARK_FLAGS_PATTERN.fullmatch(compact):
        return flag in compact
    if flag == "정":
        return "본보고서제출후정정신고" in compact
    if flag == "철":
        return "본보고서는철회" in compact or "철회(간주)" in compact
    return False


def original_language(text: str) -> str:
    # Classification only: the input is never translated or rewritten.
    if re.search(r"[가-힣]", text):
        return "ko"
    if re.search(r"[A-Za-z]", text):
        return "en"
    return "und"


def _classify_title(title: str) -> GovernanceEventType | None:
    folded = title.casefold()
    for event_type, keywords in EVENT_PATTERNS:
        if any(keyword.casefold() in folded for keyword in keywords):
            return event_type
    return None


def _metadata_layers(metadata: Mapping[str, object]) -> list[Mapping[str, object]]:
    layers = [metadata]
    for field in ("metadata", "details", "detail"):
        nested = metadata.get(field)
        if isinstance(nested, dict):
            layers.append(nested)
    return layers


def _metadata_values(metadata: Mapping[str, object], fields: Iterable[str]) -> list[str]:
    values: list[str] = []
    for layer in _metadata_layers(metadata):
        for field in fields:
            value = layer.get(field)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                values.append(text)
    return values


def _normalized_metadata_token(value: object) -> str:
    return re.sub(r"[^0-9a-z_]+", "", str(value or "").strip().casefold())


def _metadata_classification(metadata: Mapping[str, object]) -> tuple[GovernanceEventType | None, bool]:
    events: set[GovernanceEventType] = set()
    excluded = False

    for value in _metadata_values(metadata, METADATA_DETAIL_FIELDS):
        code = re.sub(r"[^0-9A-Za-z]", "", value).upper()
        if code in DETAIL_TYPE_EVENTS:
            events.add(DETAIL_TYPE_EVENTS[code])
        elif code in DETAIL_TYPE_EXCLUSIONS:
            excluded = True

    for value in _metadata_values(metadata, METADATA_EVENT_FIELDS):
        token = _normalized_metadata_token(value)
        if token in METADATA_EVENT_EXCLUSIONS:
            excluded = True
            continue
        alias = METADATA_EVENT_ALIASES.get(token)
        if alias is not None:
            events.add(alias)
            continue
        try:
            parsed = GovernanceEventType(token)
        except ValueError:
            continue
        if parsed != GovernanceEventType.OTHER:
            events.add(parsed)

    for value in _metadata_values(metadata, METADATA_ENDPOINT_FIELDS):
        token = _normalized_metadata_token(value)
        if any(excluded_endpoint in token for excluded_endpoint in ENDPOINT_TYPE_EXCLUSIONS):
            excluded = True
        for endpoint, event_type in ENDPOINT_TYPE_EVENTS.items():
            if endpoint in token:
                events.add(event_type)

    keys = {str(key).casefold() for layer in _metadata_layers(metadata) for key in layer}
    if "isu_exctv_rgist_at" in keys:
        # OpenDART elestock rows describe D002 insider ownership, not D001
        # large-shareholding reports.
        excluded = True
    if (
        "report_tp" in keys
        and {"stkrt", "stkqy"} & keys
        and {"report_resn", "repror", "stkrt_irds"} & keys
    ):
        events.add(GovernanceEventType.FIVE_PERCENT_HOLDING)
    if "reprt_code" in keys and "mendng_totamt" in keys and {"ofcps", "nm", "group"} & keys:
        events.add(GovernanceEventType.EXECUTIVE_COMPENSATION)
    if (
        "reprt_code" in keys
        and "rgist_exctv_at" in keys
        and {"chrg_job", "tenure_end_on", "ofcps"} & keys
    ):
        events.add(GovernanceEventType.BOARD)

    for value in _metadata_values(metadata, METADATA_LABEL_FIELDS):
        label_event = _classify_title(value)
        if label_event is not None:
            events.add(label_event)

    if excluded or len(events) > 1:
        return None, True
    if events:
        return next(iter(events)), True
    return None, False


def classify_governance_disclosure(
    title: str,
    metadata: Mapping[str, object] | None = None,
) -> GovernanceEventType | None:
    """Classify from exact official metadata first, then conservative title rules.

    Known metadata conflicts and explicitly excluded D002/D005-style records are
    rejected instead of being guessed from a broad keyword.
    """

    title_event = _classify_title(title)
    if metadata is None:
        return title_event
    metadata_event, decisive = _metadata_classification(metadata)
    if not decisive:
        return title_event
    if metadata_event is None:
        return None
    if title_event is not None and title_event != metadata_event:
        return None
    return metadata_event


def base_disclosure_title(title: str) -> str:
    current = title
    while True:
        stripped = REVISION_PATTERN.sub("", current, count=1).strip()
        if stripped == current:
            return re.sub(r"\s+", " ", stripped)
        current = stripped


def disclosure_collection_key(
    source: str,
    corp_code: str,
    title: str,
    filer_name: str = "",
) -> str:
    parts = [source, corp_code, base_disclosure_title(title)]
    normalized_filer = re.sub(r"\s+", " ", filer_name).strip().casefold()
    if normalized_filer:
        parts.append(normalized_filer)
    return stable_id("collection", *parts, length=32)


def dart_document_url(rcept_no: str) -> str:
    return f"{DART_VIEWER_URL}?{urlencode({'rcpNo': rcept_no})}"


def kind_document_url(receipt_no: str) -> str:
    return f"{KIND_VIEWER_URL}?{urlencode({'method': 'search', 'acptno': receipt_no, 'docno': ''})}"


def _compact_market(value: object) -> str:
    return {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "OTHER"}.get(str(value or "").strip().upper(), str(value or ""))


def _dart_datetime(value: object) -> str:
    text = re.sub(r"\D", "", str(value or ""))
    if len(text) < 8:
        raise ValueError("DART receipt date must be YYYYMMDD")
    parsed = datetime.strptime(text[:8], "%Y%m%d").replace(tzinfo=timezone.utc)
    return parsed.isoformat()


def normalize_kind_datetime(value: object) -> str:
    """Normalize KIND adapter timestamps without silently accepting naive time.

    Date-only values retain the same UTC-midnight convention as DART list
    records. A time without an offset is interpreted in the exchange's local
    timezone (Asia/Seoul), while an explicit offset is honored and normalized
    to UTC.
    """

    text = str(value or "").strip()
    digits = re.sub(r"\D", "", text)
    if re.fullmatch(r"\d{8}", digits) and re.fullmatch(r"\d{4}-?\d{2}-?\d{2}", text):
        return _dart_datetime(text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("KIND received_at must be an ISO-8601 date or timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    return parsed.astimezone(timezone.utc).isoformat()


def _kind_original_url(value: object, receipt_no: str) -> str:
    candidate = str(value or "").strip()
    if candidate:
        parsed = urlparse(candidate)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return candidate
    return kind_document_url(receipt_no)


def _content_hash(*values: object) -> str:
    return hashlib.sha256("\n".join(str(value or "") for value in values).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class OfficialDisclosure:
    source: str
    receipt_no: str
    corp_code: str
    corp_name: str
    stock_code: str
    market: str
    title: str
    received_at: str
    original_url: str
    event_type: GovernanceEventType
    identity: EventIdentity
    filer_name: str = ""
    remarks: str = ""

    @property
    def document_id(self) -> str:
        return f"{self.source.casefold()}:{self.receipt_no}"

    @property
    def collection_key(self) -> str:
        return disclosure_collection_key(self.source, self.corp_code, self.title, self.filer_name)

    @property
    def is_correction(self) -> bool:
        # OpenDART rm=정 means a later correction exists; it does not make this
        # receipt itself a corrected filing. Only the report-name prefix can do
        # that for list/search rows.
        return bool(CORRECTION_TITLE_PATTERN.search(self.title))

    @property
    def is_explicit_cancellation(self) -> bool:
        return bool(CANCEL_PATTERN.search(self.title))

    @property
    def is_revision(self) -> bool:
        """Whether this receipt can advance an existing document chain.

        A withdrawal/cancellation is a lifecycle revision even when its title
        is not a DART ``정정`` report. It is linked only when the predecessor is
        unique; ambiguous chains remain separate.
        """

        # DART rm=철 describes the lifecycle of that same receipt; it is not a
        # pointer to an earlier receipt. Linking it merely because a repeated
        # filing shares the same title/filer would merge independent reports.
        return self.is_correction or self.is_explicit_cancellation

    @property
    def has_later_correction(self) -> bool:
        return self.source.casefold() == "dart" and _dart_remark_has_flag(self.remarks, "정")

    @property
    def is_withdrawn_by_remark(self) -> bool:
        return self.source.casefold() == "dart" and _dart_remark_has_flag(self.remarks, "철")

    @property
    def is_cancelled(self) -> bool:
        return bool(
            CANCEL_PATTERN.search(self.title)
            or CANCEL_PATTERN.search(self.remarks)
            or self.is_withdrawn_by_remark
        )


def parse_dart_disclosure(row: dict[str, object]) -> OfficialDisclosure | None:
    title = str(row.get("report_nm") or "")
    if not title.strip():
        raise ValueError("DART report_nm is required")
    event_type = classify_governance_disclosure(title, row)
    if event_type is None:
        return None
    receipt_no = str(row.get("rcept_no") or "").strip()
    corp_code = str(row.get("corp_code") or "").strip()
    corp_name = str(row.get("corp_name") or "").strip()
    received_date = str(row.get("rcept_dt") or receipt_no[:8]).strip()
    if (
        re.fullmatch(r"\d{14}", receipt_no) is None
        or re.fullmatch(r"\d{8}", corp_code) is None
        or re.fullmatch(r"\d{8}", received_date) is None
        or not corp_name
    ):
        raise ValueError("invalid DART rcept_no or corp_code")
    received_at = _dart_datetime(received_date)
    filer_name = str(row.get("flr_nm") or "").strip()
    return OfficialDisclosure(
        source="DART",
        receipt_no=receipt_no,
        corp_code=corp_code,
        corp_name=corp_name,
        stock_code=str(row.get("stock_code") or "").strip(),
        market=_compact_market(row.get("corp_cls")),
        title=title,
        received_at=received_at,
        original_url=dart_document_url(receipt_no),
        event_type=event_type,
        identity=event_identity_from_mapping(
            row,
            company_id=corp_code,
            event_type=event_type,
            default_action=event_type.value,
            default_actor_name=filer_name,
        ),
        filer_name=filer_name,
        remarks=str(row.get("rm") or "").strip(),
    )


def parse_kind_disclosure(row: dict[str, object]) -> OfficialDisclosure | None:
    title = str(row.get("title") or row.get("report_nm") or row.get("disclosure_title") or "")
    if not title.strip():
        raise ValueError("KIND title is required")
    event_type = classify_governance_disclosure(title, row)
    if event_type is None:
        return None
    receipt_no = str(
        row.get("acptno") or row.get("receipt_no") or row.get("rcept_no") or ""
    ).strip()
    corp_code = str(row.get("corp_code") or row.get("dart_corp_code") or "").strip()
    corp_name = str(row.get("corp_name") or row.get("company_name") or "").strip()
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,180}", receipt_no) is None
        or re.fullmatch(r"\d{8}", corp_code) is None
        or not corp_name
    ):
        raise ValueError("KIND rows require a stable receipt number and DART corp_code")
    received = row.get("received_at") or row.get("rcept_dt") or row.get("date")
    received_at = normalize_kind_datetime(received)
    filer_name = str(row.get("filer_name") or "").strip()
    return OfficialDisclosure(
        source="KIND",
        receipt_no=receipt_no,
        corp_code=corp_code,
        corp_name=corp_name,
        stock_code=str(row.get("stock_code") or row.get("isu_cd") or "").strip(),
        market=str(row.get("market") or row.get("market_name") or "").strip(),
        title=title,
        received_at=received_at,
        original_url=_kind_original_url(row.get("original_url") or row.get("url"), receipt_no),
        event_type=event_type,
        identity=event_identity_from_mapping(
            row,
            company_id=corp_code,
            event_type=event_type,
            default_action=event_type.value,
            default_actor_name=filer_name or corp_name,
        ),
        filer_name=filer_name,
        remarks=str(row.get("remarks") or row.get("rm") or "").strip(),
    )


def parse_dart_list_payload(payload: dict[str, object]) -> tuple[list[dict[str, object]], int, int]:
    status = str(payload.get("status") or "")
    if status == "013":
        return [], 0, 0
    if status == "020":
        raise DartQuotaExceededError(
            "OpenDART request quota exhausted"
        )
    if status != "000":
        safe_status = status if re.fullmatch(r"[0-9]{3}", status) else "invalid"
        raise OfficialSourceError(
            f"OpenDART list returned non-success status {safe_status}"
        )
    rows = payload.get("list")
    if not isinstance(rows, list):
        raise OfficialSourceError("OpenDART success response omitted list")
    try:
        page = int(str(payload.get("page_no") or ""))
        total_pages = int(str(payload.get("total_page") or ""))
    except ValueError as exc:
        raise OfficialSourceError("OpenDART response has invalid pagination metadata") from exc
    if page < 1 or total_pages < page:
        raise OfficialSourceError("OpenDART response has inconsistent pagination metadata")
    return [row for row in rows if isinstance(row, dict)], page, total_pages


def parse_kind_list_payload(payload: object) -> tuple[list[dict[str, object]], int, int]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)], 1, 1
    if not isinstance(payload, dict):
        raise OfficialSourceError("KIND response must be an object or list")

    if payload.get("ok") is False or payload.get("success") is False or payload.get("error"):
        raise OfficialSourceError("KIND adapter reported a failure")
    raw_status = payload.get("status")
    if raw_status is not None:
        status = str(raw_status).strip().casefold()
        if status in {"013", "no_data", "nodata", "empty"}:
            return [], 0, 0
        if status not in {"0", "000", "200", "ok", "success", "succeeded"}:
            raise OfficialSourceError("KIND adapter reported a non-success status")

    data = payload.get("data")
    containers = [payload]
    if isinstance(data, dict):
        containers.append(data)
    raw_rows: object | None = None
    for container in containers:
        for field in ("items", "list", "results"):
            if field in container:
                raw_rows = container[field]
                break
        if raw_rows is not None:
            break
    if not isinstance(raw_rows, list):
        raise OfficialSourceError("KIND response omitted a supported item list")

    pagination_containers = list(containers)
    for container in containers:
        pagination = container.get("pagination")
        if isinstance(pagination, dict):
            pagination_containers.append(pagination)

    def first_value(fields: tuple[str, ...]) -> object | None:
        for container in pagination_containers:
            for field in fields:
                if field in container and container[field] is not None:
                    return container[field]
        return None

    raw_page = first_value(("page", "page_no", "current_page"))
    raw_total_pages = first_value(("total_pages", "total_page", "last_page"))
    explicit_unpaginated = any(container.get("unpaginated") is True for container in containers)
    if raw_page is None and raw_total_pages is None and explicit_unpaginated:
        return [row for row in raw_rows if isinstance(row, dict)], 1, 1
    if raw_page is None or raw_total_pages is None:
        raise OfficialSourceError(
            "KIND object response requires page and total_pages (or unpaginated=true)"
        )
    try:
        page = int(str(raw_page))
        total_pages = int(str(raw_total_pages))
    except ValueError as exc:
        raise OfficialSourceError("KIND response has invalid pagination metadata") from exc
    if page < 1 or total_pages < page:
        raise OfficialSourceError("KIND response has inconsistent pagination metadata")
    return [row for row in raw_rows if isinstance(row, dict)], page, total_pages


def parse_corp_code_zip(content: bytes) -> list[dict[str, object]]:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        xml_names = [name for name in archive.namelist() if name.casefold().endswith(".xml")]
        if not xml_names:
            raise OfficialSourceError("OpenDART corp-code archive omitted XML")
        root = ElementTree.fromstring(archive.read(xml_names[0]))
    companies: list[dict[str, object]] = []
    for item in root.findall(".//list"):
        corp_code = (item.findtext("corp_code") or "").strip()
        corp_name = (item.findtext("corp_name") or "").strip()
        if not re.fullmatch(r"\d{8}", corp_code) or not corp_name:
            continue
        stock_code = (item.findtext("stock_code") or "").strip()
        modify_date = (item.findtext("modify_date") or "").strip()
        master_modified_at: str | None = None
        if modify_date:
            try:
                parsed_modify_date = datetime.strptime(modify_date, "%Y%m%d").date()
            except ValueError as exc:
                raise OfficialSourceError(
                    f"OpenDART corp-code row {corp_code} has an invalid modify_date"
                ) from exc
            master_modified_at = f"{parsed_modify_date.isoformat()}T00:00:00+00:00"
        listing_status = (
            "listed"
            if re.fullmatch(r"\d{6}", stock_code)
            else ("unlisted" if not stock_code else "unknown")
        )
        companies.append(
            {
                "company_id": corp_code,
                "legal_name": corp_name,
                "stock_code": stock_code,
                "market": "",
                "listing_status": listing_status,
                "master_modified_at": master_modified_at,
                "aliases": [],
            }
        )
    return companies


def _dart_credential_rejection_reason(
    error: DartCredentialUnavailableError,
    *,
    expected_credential_id: str,
) -> str | None:
    """Recognize only the durable ledger's credential-scoped rejection.

    Class and two validated, non-secret attributes form the closed contract;
    arbitrary quota failures are never swallowed.
    """

    credential_id = error.credential_id
    reason = error.reason
    if (
        not isinstance(credential_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", credential_id) is None
        or not hmac.compare_digest(credential_id, expected_credential_id)
        or reason not in {"blocked_020", "disabled_901"}
    ):
        return None
    return str(reason)


def _opendart_provider_status(response: httpx.Response) -> str:
    """Extract only a short provider status from JSON or bounded error XML."""

    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        status = str(payload.get("status") or "")
        return status if re.fullmatch(r"[0-9]{3}", status) else ""

    content = response.content
    if not content or len(content) > 8_192:
        return ""
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError:
        return ""
    status = str(root.findtext(".//status") or root.findtext("status") or "").strip()
    return status if re.fullmatch(r"[0-9]{3}", status) else ""


class DartConnector:
    def __init__(
        self,
        api_key: str | Iterable[OpenDartCredential],
        *,
        client: httpx.Client | None = None,
        timeout: float = 20.0,
        governance_detail_codes: Iterable[str] | None = None,
        request_budget: DartRequestQuota | None = None,
        credential_availability: DartCredentialAvailability | None = None,
        quota_day_provider: Callable[[], date] | None = None,
        max_retries: int = 2,
        backoff_seconds: float = 1.0,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        credentials: tuple[OpenDartCredential, ...]
        if isinstance(api_key, str):
            value = api_key.strip()
            if not value:
                raise ValueError("DART API key is required")
            credentials = (OpenDartCredential(value, validate=False),)
        else:
            credentials = tuple(api_key)
            if not credentials or any(
                not isinstance(credential, OpenDartCredential)
                for credential in credentials
            ):
                raise ValueError("DART credential pool is required")
        self._credential_pool = OpenDartCredentialPool(
            credentials,
            availability=credential_availability,
        )
        self._quota_day_provider = quota_day_provider or (
            lambda: datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Seoul")).date()
        )
        self._client = client
        self.timeout = timeout
        if max_retries < 0:
            raise ValueError("DART max_retries cannot be negative")
        if backoff_seconds < 0:
            raise ValueError("DART backoff_seconds cannot be negative")
        self.request_budget = request_budget or DartRequestBudget()
        self.max_retries = max_retries
        self.backoff_seconds: float = backoff_seconds
        self._sleeper = sleeper
        requested_codes = DART_GOVERNANCE_DETAIL_CODES if governance_detail_codes is None else tuple(governance_detail_codes)
        normalized_codes = tuple(re.sub(r"[^0-9A-Za-z]", "", str(code)).upper() for code in requested_codes)
        unsupported = [code for code in normalized_codes if code not in DETAIL_TYPE_EVENTS]
        if unsupported:
            raise ValueError(f"unsupported governance DART detail codes: {', '.join(unsupported)}")
        self.governance_detail_codes = tuple(dict.fromkeys(normalized_codes))
        self.list_requests = 0
        self.requests_made = 0
        self.pages_fetched = 0
        self.rows_fetched = 0
        self.credential_requests: dict[str, int] = {
            credential_id: 0
            for credential_id in self._credential_pool.credential_ids
        }

    def _retry_delay(self, response: httpx.Response | None, attempt: int) -> float:
        retry_after = (
            str(response.headers.get("Retry-After", ""))
            if response is not None
            else ""
        )
        try:
            requested_delay = float(retry_after)
        except ValueError:
            requested_delay = 0.0
        exponential = self.backoff_seconds * (2**attempt)
        return float(min(30.0, max(exponential, requested_delay, 0.0)))

    def _get(self, url: str, params: dict[str, str | int]) -> httpx.Response:
        operation = "corp_code" if url == DART_CORP_CODE_URL else "list"
        quota_day = self._quota_day_provider()
        while True:
            credential = self._credential_pool.next(quota_day)
            if credential is None:
                if self._credential_pool.unavailable_reason(quota_day) == "blocked_020":
                    raise DartQuotaExceededError(
                        "OpenDART quota exhausted: all credentials are unavailable for the "
                        "current KST quota day; resume from the checkpoint later"
                    )
                raise OfficialSourceError("All OpenDART credentials are unavailable")

            credential_id = credential.credential_id
            request_params = dict(params)
            request_params["crtfc_key"] = credential.key
            retry_with_next_credential = False
            for attempt in range(self.max_retries + 1):
                # A permit is bound to the non-secret credential identity.
                # Durable state rejection for one credential is skipped while
                # global ledger failures remain fail-closed.
                try:
                    permit = self.request_budget.consume(
                        operation=operation,
                        credential_id=credential_id,
                    )
                except DartCredentialUnavailableError as exc:
                    reason = _dart_credential_rejection_reason(
                        exc,
                        expected_credential_id=credential_id,
                    )
                    if reason == "blocked_020":
                        self._credential_pool.block_for_day(credential_id, quota_day)
                        retry_with_next_credential = True
                        break
                    if reason == "disabled_901":
                        self._credential_pool.disable(credential_id)
                        retry_with_next_credential = True
                        break
                    raise
                except DartGlobalQuotaExceededError:
                    # A global daily-ledger rejection is not a connector
                    # transport failure. Surface the stable quota-exhausted
                    # contract so ingestion checkpoints resume next quota day.
                    raise DartQuotaExceededError(
                        "OpenDART global quota exhausted; resume from the "
                        "checkpoint in the next KST quota period"
                    ) from None

                self.requests_made += 1
                self.credential_requests[credential_id] += 1
                try:
                    if self._client is not None:
                        response = self._client.get(url, params=request_params)
                    else:
                        response = httpx.get(
                            url,
                            params=request_params,
                            timeout=self.timeout,
                            follow_redirects=True,
                        )
                except httpx.TransportError:
                    if attempt >= self.max_retries:
                        raise OfficialSourceError("OpenDART transport error") from None
                    self._sleeper(self._retry_delay(None, attempt))
                    continue

                retryable_status = (
                    response.status_code == 429
                    or 500 <= response.status_code <= 599
                )
                if retryable_status and attempt < self.max_retries:
                    self._sleeper(self._retry_delay(response, attempt))
                    continue
                if response.status_code >= 400:
                    # httpx's default exception renders the full request URL,
                    # including OpenDART's crtfc_key query parameter. Never let
                    # a provider secret or response body reach Actions logs.
                    raise OfficialSourceError(
                        f"OpenDART HTTP {response.status_code}"
                    )

                provider_status = _opendart_provider_status(response)
                if provider_status == "020":
                    # The durable ACK is required before this process excludes
                    # the credential and retries the exact logical request.
                    self.request_budget.block_020(permit)
                    self._credential_pool.block_for_day(credential_id, quota_day)
                    retry_with_next_credential = True
                    break
                if provider_status == "901":
                    self.request_budget.disable_901(permit)
                    self._credential_pool.disable(credential_id)
                    retry_with_next_credential = True
                    break
                return response

            if retry_with_next_credential:
                continue
            raise AssertionError("unreachable DART request retry state")

    def _iter_list_rows(
        self,
        start: date,
        end: date,
        *,
        page_count: int = 100,
        max_pages: int = 100,
        detail_code: str = "",
    ) -> Iterator[dict[str, object]]:
        page = 1
        rows_seen = 0
        expected_total_count: int | None = None
        expected_total_pages: int | None = None
        while page <= max(1, max_pages):
            params: dict[str, str | int] = {
                "bgn_de": start.strftime("%Y%m%d"),
                "end_de": end.strftime("%Y%m%d"),
                "last_reprt_at": "N",
                "sort": "date",
                "sort_mth": "asc",
                "page_no": page,
                "page_count": min(100, max(1, page_count)),
            }
            if detail_code:
                params["pblntf_detail_ty"] = detail_code
            self.list_requests += 1
            response = self._get(
                DART_LIST_URL,
                params,
            )
            raw_payload = response.json()
            if not isinstance(raw_payload, dict):
                raise OfficialSourceError("OpenDART response must be a JSON object")
            rows, current_page, total_pages = parse_dart_list_payload(raw_payload)
            self.pages_fetched += 1
            self.rows_fetched += len(rows)
            if (current_page, total_pages) == (0, 0):
                if page != 1:
                    raise OfficialSourceError(
                        f"OpenDART returned no-data status while requesting page {page}"
                    )
                return
            if current_page != page:
                raise OfficialSourceError(
                    f"OpenDART requested page {page} but received page {current_page}"
                )
            if expected_total_pages is None:
                expected_total_pages = total_pages
            elif total_pages != expected_total_pages:
                raise OfficialSourceError(
                    "OpenDART total_page changed while paginating; retry the date window"
                )
            raw_total_count = raw_payload.get("total_count")
            if raw_total_count is not None:
                try:
                    total_count = int(str(raw_total_count))
                except ValueError as exc:
                    raise OfficialSourceError("OpenDART response has invalid total_count") from exc
                if total_count < 0:
                    raise OfficialSourceError("OpenDART response has invalid total_count")
                if expected_total_count is None:
                    expected_total_count = total_count
                elif total_count != expected_total_count:
                    raise OfficialSourceError(
                        "OpenDART total_count changed while paginating; retry the date window"
                    )
            if not rows:
                raise OfficialSourceError(
                    f"OpenDART returned an empty page {page} with success status"
                )
            next_rows_seen = rows_seen + len(rows)
            if current_page >= total_pages and expected_total_count is not None:
                if next_rows_seen != expected_total_count:
                    raise OfficialSourceError(
                        f"OpenDART returned {next_rows_seen} rows but reported {expected_total_count}"
                    )
            for row in rows:
                if not detail_code:
                    yield row
                    continue
                annotated = dict(row)
                returned_code = re.sub(
                    r"[^0-9A-Za-z]",
                    "",
                    str(annotated.get("pblntf_detail_ty") or detail_code),
                ).upper()
                if returned_code != detail_code:
                    raise OfficialSourceError(
                        f"OpenDART detail query {detail_code} returned conflicting type {returned_code}"
                    )
                annotated["pblntf_ty"] = str(annotated.get("pblntf_ty") or detail_code[0])
                annotated["pblntf_detail_ty"] = detail_code
                yield annotated
            rows_seen = next_rows_seen
            if current_page >= total_pages:
                return
            if page >= max(1, max_pages):
                scope = f" detail {detail_code}" if detail_code else ""
                raise OfficialSourceError(
                    f"OpenDART{scope} result truncated at page {page} of {total_pages}; reduce the date window"
                )
            page += 1

    def iter_disclosure_rows(
        self,
        start: date,
        end: date,
        *,
        page_count: int = 100,
        max_pages: int = 100,
    ) -> Iterator[dict[str, object]]:
        """Yield annotated governance rows plus unmatched broad-list rows.

        OpenDART does not echo pblntf_detail_ty in the standard list response.
        Detail-filtered calls are therefore made first and annotated with the
        requested official code. The broad call is retained for B001/A001 and
        other title-classified disclosures. Receipt numbers deduplicate the two
        paths; contradictory detail results fail closed.
        """

        receipt_details: dict[str, str] = {}
        for detail_code in self.governance_detail_codes:
            for row in self._iter_list_rows(
                start,
                end,
                page_count=page_count,
                max_pages=max_pages,
                detail_code=detail_code,
            ):
                receipt_no = re.sub(r"\D", "", str(row.get("rcept_no") or ""))
                if receipt_no:
                    previous_detail = receipt_details.get(receipt_no)
                    if previous_detail is not None and previous_detail != detail_code:
                        raise OfficialSourceError(
                            f"OpenDART receipt {receipt_no} appeared in conflicting detail types "
                            f"{previous_detail} and {detail_code}"
                        )
                    if previous_detail == detail_code:
                        continue
                    receipt_details[receipt_no] = detail_code
                yield row

        for row in self._iter_list_rows(
            start,
            end,
            page_count=page_count,
            max_pages=max_pages,
        ):
            receipt_no = re.sub(r"\D", "", str(row.get("rcept_no") or ""))
            if receipt_no and receipt_no in receipt_details:
                continue
            yield row

    def fetch_company_master(self) -> list[dict[str, object]]:
        return parse_corp_code_zip(self._get(DART_CORP_CODE_URL, {}).content)


class KindConnector:
    """Configurable KIND JSON connector.

    KRX does not publish a stable general-purpose JSON contract comparable to
    OpenDART. The endpoint therefore stays deploy-configured and its response is
    normalized behind a contract-tested adapter.
    """

    def __init__(self, endpoint: str, *, api_key: str = "", client: httpx.Client | None = None, timeout: float = 20.0) -> None:
        self.endpoint = validate_kind_endpoint(endpoint)
        self.api_key = api_key
        self._client = client
        self.timeout = timeout
        self.list_requests = 0
        self.pages_fetched = 0
        self.rows_fetched = 0

    def iter_disclosure_rows(self, start: date, end: date, *, page_count: int = 100, max_pages: int = 100) -> Iterator[dict[str, object]]:
        page = 1
        expected_total_pages: int | None = None
        expected_total_count: int | None = None
        rows_seen = 0
        while page <= max(1, max_pages):
            params: dict[str, str | int] = {
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "page": page,
                "page_size": min(100, max(1, page_count)),
            }
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else None
            self.list_requests += 1
            if self._client is not None:
                response = self._client.get(self.endpoint, params=params, headers=headers)
            else:
                response = httpx.get(
                    self.endpoint,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                    follow_redirects=True,
                )
            if response.status_code >= 400:
                # The adapter token is carried in Authorization. Keep both
                # headers and potentially hostile response bodies out of logs.
                raise OfficialSourceError(f"KIND HTTP {response.status_code}")
            raw_payload = response.json()
            rows, current_page, total_pages = parse_kind_list_payload(raw_payload)
            self.pages_fetched += 1
            self.rows_fetched += len(rows)
            if (current_page, total_pages) == (0, 0):
                if page != 1:
                    raise OfficialSourceError(
                        f"KIND returned no-data status while requesting page {page}"
                    )
                return
            if current_page != page:
                raise OfficialSourceError(
                    f"KIND requested page {page} but received page {current_page}"
                )
            if expected_total_pages is None:
                expected_total_pages = total_pages
            elif total_pages != expected_total_pages:
                raise OfficialSourceError(
                    "KIND total_pages changed while paginating; retry the date window"
                )
            if isinstance(raw_payload, dict):
                count_containers = [raw_payload]
                data = raw_payload.get("data")
                if isinstance(data, dict):
                    count_containers.append(data)
                for container in list(count_containers):
                    pagination = container.get("pagination")
                    if isinstance(pagination, dict):
                        count_containers.append(pagination)
                raw_total_count: object | None = None
                for container in count_containers:
                    for field in ("total_count", "total_items"):
                        if field in container and container[field] is not None:
                            raw_total_count = container[field]
                            break
                    if raw_total_count is not None:
                        break
                if raw_total_count is not None:
                    try:
                        total_count = int(str(raw_total_count))
                    except ValueError as exc:
                        raise OfficialSourceError("KIND response has invalid total_count") from exc
                    if total_count < 0:
                        raise OfficialSourceError("KIND response has invalid total_count")
                    if expected_total_count is None:
                        expected_total_count = total_count
                    elif total_count != expected_total_count:
                        raise OfficialSourceError(
                            "KIND total_count changed while paginating; retry the date window"
                        )
            if not rows:
                raise OfficialSourceError(
                    f"KIND returned an empty page {page} with success status"
                )
            next_rows_seen = rows_seen + len(rows)
            if current_page >= total_pages and expected_total_count is not None:
                if next_rows_seen != expected_total_count:
                    raise OfficialSourceError(
                        f"KIND returned {next_rows_seen} rows but reported {expected_total_count}"
                    )
            yield from rows
            rows_seen = next_rows_seen
            if current_page >= total_pages:
                return
            if page >= max(1, max_pages):
                raise OfficialSourceError(
                    f"KIND result truncated at page {page} of {total_pages}; reduce the date window"
                )
            page += 1


def disclosure_storage_collection_key(disclosure: OfficialDisclosure) -> str:
    """Return a canonical-event key that cannot merge incomplete identities.

    Give the compatibility adapter the cross-source comparison key only for a
    complete identity. Otherwise isolate the event observation by receipt so a
    title cannot trigger an unsafe fallback merge.
    """

    if disclosure.identity.comparison_key:
        return disclosure.identity.comparison_key
    return stable_id("eventobs", disclosure.source, disclosure.receipt_no, length=48)


def disclosure_document_collection_key(disclosure: OfficialDisclosure) -> str:
    """Return a source-specific document chain key.

    DART and KIND can observe the same canonical event, but a correction in one
    system must never name a document from the other system as its predecessor.
    """

    if disclosure.identity.comparison_key:
        return stable_id(
            "docchain",
            disclosure.source,
            disclosure.identity.comparison_key,
            length=48,
        )
    return stable_id("docobs", disclosure.source, disclosure.receipt_no, length=48)


def link_correction_versions(disclosures: Iterable[OfficialDisclosure]) -> list[tuple[OfficialDisclosure, str | None, int, str]]:
    """Link only revisions with exactly one strict-identity predecessor.

    Corrections and explicit withdrawal/cancellation receipts both advance a
    chain. Similar titles and shared collection themes are candidate discovery
    only; a missing or conflicting identity never authorizes a link. A DART row
    whose ``rm=철`` marks that same row as withdrawn remains a standalone
    version when no unique predecessor receipt exists.
    """
    candidates_by_collection: dict[str, list[tuple[str, int, str, EventIdentity]]] = {}
    linked: list[tuple[OfficialDisclosure, str | None, int, str]] = []
    ordered = sorted(disclosures, key=lambda item: (item.received_at, item.receipt_no))
    for disclosure in ordered:
        candidates = candidates_by_collection.setdefault(disclosure.collection_key, [])
        correction_of: str | None = None
        version_no = 1
        event_id = disclosure.identity.comparison_key or stable_id(
            "event", disclosure.source, disclosure.receipt_no, length=32
        )
        matching_candidates = [
            candidate
            for candidate in candidates
            if compare_event_identities(disclosure.identity, candidate[3]).same_event
        ]
        linked_to_unique_chain = disclosure.is_revision and len(matching_candidates) == 1
        if linked_to_unique_chain:
            correction_of, previous_version, event_id, _ = matching_candidates[0]
            version_no = previous_version + 1
        linked.append((disclosure, correction_of, version_no, event_id))
        current = (disclosure.document_id, version_no, event_id, disclosure.identity)
        if linked_to_unique_chain:
            candidates[candidates.index(matching_candidates[0])] = current
        else:
            # Keep incomplete, conflicting, and duplicate candidates. A later
            # correction may link only if exactly one complete identity agrees.
            candidates.append(current)
    return linked


def cross_source_identity_conflicts(
    disclosures: Iterable[OfficialDisclosure],
) -> dict[tuple[str, str], tuple[str, ...]]:
    """Flag near-identical DART/KIND identities that disagree on one fact.

    A complete identity with a different comparison key is normally a distinct
    event.  When two official systems agree on six of the seven canonical
    dimensions and disagree on exactly one, however, publishing both without a
    review would hide an official-source mismatch.  Keep both observations
    isolated and attach the conflicting field to the editorial queue.
    """

    rows = list(disclosures)
    conflicts: dict[tuple[str, str], set[str]] = {}
    for index, left in enumerate(rows):
        if left.identity.status is not EventIdentityStatus.COMPLETE:
            continue
        for right in rows[index + 1 :]:
            if right.identity.status is not EventIdentityStatus.COMPLETE:
                continue
            if left.source.casefold() == right.source.casefold():
                continue
            if {left.source.casefold(), right.source.casefold()} != {"dart", "kind"}:
                continue
            if left.corp_code != right.corp_code or left.event_type != right.event_type:
                continue
            decision = compare_event_identities(left.identity, right.identity)
            if decision.outcome is not EventIdentityMatch.DIFFERENT:
                continue
            if len(decision.conflicting_fields) != 1:
                continue
            field = decision.conflicting_fields[0]
            conflicts.setdefault((left.source, left.receipt_no), set()).add(field)
            conflicts.setdefault((right.source, right.receipt_no), set()).add(field)
    return {key: tuple(sorted(fields)) for key, fields in conflicts.items()}


def disclosure_payloads(disclosures: Iterable[OfficialDisclosure], *, retrieved_at: datetime | None = None) -> dict[str, list[dict[str, object]]]:
    retrieved = (retrieved_at or datetime.now(timezone.utc)).isoformat()
    disclosure_rows = list(disclosures)
    cross_source_conflicts = cross_source_identity_conflicts(disclosure_rows)
    companies_by_id: dict[str, dict[str, object]] = {}
    documents: list[dict[str, object]] = []
    events: list[dict[str, object]] = []
    for disclosure, correction_of, version_no, event_id in link_correction_versions(disclosure_rows):
        companies_by_id[disclosure.corp_code] = {
            "company_id": disclosure.corp_code,
            "legal_name": disclosure.corp_name,
            "stock_code": disclosure.stock_code,
            "market": disclosure.market,
            "aliases": [],
        }
        language = original_language(disclosure.title)
        storage_collection_key = disclosure_storage_collection_key(disclosure)
        document_collection_key = disclosure_document_collection_key(disclosure)
        identity_payload = disclosure.identity.to_payload()
        conflict_fields = cross_source_conflicts.get((disclosure.source, disclosure.receipt_no), ())
        identity_review_reasons = [
            *disclosure.identity.review_reasons,
            *(f"cross_source_conflict_{field}" for field in conflict_fields),
        ]
        if conflict_fields:
            identity_payload["identity_status"] = EventIdentityStatus.NEEDS_REVIEW.value
            identity_payload["identity_review_reasons"] = identity_review_reasons
        documents.append(
            {
                "document_id": disclosure.document_id,
                "external_id": disclosure.receipt_no,
                "company_id": disclosure.corp_code,
                "source_class": "official_disclosure",
                "source_right_id": f"official:{disclosure.source.casefold()}",
                "document_type": disclosure.event_type.value,
                "original_language": language,
                "title": disclosure.title,
                "metadata": {"title_provenance": "source"},
                "original_url": disclosure.original_url,
                "content_hash": _content_hash(disclosure.title, disclosure.original_url, disclosure.remarks),
                "correction_of_document_id": correction_of,
                "version_no": version_no,
                "published_at": disclosure.received_at,
                "retrieved_at": retrieved,
                "verification_status": "official",
                "publication_status": "withdrawn" if disclosure.is_cancelled else "published",
                "collection_key": document_collection_key,
                "remarks": disclosure.remarks,
                "has_later_correction": disclosure.has_later_correction,
                "is_withdrawn_by_remark": disclosure.is_withdrawn_by_remark,
                "event_comparison_key": disclosure.identity.comparison_key,
                "event_identity_status": identity_payload["identity_status"],
                "event_identity_review_reasons": identity_review_reasons,
            }
        )
        market_sensitive = disclosure.event_type in {
            GovernanceEventType.TENDER_OFFER,
            GovernanceEventType.MERGER,
            GovernanceEventType.SPLIT,
            GovernanceEventType.RIGHTS_ISSUE,
            GovernanceEventType.CONVERTIBLE_BOND,
            GovernanceEventType.BOND_WITH_WARRANT,
            GovernanceEventType.EXCHANGEABLE_BOND,
            GovernanceEventType.DELISTING,
            GovernanceEventType.TRADING_SUSPENSION,
        }
        review_required = (
            disclosure.identity.status is EventIdentityStatus.NEEDS_REVIEW
            or bool(conflict_fields)
            or market_sensitive
            or (disclosure.is_revision and correction_of is None and not disclosure.identity.comparison_key)
        )
        event_payload: dict[str, object] = {
            "event_id": event_id,
            "company_id": disclosure.corp_code,
            "event_type": disclosure.event_type.value,
            "title": disclosure.title,
            "metadata": {"title_provenance": "source"},
            "original_language": language,
            "summary": "",
            "occurred_at": disclosure.received_at,
            "deadline_at": disclosure.identity.deadline_at or None,
            "importance": "market_sensitive" if market_sensitive else "normal",
            "verification_status": "official",
            "collection_key": storage_collection_key,
            "document_ids": [disclosure.document_id],
            "is_correction": disclosure.is_correction,
            "is_cancelled": disclosure.is_cancelled,
            "has_later_correction": disclosure.has_later_correction,
            "review_required": review_required,
            "actor_id": disclosure.identity.actor_id or None,
            "action": disclosure.identity.action,
            "target": disclosure.identity.target,
        }
        # Preserve the source filer as a reviewable Actor instead of exposing
        # only the opaque identity hash.  These nested candidates travel with
        # the event through the existing governance-snapshot transport; the
        # server stores both records as pending/inactive and requires explicit
        # actor + relation approval before the event can be published.
        actor_id = disclosure.identity.actor_id
        filer_name = disclosure.filer_name
        if actor_id and filer_name:
            filer_is_company = normalize_identity_text(filer_name) == normalize_identity_text(
                disclosure.corp_name
            )
            event_payload["actor"] = {
                "actor_id": actor_id,
                "actor_type": "company" if filer_is_company else "institution",
                "display_name": filer_name,
                "company_id": disclosure.corp_code if filer_is_company else None,
                "review_status": "pending",
                "record_status": "inactive",
            }
            event_payload["event_actor"] = {
                "event_id": event_id,
                "actor_id": actor_id,
                "actor_role": "filer",
                "review_status": "pending",
            }
        event_payload.update(identity_payload)
        events.append(event_payload)
    return {"companies": list(companies_by_id.values()), "documents": documents, "events": events}
