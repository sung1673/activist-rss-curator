from __future__ import annotations

from curator.story_signature import event_tokens_for_text, story_signature_decision


def test_generic_rights_issue_refiling_signature_does_not_require_specific_company() -> None:
    tokens = event_tokens_for_text("ABC바이오, 유상증자 관련 정정신고서 제출…금감원 제동 뒤 재추진")

    assert "유증정정" in tokens
    assert "금감원정정요구" in tokens


def test_generic_minority_shareholder_group_controversy_signature() -> None:
    tokens = event_tokens_for_text("소액주주 단체, 단체명 혼용에 대표성 논란…배후 의혹도 제기")

    assert "소액주주단체논란" in tokens
    assert "단체실체논란" in tokens


def test_duplicate_listing_policy_signature_without_company_name() -> None:
    left_tokens = event_tokens_for_text("중복상장, 주주 동의 어떻게 받을까? 모회사 주주 동의 범위 쟁점")
    right_tokens = event_tokens_for_text("원칙 금지·예외 허용 중복상장 가닥...주주보호 장치 쟁점")

    decision = story_signature_decision(
        "중복상장, 주주 동의 어떻게 받을까?...MoM 놓고 기관과 PE·증권사 격돌",
        "원칙 금지·예외 허용 중복상장 가닥...주주보호 장치 쟁점",
        left_event_tokens=left_tokens,
        right_event_tokens=right_tokens,
        title_score=35,
    )

    assert {"중복상장규제", "주주동의", "모회사주주"} & set(left_tokens)
    assert {"중복상장규제", "주주보호장치"} <= set(right_tokens)
    assert decision.same_story
    assert decision.reason == "duplicate_listing_policy_signature"


def test_story_signature_allows_same_company_contextual_event_low_title_overlap() -> None:
    left_tokens = event_tokens_for_text("한화솔루션, 1.8조 유증 또 줄이진 않아…금감원 제동 뒤 재추진")
    right_tokens = event_tokens_for_text("두 차례 반려 한화솔루션, 유상증자 관련 정정신고서 제출")

    decision = story_signature_decision(
        "한화솔루션, 1.8조 유증 또 줄이진 않아…금감원 제동 뒤 재추진",
        "두 차례 반려 한화솔루션, 유상증자 관련 정정신고서 제출",
        left_companies=["한화솔루션"],
        right_companies=["한화솔루션"],
        left_event_tokens=left_tokens,
        right_event_tokens=right_tokens,
        title_score=47.7,
    )

    assert decision.same_story
    assert decision.reason == "same_company_contextual_event"


def test_story_signature_blocks_same_company_without_specific_event_overlap() -> None:
    decision = story_signature_decision(
        "고려아연 소액주주 단체 배후설 논란",
        "고려아연 황산 거래 중단은 경영권 분쟁 수단",
        left_companies=["고려아연"],
        right_companies=["고려아연"],
        left_event_tokens=event_tokens_for_text("고려아연 소액주주 단체 배후설 논란"),
        right_event_tokens=event_tokens_for_text("고려아연 황산 거래 중단은 경영권 분쟁 수단"),
        title_score=45,
    )

    assert not decision.same_story
