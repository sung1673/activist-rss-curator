from __future__ import annotations

from curator.relevance import classify_relevance


def test_high_medium_low_relevance() -> None:
    assert classify_relevance("KCGI 공개서한에 이사회 교체 요구", "") == "high"
    assert classify_relevance("신한금융 밸류업 2.0 주주환원 확대", "") == "medium"
    assert classify_relevance("특징주 장중 목표가 상향 리포트 브리핑", "") == "low"
    assert classify_relevance("증시요약(5) - 특징 종목(코스닥)", "") == "low"
