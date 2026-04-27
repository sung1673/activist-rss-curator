from __future__ import annotations

from curator.relevance import classify_relevance


def test_high_medium_low_relevance() -> None:
    assert classify_relevance("KCGI 공개서한에 이사회 교체 요구", "") == "high"
    assert classify_relevance("신한금융 밸류업 2.0 주주환원 확대", "") == "medium"
    assert classify_relevance("중복상장 심사 강화, 모회사 주주 보호 의무화", "") == "high"
    assert classify_relevance("금감원, 유상증자 정정신고서 중점 심사", "") == "medium"
    assert classify_relevance("주주활동 길 터준다 금융위 5%룰 기준 명확화", "") == "high"
    assert classify_relevance("특징주 장중 목표가 상향 리포트 브리핑", "") == "low"
    assert classify_relevance("증시요약(5) - 특징 종목(코스닥)", "") == "low"
    assert classify_relevance("로저스 커뮤니케이션, 주총서 이사 14명·KPMG 감사인 선임 승인", "") == "low"
    assert classify_relevance(
        "법무법인 동인, 제4대 경영대표변호사에 원창연 변호사 선출",
        "경영권 분쟁부터 해외 딜까지 기업 자문 M&A 역량 확대",
    ) == "low"
    assert classify_relevance(
        "한미사이언스, 경영권 분쟁 일단락 후 실적 발표 앞두고 보합권 혼조세",
        "",
    ) == "low"
    assert classify_relevance(
        "영풍, 고려아연 경영권 분쟁 심화와 비철금속 섹터 강세 속 1%대 반등 성공",
        "",
    ) == "low"
