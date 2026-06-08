from __future__ import annotations

from curator.relevance import classify_relevance


def test_capital_market_recall_terms_are_kept_relevant() -> None:
    assert classify_relevance(
        "\uc77c\uc131\uc544\uc774\uc5d0\uc2a4, \uc9c0\ubc30\uad6c\uc870 \ud575\uc2ec\uc9c0\ud45c 15\uac1c \uc911 13\uac1c \ubbf8\uc900\uc218",
        "",
    ) == "medium"
    assert classify_relevance(
        "\uae08\uc735\uacc4\uc5f4 \uc2b9\uacc4 \uc2dc\uc791\ub410\ub2e4",
        "",
    ) in {"high", "medium"}
    assert classify_relevance(
        "DB\uc190\ubcf4, \uc2e4\uc801 \ubd80\uc9c4\uc5d0\ub3c4 \ubc30\ub2f9\uc740 \ub298\ub9b0\ub2e4",
        "\uace0\ubc30\ub2f9\uacfc \ubc30\ub2f9\uc131\ud5a5, \uc0c8 \ubc38\ub958\uc5c5 \uc815\ucc45 \uae30\ub300",
    ) in {"high", "medium"}
