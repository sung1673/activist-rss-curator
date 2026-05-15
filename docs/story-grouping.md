# 기사 묶음 판정 운영 메모

`news.bside.ai`의 기사 묶음은 제목 유사도만으로 판단하지 않는다. 같은 회사의 넓은 주제만으로 묶으면 오탐이 늘고, 제목 표현이 다른 같은 사건은 누락될 수 있기 때문이다.

## 기본 흐름

1. 기사 제목과 요약에서 사건 시그니처를 추출한다.
   - 기업/기관명
   - 사건 토큰
   - 규제기관/절차/행위 표현
   - 제목 유사도
2. 기존 RapidFuzz 제목 유사도와 사건 시그니처 overlap을 함께 계산한다.
3. 같은 회사와 구체적 사건 토큰이 충분히 겹치면 같은 story 후보로 본다.
4. 같은 회사라도 사건 토큰이 다르면 별도 story로 둔다.
5. 애매한 후보는 GitHub Models 기반 `story_judge`가 보수적으로 판정한다.

## 운영 규칙 추가

자주 반복되는 사건 패턴은 Python 코드가 아니라 `data/story_rules.yaml`에 추가한다.

예:

```yaml
rules:
  - id: example_regulator_refiling
    tokens:
      - 정정신고서
      - 감독기관정정요구
    require_any_groups:
      - [유상증자, 유증]
      - [정정신고서, 정정, 반려, 감독기관]
```

각 `require_any_groups`는 그룹마다 하나 이상의 표현이 기사 텍스트에 있어야 한다. 이 방식은 특정 회사명에 고정되지 않으므로 향후 유사 케이스에도 재사용된다.

## 주의사항

- `밸류업`, `주주환원`, `지배구조`, `소액주주` 같은 넓은 단어만으로는 같은 사건으로 묶지 않는다.
- `같은 회사 + 같은 구체 이벤트`가 핵심 조건이다.
- 규칙을 추가한 뒤에는 최소한 클러스터 테스트와 요약 테스트를 돌린다.

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_story_signature.py tests/test_cluster.py tests/test_summaries.py
```
