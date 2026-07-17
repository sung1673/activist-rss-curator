# 미디어 발견 피드 범위 정책 / Media discovery feed scope policy

미디어 수집기는 네트워크 요청을 보내기 전에 `config.yaml`의 `media_feed_policy`를 적용한다. 현재 운영 설정은 fail-closed이며 `korean_governance`와 `korean_governance_context`로 분류된 피드만 요청한다. 범위가 없거나 허용 목록 밖인 피드는 기사를 받은 뒤 거르는 것이 아니라 **처음부터 가져오지 않는다**.

## 설정 계약

범위 결정 우선순위는 다음과 같다.

1. 개별 피드의 `scope`
2. `media_feed_policy.feed_scopes`의 정확한 피드명 매핑
3. `media_feed_policy.category_scopes`의 카테고리 매핑
4. `media_feed_policy.default_scope`

개별 피드의 `enabled: false`는 범위와 관계없이 즉시 수집을 중단하는 운영 차단 스위치다. `enforce: true`일 때는 최종 범위가 `allowed_scopes`에 있어야 한다. 허용 범위 목록이 비어 있거나 형식이 잘못돼도 아무 피드도 요청하지 않는다.

기존 외부 설정과의 호환성을 위해 `media_feed_policy`가 없거나 `enforce: false`이면 `enabled: false`가 아닌 기존 피드를 그대로 사용한다. 저장소의 운영 `config.yaml`은 정책을 명시적으로 켜 둔다.

## 현재 운영 분류

저장소의 97개 Google News 발견 피드 중 69개만 운영 요청 대상이다.

- 포함: 한국 행동주의·주주제안·주총·공개매수·합병/분할·희석성 자금조달·자사주·밸류업·상장폐지·이사회/보수, 그리고 한국 거버넌스를 명시한 영문 검색
- 제외: STO, ISA, 증권사 IB, ETF 상품 주제, 해외부동산펀드, 한국 기업과 연결되지 않은 일반 해외 행동주의·일본·유럽·아시아 캠페인
- 신규 거시경제·반도체·종목 시황 등은 별도 범위를 부여하지 않는 한 `unclassified`로 남아 요청되지 않는다.

한국을 명시한 영문 피드도 제목과 본문을 번역하지 않는다. 언어는 원문 그대로 유지하며 `korean_governance_context`는 주제 범위일 뿐 언어 변환 지시가 아니다.

## 비공개 보조 피드

현재 운영 정책에서 `CURATOR_FEEDS`의 단순 URL/줄바꿈 형식은 계속 파싱되지만 범위 정보가 없어 `unclassified`로 차단된다. 운영에서 비공개 피드를 사용하려면 같은 Secret에 다음과 같은 JSON 배열을 저장한다.

```json
[
  {
    "name": "licensed-korea-governance",
    "category": "private",
    "scope": "korean_governance",
    "enabled": true,
    "url": "https://example.invalid/private-feed.xml"
  }
]
```

범위 승인은 피드 콘텐츠 이용권한을 대신하지 않는다. 비공개 피드는 별도의 유효한 `SourceRight`와 운영 증빙을 갖춰야 한다.

## Google News 처리

Google News는 계속 발견 전용 소스다. RSS 항목의 원문 URL을 즉시 풀지 못해도 기사를 폐기하거나 핵심 수집 경로를 기다리게 하지 않고 `discovered → resolving → resolved/expired` 큐로 넘긴다. 이 범위 정책은 어떤 검색 피드를 요청할지만 결정하며 URL 해결 상태나 원문 언어를 변경하지 않는다.

## 변경 절차

새 피드를 추가할 때는 피드명·질의·한국 기업 거버넌스와의 연결·언어·이용권한을 검토한다. 승인된 경우에만 개별 `scope` 또는 정확한 `feed_scopes` 매핑을 추가한다. 범용 시장·산업·해외 행동주의 참고자료는 공개 사건 수집기가 아니라 편집자용 별도 조사 경로에서 다룬다.
