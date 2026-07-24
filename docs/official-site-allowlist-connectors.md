# 회사·행동주주 공식사이트 allowlist 커넥터

이 커넥터는 DART·KIND 사건량과 중요도에서 실제로 선정된 회사 최대 20곳, 행동주주 최대 10곳의 공식 자료를 수집하기 위한 기반이다. URL을 추정하거나 검색 결과에서 자동 등록하지 않는다. 비공개 API가 반환한 후보와 운영자가 등록한 manifest가 정확히 1:1로 일치할 때만 실행된다.

## 운영 전제

1. `GET /api/v1/ops/official-site-candidates`가 반환한 후보를 확인한다.
2. 각 후보의 실제 공식 사이트와 JSON adapter URL을 사람이 확인한다.
3. 각 connector에 대응하는 `SourceRight`를 관리자 API에 먼저 등록한다.
4. 아래 manifest를 UTF-8 JSON으로 작성하고 base64로 인코딩해 GitHub Environment secret `OFFICIAL_SITE_ALLOWLIST_B64`에 저장한다.
5. `GOVERNANCE_PIPELINE_MODE=shadow|live`일 때만 workflow가 실행된다. Telegram·governance outbound 발송은 사용하지 않는다.

실제 후보 데이터가 나오기 전에는 URL, connector 또는 권리를 임의로 만들지 않는다. 후보가 바뀌면 manifest도 같은 실행 전에 정확히 교체해야 하며, 누락·중복·추가 connector가 하나라도 있으면 전체 실행이 실패한다.

## manifest v1

```json
{
  "schema_version": 1,
  "connectors": [
    {
      "connector_id": "company-site:00123456",
      "entity_type": "company",
      "entity_id": "00123456",
      "source_class": "company_statement",
      "source_right_id": "right:company-site:00123456",
      "endpoint": "https://verified.example.com/bside-feed.json",
      "allowed_hosts": ["verified.example.com"],
      "page_size": 100,
      "active": true
    }
  ]
}
```

- `entity_type=company`에는 `company_statement`, `entity_type=actor`에는 `activist_statement`만 허용한다.
- endpoint는 인증정보·query·fragment·redirect가 없는 공개 HTTPS URL이어야 한다. runner는 source endpoint에 Authorization, Cookie 등 자격증명을 보내지 않는다.
- `allowed_hosts`에는 endpoint host와 item의 `original_url`에 필요한 정확한 host를 명시한다. 다른 host로 빠지는 응답은 실패한다.
- connector ID는 회사의 경우 `company-site:{company_id}`, 행동주주의 경우 `activist-site:{actor_id}`로 결정한다(64자를 넘는 actor ID는 구현이 정한 SHA-256 축약 ID 사용). SourceRight ID는 `right:{connector_id}`이며 64자를 넘으면 같은 구현의 SHA-256 축약 ID를 사용한다. candidate 한 건마다 활성 connector가 정확히 하나여야 한다.

## SourceRight 조건

예약 수집은 최소 권한 `BSIDE_OPS_TOKEN`으로
`GET /api/v1/ops/official-site-rights`의 모든 페이지를 읽어 다음을 모두 확인한다.
증빙 URI·해시 원문은 반환하지 않고 `evidence_present`만 제공한다. 등록·변경은
기존 관리자 `POST /api/v1/admin/source-rights`에서만 수행한다.

- `source_right_id`가 manifest와 일치
- `status=active`
- `source_type`이 connector의 `source_class`와 일치
- `source_key`가 `connector_id`와 일치
- 권한 범위와 증빙 URI 또는 증빙 해시 존재
- 현재 시점이 `valid_from` 이후이며 `valid_until`·`revoked_at` 이전
- `redistribution_allowed=true`

하나라도 충족하지 않으면 해당 item만 건너뛰지 않고 실행 전체를 실패시킨다. 만료·철회 권리가 AI 입력이나 공개 데이터로 흘러가는 것을 막기 위한 정책이다.

## adapter 응답 v1

모든 페이지는 `page`, `total_pages`, `total_count`를 명시하며 page와 total은 실행 중 변할 수 없다.

```json
{
  "schema_version": 1,
  "connector_id": "company-site:00123456",
  "page": 1,
  "total_pages": 1,
  "total_count": 1,
  "items": [
    {
      "operation": "upsert",
      "external_id": "notice-2026-001",
      "title": "원문 제목",
      "body": "원문 본문",
      "language": "ko",
      "original_url": "https://verified.example.com/notices/notice-2026-001",
      "published_at": "2026-07-22T09:00:00+09:00",
      "identity": {
        "company_id": "00123456",
        "event_type": "shareholder_proposal",
        "action": "제안",
        "target": "정관 변경",
        "actor_id": "actor:verified-proponent",
        "effective_at": "2026-07-22T09:00:00+09:00",
        "deadline_at": "2027-03-31T09:00:00+09:00"
      }
    }
  ]
}
```

삭제는 다음 모양만 허용한다.

```json
{
  "operation": "delete",
  "external_id": "notice-2026-001",
  "deleted_at": "2026-07-23T10:00:00+09:00",
  "original_url": "https://verified.example.com/notices/notice-2026-001"
}
```

page drift, 중간 빈 페이지, count 불일치, external ID 중복, redirect, 비 JSON 응답, host 이탈은 전체 실패다. 제목·본문·언어는 자르거나 번역하거나 Unicode 정규화하지 않고 그대로 artifact에 보존한다.

## 출력과 적용 경계

완전한 7필드 identity의 upsert는 `publication_status=draft`, `review_status=pending`, `review_required=true`인 문서·사건 후보가 된다. 불완전 identity는 `review_items`, delete는 `tombstones`에만 기록된다. 어느 경우도 자동 공개 또는 자동 삭제되지 않는다.

수집 결과는 커넥터별로 `upsert_official_site_snapshot` HMAC API에 원자 적용한다.
각 요청은 snapshot/receipt/payload hash와 회사·문서·사건·관측·검수·tombstone의
예상 건수를 포함하며, 서버의 정확한 ACK가 하나라도 다르면 workflow가 실패한다.
같은 요청 재전송은 idempotent receipt로 처리한다. identity 변경 시 이전 연결은
이력으로 보존하고 새 관측으로 교체하며, 원문의 delete는 자동 삭제하지 않고
review-only tombstone으로만 적재한다. 최종 receipt는 적용 결과까지 포함해 90일
보존한다.

artifact에는 manifest 원문, adapter endpoint, API token 또는 secret을 넣지 않는다. connector·entity·SourceRight ID, payload 해시, 원문 자료, draft/review/tombstone과 집계만 저장한다.
