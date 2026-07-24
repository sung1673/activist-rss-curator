# 사용자·법률 검증 실행 가이드

이 문서는 정식 공개 전 사람이 수행해야 하는 두 작업을 실제 릴리스 증빙으로 만드는 절차다. 사용자 평가는 제품의 사용성을 검증하고, 법률 검토는 공개 범위와 편집 정책을 승인한다. 두 절차 모두 14일 shadow와 병행할 수 있지만, 평가한 코드 SHA와 공개할 코드 SHA가 다르면 증빙을 다시 만들어야 한다.

## 시작 조건

- DART와 KIND가 모두 동일 release candidate SHA로 `shadow` 운영 중이어야 한다.
- release state는 `preview`이고 `PAGES_OWNER=legacy`여야 한다.
- preview token은 URL fragment로 전달하고 query string, 메일 본문, 화면 녹화 또는 분석 로그에 남기지 않는다.
- 평가자는 실제 운영 데이터만 사용한다. 예시·합성 사건·미리 알려 준 정답은 출시 증빙에서 제외한다.
- 평가자·승인자 이름, 이메일, IP 주소는 증빙 JSON에 넣지 않는다. 별도 비공개 대장의 가명 reference만 기록한다.

코드 SHA, 평가 데이터와 정책 문서의 최종본을 먼저 고정한다. 이후 identity 규칙이나 사용자 화면을 바꾸면 14일 shadow와 사용자 평가를 모두 다시 시작한다. 오탈자 수정이라도 배포 SHA가 달라지면 기존 증빙으로 자동 전환할 수 없다.

## 사용자 평가자 구성

각 집단에서 최소 5명, 합계 최소 15명을 섭외한다.

| segment 값 | 대상 | 최소 인원 |
|---|---|---:|
| `institution` | 국내 기관투자자·거버넌스/IR 실무자 | 5 |
| `high_net_worth` | 고액자산가·전문 개인투자자 | 5 |
| `international_institution` | 해외기관·영문 UI 이용 실무자 | 5 |

한 사람이 여러 segment를 대표하지 않는다. 개발자, 편집자, 데이터 라벨러처럼 정답 구조를 이미 아는 사람은 출시 평가자에 포함하지 않는다.

## 평가 진행

진행자는 평가자마다 아직 보지 않은 공개 가능 사건 하나를 무작위로 배정하고 다음 지시만 제공한다.

> BSIDE preview에서 배정된 기업 또는 사건을 찾고, 현재 발생한 사건, 핵심 당사자, 공식 근거 문서, 현재 상태를 확인해 주세요. 완료했다고 판단하면 말씀해 주세요.

1. preview 세션을 연 직후 타이머를 시작한다.
2. 진행자는 검색어나 이동 경로, 정답 위치를 안내하지 않는다.
3. 평가자가 완료를 선언하거나 180초가 지나면 타이머를 멈춘다.
4. 평가자가 말한 네 답을 공식 사건 레코드와 대조한다.
5. 아래 필드를 즉시 기록한다.

```json
{
  "evaluation_id": "eval-institution-001",
  "segment": "institution",
  "completed_at": "2026-09-01T10:15:30+09:00",
  "duration_seconds": 142.4,
  "identified_event": true,
  "identified_actors": true,
  "identified_official_evidence": true,
  "identified_current_status": true,
  "succeeded": true
}
```

`succeeded`는 네 확인값이 모두 `true`이고 `duration_seconds <= 180`일 때만 `true`다. 진행자의 주관으로 결과를 올리거나, 실패한 평가를 다른 사건으로 대체하지 않는다. 출시 하한은 전체 15명 중 12명 이상 성공이며 각 segment에 실제 평가 5건 이상이 있어야 한다.

## `usability.json` 생성

평가 행의 순서를 고정하고 각 행을 key 정렬·공백 없는 JSON으로 직렬화한 뒤 줄바꿈으로 연결한다. 이 바이트열의 SHA-256을 `dataset_sha256`으로 기록한다. `evaluator_count`, `succeeded_evaluator_count`, `success_rate`는 원시 행에서 다시 계산한다. 스키마는 [`schemas/usability-evidence.schema.json`](schemas/usability-evidence.schema.json)을 사용하고 최종 exporter가 해시와 원시 분모를 다시 검증한다.

```json
{
  "schema_version": 1,
  "environment": "production",
  "evidence_source": "human_usability_export",
  "is_synthetic": false,
  "collected_at": "2026-09-01T17:00:00+09:00",
  "code_revision": "<평가한 full 40자 Git SHA>",
  "dataset_sha256": "<64자 SHA-256>",
  "target_seconds": 180,
  "evaluator_count": 15,
  "succeeded_evaluator_count": 12,
  "success_rate": 0.8,
  "evaluations": []
}
```

위 블록은 형식 설명이며 출시 증빙으로 사용하지 않는다. `evaluations`에는 실제 15건 이상의 행을 넣는다.

## 법률·편집 검토 패키지

법률 검토자에게 다음 최종본과 공개 화면 캡처를 하나의 버전 고정 패키지로 전달한다.

- 제품 범위: 공개 기록·검색·근거 추적이며 매수·매도 권고, 목표가, 종목 추천, 의결권 위임을 제공하지 않는다는 설명
- 원문 정책: 제목·본문의 원문 언어 보존, AI 후보 생성 범위, 사람 승인 대상
- 출처 정책: DART·KIND 우선, 공식 당사자 자료, 허가된 Telegram의 내부 signal-only 처리, SourceRight 만료·철회 차단
- 편집 정책: 사실·당사자 주장·언론 보도·편집 분석 구분, 고위험 사건 승인, 이해상충 표시
- 정정·답변 정책: 공개 정정 로그, 당사자 답변 접수, 자동 공개 금지, 원문과 감사 기록 보존
- 개인정보 정책: Web Vitals의 route template·metric·값·device class·build SHA만 수집하고 IP·query·사용자 식별자를 저장하지 않으며 30일 후 삭제
- 배포·롤백 정책: `closed → preview → live`, 90일 호환, 신규 DB와 권리 철회 기록 보존
- KIND 서면 이용 조건과 Telegram 구두 승인 인쇄 대장의 보관 reference
- OpenAPI, 실제 preview URL, 사용자 평가 방식, 최종 benchmark 결과

검토 의견은 문서별로 `승인`, `조건부 승인`, `반려` 중 하나로 기록한다. 조건부 승인은 조건을 반영한 새 문서 해시와 명시적인 최종 승인 없이는 `approved`로 바꾸지 않는다. 투자권고 경계와 공개 재배포 범위는 관련 자격을 가진 검토자가 판단해야 하며, 이 저장소의 자동 검사는 법률 의견을 대체하지 않는다.

## 승인 증빙

법률, 편집, 제품 책임자가 각각 하나의 최종 승인 문서를 남긴다. 문서의 원본 바이트 SHA-256과 접근 제한된 보관 위치를 `release-approval.json`에 기록한다.

```json
{
  "role": "legal",
  "decision": "approved",
  "decided_at": "2026-09-01T18:00:00+09:00",
  "approver_reference": "private-register:legal-2026-09-01",
  "evidence_uri": "urn:bside:approval:legal:2026-09-01",
  "evidence_sha256": "<승인 문서 원본의 64자 SHA-256>"
}
```

정확히 `legal`, `editorial`, `product` 세 역할이 한 번씩 있어야 하고 세 결정이 모두 `approved`일 때만 `release_approved=true`다. `approved_revision`은 평가·shadow·cutover artifact와 같은 full Git SHA여야 한다. `usability_dataset_sha256`, `same_story_dataset_sha256`, `relevance_dataset_sha256`은 해당 실제 증빙에서 복사하며 임의로 다시 계산하거나 입력하지 않는다.

## 보호된 artifact 등록

세 파일 `benchmark.json`, `usability.json`, `release-approval.json`을 공개 저장소에 커밋하지 않는다. [`production-evidence-inputs-runbook.md`](production-evidence-inputs-runbook.md)의 일회성 Environment Secret 절차로 업로드하고 workflow 성공 여부와 관계없이 세 Secret을 즉시 삭제한다.

등록 후 다음을 확인한다.

- `governance-human-evidence` artifact가 기본 브랜치의 동일 SHA에서 생성됐는가
- artifact digest와 run ID를 비공개 릴리스 대장에 기록했는가
- 세 일회성 Secret을 삭제했는가
- 14일 shadow, 최근 7일 운영·성능, KIND 실제 관측이 모두 갖춰진 뒤 최종 evidence exporter가 통과했는가
- cutover 전 rollback drill과 법률 승인 문서 해시를 다시 대조했는가

