# 공식 수집 durable slot claim

정식 shadow와 release evidence는 DB의 성공 run 개수를 분모로 삼지 않고 `official-v1-82-slots` cadence를 사용한다. KST 00:00~06:30은 30분 간격 14개, 07:00~23:45은 15분 간격 68개로, DART와 KIND 각각 일 82개 slot이다.

## Claim 규칙과 identity

예약 run은 수집 전 `POST /api/v1/ops/official-slot-claims`로 slot을 claim한다. GitHub `workflow_run.created_at`을 경계로 내림해 slot을 추정하지 않는다. 서버가 MySQL에서 해당 cron family의 가장 오래된 due/unclaimed slot을 원자적으로 선점하고 `claim_id`, GitHub run ID·attempt, slot·trigger·claim·next-cadence 시각, 두 lag, claim-time `late`, status를 ACK한다.

첫 접촉은 다음 완전한 KST 날짜의 00:00을 `active_from`으로 등록하고 409 `official_slot_claim_activated`를 반환한다. 모호한 첫 run에는 slot을 부여하지 않는다.

## 완료·재실행·repair

- run ID는 `claim_id`만으로 결정하므로 window, 실행 날짜, GitHub attempt가 바뀌어도 같다.
- 경계 전 실패는 `status=failed`, `terminal_reason=null`로 남고 더 높은 attempt에서 재시도할 수 있다.
- 성공 완료는 전체·source별 raw/ACK가 정확히 일치해야 한다. 선택된 소스가 0건이면 0/0 ACK를 명시적으로 저장한다.
- 완료된 claim의 재실행은 collector를 호출하지 않고 DB row를 수정하지 않는 terminal no-op이다.
- claim 또는 완료가 다음 cadence 경계를 넘으면 `claim_after_next_cadence`, `completion_after_next_cadence`, `rerun_after_next_cadence` 중 하나를 영구 실패 사유로 저장한다. claim-time `late`는 수정하지 않는다.
- repair는 `expected_slot_at`이 세 cron family 전체의 가장 오래된 due/unclaimed slot과 정확히 같을 때만 허용하며 source poll을 한 번만 실행한다.

KIND 네트워크 contract 사전 확인은 수집 workflow 밖의 수동 preflight에서만 수행한다. 예약·repair 경로에 예비 KIND 요청을 추가하지 않는다.

## Epoch reset

slot 소유권을 삭제하거나 과거 slot을 재사용하지 않는다. reset은 `GOVERNANCE_PIPELINE_MODE=off`, release state `closed`, 보호된 `governance-release` environment에서만 실행한다. admin token, optimistic `expected_epoch_version`, 20~500자 사유, code revision, `RESET_OFFICIAL_SLOT_EPOCH_AT_NEXT_KST_DAY` confirmation을 요구한다.

새 epoch은 현재 시각과 기존 `active_from`보다 뒤인 다음 완전한 KST 날짜의 00:00에서 시작한다. 기존 claim과 epoch history는 보존하고 append-only audit row를 추가한다. release evidence 범위가 epoch 경계를 가로지르면 409로 거부한다.

## 증빙·migration

`/api/v1/ops/official-run-ledger`와 `/api/v1/ops/release-evidence`는 collection run과 claim을 exact ID로 join한다. claim만 있는 slot도 incomplete row로 노출하며 claimed·late·incomplete·terminal 원시 count와 DART·KIND별 expected·succeeded·missing·failed count를 함께 반환한다.

Migration 010 manifest checksum은 다음 canonical identity의 SHA-256으로 고정한다.

```text
SHA-256("bside-governance-migration-v1:10:010_official_slot_claim_ledger")
= 2b8be6264c8a4f3be038729fbf6bbe22e720457874f02c89c82d33db9dc78f51
```
