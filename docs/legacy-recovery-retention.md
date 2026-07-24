# 레거시 Pages 90일 호환·복구 자산 보존

거버넌스 공개 전환 뒤에도 기존 `/feed.xml`과 `/feed/YYYY-MM-DD.html`은 전환 시각부터 90일간 보존한다. GitHub Actions artifact의 최대 보존 기간과 호환 기간이 같으므로, 전환 전에 만든 원본 artifact 하나만 참조하면 만료 시점이 호환 종료보다 먼저 온다. 이 프로젝트는 검증된 복구 번들을 성공한 기본 브랜치 실행마다 다시 업로드해 보존 기간을 갱신한다.

## 고정 원본

전환 후보를 만들기 전에 다음 repository variable 네 개를 동일한 정상 `build-feed.yml` 실행으로 고정한다.

- `LEGACY_ROLLBACK_RUN_ID`
- `LEGACY_ROLLBACK_ARTIFACT_NAME`
- `LEGACY_ROLLBACK_CODE_REVISION`
- `LEGACY_ROLLBACK_ARTIFACT_DIGEST`

원본은 기본 브랜치의 성공한 `build-feed.yml` 실행이어야 하고, 이름이 정확히 일치하는 만료되지 않은 artifact가 하나만 있어야 한다. SHA-256 digest와 full 40자리 code revision도 일치해야 한다. 연속된 실제 일별 페이지가 90개 미만이면 준비가 실패한다. 빈 날짜를 합성하거나 과거 페이지를 새 날짜로 복제하지 않는다.

## 복구 번들

`curator.legacy_recovery_bundle`은 원본 artifact ZIP 자체의 digest를 확인한 뒤 다음 세 항목만 포함한 번들을 만든다.

- `full-site/`: 검증된 전체 레거시 Pages 롤백 자산
- `compatibility/`: 가장 최근의 연속된 실제 90일 feed 자산
- `legacy-recovery-bundle.json`: 원본 run·artifact ID·이름·code SHA·digest와 전체 파일별 byte 수·SHA-256

ZIP 경로 이탈, 절대 경로, 대소문자 중복, symlink, 특수 파일, 암호화 entry, 허용되지 않은 파일, 크기·개수 예산 초과를 거부한다. 이후 carry-forward artifact를 사용할 때에도 내부 manifest와 전체 파일 inventory, 90일 compatibility manifest를 모두 다시 계산한다.

## 보존 갱신

`daily.yml`과 보호된 `governance-cutover.yml`은 다음 순서로 `legacy-recovery-carry-forward`를 만든다.

1. 기본 브랜치의 성공한 이전 `daily.yml` 또는 `governance-cutover.yml`에서 만료되지 않은 동일 이름 artifact 하나를 찾는다.
2. 없으면 아직 만료되지 않은 고정 원본으로 최초 번들을 만든다.
3. 다운로드 digest와 내부 원본 pin·파일 inventory를 검증한다.
4. 성공한 실행의 마지막 단계에서 같은 내용을 `retention-days: 90`으로 다시 업로드한다.

실패한 실행, 다른 브랜치, 다른 workflow, 현재 실행, 만료된 artifact, digest가 없는 artifact, 같은 실행의 중복 artifact는 사용하지 않는다. daily 실행이 실패하면 그 실행의 artifact도 다음 체인의 출처가 될 수 없다.

## 롤백

보호된 rollback workflow는 API를 `closed`로 바꾸기 전에 carry-forward 또는 고정 원본을 다운로드하고 복구 번들 전체를 검증한다. 같은 rollback 실행에 짧은 수명의 검증 완료 artifact를 다시 올린 뒤에만 API를 닫는다. Pages에는 `full-site/`만 배포하며 신규 MySQL 데이터와 SourceRight 철회 기록은 삭제하지 않는다.

전환 workflow 자체가 실패하면 같은 실행에서 검증한 번들의 `full-site/`를 사용해 자동 복구한다. 성공한 전환 뒤에는 daily 실행이 호환 기간 동안 보존 시계를 매일 갱신한다. 90일 동안 daily가 한 번도 성공하지 않는 상태는 가용성·운영 release gate 위반이며 자동으로 정상으로 간주하지 않는다.
