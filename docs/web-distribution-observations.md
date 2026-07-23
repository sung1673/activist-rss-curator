# Web distribution observation producer

`.github/scripts/record-web-distribution.py` records one real GitHub Actions
web deployment outcome at
`POST /api/v1/ops/web-distribution-observations`. It accepts only a query-free
HTTPS `BSIDE_API_BASE_URL`, sends `BSIDE_OPS_TOKEN` in an `Authorization:
Bearer` header, and never writes the token to output or an artifact.

The request contains exactly one observation with these fields:

- `observation_id`: deterministic
  `github-actions:<run_id>:<run_attempt>:<target>:<operation>` identity
- `observed_at`: UTC start of the actual deployment action
- `distribution_target`: `pages` or `api`
- `duration_ms`: wall-clock duration through the final outcome
- `succeeded`: JSON boolean
- `build_sha`: full lowercase 40-character revision being deployed
- `workflow_run_id` and `workflow_run_attempt`: positive GitHub integers
- `failure_detected_at`: UTC detection time after a failure, otherwise `null`
- `source`: `github_actions`

An observation is durable only when the endpoint returns exactly HTTP 202 with
`ok=true`, `accepted_count=1`, and balanced `inserted_count + duplicate_count =
1`. A duplicate count of one is a successful idempotent retry. Transient
network errors, rate limits, and 5xx responses are retried with the same body;
all other statuses and malformed acknowledgements fail closed.

## Workflow semantics

| Workflow | Recorded operation | Non-attempts |
|---|---|---|
| `build-feed.yml` | One final `legacy-shadow-preview` Pages outcome whenever the legacy-owned artifact actually includes and deploys `/governance/`. Its duration includes all sequential retries and success requires final Pages verification. | Ordinary legacy-only Pages deployments, disabled Pages, or failure before the first deploy action. |
| `daily.yml` | One final governance Pages outcome for the run attempt. Its duration includes all sequential retries and success requires final Pages verification. | Preview/closed state, disabled ownership, non-default branch, or failure before the first deploy action. |
| `governance-cutover.yml` | The primary validated governance Pages deployment, including a durable failed result before automatic recovery runs. | Validation or approval failure before the deploy action. |
| `governance-rollback.yml` | The pinned legacy Pages deployment, using `LEGACY_ROLLBACK_CODE_REVISION` as `build_sha`. | Close/artifact validation failure before the deploy action. |

During the 14-day shadow period `PAGES_OWNER=legacy`, so `daily.yml` correctly
does not deploy governance-owned Pages. The scheduled `build-feed.yml` artifact
is the real shadow web-distribution path: it includes `/governance/` only when
both `deploy_pages=true` and `governance_preview=true`. Recording this final
outcome supplies one actual `web_distribution_days` observation for each day
that the shadow preview was deployed.

Record steps use `always() && !cancelled()` and additionally require evidence
that the deployment action actually reached a terminal `success` or `failure`
outcome. Thus a failed action is posted before its job concludes as failed,
while cancelled jobs and artifact-only previews do not inflate the attempt
denominator. A missing operations credential or an unacknowledged POST also
fails the workflow so evidence loss is visible.

The database deliberately enforces
`UNIQUE(workflow_run_id, workflow_run_attempt, distribution_target)`. A failed
cutover may deploy the pinned legacy artifact as an automatic compensating
action in the same run attempt. That recovery is not posted as a second Pages
denominator row: the already-recorded primary governance outcome remains the
single truthful cutover attempt. Recovery success is still enforced by the
workflow before ownership declarations are restored.

There is currently no GitHub Actions workflow that deploys the PHP API; the
repository workflows only lint and smoke-test it. Consequently no
`distribution_target=api` observation is emitted. API recording must be added
only alongside a real, separately authorized PHP deployment workflow.

All producer workflows keep Telegram and governance outbound delivery
disabled. The only new network write is the authenticated operational evidence
POST described above.
