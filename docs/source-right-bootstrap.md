# Protected SourceRight bootstrap

`source-right-bootstrap.yml` is the one protected, manual path for registering
the Production Alpha metadata grants and moving only `pending_rights`
connectors to `configured`. Existing `active` or `configured` connectors keep
their state. It is not an ingestion or release workflow.

## Fixed scope

| Source | Coverage | Registered data | Explicit exclusions |
|---|---|---|---|
| `official:dart` | KR `market-wide` | Company/filing identifiers, original title and language, filing date/time, official URL, filing type, correction relationship | Filing body, attachments, media, third-party content |
| `official:sec-edgar` | US `market-wide` | Issuer/accession identifiers, original title or source description, form type, filing/acceptance date/time, official URL, amendment relationship | Filing body, exhibits, attachments, media, third-party content |
| `official:ca-issuer-ir` | CA `link-only`, optional | Human-approved issuer IR link metadata from `CA_OFFICIAL_LINKS_JSON` | URL fetches, document bodies, SEDAR+ content |
| `official:asic-register` | AU `link-only`, optional | Human-approved ASIC-host link metadata from `AU_OFFICIAL_LINKS_JSON` | URL fetches, document bodies, ASX content |

Every grant has `ai_allowed=false`. The API's coarse
`redistribution_allowed=true` flag applies only to the metadata named in the
grant's permission scope; the scope expressly excludes full text.

EDINET and Companies House are deliberately not supported by this operation.
They stay `pending_rights` until their authenticated connector and separate
approval path are available.

## Protection and preconditions

Configure `governance-release` with a required human reviewer before the first
run. The workflow has read-only repository permission and serializes with the
protected cutover/rollback workflow.

The operator must confirm all of the following:

- The workflow is dispatched on the default branch.
- `expected_release_sha` is the exact deployed 40-character `main` SHA.
- Both v1 and v2 API release states are `closed`.
- `PAGES_OWNER=legacy`.
- `GOVERNANCE_PIPELINE_MODE=off`.
- `GLOBAL_ALPHA_OBSERVATION_ENABLED=false`.
- `KIND_CONNECTOR_MODE=off`.
- Both delivery variables are `false`.
- `BSIDE_ADMIN_TOKEN` and `BSIDE_API_BASE_URL` are available as
  `governance-release` environment Secrets.
- Any selected CA/AU allowlist has already received human approval.

The protected API Secret must be exactly
`https://alignpe.gabia.io/activist/api.php/api/v1`. There is no repository
variable fallback. The client validates that exact hostname and path before it
constructs any authenticated request. It also requires v2 health to report
schema 11, service `bside-global-market-terminal`, and the exact approved SHA.

## Run

From the GitHub Actions page, select **Bootstrap protected metadata-only
SourceRights**, choose `main`, enter the exact SHA and an auditable reason, and
type:

```text
BOOTSTRAP_DART_SEC_METADATA_RIGHTS_AT_EXACT_RELEASE_SHA
```

The equivalent CLI dispatch is:

```powershell
$sha = gh api repos/sung1673/activist-rss-curator/commits/main --jq .sha
gh workflow run source-right-bootstrap.yml --ref main `
  -f expected_release_sha=$sha `
  -f reason="Human approved Production Alpha metadata-only DART and SEC rights." `
  -f confirmation=BOOTSTRAP_DART_SEC_METADATA_RIGHTS_AT_EXACT_RELEASE_SHA `
  -f include_ca=false `
  -f include_au=false
```

Set an optional market to `true` only when its non-empty repository allowlist
variable is the exact reviewed manifest. The bootstrap validates the full
manifest and binds its canonical SHA-256 as evidence before making any API
write.

## Operation and evidence

Before the first write, the client validates every selected SourceRight and
every seeded connector. Existing SourceRight IDs may not move to a different
`source_type` or `source_key`.

A SourceRight in `revoked` or `expired` state, or any SourceRight with a
non-null `revoked_at`, stops the entire operation without a write. An
`inactive` connector is treated as an operator emergency stop and is never
reactivated by bootstrap. Those states require a separate, reviewed recovery
operation.

Only a missing or pending SourceRight and a `pending_rights` connector may be
promoted by this workflow. An already active SourceRight is accepted only when
its source name, permission scope, evidence, expiry, AI flag, redistribution
flag, and status exactly match the fixed metadata-only grant. A matching
active grant and an already `active` or `configured` connector are verified
but not rewritten or downgraded, making exact reruns idempotent.

For a missing or pending right, the request carries the exact preflight
`expected_status` and `expected_updated_at`. The PHP transaction locks the
SourceRight row and returns `409 stale_source_right` if it was created,
changed, expired, or revoked after preflight. A concurrent revocation can
therefore never be overwritten by this workflow.

For each selected source, the client:

1. registers the fixed metadata-only grant through v1;
2. verifies both collection and public eligibility through v2;
3. re-reads the connector and uses its exact `updated_at` optimistic version;
4. changes a `pending_rights` connector only to `configured`, while preserving
   an existing `active` or `configured` state;
5. verifies the connector/right identity and eligibility again.

It rechecks both release states and the deployed SHA after all changes. The
workflow log contains only source IDs, connector IDs, boolean eligibility, and
the release SHA. It does not print the admin token or the SourceRight revision
hashes.

There is no multi-endpoint database transaction. If an API or network failure
occurs after one source was registered, the public APIs remain closed and the
workflow fails. Correct the cause and rerun the same SHA and inputs; the
SourceRight upsert and connector status update are designed for a safe,
audited retry. Do not activate the pipeline or observation window until this
workflow succeeds.
