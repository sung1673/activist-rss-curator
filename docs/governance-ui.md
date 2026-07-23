# Governance public UI contract

The same relative asset bundle is staged at `/` and `/governance/`. Internal
navigation therefore uses fragment routes, while `apiBase` and `webBase` are
separate public configuration values. The production defaults are the Pages
site `https://news.bside.ai` and the configured PHP `/api/v1` endpoint.

The public bundle contains company, actor, event, campaign, document,
calendar, search, public-revision, and private feedback views. It does not
contain administration or messaging-channel assets. Records whose
`verification_status` is `signal`, campaigns whose stage is `initial_signal`,
and documents outside the public source-class allowlist are rejected by the
client as a second line of defence. The API remains the authoritative release
and visibility boundary.

## Preview entry

A reviewer opens this one-time entry shape:

```text
https://news.bside.ai/#preview=<opaque-token>
```

The token must be 32–512 URL-safe characters. The application stores it only
in `sessionStorage`, immediately replaces the address with `#/today`, and sends
it to the configured API as an `Authorization: Bearer` header. Query-string
tokens, checked-in tokens, local storage, and token-bearing asset URLs are not
supported. Closing the browser session clears the credential.

## Public endpoint expectations

The actor page requires `GET /actors/{actor_id}` returning `data.actor` and an
optional `data.campaigns` list. Its complete event history is independently
paginated through `GET /events?actor_id=…&page=…&limit=50`. Company timelines
use the same list endpoint with `company_id`; the embedded company-detail event
sample is not treated as a complete history.

The public correction log requires `GET /revisions`. Only published public
editorial revisions belong in this response. Each item should include
`revision_id`, `entity_type`, `entity_id`, `reason`, `published_at`, and either
`is_public=true` or `publication_status=published`. Internal approval and
review history must never be returned by this endpoint.

Event list, search, feed, and export implementations should accept the common
filters `company_id`, `actor_id`, `event_type`, `source_class`,
`verification_status`, `from`, and `to`. The event UI serializes non-empty
values into its fragment URL so a filtered view can be shared without placing
credentials in the URL.

Large document bodies use UTF-8 byte offsets:

```text
GET /documents/{document_id}?include=body&body_offset=0&body_limit_bytes=65536
```

The response should return `body_text`, `body_truncated`,
`body_next_offset`, and preferably `body_bytes_returned`. The UI requests the
next byte range only after user action and appends text with DOM text nodes,
without translating or interpreting the source text. The server must keep one
response within the 250 KB API budget and align byte ranges at valid UTF-8
boundaries.

`POST /feedback` remains private by default. The UI supports corrections,
right of reply, source-right issues, and general feedback. Right-of-reply
submissions require a name and contact address; actor references use
`entity_type=actor`.

## Front-end budgets

- Initial HTML: at most 250 KB.
- Combined gzip size of JavaScript and CSS: at most 250 KB.
- API list page: 50 records in this client, never more than the server maximum.
- Interactive controls: at least 44 CSS pixels high, visible focus, keyboard
  operation, reduced-motion support, and WCAG 2.2 AA automated checks.
- Source title, body, claim, demand, and revision text are rendered with
  `textContent`/text nodes and their original language metadata. No HTML from a
  source record is interpreted.

Production builds place the immutable Git revision in `buildSha`. On
`visibilitychange` or `pagehide`, supported browser observers send LCP, CLS,
and INP to `POST /metrics/web-vitals` with `keepalive`. Each JSON body contains
exactly `route_template`, `metric`, `value`, `device_class`, and `build_sha`.
LCP and INP values are milliseconds; CLS is the unitless cumulative score.
The route is a template such as `/events/:id`, never a URL, query, entity ID,
referrer, IP address, or user identifier. Preview sessions use the same Bearer
header as other API calls. Collection failures are silent; the server applies
rate limiting and deletes observations after 30 days.
