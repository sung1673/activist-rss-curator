(() => {
  "use strict";

  const app = document.getElementById("app");
  const announcer = document.getElementById("announcer");
  if (!app || !announcer) return;

  const config = window.__BSIDE_GOVERNANCE_CONFIG__ || {};
  const labels = {
    eventType: {
      five_percent_holding: "5% 보유 / 5% holding",
      shareholder_proposal: "주주제안 / Shareholder proposal",
      general_meeting: "주주총회 / General meeting",
      board: "이사회 / Board",
      executive_compensation: "임원보수 / Executive compensation",
      dividend: "배당 / Dividend",
      treasury_shares: "자사주 / Treasury shares",
      merger: "합병 / Merger",
      split: "분할 / Split",
      duplicate_listing: "중복상장 / Duplicate listing",
      rights_issue: "유상증자 / Rights issue",
      convertible_bond: "전환사채 / Convertible bond",
      bond_with_warrant: "신주인수권부사채 / Bond with warrant",
      exchangeable_bond: "교환사채 / Exchangeable bond",
      tender_offer: "공개매수 / Tender offer",
      delisting: "상장폐지 / Delisting",
      trading_suspension: "거래정지 / Trading suspension",
      value_up: "기업가치 제고 / Value-up",
      proposal_vote: "의안 표결 / Proposal vote",
      other: "기타 / Other"
    },
    importance: {
      critical: "시장 민감 / Market-sensitive",
      high: "높음 / High",
      medium: "보통 / Medium",
      low: "낮음 / Low"
    },
    verification: {
      signal: "신호 / Signal",
      unverified: "미확인 / Unverified",
      corroborated: "추가 근거 / Corroborated",
      official: "공식 근거 / Official",
      confirmed: "확인 / Confirmed",
      disputed: "다툼 있음 / Disputed",
      corrected: "정정 / Corrected",
      withdrawn: "철회 / Withdrawn"
    },
    campaignStage: {
      initial_signal: "초기 신호 / Initial signal",
      private_engagement: "비공개 관여 / Private engagement",
      public_letter: "공개서한·질의 / Public letter",
      public_campaign: "공개 캠페인 / Public campaign",
      shareholder_proposal: "주주제안 / Shareholder proposal",
      proxy_vote: "위임·표결 / Proxy vote",
      resolution: "합의·결과 / Resolution",
      implementation_tracking: "이행 추적 / Implementation tracking",
      closed: "종료 / Closed"
    },
    outcome: {
      settled: "합의 / Settled",
      withdrawn: "철회 / Withdrawn",
      passed: "가결 / Passed",
      failed: "부결 / Failed",
      pending: "대기 / Pending"
    },
    sourceClass: {
      official_disclosure: "공식 공시 / Official disclosure",
      company_statement: "회사 입장 / Company statement",
      activist_statement: "행동주주 입장 / Activist statement",
      media_report: "언론 보도 / Media report",
      authorized_telegram: "허가된 Telegram / Authorized Telegram",
      licensed_telegram: "허가된 Telegram / Licensed Telegram",
      editorial_analysis: "편집 분석 / Editorial analysis"
    },
    claimType: {
      actor_claim: "당사자 주장 / Actor claim",
      company_response: "회사 반론 / Company response",
      official_fact: "공식 사실 / Official fact",
      media_report: "보도 / Media report",
      editorial_interpretation: "편집 해석 / Editorial interpretation"
    },
    commitment: {
      planned: "계획 / Planned",
      announced: "발표 / Announced",
      in_progress: "진행 중 / In progress",
      met: "이행 / Met",
      partially_met: "부분 이행 / Partially met",
      missed: "미이행 / Missed",
      cancelled: "취소 / Cancelled"
    },
    actorType: {
      company: "회사 / Company",
      activist_shareholder: "행동주주 / Activist shareholder",
      institution: "기관 / Institution",
      shareholder_coalition: "주주연대 / Shareholder coalition",
      regulator: "규제기관 / Regulator",
      advisor: "자문사 / Advisor"
    },
    kind: {
      company: "기업 / Company",
      actor: "당사자 / Actor",
      event: "사건 / Event",
      campaign: "캠페인 / Campaign",
      document: "문서 / Document"
    }
  };

  let routeController = null;

  class ApiError extends Error {
    constructor(message, code, status) {
      super(message);
      this.name = "ApiError";
      this.code = code || "request_failed";
      this.status = status || 0;
    }
  }

  function element(tag, options, children) {
    const node = document.createElement(tag);
    const settings = options || {};
    if (settings.className) node.className = settings.className;
    if (settings.text !== undefined && settings.text !== null) node.textContent = String(settings.text);
    if (settings.lang && /^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$/.test(settings.lang)) node.lang = settings.lang;
    if (settings.attrs) {
      Object.entries(settings.attrs).forEach(([name, value]) => {
        if (value !== undefined && value !== null && value !== false) node.setAttribute(name, String(value));
      });
    }
    if (settings.dataset) {
      Object.entries(settings.dataset).forEach(([name, value]) => { node.dataset[name] = String(value); });
    }
    (children || []).forEach((child) => {
      if (child instanceof Node) node.append(child);
      else if (child !== undefined && child !== null) node.append(document.createTextNode(String(child)));
    });
    return node;
  }

  function fragment(children) {
    const value = document.createDocumentFragment();
    (children || []).forEach((child) => { if (child) value.append(child); });
    return value;
  }

  function sourceNode(tag, text, language, className) {
    return element(tag, { text: text || "—", lang: language || "", className: className || "source-text" });
  }

  function label(group, value) {
    const key = String(value || "");
    return (labels[group] && labels[group][key]) || key || "—";
  }

  function normalizedDate(value) {
    const text = String(value || "").trim();
    if (!text) return null;
    const iso = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(text) ? `${text.replace(" ", "T")}Z` : text;
    const date = new Date(iso);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  function formatDate(value, withTime) {
    const date = normalizedDate(value);
    if (!date) return String(value || "—");
    const options = withTime
      ? { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", timeZone: "Asia/Seoul" }
      : { year: "numeric", month: "short", day: "numeric", timeZone: "Asia/Seoul" };
    return new Intl.DateTimeFormat("ko-KR", options).format(date);
  }

  function apiBase() {
    try {
      const url = new URL(String(config.apiBase || "/api/v1"), window.location.origin);
      if (!/^https?:$/.test(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error("unsafe API base");
      url.pathname = `${url.pathname.replace(/\/$/, "")}/`;
      return url;
    } catch (_error) {
      return new URL("/api/v1/", window.location.origin);
    }
  }

  const baseUrl = apiBase();

  function endpoint(path, params) {
    const url = new URL(String(path || "").replace(/^\//, ""), baseUrl);
    Object.entries(params || {}).forEach(([name, value]) => {
      if (value !== undefined && value !== null && String(value) !== "") url.searchParams.set(name, String(value));
    });
    return url;
  }

  document.querySelectorAll("[data-api-link]").forEach((anchor) => {
    anchor.href = endpoint(anchor.dataset.apiLink).toString();
  });

  async function request(path, options) {
    const settings = options || {};
    const response = await fetch(endpoint(path, settings.params), {
      method: settings.method || "GET",
      headers: settings.body ? { "Content-Type": "application/json", Accept: "application/json" } : { Accept: "application/json" },
      body: settings.body ? JSON.stringify(settings.body) : undefined,
      credentials: "omit",
      cache: settings.method === "POST" ? "no-store" : "no-cache",
      signal: settings.signal
    });
    const contentType = response.headers.get("content-type") || "";
    const payload = contentType.includes("application/json") ? await response.json() : null;
    if (!response.ok || !payload || payload.ok === false) {
      const code = payload && payload.error ? String(payload.error) : `http_${response.status}`;
      throw new ApiError("API request failed", code, response.status);
    }
    return payload;
  }

  function validEntityId(value) { return /^[A-Za-z0-9_.:-]{1,96}$/.test(String(value || "")); }
  function validCompanyId(value) { return /^\d{8}$/.test(String(value || "")); }

  function routeLink(text, hash, options) {
    return element("a", {
      text,
      lang: options && options.lang,
      className: options && options.className,
      attrs: { href: hash }
    });
  }

  function externalLink(text, href, className) {
    let safeHref = "";
    try {
      const url = new URL(String(href || ""), window.location.origin);
      if (/^https?:$/.test(url.protocol)) safeHref = url.toString();
    } catch (_error) {
      safeHref = "";
    }
    if (!safeHref) return element("span", { text, className });
    return element("a", { text, className, attrs: { href: safeHref, target: "_blank", rel: "noopener noreferrer" } });
  }

  function badge(value, group) {
    const key = String(value || "unknown");
    return element("span", { text: label(group, key), className: `badge badge--${key.replace(/[^a-z0-9_-]/gi, "")}` });
  }

  function metadata(items) {
    const row = element("div", { className: "meta-row" });
    items.filter((item) => item && item.value).forEach((item, index) => {
      if (index) row.append(element("span", { text: "·", attrs: { "aria-hidden": "true" } }));
      row.append(item.node || element("span", { text: item.value }));
    });
    return row;
  }

  function pageHeader(eyebrow, title, description, actions, language) {
    const copy = element("div", {}, [
      element("p", { text: eyebrow, className: "eyebrow" }),
      sourceNode("h1", title, language),
      description ? element("p", { text: description, className: "lede" }) : null
    ].filter(Boolean));
    const children = [copy];
    if (actions && actions.length) children.push(element("div", { className: "header-actions" }, actions));
    return element("header", { className: "page-header" }, children);
  }

  function sectionHeading(title, note, id) {
    return element("div", { className: "section-heading" }, [
      element("h2", { text: title, attrs: id ? { id } : {} }),
      note ? element("p", { text: note }) : null
    ].filter(Boolean));
  }

  function emptyState(title, message) {
    return element("div", { className: "empty-state" }, [
      element("h2", { text: title }),
      element("p", { text: message })
    ]);
  }

  function addLoadMore(section, pagination, loadPage) {
    let page = pagination && pagination.next_page ? Number(pagination.next_page) : 0;
    if (!page) return;
    const button = element("button", { text: "더 보기 / Load more", attrs: { type: "button" } });
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        const next = await loadPage(page);
        page = next && next.next_page ? Number(next.next_page) : 0;
        if (!page) button.remove();
      } catch (error) {
        announceError(error);
      } finally {
        if (page) button.disabled = false;
      }
    });
    section.append(element("div", { className: "load-more" }, [button]));
  }

  function eventCard(event, rank) {
    const titleLink = routeLink(event.title || "제목 없음", `#/events/${encodeURIComponent(event.event_id || "")}`, { lang: event.original_language });
    const company = event.company_id
      ? routeLink(event.company_name || event.company_id, `#/companies/${encodeURIComponent(event.company_id)}`)
      : element("span", { text: event.company_name || "—" });
    return element("article", {
      className: "event-card",
      dataset: { importance: event.importance || "unknown" }
    }, [
      element("div", { className: "event-card__top" }, [
        rank ? element("span", { text: String(rank).padStart(2, "0"), className: "rank", attrs: { "aria-label": `${rank}위` } }) : element("span"),
        element("div", { className: "badge-row" }, [badge(event.importance, "importance"), badge(event.verification_status, "verification")])
      ]),
      element("h3", {}, [titleLink]),
      metadata([
        { node: company, value: event.company_name || event.company_id },
        { value: label("eventType", event.event_type) },
        { value: formatDate(event.occurred_at, false) },
        event.deadline_at ? { value: `기한 / Deadline ${formatDate(event.deadline_at, false)}` } : null
      ])
    ]);
  }

  function compactEvent(event) {
    return element("li", { className: "compact-item" }, [
      routeLink(event.title || "제목 없음", `#/events/${encodeURIComponent(event.event_id || "")}`, { lang: event.original_language }),
      element("p", { text: `${event.company_name || event.company_id || "—"} · ${label("verification", event.verification_status)} · ${formatDate(event.occurred_at, false)}` })
    ]);
  }

  function archiveEvent(event) {
    return element("article", { className: "archive-row" }, [
      element("h3", {}, [routeLink(event.title || "제목 없음", `#/events/${encodeURIComponent(event.event_id || "")}`, { lang: event.original_language })]),
      metadata([
        { value: event.company_name || event.company_id },
        { value: label("eventType", event.event_type) },
        { value: label("verification", event.verification_status) },
        { value: formatDate(event.occurred_at, false) }
      ])
    ]);
  }

  function eventScore(event) {
    const importance = { critical: 80, high: 55, medium: 25, low: 5 }[event.importance] || 0;
    const verification = { withdrawn: 28, official: 26, confirmed: 24, corroborated: 15, corrected: 10, disputed: 8, signal: 3, unverified: 0 }[event.verification_status] || 0;
    const date = normalizedDate(event.occurred_at);
    return importance + verification + (date ? date.getTime() / 1e13 : 0);
  }

  function watchCandidate(event) {
    if (["signal", "unverified", "disputed", "corrected"].includes(event.verification_status)) return true;
    const deadline = normalizedDate(event.deadline_at);
    if (!deadline) return false;
    const days = (deadline.getTime() - Date.now()) / 86400000;
    return days >= -2 && days <= 45;
  }

  async function renderToday(signal) {
    const payload = await request("/events", { params: { page: 1, limit: 50 }, signal });
    const events = Array.isArray(payload.data) ? payload.data : [];
    const top = [...events].sort((a, b) => eventScore(b) - eventScore(a)).slice(0, 5);
    const topIds = new Set(top.map((item) => item.event_id));
    const remaining = events.filter((item) => !topIds.has(item.event_id));
    const watch = remaining.filter(watchCandidate).slice(0, 10);
    const hiddenIds = new Set([...top, ...watch].map((item) => item.event_id));
    const archive = remaining.filter((item) => !hiddenIds.has(item.event_id));

    const topList = element("div", { className: "card-list" }, top.map((item, index) => eventCard(item, index + 1)));
    const watchList = element("ul", { className: "compact-list" }, watch.map(compactEvent));
    const archiveList = element("div", { className: "data-list", attrs: { id: "today-archive" } }, archive.map(archiveEvent));
    const archiveEmpty = emptyState("아카이브가 비어 있습니다.", "공개 승인된 사건이 추가되면 여기에 표시됩니다.");
    const archiveSection = element("section", { className: "section-block", attrs: { "aria-labelledby": "archive-title" } }, [
      element("div", { className: "section-heading" }, [
        element("h2", { text: "전체 아카이브 / Full archive", attrs: { id: "archive-title" } }),
        element("p", { text: "최신 사건부터 시간순" })
      ]),
      archive.length ? archiveList : archiveEmpty
    ]);

    const nextPage = payload.pagination && payload.pagination.next_page;
    if (nextPage) {
      const button = element("button", { text: "더 보기 / Load more", attrs: { type: "button" } });
      let page = Number(nextPage);
      button.addEventListener("click", async () => {
        button.disabled = true;
        try {
          const more = await request("/events", { params: { page, limit: 50 } });
          const rows = Array.isArray(more.data) ? more.data : [];
          if (rows.length && !archiveList.isConnected) archiveEmpty.replaceWith(archiveList);
          rows.forEach((item) => archiveList.append(archiveEvent(item)));
          page = more.pagination && more.pagination.next_page ? Number(more.pagination.next_page) : 0;
          if (!page) button.remove();
        } catch (error) {
          announceError(error);
          button.disabled = false;
        }
        if (page) button.disabled = false;
      });
      archiveSection.append(element("div", { className: "load-more" }, [button]));
    }

    app.replaceChildren(
      pageHeader(
        "TODAY / 오늘",
        "거버넌스 사건을 근거와 결과까지 추적합니다.",
        "중요 사건, 확인이 필요한 신호, 전체 기록을 한 화면에서 확인하세요.",
        [
          routeLink("사건 검색 / Search", "#/search", { className: "text-link" }),
          externalLink("Atom feed", endpoint("/feeds/events.atom").toString(), "text-link")
        ]
      ),
      element("div", { className: "today-grid" }, [
        element("section", { className: "section-block", attrs: { "aria-labelledby": "top-title" } }, [
          element("div", { className: "section-heading" }, [
            element("h2", { text: "Top 5", attrs: { id: "top-title" } }),
            element("p", { text: "중요도·근거 상태 기준" })
          ]),
          top.length ? topList : emptyState("공개된 사건이 없습니다.", "검수 완료된 사건이 추가되면 표시됩니다.")
        ]),
        element("aside", { className: "watch-panel", attrs: { "aria-labelledby": "watch-title" } }, [
          element("div", { className: "section-heading" }, [
            element("h2", { text: "Watch", attrs: { id: "watch-title" } }),
            element("p", { text: "추가 확인·기한 추적" })
          ]),
          watch.length ? watchList : element("p", { text: "현재 추적 중인 신호가 없습니다." })
        ])
      ]),
      archiveSection
    );
  }

  function companyRow(company) {
    const title = company.legal_name || company.short_name || company.company_id || "—";
    const aliases = Array.isArray(company.aliases) && company.aliases.length ? ` · ${company.aliases.join(", ")}` : "";
    return element("article", { className: "company-row" }, [
      element("h3", {}, [routeLink(title, `#/companies/${encodeURIComponent(company.company_id || "")}`)]),
      metadata([
        { value: company.stock_code || "" },
        { value: company.market || "" },
        { value: `사건 ${Number(company.event_count || 0)} / Events` },
        { value: `진행 캠페인 ${Number(company.active_campaign_count || 0)} / Active` }
      ]),
      company.legal_name_en ? element("p", { text: company.legal_name_en, lang: "en", className: "field-hint" }) : null,
      aliases ? element("p", { text: aliases.replace(/^ · /, ""), className: "field-hint" }) : null
    ].filter(Boolean));
  }

  async function renderCompanies(query, signal) {
    const q = String(query.get("q") || "").trim();
    const params = { page: 1, limit: 50 };
    if (q.length >= 2) params.q = q;
    const payload = await request("/companies", { params, signal });
    const companies = Array.isArray(payload.data) ? payload.data : [];
    const input = element("input", { attrs: { type: "search", name: "q", value: q, minlength: "2", maxlength: "100", placeholder: "회사명 또는 종목코드", "aria-label": "회사 검색 / Search companies" } });
    const form = element("form", { className: "inline-form", attrs: { role: "search" } }, [
      input,
      element("button", { text: "찾기 / Find", attrs: { type: "submit" } })
    ]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const value = input.value.trim();
      window.location.hash = value ? `#/companies?q=${encodeURIComponent(value)}` : "#/companies";
    });
    const companyList = element("div", { className: "data-list" }, companies.map(companyRow));
    const resultsSection = element("section", { className: "section-block", attrs: { "aria-labelledby": "company-results-title" } }, [
      sectionHeading(q ? `“${q}” 검색 결과` : "전체 기업 / All companies", `${companies.length}개 표시`, "company-results-title"),
      companies.length ? companyList : emptyState("기업을 찾지 못했습니다.", "회사명 또는 종목코드를 다시 확인해 주세요.")
    ]);
    if (companies.length) {
      addLoadMore(resultsSection, payload.pagination, async (page) => {
        const more = await request("/companies", { params: { ...params, page } });
        (Array.isArray(more.data) ? more.data : []).forEach((item) => companyList.append(companyRow(item)));
        return more.pagination;
      });
    }
    app.replaceChildren(
      pageHeader("COMPANIES / 기업", "기업별 거버넌스 기록", "DART corp_code를 기준으로 사건, 캠페인, 약속과 이행을 연결합니다."),
      element("section", { className: "filter-form", attrs: { "aria-label": "기업 검색" } }, [form]),
      resultsSection
    );
  }

  function facts(items) {
    const dl = element("dl", { className: "facts" });
    items.filter((item) => item && item.value !== undefined && item.value !== null && item.value !== "").forEach((item) => {
      dl.append(element("div", {}, [element("dt", { text: item.label }), item.node || element("dd", { text: item.value })]));
    });
    return dl;
  }

  function campaignRow(campaign) {
    return element("article", { className: "archive-row" }, [
      element("h3", {}, [routeLink(campaign.title || "제목 없음", `#/campaigns/${encodeURIComponent(campaign.campaign_id || "")}`, { lang: campaign.original_language })]),
      metadata([
        { value: label("campaignStage", campaign.stage) },
        campaign.outcome ? { value: label("outcome", campaign.outcome) } : null,
        { value: formatDate(campaign.started_at, false) }
      ])
    ]);
  }

  function commitmentCard(item) {
    return element("article", { className: "commitment-card" }, [
      element("div", { className: "badge-row" }, [badge(item.status, "commitment")]),
      sourceNode("h3", item.commitment_text, item.original_language),
      item.actual_action ? sourceNode("p", item.actual_action, item.original_language) : null,
      metadata([
        item.target_at ? { value: `기한 / Target ${formatDate(item.target_at, false)}` } : null
      ])
    ].filter(Boolean));
  }

  async function renderCompany(companyId, signal) {
    if (!validCompanyId(companyId)) throw new ApiError("Invalid company ID", "invalid_company_id", 400);
    const payload = await request(`/companies/${encodeURIComponent(companyId)}`, { signal });
    const data = payload.data || {};
    const company = data.company || {};
    const events = Array.isArray(data.events) ? data.events.map((item) => ({ ...item, company_id: companyId, company_name: company.legal_name })) : [];
    const campaigns = Array.isArray(data.campaigns) ? data.campaigns : [];
    const commitments = Array.isArray(data.commitments) ? data.commitments : [];
    const homepage = company.homepage_url ? externalLink("회사 웹사이트 / Website", company.homepage_url, "text-link") : null;
    app.replaceChildren(
      pageHeader(
        `${company.market || "COMPANY"}${company.stock_code ? ` · ${company.stock_code}` : ""}`,
        company.legal_name || companyId,
        company.legal_name_en || "기업별 사건·캠페인·약속 이행 기록",
        [homepage, routeLink("정정 요청 / Feedback", `#/feedback?entity_type=company&entity_id=${encodeURIComponent(companyId)}`, { className: "text-link" })].filter(Boolean)
      ),
      element("div", { className: "detail-grid" }, [
        element("div", {}, [
          element("section", { attrs: { "aria-labelledby": "campaigns-title" } }, [
            sectionHeading("현재 캠페인 / Campaigns", `${campaigns.length}개`, "campaigns-title"),
            campaigns.length ? element("div", { className: "data-list" }, campaigns.map(campaignRow)) : emptyState("공개 캠페인이 없습니다.", "새 캠페인이 확인되면 표시됩니다.")
          ]),
          element("section", { className: "section-block", attrs: { "aria-labelledby": "timeline-title" } }, [
            sectionHeading("거버넌스 타임라인 / Timeline", `${events.length}개 사건`, "timeline-title"),
            events.length ? element("div", { className: "card-list" }, events.map((item) => eventCard(item))) : emptyState("공개 사건이 없습니다.", "검수 완료된 사건이 추가되면 표시됩니다.")
          ]),
          element("section", { className: "section-block", attrs: { "aria-labelledby": "commitments-title" } }, [
            sectionHeading("밸류업 약속·이행 / Commitments", `${commitments.length}개`, "commitments-title"),
            commitments.length ? element("div", { className: "data-list" }, commitments.map(commitmentCard)) : emptyState("등록된 약속이 없습니다.", "회사 계획과 실제 이행이 연결되면 표시됩니다.")
          ])
        ]),
        element("aside", { className: "detail-sidebar", attrs: { "aria-label": "기업 정보 / Company information" } }, [
          element("h2", { text: "기업 정보 / Company" }),
          facts([
            { label: "DART corp_code", value: company.company_id || companyId },
            { label: "종목코드 / Ticker", value: company.stock_code },
            { label: "시장 / Market", value: company.market },
            { label: "약칭 / Short name", value: company.short_name },
            { label: "최종 갱신 / Updated", value: formatDate(company.updated_at, true) }
          ])
        ])
      ])
    );
  }

  function timeline(items) {
    return element("ol", { className: "timeline" }, items.map((item) => element("li", {}, [
      element("time", { text: formatDate(item.occurred_at, true), attrs: { datetime: item.occurred_at || "" } }),
      sourceNode("h3", item.title || item.entry_type, item.original_language),
      item.description ? sourceNode("p", item.description, item.original_language) : null
    ].filter(Boolean))));
  }

  function documentRow(documentItem) {
    const detail = routeLink(documentItem.title || "문서", `#/documents/${encodeURIComponent(documentItem.document_id || "")}`, { lang: documentItem.original_language });
    return element("article", { className: "document-row" }, [
      element("div", { className: "badge-row" }, [badge(documentItem.source_class, "sourceClass"), badge(documentItem.verification_status, "verification")]),
      element("h3", {}, [detail]),
      metadata([
        { value: documentItem.document_type || "" },
        { value: formatDate(documentItem.published_at, false) },
        { value: documentItem.version_no ? `v${documentItem.version_no}` : "" }
      ]),
      documentItem.original_url ? externalLink("원문 열기 / Open source", documentItem.original_url, "text-link") : null
    ].filter(Boolean));
  }

  function claimCard(claim) {
    return element("article", { className: "claim-card" }, [
      element("div", { className: "badge-row" }, [badge(claim.claim_type, "claimType")]),
      claim.actor_name ? element("h3", { text: claim.actor_name }) : null,
      sourceNode("p", claim.claim_text, claim.original_language),
      claim.document_title ? element("p", {}, [
        element("strong", { text: "근거 / Evidence: " }),
        claim.original_url ? externalLink(claim.document_title, claim.original_url) : element("span", { text: claim.document_title })
      ]) : null,
      claim.evidence_locator ? element("p", { text: claim.evidence_locator, className: "field-hint" }) : null
    ].filter(Boolean));
  }

  async function renderEvent(eventId, signal) {
    if (!validEntityId(eventId)) throw new ApiError("Invalid event ID", "invalid_event_id", 400);
    const payload = await request(`/events/${encodeURIComponent(eventId)}`, { signal });
    const data = payload.data || {};
    const event = data.event || {};
    const actors = Array.isArray(data.actors) ? data.actors : [];
    const claims = Array.isArray(data.claims) ? data.claims : [];
    const documents = Array.isArray(data.documents) ? data.documents : [];
    const entries = Array.isArray(data.timeline) ? data.timeline : [];
    const revisions = Array.isArray(data.revisions) ? data.revisions : [];

    const actorList = element("ul", { className: "compact-list" }, actors.map((actor) => element("li", { className: "compact-item" }, [
      sourceNode("strong", actor.display_name, actor.original_language),
      element("p", { text: `${label("actorType", actor.actor_type)} · ${actor.actor_role || "participant"}` }),
      actor.display_name_en ? element("p", { text: actor.display_name_en, lang: "en" }) : null
    ].filter(Boolean))));

    app.replaceChildren(
      pageHeader(
        label("eventType", event.event_type),
        event.title || eventId,
        `${event.company_name || event.company_id || ""} · ${formatDate(event.occurred_at, true)}`,
        [
          event.company_id ? routeLink("기업 기록 / Company", `#/companies/${encodeURIComponent(event.company_id)}`, { className: "text-link" }) : null,
          routeLink("정정·답변 / Feedback", `#/feedback?entity_type=event&entity_id=${encodeURIComponent(eventId)}`, { className: "text-link" })
        ].filter(Boolean),
        event.original_language
      ),
      element("div", { className: "detail-grid" }, [
        element("div", {}, [
          element("section", { attrs: { "aria-labelledby": "claims-title" } }, [
            sectionHeading("주장·반론·공식 사실 / Claims & evidence", `${claims.length}개`, "claims-title"),
            claims.length ? element("div", { className: "data-list" }, claims.map(claimCard)) : emptyState("공개된 주장 기록이 없습니다.", "공식 근거나 승인된 기록이 연결되면 표시됩니다.")
          ]),
          element("section", { className: "section-block", attrs: { "aria-labelledby": "documents-title" } }, [
            sectionHeading("공식 근거·보도 / Sources", `${documents.length}개`, "documents-title"),
            documents.length ? element("div", { className: "data-list" }, documents.map(documentRow)) : emptyState("공개 가능한 근거가 없습니다.", "이용권한과 검수가 확인된 문서만 표시됩니다.")
          ]),
          element("section", { className: "section-block", attrs: { "aria-labelledby": "event-timeline-title" } }, [
            sectionHeading("사건 이력 / Timeline", `${entries.length}개`, "event-timeline-title"),
            entries.length ? timeline(entries) : emptyState("등록된 이력이 없습니다.", "후속 조치와 결과가 확인되면 이어서 표시됩니다.")
          ]),
          revisions.length ? element("section", { className: "section-block" }, [
            sectionHeading("정정 이력 / Revisions", `${revisions.length}개`),
            element("div", { className: "data-list" }, revisions.map((revision) => element("article", { className: "archive-row" }, [
              element("h3", { text: revision.reason }),
              metadata([{ value: revision.field_name || "record" }, { value: formatDate(revision.published_at, true) }])
            ])))
          ]) : null
        ].filter(Boolean)),
        element("aside", { className: "detail-sidebar", attrs: { "aria-label": "사건 상태 / Event status" } }, [
          element("div", { className: "badge-row" }, [badge(event.importance, "importance"), badge(event.verification_status, "verification")]),
          facts([
            { label: "회사 / Company", node: element("dd", {}, [routeLink(event.company_name || event.company_id, `#/companies/${encodeURIComponent(event.company_id || "")}`)]) },
            { label: "발생 / Occurred", value: formatDate(event.occurred_at, true) },
            { label: "기한 / Deadline", value: event.deadline_at ? formatDate(event.deadline_at, true) : "—" },
            { label: "사건 ID / Event ID", value: event.event_id || eventId },
            { label: "언어 / Language", value: event.original_language || "—" }
          ]),
          element("section", { className: "section-block" }, [
            element("h2", { text: "당사자 / Parties" }),
            actors.length ? actorList : element("p", { text: "등록된 당사자가 없습니다." })
          ])
        ])
      ])
    );
  }

  function voteCard(vote) {
    return element("article", { className: "vote-card" }, [
      element("div", { className: "badge-row" }, [badge(vote.result, "outcome")]),
      sourceNode("h3", vote.agenda_title, vote.original_language),
      metadata([
        { value: formatDate(vote.meeting_at, true) },
        vote.agenda_no ? { value: `의안 ${vote.agenda_no}` } : null,
        vote.recommendation_source ? { value: vote.recommendation_source } : null
      ]),
      vote.recommendation ? sourceNode("p", vote.recommendation, vote.original_language) : null,
      (vote.votes_for !== null && vote.votes_for !== undefined) ? element("p", { text: `찬성 ${vote.votes_for}% · 반대 ${vote.votes_against || 0}% · 기권 ${vote.votes_abstain || 0}%`, className: "field-hint" }) : null
    ].filter(Boolean));
  }

  async function renderCampaign(campaignId, signal) {
    if (!validEntityId(campaignId)) throw new ApiError("Invalid campaign ID", "invalid_campaign_id", 400);
    const payload = await request(`/campaigns/${encodeURIComponent(campaignId)}`, { signal });
    const data = payload.data || {};
    const campaign = data.campaign || {};
    const entries = Array.isArray(data.timeline) ? data.timeline : [];
    const votes = Array.isArray(data.votes) ? data.votes : [];
    const commitments = Array.isArray(data.commitments) ? data.commitments : [];
    app.replaceChildren(
      pageHeader(
        label("campaignStage", campaign.stage),
        campaign.title || campaignId,
        `${campaign.company_name || campaign.company_id || ""}${campaign.lead_actor_name ? ` · ${campaign.lead_actor_name}` : ""}`,
        [
          campaign.company_id ? routeLink("기업 기록 / Company", `#/companies/${encodeURIComponent(campaign.company_id)}`, { className: "text-link" }) : null,
          routeLink("정정·답변 / Feedback", `#/feedback?entity_type=campaign&entity_id=${encodeURIComponent(campaignId)}`, { className: "text-link" })
        ].filter(Boolean),
        campaign.original_language
      ),
      element("div", { className: "detail-grid" }, [
        element("div", {}, [
          campaign.demand_text ? element("section", {}, [
            sectionHeading("요구사항 / Demands"),
            sourceNode("p", campaign.demand_text, campaign.original_language, "long-text")
          ]) : null,
          element("section", { className: "section-block" }, [
            sectionHeading("캠페인 이력 / Timeline", `${entries.length}개`),
            entries.length ? timeline(entries) : emptyState("등록된 이력이 없습니다.", "공개된 캠페인 진행 이력이 추가되면 표시됩니다.")
          ]),
          element("section", { className: "section-block" }, [
            sectionHeading("주주제안·표결 / Proposals & votes", `${votes.length}개`),
            votes.length ? element("div", { className: "data-list" }, votes.map(voteCard)) : emptyState("표결 기록이 없습니다.", "주총 결과가 확인되면 표시됩니다.")
          ]),
          element("section", { className: "section-block" }, [
            sectionHeading("합의·이행 / Outcomes", `${commitments.length}개`),
            commitments.length ? element("div", { className: "data-list" }, commitments.map(commitmentCard)) : emptyState("이행 기록이 없습니다.", "합의 또는 회사 약속의 후속 조치를 추적합니다.")
          ])
        ].filter(Boolean)),
        element("aside", { className: "detail-sidebar", attrs: { "aria-label": "캠페인 상태 / Campaign status" } }, [
          element("div", { className: "badge-row" }, [badge(campaign.stage, "campaignStage"), campaign.outcome ? badge(campaign.outcome, "outcome") : null].filter(Boolean)),
          facts([
            { label: "대상 / Company", node: element("dd", {}, [routeLink(campaign.company_name || campaign.company_id, `#/companies/${encodeURIComponent(campaign.company_id || "")}`)]) },
            { label: "제안 주체 / Lead", value: campaign.lead_actor_name },
            { label: "시작 / Started", value: formatDate(campaign.started_at, false) },
            { label: "종료 / Ended", value: campaign.ended_at ? formatDate(campaign.ended_at, false) : "진행 중 / Active" },
            { label: "캠페인 ID", value: campaign.campaign_id || campaignId }
          ])
        ])
      ])
    );
  }

  async function renderDocument(documentId, signal) {
    if (!validEntityId(documentId)) throw new ApiError("Invalid document ID", "invalid_document_id", 400);
    const payload = await request(`/documents/${encodeURIComponent(documentId)}`, { params: { include: "body" }, signal });
    const documentItem = payload.data || {};
    const body = documentItem.body_text || documentItem.body_excerpt || "본문이 공개되지 않았습니다. / Body not available.";
    app.replaceChildren(
      pageHeader(
        label("sourceClass", documentItem.source_class),
        documentItem.title || documentId,
        `${documentItem.company_name || ""} · ${formatDate(documentItem.published_at, true)}`,
        [
          documentItem.original_url ? externalLink("원문 열기 / Open source", documentItem.original_url, "text-link") : null,
          routeLink("정정 요청 / Feedback", `#/feedback?entity_type=document&entity_id=${encodeURIComponent(documentId)}`, { className: "text-link" })
        ].filter(Boolean),
        documentItem.original_language
      ),
      element("div", { className: "detail-grid" }, [
        element("section", { attrs: { "aria-labelledby": "document-body" } }, [
          element("h2", { text: "원문 / Source text", attrs: { id: "document-body" } }),
          sourceNode("div", body, documentItem.original_language, "long-text")
        ]),
        element("aside", { className: "detail-sidebar" }, [
          element("div", { className: "badge-row" }, [badge(documentItem.verification_status, "verification")]),
          facts([
            { label: "문서 유형 / Type", value: documentItem.document_type },
            { label: "외부 ID / External ID", value: documentItem.external_id },
            { label: "버전 / Version", value: documentItem.version_no },
            { label: "언어 / Language", value: documentItem.original_language },
            { label: "수집 / Retrieved", value: formatDate(documentItem.retrieved_at, true) }
          ])
        ])
      ])
    );
  }

  function isoDate(date) {
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
  }

  function calendarItem(item) {
    const date = normalizedDate(item.scheduled_at);
    const dateLabel = date ? new Intl.DateTimeFormat("ko-KR", { month: "short", day: "numeric", weekday: "short", timeZone: "Asia/Seoul" }).format(date) : "—";
    const title = item.item_type === "event" && validEntityId(item.entity_id)
      ? routeLink(item.title || "사건", `#/events/${encodeURIComponent(item.entity_id)}`, { lang: item.original_language })
      : sourceNode("span", item.title, item.original_language);
    return element("article", { className: "calendar-item" }, [
      element("div", { text: dateLabel, className: "calendar-date" }),
      element("div", {}, [
        element("div", { className: "badge-row" }, [badge(item.category, "eventType")]),
        element("h3", {}, [title]),
        metadata([
          { node: routeLink(item.company_name || item.company_id, `#/companies/${encodeURIComponent(item.company_id || "")}`), value: item.company_name || item.company_id },
          { value: formatDate(item.scheduled_at, true) }
        ])
      ])
    ]);
  }

  async function renderCalendar(query, signal) {
    const now = new Date();
    const future = new Date(now.getTime() + 90 * 86400000);
    const from = /^\d{4}-\d{2}-\d{2}$/.test(query.get("from") || "") ? query.get("from") : isoDate(now);
    const to = /^\d{4}-\d{2}-\d{2}$/.test(query.get("to") || "") ? query.get("to") : isoDate(future);
    const payload = await request("/calendar", { params: { from, to, page: 1, limit: 100 }, signal });
    const items = Array.isArray(payload.data) ? payload.data : [];
    const fromInput = element("input", { attrs: { id: "calendar-from", type: "date", name: "from", value: from, required: "required" } });
    const toInput = element("input", { attrs: { id: "calendar-to", type: "date", name: "to", value: to, required: "required" } });
    const form = element("form", { className: "field-grid" }, [
      element("div", { className: "field" }, [element("label", { text: "시작 / From", attrs: { for: "calendar-from" } }), fromInput]),
      element("div", { className: "field" }, [element("label", { text: "종료 / To", attrs: { for: "calendar-to" } }), toInput]),
      element("div", { className: "field--full" }, [element("button", { text: "기간 적용 / Apply", attrs: { type: "submit" } })])
    ]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      window.location.hash = `#/calendar?from=${encodeURIComponent(fromInput.value)}&to=${encodeURIComponent(toInput.value)}`;
    });
    const groups = new Map();
    items.forEach((item) => {
      const month = String(item.scheduled_at || "").slice(0, 7) || "기타";
      if (!groups.has(month)) groups.set(month, []);
      groups.get(month).push(item);
    });
    const groupNodes = [...groups.entries()].map(([month, values]) => element("section", { className: "calendar-group" }, [
      element("h2", { text: month }),
      element("div", { className: "data-list" }, values.map(calendarItem))
    ]));
    app.replaceChildren(
      pageHeader("CALENDAR / 캘린더", "주총·공개매수·주요 기한", "사건 기한과 의안 표결 일정을 시간순으로 확인합니다."),
      element("section", { className: "filter-form", attrs: { "aria-label": "캘린더 기간" } }, [form]),
      items.length ? fragment(groupNodes) : emptyState("해당 기간의 일정이 없습니다.", "기간을 넓혀 다시 확인해 주세요.")
    );
  }

  function resultRoute(item) {
    if (item.kind === "company") return `#/companies/${encodeURIComponent(item.entity_id || "")}`;
    if (item.kind === "event") return `#/events/${encodeURIComponent(item.entity_id || "")}`;
    if (item.kind === "campaign") return `#/campaigns/${encodeURIComponent(item.entity_id || "")}`;
    if (item.kind === "document") return `#/documents/${encodeURIComponent(item.entity_id || "")}`;
    return "";
  }

  function searchResult(item) {
    const route = resultRoute(item);
    const title = route
      ? routeLink(item.title || item.entity_id || "결과", route)
      : element("span", { text: item.title || item.entity_id || "결과" });
    const subtitle = item.kind === "actor" ? label("actorType", item.subtitle)
      : item.kind === "event" ? label("eventType", item.subtitle)
        : item.kind === "campaign" ? label("campaignStage", item.subtitle)
          : item.kind === "document" ? label("sourceClass", item.subtitle)
            : item.subtitle;
    return element("article", { className: "search-result" }, [
      element("div", { className: "badge-row" }, [badge(item.kind, "kind")]),
      element("h3", {}, [title]),
      metadata([
        { value: subtitle || "" },
        { value: item.company_id || "" },
        { value: formatDate(item.occurred_at || item.sort_at, false) }
      ])
    ]);
  }

  async function renderSearch(query, signal) {
    const q = String(query.get("q") || "").trim();
    const input = element("input", { attrs: { type: "search", name: "q", value: q, minlength: "2", maxlength: "100", required: "required", placeholder: "기업·행동주주·사건·공시 검색", "aria-label": "통합 검색어 / Search query" } });
    const form = element("form", { className: "inline-form", attrs: { role: "search" } }, [input, element("button", { text: "검색 / Search", attrs: { type: "submit" } })]);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (input.reportValidity()) window.location.hash = `#/search?q=${encodeURIComponent(input.value.trim())}`;
    });
    let results = [];
    let resultPayload = null;
    if (q.length >= 2) {
      resultPayload = await request("/search", { params: { q, page: 1, limit: 50 }, signal });
      results = Array.isArray(resultPayload.data) ? resultPayload.data : [];
    }
    const resultList = element("div", { className: "data-list" }, results.map(searchResult));
    const resultSection = q.length >= 2 ? element("section", { className: "section-block" }, [
      sectionHeading(`“${q}” 검색 결과`, `${results.length}개 표시`),
      results.length ? resultList : emptyState("검색 결과가 없습니다.", "다른 회사명, 사건명 또는 공시 제목을 입력해 주세요.")
    ]) : emptyState("검색어를 입력해 주세요.", "두 글자 이상 입력하면 공개 기록 전체를 검색합니다.");
    if (resultPayload && results.length) {
      addLoadMore(resultSection, resultPayload.pagination, async (page) => {
        const more = await request("/search", { params: { q, page, limit: 50 } });
        (Array.isArray(more.data) ? more.data : []).forEach((item) => resultList.append(searchResult(item)));
        return more.pagination;
      });
    }
    app.replaceChildren(
      pageHeader("SEARCH / 검색", "통합 검색", "회사, 행동주주, 사건 유형, 공시와 캠페인을 한 번에 찾습니다."),
      element("section", { className: "filter-form" }, [form]),
      resultSection
    );
  }

  function selectField(labelText, name, values, selected) {
    const select = element("select", { attrs: { name, id: `feedback-${name}` } });
    values.forEach(([value, text]) => {
      const option = element("option", { text, attrs: { value } });
      if (value === selected) option.selected = true;
      select.append(option);
    });
    return element("div", { className: "field" }, [element("label", { text: labelText, attrs: { for: `feedback-${name}` } }), select]);
  }

  function feedbackError(code) {
    const messages = {
      invalid_feedback_type: "접수 유형을 확인해 주세요. / Check feedback type.",
      invalid_message_length: "내용은 10자 이상 10,000자 이하로 입력해 주세요. / Message must be 10–10,000 characters.",
      entity_type_and_id_required_together: "대상 유형과 ID를 함께 입력해 주세요. / Entity type and ID are required together.",
      invalid_entity_reference: "대상 기록을 확인해 주세요. / Check the referenced record.",
      feedback_rate_limited: "잠시 후 다시 시도해 주세요. / Please try again later."
    };
    return messages[code] || "접수 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요. / Submission failed. Please try again later.";
  }

  function renderFeedback(query) {
    const entityType = String(query.get("entity_type") || "");
    const entityId = String(query.get("entity_id") || "");
    const typeField = selectField("접수 유형 / Request type", "feedback_type", [
      ["correction", "정정 요청 / Correction"],
      ["right_of_reply", "당사자 답변 / Right of reply"],
      ["source_rights", "소스 권한 / Source rights"],
      ["general", "일반 문의 / General"]
    ], "correction");
    const entityField = selectField("대상 유형 / Entity type", "entity_type", [
      ["", "선택 안 함 / None"],
      ["company", "기업 / Company"],
      ["event", "사건 / Event"],
      ["campaign", "캠페인 / Campaign"],
      ["document", "문서 / Document"]
    ], entityType);
    const idInput = element("input", { attrs: { id: "feedback-entity-id", name: "entity_id", value: entityId, maxlength: "96", pattern: "[A-Za-z0-9_.:-]{1,96}" } });
    const nameInput = element("input", { attrs: { id: "feedback-name", name: "submitter_name", maxlength: "191", autocomplete: "name" } });
    const contactInput = element("input", { attrs: { id: "feedback-contact", name: "submitter_contact", maxlength: "320", autocomplete: "email", placeholder: "email or contact" } });
    const messageInput = element("textarea", { attrs: { id: "feedback-message", name: "message", minlength: "10", maxlength: "10000", required: "required" } });
    const evidenceInput = element("textarea", { attrs: { id: "feedback-evidence", name: "evidence_urls", maxlength: "10000", placeholder: "https://…\nhttps://…" } });
    const websiteInput = element("input", { attrs: { id: "feedback-website", name: "website", tabindex: "-1", autocomplete: "off" } });
    const status = element("p", { className: "form-status", attrs: { role: "status", "aria-live": "polite" } });
    const submit = element("button", { text: "비공개 접수 / Submit privately", attrs: { type: "submit" } });
    const form = element("form", { className: "feedback-form" }, [
      element("div", { className: "field-grid" }, [
        typeField,
        entityField,
        element("div", { className: "field field--full" }, [element("label", { text: "대상 ID / Entity ID", attrs: { for: "feedback-entity-id" } }), idInput]),
        element("div", { className: "field" }, [element("label", { text: "이름 / Name", attrs: { for: "feedback-name" } }), nameInput]),
        element("div", { className: "field" }, [element("label", { text: "연락처 / Contact", attrs: { for: "feedback-contact" } }), contactInput]),
        element("div", { className: "field field--full" }, [element("label", { text: "요청 내용 / Message", attrs: { for: "feedback-message" } }), messageInput, element("span", { text: "사실관계와 원하는 정정 또는 답변 내용을 구체적으로 적어 주세요.", className: "field-hint" })]),
        element("div", { className: "field field--full" }, [element("label", { text: "근거 URL / Evidence URLs", attrs: { for: "feedback-evidence" } }), evidenceInput, element("span", { text: "한 줄에 하나씩, 최대 10개", className: "field-hint" })]),
        element("div", { className: "sr-only", attrs: { "aria-hidden": "true" } }, [element("label", { text: "Website", attrs: { for: "feedback-website" } }), websiteInput])
      ]),
      element("p", { text: "제출 내용은 자동 공개되지 않으며 편집 검수 전까지 비공개로 보관됩니다. / Submissions stay private pending editorial review.", className: "form-note" }),
      element("div", { className: "form-actions" }, [submit, status])
    ]);
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const selectedType = form.elements.entity_type.value;
      const selectedId = idInput.value.trim();
      if ((selectedType && !selectedId) || (!selectedType && selectedId)) {
        status.textContent = feedbackError("entity_type_and_id_required_together");
        return;
      }
      const evidenceUrls = evidenceInput.value.split(/\r?\n/).map((value) => value.trim()).filter(Boolean).slice(0, 10);
      const body = {
        feedback_type: form.elements.feedback_type.value,
        message: messageInput.value.trim(),
        submitter_name: nameInput.value.trim(),
        submitter_contact: contactInput.value.trim(),
        evidence_urls: evidenceUrls,
        website: websiteInput.value
      };
      if (selectedType && selectedId) {
        body.entity_type = selectedType;
        body.entity_id = selectedId;
      }
      submit.disabled = true;
      status.textContent = "접수 중 / Submitting…";
      try {
        const payload = await request("/feedback", { method: "POST", body });
        form.reset();
        status.textContent = `접수되었습니다. 검수 상태: ${payload.status || "pending"} / Submitted for private review.`;
      } catch (error) {
        status.textContent = feedbackError(error.code);
      } finally {
        submit.disabled = false;
      }
    });
    app.replaceChildren(
      pageHeader("FEEDBACK / 정정·답변", "정정 요청과 당사자 답변", "사실 오류, 당사자 입장, 소스 권한 문제를 편집팀에 비공개로 제출합니다."),
      form
    );
  }

  function renderNotFound() {
    app.replaceChildren(element("section", { className: "error-state" }, [
      element("p", { text: "404", className: "eyebrow" }),
      element("h1", { text: "페이지를 찾을 수 없습니다." }),
      element("p", { text: "주소를 확인하거나 오늘 기록으로 돌아가 주세요. / Page not found." }),
      routeLink("오늘 기록 / Today", "#/today", { className: "button" })
    ]));
  }

  function errorMessage(code) {
    const known = {
      company_not_found: "기업 기록을 찾을 수 없습니다. / Company not found.",
      event_not_found: "사건 기록을 찾을 수 없습니다. / Event not found.",
      campaign_not_found: "캠페인 기록을 찾을 수 없습니다. / Campaign not found.",
      document_not_found: "공개 가능한 문서를 찾을 수 없습니다. / Public document not found.",
      response_budget_exceeded: "응답 범위를 줄여 다시 시도해 주세요. / Narrow the request and try again."
    };
    return known[code] || "기록을 불러오지 못했습니다. 잠시 후 다시 시도해 주세요. / Unable to load records.";
  }

  function renderError(error) {
    const code = error && error.code ? error.code : "request_failed";
    app.replaceChildren(element("section", { className: "error-state" }, [
      element("p", { text: "DATA UNAVAILABLE", className: "eyebrow" }),
      element("h1", { text: "기록을 불러오지 못했습니다." }),
      element("p", { text: errorMessage(code) }),
      element("code", { text: code, className: "error-code" }),
      element("div", {}, [element("button", { text: "다시 시도 / Retry", attrs: { type: "button" } })])
    ]));
    const retry = app.querySelector("button");
    if (retry) retry.addEventListener("click", navigate);
  }

  function announceError(error) {
    announcer.textContent = errorMessage(error && error.code);
  }

  function parseRoute() {
    const raw = window.location.hash.startsWith("#/") ? window.location.hash.slice(1) : "/today";
    const question = raw.indexOf("?");
    const path = question >= 0 ? raw.slice(0, question) : raw;
    const query = new URLSearchParams(question >= 0 ? raw.slice(question + 1) : "");
    const segments = path.split("/").filter(Boolean).map((segment) => {
      try { return decodeURIComponent(segment); } catch (_error) { return ""; }
    });
    return { path, query, segments };
  }

  function activeNav(route) {
    const first = route.segments[0] || "today";
    const active = first === "documents" || first === "events" || first === "campaigns" ? "today" : first;
    document.querySelectorAll("[data-nav]").forEach((link) => {
      if (link.dataset.nav === active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
  }

  function renderLoading() {
    app.setAttribute("aria-busy", "true");
    app.replaceChildren(element("section", { className: "loading-state" }, [
      element("p", { text: "LOADING / 불러오는 중", className: "eyebrow" }),
      element("h1", { text: "공개 기록을 확인하고 있습니다." }),
      element("p", { text: "공식 근거와 최신 상태를 불러옵니다. / Loading evidence and status." })
    ]));
  }

  function finishNavigation(route) {
    app.removeAttribute("aria-busy");
    activeNav(route);
    const heading = app.querySelector("h1");
    const title = heading ? heading.textContent : "거버넌스 인텔리전스";
    document.title = `${title} | BSIDE Governance Intelligence`;
    announcer.textContent = `${title} 페이지를 열었습니다.`;
    if (heading) {
      heading.setAttribute("tabindex", "-1");
      heading.focus({ preventScroll: true });
    }
    window.scrollTo({ top: 0, behavior: "auto" });
  }

  async function navigate() {
    if (!window.location.hash.startsWith("#/")) {
      window.location.hash = "#/today";
      return;
    }
    if (routeController) routeController.abort();
    routeController = new AbortController();
    const signal = routeController.signal;
    const route = parseRoute();
    renderLoading();
    try {
      const [first, second] = route.segments;
      if (!first || first === "today") await renderToday(signal);
      else if (first === "companies" && second) await renderCompany(second, signal);
      else if (first === "companies") await renderCompanies(route.query, signal);
      else if (first === "events" && second) await renderEvent(second, signal);
      else if (first === "campaigns" && second) await renderCampaign(second, signal);
      else if (first === "documents" && second) await renderDocument(second, signal);
      else if (first === "calendar") await renderCalendar(route.query, signal);
      else if (first === "search") await renderSearch(route.query, signal);
      else if (first === "feedback") renderFeedback(route.query);
      else renderNotFound();
      finishNavigation(route);
    } catch (error) {
      if (error && error.name === "AbortError") return;
      renderError(error);
      finishNavigation(route);
    }
  }

  window.addEventListener("hashchange", navigate);
  navigate();
})();
