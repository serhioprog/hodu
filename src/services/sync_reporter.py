"""
Per-domain sync reporting.

Builds a structured DomainSyncReport for each scraped domain and renders
it as an HTML-formatted Telegram message.

Decoupled from daily_sync.py so the formatting logic can be tested in
isolation and reused for future report destinations (email, Slack, etc).

Status semantics:
  • OK         — green, routine summary (silent notification)
  • WARNING    — yellow, partial failure or anomaly (audible)
  • CRITICAL   — red, scraper completely failed (audible, urgent)
  • CLOUDFLARE — special case of CRITICAL: blocked by anti-bot
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from src.services.cost_tracker import CostSnapshot
from src.services.telegram_notifier import telegram_notifier


# =====================================================================
# REPORT DATACLASS
# =====================================================================
@dataclass
class DomainSyncReport:
    """All metrics for a single domain's sync run."""
    domain:           str
    started_at:       datetime
    finished_at:      Optional[datetime] = None

    # --- Scrape phase --------------------------------------
    listings_found:   int = 0           # after whitelist filter
    db_count_before:  int = 0           # rows in DB pre-sync
    new_count:        int = 0
    delisted_count:   int = 0
    price_changed:    int = 0
    revived_count:    int = 0

    # --- Quality (filled post-ingest from DB) --------------
    total_after:      Optional[int] = None
    avg_desc_len:     Optional[int] = None
    pct_with_features: Optional[int] = None
    pct_with_price:   Optional[int] = None

    # --- AI usage (snapshot for THIS domain) ---------------
    cost_snapshot: Optional[CostSnapshot] = None

    # --- Status flags --------------------------------------
    cloudflare_blocked: bool = False
    error_message:      Optional[str] = None

    @property
    def duration_seconds(self) -> int:
        end = self.finished_at or datetime.now(timezone.utc)
        return max(0, int((end - self.started_at).total_seconds()))

    @property
    def status(self) -> str:
        if self.cloudflare_blocked:
            return "CLOUDFLARE"
        if self.error_message:
            return "CRITICAL"
        # Anomaly heuristics — could be tuned later via settings
        if self.listings_found == 0:
            return "WARNING"
        if (
            self.pct_with_features is not None
            and self.pct_with_features < 50
        ):
            return "WARNING"
        if (
            self.pct_with_price is not None
            and self.pct_with_price < 90
        ):
            return "WARNING"
        return "OK"


# =====================================================================
# FORMATTING HELPERS
# =====================================================================
_STATUS_ICON = {
    "OK":         "🟢",
    "WARNING":    "🟡",
    "CRITICAL":   "🔴",
    "CLOUDFLARE": "🛡",
}


def _fmt_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


def _fmt_money(usd: float) -> str:
    if usd == 0:
        return "$0"
    if usd < 0.01:
        return f"${usd:.4f}"
    if usd < 1:
        return f"${usd:.3f}"
    return f"${usd:.2f}"


def _fmt_int(n: int) -> str:
    """Thin-space thousands separator: 16234 -> '16 234'."""
    return f"{n:,}".replace(",", " ")


# =====================================================================
# PER-DOMAIN REPORT
# =====================================================================
def format_domain_report(r: DomainSyncReport) -> str:
    """Render a DomainSyncReport as Telegram HTML."""
    icon = _STATUS_ICON.get(r.status, "⚪")
    n = telegram_notifier.escape  # alias: HTML escape
    domain = n(r.domain)
    started = r.started_at.astimezone().strftime("%d.%m %H:%M")
    duration = _fmt_duration(r.duration_seconds)

    # ── Special-case: Cloudflare / critical ──────────────────────
    if r.status in ("CLOUDFLARE", "CRITICAL"):
        title_label = "CLOUDFLARE BLOCKED" if r.cloudflare_blocked else "CRITICAL"
        lines = [
            f"{icon} <b>{domain}</b> — {title_label}",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"📅 {started}  ·  duration: <i>{duration}</i>",
            "",
        ]
        if r.cloudflare_blocked:
            lines += [
                "⛔ <b>Cloudflare returned 403 / 503</b>",
                f"   → 0 listings collected",
                f"   → DB before sync: <b>{r.db_count_before}</b> properties",
                f"   → Delisting <b>NOT applied</b> (false-DELIST guard)",
                "",
                "🛡 Possible actions:",
                "   • change curl_cffi browser profile",
                "   • run via playwright",
                "   • increase sleep between requests",
            ]
        else:
            lines += [
                "⛔ <b>Error:</b>",
                f"<pre>{n(r.error_message or 'unknown')[:400]}</pre>",
            ]
        return "\n".join(lines)

    # ── Routine OK / WARNING report ──────────────────────────────
    lines = [
        f"{icon} <b>{domain}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 {started}  ·  <i>{duration}</i>",
        "",
        "<b>📊 SCRAPING</b>",
        f"  Listings found:     <b>{r.listings_found}</b>",
        f"  ➕ New:              <b>{r.new_count}</b>",
        f"  ➖ Delisted:         <b>{r.delisted_count}</b>",
        f"  💱 Price changed:    <b>{r.price_changed}</b>",
        f"  🔄 Recovered:        <b>{r.revived_count}</b>",
    ]

    # Quality block (only if collected)
    if r.total_after is not None:
        lines += [
            "",
            "<b>📐 DATA QUALITY</b>",
            f"  Properties in DB:   <b>{r.total_after}</b>",
        ]
        if r.avg_desc_len is not None:
            lines.append(
                f"  Avg description:    <b>{r.avg_desc_len}</b> chars"
            )
        if r.pct_with_features is not None:
            lines.append(
                f"  With features (≥5): <b>{r.pct_with_features}%</b>"
            )
        if r.pct_with_price is not None:
            lines.append(
                f"  With price:         <b>{r.pct_with_price}%</b>"
            )

    # AI usage block (only if anything was charged)
    snap = r.cost_snapshot
    if snap and snap.total_cost_usd > 0:
        lines += [
            "",
            "<b>🧠 AI EXTRACTION</b>",
        ]
        if snap.llm.calls:
            lines += [
                f"  LLM calls:          <b>{snap.llm.calls}</b>",
                f"  • Tokens (in/out):  {_fmt_int(snap.llm.in_tokens)} / {_fmt_int(snap.llm.out_tokens)}",
                f"  • Cost:             <b>{_fmt_money(snap.llm.cost_usd)}</b>",
            ]
            if snap.llm.failed_calls:
                lines.append(
                    f"  ⚠️ Failed:           {snap.llm.failed_calls}"
                )
        if snap.vision.calls:
            lines += [
                "",
                f"  👁 Vision calls:    <b>{snap.vision.calls}</b>",
                f"  • Cost:             <b>{_fmt_money(snap.vision.cost_usd)}</b>",
            ]
            if snap.vision.failed_calls:
                lines.append(
                    f"  ⚠️ Image dl failed:  {snap.vision.failed_calls}"
                )
        if snap.embedding.calls:
            lines += [
                "",
                f"  🧬 Embeddings:      <b>{snap.embedding.calls}</b>",
                f"  • Cost:             <b>{_fmt_money(snap.embedding.cost_usd)}</b>",
            ]
        lines += [
            "",
            f"  💰 <b>Total: {_fmt_money(snap.total_cost_usd)}</b>",
        ]

    return "\n".join(lines)


# =====================================================================
# DAILY SUMMARY
# =====================================================================
def format_daily_summary(
    domains: list[DomainSyncReport],
    daily_costs: CostSnapshot,
    *,
    dedup_stats:  Optional[dict] = None,
    funnel_stats: Optional[dict] = None,
) -> str:
    """End-of-cycle summary across all domains.

    Args:
      domains:      per-domain reports
      daily_costs:  aggregated cost snapshot for the day
      dedup_stats:  optional dict from daily_sync._collect_dedup_stats
      funnel_stats: optional dict {stage_num: {ok, fail, avg_ms}, ...}
                    keyed by stage number (0, 1, 2, ...). When provided,
                    a 🪜 FETCH FUNNEL section is added to the summary so
                    operators can see how often each stage was hit and
                    whether it's healthy. Built by daily_sync from the
                    fetch_attempts table.
    """
    when = datetime.now().strftime("%d.%m.%Y %H:%M")
    n = telegram_notifier.escape

    # Group domains by status
    domains_ok       = [d for d in domains if d.status == "OK"]
    domains_warning  = [d for d in domains if d.status == "WARNING"]
    domains_critical = [d for d in domains if d.status in ("CRITICAL", "CLOUDFLARE")]

    total_listings  = sum(d.listings_found for d in domains)
    total_new       = sum(d.new_count for d in domains)
    total_delisted  = sum(d.delisted_count for d in domains)
    total_revived   = sum(d.revived_count for d in domains)
    total_price     = sum(d.price_changed for d in domains)

    overall_icon = "📁" if not domains_critical else "⚠️"

    lines = [
        f"{overall_icon} <b>SESSION — {when}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "<b>🌐 DOMAINS</b>",
    ]

    # Always show all 3 categories — empty ones have no domain list under
    lines.append(f"  🟢 <b>Done</b> ({len(domains_ok)}):")
    for d in domains_ok:
        lines.append(f"     {n(d.domain)}")

    lines.append(f"  🟡 <b>Warning</b> ({len(domains_warning)}):")
    for d in domains_warning:
        lines.append(f"     {n(d.domain)}")

    lines.append(f"  🔴 <b>Critical</b> ({len(domains_critical)}):")
    for d in domains_critical:
        # Annotate with reason short-form
        reason = "cloudflare" if d.cloudflare_blocked else "error"
        lines.append(f"     {n(d.domain)}  <i>({reason})</i>")

    lines += [
        "",
        "<b>🕐 TOTAL PER DAY</b>",
        f"  ♾️ Listings checked: <b>{total_listings}</b>",
        f"  ➕ New:              <b>{total_new}</b>",
        f"  ➖ Delisted:         <b>{total_delisted}</b>",
        f"  💱 Price change:     <b>{total_price}</b>",
        f"  🔄 Recovered:        <b>{total_revived}</b>",
    ]

    # ── Dedup statistics block (clusters touched this run) ────────
    if dedup_stats and dedup_stats.get("clusters_touched", 0) > 0:
        lines += [
            "",
            "<b>🔍 DUPLICATES</b>",
            f"  📄 Clusters found:  <b>{dedup_stats['clusters_touched']}</b>",
            f"  ✅ Approved:         <b>{dedup_stats.get('approved', 0)}</b>",
            f"  ⏳ Pending:          <b>{dedup_stats.get('pending', 0)}</b>",
        ]

    # ── Fetch funnel health block ─────────────────────────────────
    # Per-stage success counts and avg latency over the last 24h.
    # Useful for spotting drift: if stage 1 traffic share is climbing,
    # something on a domain has degraded even before quality metrics
    # show it.
    if funnel_stats:
        stage_names = {
            0: "curl_cffi",
            1: "Playwright",
            2: "flaresolverr",
            3: "Browserless",
            4: "ScrapingBee",
        }
        # Filter to stages that actually saw traffic — no point listing
        # zero-attempt stages just because they're enabled.
        active_stages = sorted(
            s for s, st in funnel_stats.items()
            if (st.get("ok", 0) + st.get("fail", 0)) > 0
        )
        if active_stages:
            lines += [
                "",
                "<b>🪜 FETCH FUNNEL</b>",
            ]
            for s in active_stages:
                st = funnel_stats[s]
                ok = st.get("ok", 0)
                fail = st.get("fail", 0)
                total = ok + fail
                rate = (100 * ok / total) if total else 0
                avg_ms = int(st.get("avg_ms", 0) or 0)
                name = stage_names.get(s, f"stage {s}")
                lines.append(
                    f"  Stage {s} ({name}):"
                    f"  <b>{ok}</b> ok / {total}"
                    f"  ({rate:.1f}%)"
                    f"  · avg <b>{avg_ms}</b>ms"
                )

    # ── AI cost block ─────────────────────────────────────────────
    lines += [
        "",
        f"<b>💰 SESSION COST: {_fmt_money(daily_costs.total_cost_usd)}</b>",
    ]
    if daily_costs.total_cost_usd > 0:
        if daily_costs.llm.calls:
            failed = (
                f", {daily_costs.llm.failed_calls} failed"
                if daily_costs.llm.failed_calls else ""
            )
            lines.append(
                f"  🧠 LLM:        "
                f"{_fmt_money(daily_costs.llm.cost_usd)} "
                f"({daily_costs.llm.calls} calls{failed})"
            )
        if daily_costs.vision.calls:
            failed = (
                f", {daily_costs.vision.failed_calls} failed"
                if daily_costs.vision.failed_calls else ""
            )
            lines.append(
                f"  👁 Vision:     "
                f"{_fmt_money(daily_costs.vision.cost_usd)} "
                f"({daily_costs.vision.calls} calls{failed})"
            )
        if daily_costs.embedding.calls:
            lines.append(
                f"  🧬 Embedding:  "
                f"{_fmt_money(daily_costs.embedding.cost_usd)} "
                f"({daily_costs.embedding.calls} calls, "
                f"{_fmt_int(daily_costs.embedding.in_tokens)} tokens)"
            )

    return "\n".join(lines)
