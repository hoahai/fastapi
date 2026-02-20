from __future__ import annotations

from datetime import datetime
import html as html_lib
from pathlib import Path
from string import Template
from zoneinfo import ZoneInfo

from shared.logger import get_client_id, get_request_id
from shared.tenant import get_tenant_id, get_timezone


def build_google_ads_result_email(*, full_report: dict) -> str:
    """
    Build a plain-text Google Ads update report email
    from the full pipeline result.
    """

    lines: list[str] = []

    # =====================================================
    # HEADER
    # =====================================================
    lines.append("SpendSphere - Google Ads Update Report")
    lines.append("=" * 60)
    lines.append(f"Generated at: {datetime.utcnow().isoformat()} UTC")
    lines.append("")

    # =====================================================
    # RUN CONFIGURATION
    # =====================================================
    lines.append("Run configuration:")
    lines.append(f"- Dry run: {full_report.get('dry_run')}")
    lines.append(f"- Account codes: {full_report.get('account_codes')}")
    lines.append("")

    # =====================================================
    # OVERALL SUMMARY
    # =====================================================
    overall = full_report.get("overall_summary", {})

    lines.append("Overall summary:")
    lines.append(f"- Total operations: {overall.get('total', 0)}")
    lines.append(f"- Succeeded: {overall.get('succeeded', 0)}")
    lines.append(f"- Failed: {overall.get('failed', 0)}")
    lines.append("")

    # =====================================================
    # PER-CUSTOMER RESULTS
    # =====================================================
    mutation_results = full_report.get("mutation_results", [])

    if not mutation_results:
        lines.append("No Google Ads mutations were executed.")
        lines.append("")
        lines.append("=" * 60)
        lines.append("End of report")
        return "\n".join(lines)

    lines.append("Per-customer results:")
    lines.append("")

    for r in mutation_results:
        lines.append("-" * 60)
        lines.append(f"Customer ID: {r.get('customerId')}")
        lines.append(f"Operation: {r.get('operation')}")

        summary = r.get("summary", {})
        lines.append(
            f"Summary -> "
            f"Total: {summary.get('total', 0)}, "
            f"Succeeded: {summary.get('succeeded', 0)}, "
            f"Failed: {summary.get('failed', 0)}"
        )

        # ---------------------
        # Successes
        # ---------------------
        successes = r.get("successes", [])
        if successes:
            lines.append("Successes:")
            for s in successes:
                lines.append(f"  - {s}")
        else:
            lines.append("Successes: none")

        # ---------------------
        # Failures
        # ---------------------
        failures = r.get("failures", [])
        if failures:
            lines.append("Failures:")
            for f in failures:
                lines.append(f"  - {f}")
        else:
            lines.append("Failures: none")

        lines.append("")

    # =====================================================
    # FOOTER
    # =====================================================
    lines.append("=" * 60)
    lines.append("End of report")

    return "\n".join(lines)


def build_google_ads_alert_email(
    *,
    full_report: dict,
    template_path: str | Path | None = None,
) -> tuple[str, str, str]:
    """
    Build subject, plain-text, and HTML alert email bodies for failures/warnings.
    """

    overall = full_report.get("overall_summary", {}) if full_report else {}
    failed_count = int(overall.get("failed", 0) or 0)
    warning_count = int(overall.get("warnings", 0) or 0)
    total_count = int(overall.get("total", 0) or 0)
    succeeded_count = int(overall.get("succeeded", 0) or 0)
    dry_run = bool(full_report.get("dry_run")) if full_report else False
    tz = ZoneInfo(get_timezone())
    now_local = datetime.now(tz)
    generated_at = now_local.strftime("%m/%d/%Y %H:%M:%S")
    short_timestamp = (
        f"{now_local.month}/{now_local.day}/{now_local:%y} {now_local:%H:%M}"
    )

    if failed_count > 0 and warning_count > 0:
        alert_label = "Failures & Warnings"
    elif failed_count > 0:
        alert_label = "Failures"
    else:
        alert_label = "Warnings"

    tenant_id = get_tenant_id() or "Not Found"
    subject = (
        f"SpendSphere Alert [{tenant_id}]: {alert_label} - {short_timestamp}"
    )
    app_label = "SpendSphere"

    mutation_results = full_report.get("mutation_results") or []
    budget_failures: list[dict] = []
    budget_warnings: list[dict] = []
    campaign_status_total = 0
    campaign_status_failed = 0

    for r in mutation_results:
        operation = r.get("operation")
        summary = r.get("summary") or {}
        if operation == "update_campaign_statuses":
            campaign_status_total += int(summary.get("total", 0) or 0)
            campaign_status_failed += int(summary.get("failed", 0) or 0)
            continue
        if operation != "update_budgets":
            continue

        account_code = r.get("accountCode")
        for item in r.get("failures") or []:
            budget_failures.append(_normalize_budget_issue(item, account_code))
        for item in r.get("warnings") or []:
            budget_warnings.append(_normalize_budget_issue(item, account_code))

    campaign_status_summary = "update_campaign_statuses"

    def _issue_lines(issues: list[dict]) -> list[str]:
        lines: list[str] = []
        for issue in issues:
            title = _format_issue_title(issue)
            detail = _format_issue_detail_text(issue)
            lines.append(f"- {title}")
            lines.append(f"  - {detail}")
        return lines

    request_id = get_request_id() or "Not Found"
    client_id = get_client_id() or "Not Found"
    text_lines = [
        f"{app_label} - Google Ads Alert",
        "=" * 60,
        f"Generated at: {generated_at}",
        f"Request ID: {request_id}",
        f"Client ID: {client_id}",
        f"Tenant ID: {tenant_id}",
        f"Operation: {campaign_status_summary}",
        f"Dry run: {dry_run}",
        "",
        "Summary:",
        f"- Total operations: {total_count}",
        f"- Succeeded: {succeeded_count}",
        f"- Failed: {failed_count}",
        f"- Warnings: {warning_count}",
    ]

    if budget_warnings:
        text_lines.append("")
        text_lines.append("Warnings:")
        text_lines.extend(_issue_lines(budget_warnings))
    if budget_failures:
        text_lines.append("")
        text_lines.append("Failures:")
        text_lines.extend(_issue_lines(budget_failures))

    text_lines.append("")
    text_lines.append("=" * 60)
    text_lines.append("End of alert")
    text_body = "\n".join(text_lines)

    def esc(value: object) -> str:
        return html_lib.escape("" if value is None else str(value))

    def _html_issue_list(issues: list[dict]) -> str:
        if not issues:
            return "<p style=\"margin:0;color:#6b7280;\">None</p>"
        items_html = []
        for issue in issues:
            header = esc(_format_issue_title(issue))
            detail = _format_issue_detail_html(issue)
            items_html.append(
                "<div style=\"padding:12px;border:1px solid #e5e7eb;border-radius:12px;margin-bottom:10px;\">"
                f"<div style=\"font-weight:600;margin-bottom:6px;\">{header}</div>"
                f"<div style=\"font-size:13px;color:#374151;\">{detail}</div>"
                "</div>"
            )
        return "".join(items_html)

    stats_html = (
        _html_stat_card("Total", total_count, "#0f172a", "#e2e8f0")
        + _html_stat_card("Succeeded", succeeded_count, "#065f46", "#d1fae5")
        + _html_stat_card("Failed", failed_count, "#991b1b", "#fee2e2")
        + _html_stat_card("Warnings", warning_count, "#f97316", "#ffedd5")
    )

    resolved_template_path = (
        Path(template_path)
        if template_path is not None
        else Path(__file__).resolve().parents[5]
        / "static"
        / "google_ads_alert_email.html"
    )
    template = _load_alert_template(resolved_template_path)
    html_body = Template(template).safe_substitute(
        {
            "subject": esc(subject),
            "app_label": esc(app_label),
            "generated_at": esc(generated_at),
            "request_id": esc(request_id),
            "client_id": esc(client_id),
            "tenant_id": esc(tenant_id),
            "dry_run": esc(dry_run),
            "campaign_status_summary": esc(campaign_status_summary),
            "stats_html": stats_html,
            "failures_html": _html_issue_list(budget_failures),
            "warnings_html": _html_issue_list(budget_warnings),
        }
    )

    return subject, text_body, html_body


def _html_stat_card(label: str, value: int, text_color: str, bg_color: str) -> str:
    return (
        "<td valign=\"top\" style=\"background:"
        + bg_color
        + ";border-radius:10px;padding:12px 16px 14px;width:110px;\">"
        + f"<div style=\"font-size:13px;color:#6b7280;margin-bottom:8px;\">{html_lib.escape(label)}</div>"
        + f"<div style=\"font-size:24px;font-weight:700;line-height:1;color:{html_lib.escape(text_color)};\">{value}</div>"
        + "</td>"
    )


def _normalize_budget_issue(issue: object, fallback_account: str | None) -> dict:
    if not isinstance(issue, dict):
        return {
            "account_code": fallback_account or "Unknown",
            "campaign_names": ["Unknown Campaign"],
            "budget_id": "Unknown",
            "current_amount": None,
            "new_amount": None,
            "message": str(issue),
        }

    account_code = issue.get("accountCode") or fallback_account or "Unknown"
    campaign_names = issue.get("campaignNames")
    if isinstance(campaign_names, str):
        campaign_names_list = [n.strip() for n in campaign_names.splitlines() if n.strip()]
    elif isinstance(campaign_names, list):
        campaign_names_list = [str(n).strip() for n in campaign_names if str(n).strip()]
    else:
        campaign_names_list = []
    if not campaign_names_list:
        campaign_names_list = ["Unknown Campaign"]

    budget_id = issue.get("budgetId") or "Unknown"
    current_amount = issue.get("currentAmount")
    if current_amount is None:
        current_amount = issue.get("oldAmount")
    new_amount = issue.get("newAmount")
    message = issue.get("error") or issue.get("message") or "Unknown"

    return {
        "account_code": str(account_code),
        "campaign_names": campaign_names_list,
        "budget_id": str(budget_id),
        "current_amount": current_amount,
        "new_amount": new_amount,
        "message": str(message),
    }


def _format_issue_title(issue: dict) -> str:
    account_code = issue.get("account_code") or "Unknown"
    campaign_names = issue.get("campaign_names") or ["Unknown Campaign"]
    if isinstance(campaign_names, list):
        campaigns = ", ".join(campaign_names)
    else:
        campaigns = str(campaign_names)
    budget_id = issue.get("budget_id") or "Unknown"
    return f"{account_code} - {campaigns} ({budget_id})"


def _format_issue_detail_text(issue: dict) -> str:
    current_amount = _format_amount(issue.get("current_amount"))
    new_amount = _format_amount(issue.get("new_amount"))
    message = issue.get("message") or "Unknown"
    return (
        f"Current budget: {current_amount} | "
        f"New Budget: {new_amount}\n"
        f"{message}"
    )


def _format_issue_detail_html(issue: dict) -> str:
    current_amount = html_lib.escape(_format_amount(issue.get("current_amount")))
    new_amount = html_lib.escape(_format_amount(issue.get("new_amount")))
    message = html_lib.escape(str(issue.get("message") or "Unknown"))
    return (
        f"Current budget: {current_amount} | "
        f"New Budget: {new_amount}<br>"
        f"{message}"
    )


def _format_amount(value: object) -> str:
    if value is None:
        return "N/A"
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


_ALERT_TEMPLATE_CACHE: dict[str, str] = {}


def _load_alert_template(template_path: Path) -> str:
    key = str(template_path)
    cached = _ALERT_TEMPLATE_CACHE.get(key)
    if cached is not None:
        return cached

    try:
        content = template_path.read_text(encoding="utf-8")
    except OSError:
        content = (
            "<!doctype html><html><head><meta charset=\"utf-8\"/>"
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>"
            "<title>$subject</title></head><body>"
            "<h2>$app_label Google Ads Alert</h2>"
            "<p>Timestamp: $generated_at</p>"
            "<p>Request ID: $request_id</p>"
            "<p>Client ID: $client_id</p>"
            "<p>Tenant ID: $tenant_id</p>"
            "<p>Operation: $campaign_status_summary</p>"
            "<p>Dry run: $dry_run</p>"
            "<div>$stats_html</div>"
            "<h3>Warnings</h3>$warnings_html"
            "<h3>Failures</h3>$failures_html"
            "</body></html>"
        )

    _ALERT_TEMPLATE_CACHE[key] = content
    return content
