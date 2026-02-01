from __future__ import annotations

from datetime import datetime
import json
import urllib.error
import urllib.request

from shared.utils import load_env
from shared.tenant import get_env

# =========================================================
# ENV
# =========================================================

load_env()


# =========================================================
# EMAIL BODY BUILDER
# =========================================================


def build_google_ads_result_email(*, full_report: dict) -> str:
    """
    Build a plain-text Google Ads update report email
    from the full pipeline result.
    """

    lines: list[str] = []

    # =====================================================
    # HEADER
    # =====================================================
    lines.append("Spendsphere – Google Ads Update Report")
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
            f"Summary → "
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


# =========================================================
# EMAIL SENDER
# =========================================================


def send_google_ads_result_email(
    subject: str,
    body: str,
    *,
    html: str | None = None,
    attachments: list[dict] | None = None,
    app_name: str | None = None,
    return_response: bool = False,
    return_payload: bool = False,
):
    """
    Send Google Ads mutation result email using Resend.
    """

    api_key = get_env("RESEND_API_KEY")
    resolved_app = app_name or "SpendSphere"
    email_from = str(
        get_env(
            "EMAIL_FROM",
            "noreply@theautoadagency.com",
        )
        or ""
    ).strip()
    email_to_raw = get_env(
        "EMAIL_TO",
        "hai@theautoadagency.com",
    )
    email_to = [e.strip() for e in str(email_to_raw).split(",") if e.strip()]

    if not api_key:
        raise RuntimeError("RESEND_API_KEY is not configured")
    if not email_from:
        raise RuntimeError("EMAIL_FROM is not configured")
    if not email_to:
        raise RuntimeError("EMAIL_TO is not configured")

    subject_value = subject

    if str(email_from).lower().endswith("@resend.dev"):
        from_value = email_from
    else:
        from_value = f"{resolved_app} <{email_from}>" if resolved_app else email_from

    to_value: str | list[str] = email_to[0] if len(email_to) == 1 else email_to

    payload: dict[str, object] = {
        "from": from_value,
        "to": to_value,
        "subject": subject_value,
        "text": body,
    }
    if html is not None:
        payload["html"] = html
    if attachments:
        payload["attachments"] = attachments

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )

    if return_payload:
        return {
            "payload": payload,
            "headers": {
                "Content-Type": "application/json",
                "Authorization": "Bearer [redacted]",
            },
        }

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body_bytes = resp.read()
            response_text = body_bytes.decode("utf-8") if body_bytes else ""
            if resp.status >= 400:
                raise RuntimeError(
                    f"Resend API error {resp.status}: {response_text}"
                )
            if return_response:
                return {"status": resp.status, "body": response_text}
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"Resend API error {exc.code}: {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Resend connection error: {exc.reason}") from exc
