from __future__ import annotations

from datetime import datetime
import smtplib
from email.message import EmailMessage

from services.utils import load_env
from services.tenant import get_env

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


def send_google_ads_result_email(subject: str, body: str):
    """
    Send Google Ads mutation result email using environment variables.
    """

    smtp_host = get_env("SMTP_HOST")
    smtp_port = int(get_env("SMTP_PORT", "587"))
    smtp_username = get_env("SMTP_USERNAME")
    smtp_password = get_env("SMTP_PASSWORD")

    email_from = get_env(
        "EMAIL_FROM",
        "noreply@theautoadagency.com",
    )
    email_to_raw = get_env(
        "EMAIL_TO",
        "hai@theautoadagency.com",
    )
    email_to = [e.strip() for e in str(email_to_raw).split(",") if e.strip()]

    if not all([smtp_host, smtp_username, smtp_password]):
        raise RuntimeError("SMTP environment variables are not fully configured")

    msg = EmailMessage()
    msg["From"] = f"SpendSphere <{email_from}>"
    msg["To"] = ", ".join(email_to)
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.send_message(msg)
