from __future__ import annotations

from html import escape as html_escape


def escape_html(value: object | None) -> str:
    return html_escape("" if value is None else str(value), quote=True)


def normalize_text(value: object | None) -> str:
    return " ".join(str(value or "").split())


def normalize_multiline_text(value: object | None) -> str:
    lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
    return "\n".join(lines)


def build_key_value_text(rows: list[tuple[str, object | None]]) -> str:
    lines: list[str] = []
    for label, value in rows:
        text = normalize_text(value)
        if not text:
            continue
        lines.append(f"{label}: {text}")
    return "\n".join(lines)


def build_bulleted_text(items: list[str]) -> str:
    lines = [f"- {normalize_text(item)}" for item in items if normalize_text(item)]
    return "\n".join(lines)


def build_key_value_html(rows: list[tuple[str, object | None]]) -> str:
    cells: list[str] = []
    for label, value in rows:
        text = normalize_text(value)
        if not text:
            continue
        cells.append(
            "<tr>"
            f"<td style=\"padding:6px 0;vertical-align:top;font-size:13px;color:#64748b;width:168px;\">{escape_html(label)}</td>"
            f"<td style=\"padding:6px 0;vertical-align:top;font-size:13px;color:#0f172a;font-weight:600;\">{escape_html(text)}</td>"
            "</tr>"
        )
    if not cells:
        return ""
    return (
        "<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%;border-collapse:collapse;\">"
        + "".join(cells)
        + "</table>"
    )


def build_status_badge_html(*, label: str, tone: str) -> str:
    badge_styles = {
        "success": ("#dcfce7", "#166534"),
        "warning": ("#fef3c7", "#92400e"),
        "danger": ("#fee2e2", "#991b1b"),
        "info": ("#dbeafe", "#1d4ed8"),
        "neutral": ("#e2e8f0", "#334155"),
    }
    background_color, text_color = badge_styles.get(tone, badge_styles["neutral"])
    return (
        "<span style=\"display:inline-block;border-radius:999px;padding:7px 12px;"
        f"background:{background_color};color:{text_color};font-size:12px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;\">"
        f"{escape_html(label)}</span>"
    )


def build_button_html(*, label: str, href: str) -> str:
    clean_label = normalize_text(label)
    clean_href = normalize_text(href)
    if not clean_label or not clean_href:
        return ""
    return (
        "<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" style=\"margin:0;\">"
        "<tr>"
        "<td style=\"border-radius:12px;background:#1d4ed8;\">"
        f"<a href=\"{escape_html(clean_href)}\" style=\"display:inline-block;padding:14px 20px;border-radius:12px;color:#ffffff;text-decoration:none;font-size:14px;font-weight:700;line-height:1;\">"
        f"{escape_html(clean_label)}"
        "</a>"
        "</td>"
        "</tr>"
        "</table>"
    )


def build_callout_html(
    *,
    title: str,
    body_html: str,
    tone: str = "neutral",
) -> str:
    tone_styles = {
        "success": ("#ecfdf5", "#d1fae5", "#065f46"),
        "warning": ("#fffbeb", "#fde68a", "#92400e"),
        "danger": ("#fef2f2", "#fecaca", "#991b1b"),
        "info": ("#eff6ff", "#bfdbfe", "#1d4ed8"),
        "neutral": ("#f8fafc", "#e2e8f0", "#334155"),
    }
    background_color, border_color, title_color = tone_styles.get(tone, tone_styles["neutral"])
    return (
        "<div style=\"margin-top:16px;border-radius:18px;padding:16px 18px;"
        f"background:{background_color};border:1px solid {border_color};\">"
        f"<div style=\"font-size:13px;font-weight:800;color:{title_color};text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px;\">{escape_html(title)}</div>"
        f"<div style=\"font-size:14px;line-height:1.7;color:#0f172a;\">{body_html}</div>"
        "</div>"
    )


def build_email_shell_html(
    *,
    app_label: str,
    title: str,
    subtitle: str,
    preheader: str,
    status_label: str | None = None,
    status_tone: str = "neutral",
    greeting: str | None = None,
    summary_rows: list[tuple[str, object | None]] | None = None,
    sections_html: list[str] | None = None,
    cta_label: str | None = None,
    cta_href: str | None = None,
    footer_note: str | None = None,
    accent_color: str = "#1d4ed8",
) -> str:
    summary_html = build_key_value_html(summary_rows or [])
    section_blocks = [section for section in (sections_html or []) if normalize_text(section)]
    cta_html = build_button_html(label=cta_label or "", href=cta_href or "") if cta_label and cta_href else ""
    status_html = (
        build_status_badge_html(label=status_label or "", tone=status_tone)
        if status_label
        else ""
    )
    greeting_html = (
        f"<p style=\"margin:0 0 12px 0;font-size:16px;line-height:1.7;color:#0f172a;\">{escape_html(greeting)}</p>"
        if greeting
        else ""
    )
    footer_html = (
        f"<div style=\"margin-top:18px;font-size:12px;line-height:1.7;color:#64748b;\">{escape_html(footer_note)}</div>"
        if footer_note
        else ""
    )
    sections_joined = "".join(section_blocks)
    preheader_text = normalize_text(preheader)
    body_background = "#eef2f7"
    card_border = "#dbe4ee"
    muted_text = "#475569"
    title_color = "#0f172a"

    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>"
        f"<title>{escape_html(title)}</title>"
        "</head>"
        f"<body style=\"margin:0;padding:0;background:{body_background};font-family:Arial,Helvetica,sans-serif;color:{title_color};\">"
        f"<div style=\"display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;\">{escape_html(preheader_text)}</div>"
        "<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\" style=\"border-collapse:collapse;\">"
        "<tr><td align=\"center\" style=\"padding:32px 16px;\">"
        "<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\" style=\"max-width:680px;border-collapse:separate;\">"
        f"<tr><td style=\"padding:0 2px 16px 2px;\">"
        f"<div style=\"font-size:12px;line-height:1.4;letter-spacing:0.16em;text-transform:uppercase;color:{accent_color};font-weight:800;\">{escape_html(app_label)}</div>"
        f"<div style=\"margin-top:10px;font-size:30px;line-height:1.12;font-weight:800;letter-spacing:-0.03em;color:{title_color};\">{escape_html(title)}</div>"
        f"<div style=\"margin-top:10px;font-size:15px;line-height:1.7;color:{muted_text};max-width:560px;\">{escape_html(subtitle)}</div>"
        f"</td></tr>"
        "<tr><td style=\"background:#ffffff;border:1px solid "
        f"{card_border};border-radius:24px;box-shadow:0 10px 26px rgba(15,23,42,0.05);overflow:hidden;\">"
        f"<div style=\"padding:22px 24px 0 24px;border-top:4px solid {accent_color};\">"
        f"{status_html}"
        f"{greeting_html}"
        f"{summary_html}"
        f"{sections_joined}"
        f"{cta_html}"
        f"{footer_html}"
        "</div>"
        "</td></tr>"
        "</table></td></tr></table></body></html>"
    )
