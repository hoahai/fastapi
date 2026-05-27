from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formataddr, make_msgid
import smtplib
import ssl


class SmtpSendError(RuntimeError):
    pass


@dataclass(frozen=True)
class SmtpSettings:
    host: str
    port: int
    from_email: str
    username: str | None = None
    password: str | None = None
    from_name: str | None = None
    reply_to: str | None = None
    use_tls: bool = True
    use_ssl: bool = False
    timeout_seconds: float = 20.0


def _normalize_address_list(value: list[str] | None, *, field: str) -> list[str]:
    addresses: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(value or []):
        text = str(item or "").strip()
        if not text:
            continue
        if "@" not in text:
            raise ValueError(f"{field}[{index}] must be an email-like value")
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        addresses.append(lowered)
    return addresses


def _validate_settings(settings: SmtpSettings) -> None:
    if not str(settings.host or "").strip():
        raise ValueError("smtp.host is required")
    if int(settings.port) <= 0 or int(settings.port) > 65535:
        raise ValueError("smtp.port must be between 1 and 65535")
    if not str(settings.from_email or "").strip() or "@" not in str(settings.from_email):
        raise ValueError("smtp.from_email is required and must be email-like")
    if settings.username and not settings.password:
        raise ValueError("smtp.password is required when smtp.username is provided")
    if settings.use_ssl and settings.use_tls:
        raise ValueError("smtp.use_ssl and smtp.use_tls cannot both be true")


def send_smtp_email(
    *,
    settings: SmtpSettings,
    to_addresses: list[str],
    cc_addresses: list[str] | None = None,
    bcc_addresses: list[str] | None = None,
    subject: str,
    text_body: str,
    html_body: str | None = None,
) -> dict[str, object]:
    _validate_settings(settings)

    to_list = _normalize_address_list(to_addresses, field="to_addresses")
    cc_list = _normalize_address_list(cc_addresses, field="cc_addresses")
    bcc_list = _normalize_address_list(bcc_addresses, field="bcc_addresses")
    if not to_list:
        raise ValueError("to_addresses is required")

    normalized_subject = str(subject or "").strip()
    if not normalized_subject:
        raise ValueError("subject is required")

    all_recipients = to_list + cc_list + bcc_list
    if not all_recipients:
        raise ValueError("At least one recipient is required")

    msg = EmailMessage()
    msg["Subject"] = normalized_subject
    msg["From"] = formataddr((str(settings.from_name or "").strip(), settings.from_email))
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    if settings.reply_to:
        msg["Reply-To"] = str(settings.reply_to).strip()
    msg["Message-ID"] = make_msgid()

    normalized_text_body = str(text_body or "").strip()
    normalized_html_body = str(html_body or "").strip()
    if normalized_html_body:
        msg.set_content(normalized_text_body or " ")
        msg.add_alternative(normalized_html_body, subtype="html")
    else:
        msg.set_content(normalized_text_body or " ")

    try:
        if settings.use_ssl:
            with smtplib.SMTP_SSL(
                host=settings.host,
                port=int(settings.port),
                timeout=float(settings.timeout_seconds),
                context=ssl.create_default_context(),
            ) as client:
                if settings.username:
                    client.login(settings.username, settings.password or "")
                refused = client.send_message(
                    msg,
                    from_addr=settings.from_email,
                    to_addrs=all_recipients,
                )
        else:
            with smtplib.SMTP(
                host=settings.host,
                port=int(settings.port),
                timeout=float(settings.timeout_seconds),
            ) as client:
                client.ehlo()
                if settings.use_tls:
                    client.starttls(context=ssl.create_default_context())
                    client.ehlo()
                if settings.username:
                    client.login(settings.username, settings.password or "")
                refused = client.send_message(
                    msg,
                    from_addr=settings.from_email,
                    to_addrs=all_recipients,
                )
    except (smtplib.SMTPException, OSError) as exc:
        raise SmtpSendError(str(exc) or "SMTP send failed") from exc

    refused_addresses = sorted([str(addr).strip().lower() for addr in (refused or {}).keys() if str(addr).strip()])
    if refused_addresses:
        raise SmtpSendError(
            "SMTP refused recipient(s): " + ", ".join(refused_addresses)
        )

    message_id = str(msg.get("Message-ID") or "").strip().strip("<>").strip()
    return {
        "message_id": message_id or None,
        "to": to_list,
        "cc": cc_list,
        "bcc": bcc_list,
    }
