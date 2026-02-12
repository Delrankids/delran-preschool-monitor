import smtplib
import ssl
from email.message import EmailMessage
from typing import List, Dict, Optional
from html import escape as html_escape


def _build_email_message(
    subject: str,
    html_body: str,
    to_addr: str,
    from_addr: str,
    reply_to: Optional[str] = None,
) -> EmailMessage:
    """
    Build an HTML email with a plain-text fallback.

    Returns the EmailMessage object (used both for sending and saving .eml).
    """
    # Normalize recipients
    recipients = [x.strip() for x in (to_addr or "").replace(";", ",").split(",") if x.strip()]
    if not recipients:
        raise ValueError("send_email: no valid recipient addresses found in to_addr.")
    if not from_addr:
        raise ValueError("send_email: from_addr is empty.")

    msg = EmailMessage()
    msg["Subject"] = subject or "Delran Preschool Monitor"
    msg["From"] = from_addr
    msg["To"] = ", ".join(recipients)
    if reply_to and reply_to.strip():
        msg["Reply-To"] = reply_to.strip()

    # Provide a plain-text fallback
    msg.set_content("This email requires an HTML-compatible client.")
    msg.add_alternative(html_body or "<html><body><p>(empty)</p></body></html>", subtype="html")
    return msg


def send_email(
    subject: str,
    html_body: str,
    to_addr: str,
    from_addr: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    reply_to: Optional[str] = None,
) -> bytes:
    """
    Sends an HTML email using STARTTLS (587) or implicit SSL (465).
    Returns the raw .eml bytes of the message that was sent.
    """
    msg = _build_email_message(subject, html_body, to_addr, from_addr, reply_to=reply_to)
    eml_bytes = msg.as_bytes()

    context = ssl.create_default_context()
    try:
        if int(smtp_port) == 465:
            with smtplib.SMTP_SSL(smtp_host, int(smtp_port), timeout=60, context=context) as server:
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, int(smtp_port), timeout=60) as server:
                server.starttls(context=context)
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
    except smtplib.SMTPResponseException as ex:
        code = getattr(ex, "smtp_code", None)
        err  = getattr(ex, "smtp_error", b"").decode("utf-8", "ignore")
        raise RuntimeError(f"SMTPResponseException {code}: {err}") from ex
    except Exception as ex:
        raise RuntimeError(f"SMTP send failed: {ex}") from ex

    return eml_bytes


def render_html_report(results: List[Dict]) -> str:
    """
    Builds the HTML email body from the scraper results.
    """
    if not results:
        body_html = "<p>No preschool-related mentions were found in this period’s BOE minutes.</p>"
    else:
        items: List[str] = []
        for r in results:
            url = r.get("url") or ""
            title = r.get("title") or "Meeting Item"
            date_val = r.get("date") or ""

            url_esc = html_escape(url)
            title_esc = html_escape(title)
            date_html = f"<p><strong>Date:</strong> {html_escape(date_val)}</p>" if date_val else ""

            mention_li: List[str] = []
            for m in (r.get("mentions") or []):
                kw = html_escape(m.get("keyword", ""))
                snip = html_escape(m.get("snippet", ""))
                mention_li.append(f"<li><strong>{kw}</strong>: {snip}</li>")
            mentions_html = "<ul>" + "".join(mention_li) + "</ul>" if mention_li else ""

            items.append(
                "<li style=\"margin-bottom: 20px;\">"
                f"<p><strong>Title:</strong> {title_esc}</p>"
                f"{date_html}"
                "<p><strong>URL:</strong> "
                f"<a href=\"{url_esc}\" target=\"_blank\" rel=\"noopener noreferrer\">{url_esc}</a>"
                "</p>"
                f"{mentions_html}"
                "</li>"
            )

        body_html = "<ol>" + "".join(items) + "</ol>"

    html = (
        "<!DOCTYPE html>"
        "<html>"
        "<head>"
        "<meta charset=\"utf-8\" />"
        "<title>Delran BOE – Preschool Mentions</title>"
        "</head>"
        "<body style=\"font-family: Arial, sans-serif; line-height: 1.6; color: #222;\">"
        "<h2>Delran BOE – Preschool Mentions (Monthly Report)</h2>"
        f"{body_html}"
        "<hr>"
        "<p style=\"color: #888; font-size: 12px;\">"
        "This report was generated automatically by your Delran Preschool Monitor."
        "</p>"
        "</body>"
        "</html>"
    )
    return html
