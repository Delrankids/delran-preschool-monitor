import smtplib
import ssl
from email.message import EmailMessage
from typing import List, Dict
from html import escape as html_escape


def send_email(
    subject: str,
    html_body: str,
    to_addr: str,
    from_addr: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str
) -> None:
    """
    Sends an HTML email using STARTTLS (587) or implicit SSL (465).
    Supports comma- or semicolon-separated recipients in `to_addr`.
    """
    # Normalize recipients
    recipients = [x.strip() for x in (to_addr or "").replace(";", ",").split(",") if x.strip()]
    if not recipients:
        raise ValueError("send_email: no valid recipient addresses found in to_addr.")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(recipients)

    # Provide a plain-text fallback
    msg.set_content("This email requires an HTML-compatible client.")
    msg.add_alternative(html_body or "<html><body><p>(empty)</p></body></html>", subtype="html")

    context = ssl.create_default_context()

    if int(smtp_port) == 465:
        # Implicit SSL
        with smtplib.SMTP_SSL(smtp_host, int(smtp_port), timeout=60, context=context) as server:
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
    else:
        # STARTTLS
        with smtplib.SMTP(smtp_host, int(smtp_port), timeout=60) as server:
            server.starttls(context=context)
            server.login(smtp_user, smtp_password)
            server.send_message(msg)


def render_html_report(results: List[Dict]) -> str:
    """
    Builds the HTML email body from the scraper results.

    Each result item should look like:
      {
        "title": str,
        "url": str,
        "date": Optional[str],
        "mentions": [{"keyword": str, "snippet": str}, ...]
      }
    """
    if not results:
        body = "<p>No preschool-related mentions were found in this period’s BOE minutes.</p>"
    else:
        rows = []
        for r in results:
            # Defensive extraction + escaping
            url = r.get("url") or ""
            url_esc = html_escape(url)
            title = r.get("title") or "Meeting Item"
            title_esc = html_escape(title)
            date_html = f"<p><strong>Date:</strong> {html_escape(r['date'])}</p>" if r.get("date") else ""

            snippet_items = "".join(
                f"<li><strong>{html_escape(m.get('keyword',''))}</strong>: {html_escape(m.get('snippet',''))}</li>"
                for m in (r.get("mentions") or [])
            )

            rows.append(
                (
                    "<li style=\"margin-bottom: 20px;\">"
                    f"<p><strong>Title:</strong> {title_esc}</p>"
                    f"{date_html}"
                    f"<p><strong>URL:</strong> "
                    f"<a href=\"{url_esc}\" target=\"_blank\" rel=\"noopener noreferrer\">{url_esc}</a>"
                    f"</p>"
                    f"<ul>{snippet_items}</ul>"
                    "</li>"
                )
            )

        body = f"<ol>{''.join(rows)}</ol>"

    # Wrap in complete HTML
    return (
        "<!DOCTYPE html>"
        "<html>"
        "  <head>"
        '    <meta charset="utf-8" />'
        "    <title>Delran BOE – Preschool Mentions</title>"
        "  </head>"
        '  <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #222;">'
        "    <h2>Delran BOE – Preschool Mentions (Monthly Report)</h2>"
        f"    {body}"
        "    <hr>"
        '    <p style="color: #888; font-size: 12px;">'
        "      This report was generated automatically by your Delran Preschool Monitor."
        "    </p>"
        "  </body>"
        "</html>"
    )
