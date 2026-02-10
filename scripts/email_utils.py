import smtplib
import ssl
from email.message import EmailMessage
from typing import List, Dict


def send_email(
    subject: str,
    html_body: str,
    to_addr: str,
    from_addr: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str
):
    """
    Sends an HTML email using STARTTLS (587) or implicit SSL (465).
    Supports comma-separated recipients in `to_addr`.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    # Support multiple recipients separated by comma/semicolon
    recipients = [x.strip() for x in to_addr.replace(";", ",").split(",") if x.strip()]
    msg["To"] = ", ".join(recipients)
    msg.set_content("This email requires an HTML-compatible client.")
    msg.add_alternative(html_body, subtype="html")

    context = ssl.create_default_context()

    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=60, context=context) as server:
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as server:
            server.starttls(context=context)
            server.login(smtp_user, smtp_password)
            server.send_message(msg)


def render_html_report(results: List[Dict]) -> str:
    """
    Builds the HTML email body from the scraper results.

    Each result should be:
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
            date_html = f"<p><strong>Date:</strong> {r['date']}</p>" if r.get("date") else ""
            snippet_items = "".join(
                f"<li><strong>{m['keyword']}</strong>: {m['snippet']}</li>"
                for m in r.get("mentions", [])
            )
            rows.append(f"""
                <li style="margin-bottom: 20px;">
                    <p><strong>Title:</strong> {r.get('title') or 'Meeting Item'}</p>
                    {date_html}
                    <p><strong>URL:</strong>
                        <a href="{r['url']}" target="_blank" rel="noopener noreferrer">{r['url']}</a>
                    </p>
                    <ul>{snippet_items}</ul>
                </li>
            """)
        body = f"<ol>{''.join(rows)}</ol>"

    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; line-height: 1.6;">
        <h2>Delran BOE – Preschool Mentions (Monthly Report)</h2>
        {body}
        <hr>
        <p style="color: #888; font-size: 12px;">
          This report was generated automatically by your Delran Preschool Monitor.
        </p>
      </body>
    </html>
    """
