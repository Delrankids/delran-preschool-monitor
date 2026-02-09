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
    Sends an HTML email using STARTTLS (recommended for Office365 / most SMTP servers).
    """

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content("This email requires an HTML-compatible client.")
    msg.add_alternative(html_body, subtype="html")

    # Create secure connection
    context = ssl.create_default_context()

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls(context=context)
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


def render_html_report(results: List[Dict]) -> str:
    """
    Builds the HTML email body from the scraper results.
    """

    if not results:
        body = "<p>No preschool-related mentions were found in this month's BOE minutes.</p>"
    else:
        rows = []
        for r in results:
            # Generate list of keyword/snippet items
            snippet_items = "".join(
                f"<li><strong>{m['keyword']}</strong>: {m['snippet']}</li>"
                for m in r["mentions"]
            )

            rows.append(f"""
                <li style="margin-bottom: 20px;">
                    <p><strong>Title:</strong> {r['title']}</p>
                    <p><strong>URL:</strong>
                        <a href="{r['url']}" target="_blank">{r['url']}</a>
                    </p>
                    <ul>{snippet_items}</ul>
                </li>
            """)

        body = f"<ol>{''.join(rows)}</ol>"

    # Wrap in complete HTML
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
