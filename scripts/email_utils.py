import smtplib, ssl
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
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content("HTML email required. Please view in an HTML-capable client.")
    msg.add_alternative(html_body, subtype="html")

    # Use STARTTLS (587) by default; change to SSL if your SMTP requires 465.
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls(context=ssl.create_default_context())
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

def render_html_report(results: List[Dict]) -> str:
    if not results:
        body = "<p>No preschool-related mentions found this month.</p>"
    else:
        rows = []
        for r in results:
            snippets = "".join(
                f"<li><strong>{m['keyword']}</strong>: {m['snippet']}</li>"
                for m in r["mentions"]
            )
            rows.append(f"""
                <li>
                    <p><strong>Title:</strong> {r['title']}</p>
                    <p><strong>URL:</strong> <a href="{r['url']}">{r['url']}</a></p>
                    <ul>{snippets}</ul>
                </li>
            """)
        body = f"<ol>{''.join(rows)}</ol>"

    return f"""
    <html>
      <body>
        <h2>Delran BOE – Preschool Mentions (Monthly)</h2>
        {body}
        <p style="color:#666;margin-top:16px;">Automated report</p>
      </body>
    </html>
    """
