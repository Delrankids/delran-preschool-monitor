#!/usr/bin/env python3
"""
SMTP email sender for GitHub Actions with multi-attachment support.

Env vars required:
- SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD
- MAIL_FROM, MAIL_TO

Args:
  --subject <str>
  --text-body <path> (optional)
  --html-body <path> (optional)
  --attachment <path>  # can be provided multiple times
"""
import os
import sys
import argparse
import mimetypes
import smtplib
from email.message import EmailMessage
from email.utils import formatdate

def load_optional(path: str):
    if not path:
        return None
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        return f.read()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--subject', required=True)
    ap.add_argument('--text-body')
    ap.add_argument('--html-body')
    ap.add_argument('--attachment', action='append', default=[])
    args = ap.parse_args()

    host = os.environ.get('SMTP_HOST')
    port = int(os.environ.get('SMTP_PORT') or 587)
    user = os.environ.get('SMTP_USERNAME')
    password = os.environ.get('SMTP_PASSWORD')
    mail_from = os.environ.get('MAIL_FROM')
    mail_to = os.environ.get('MAIL_TO')

    missing = [k for k, v in [
        ('SMTP_HOST', host), ('SMTP_PORT', port), ('SMTP_USERNAME', user),
        ('SMTP_PASSWORD', password), ('MAIL_FROM', mail_from), ('MAIL_TO', mail_to)
    ] if not v]
    if missing:
        print(f"Missing required env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    msg = EmailMessage()
    msg['From'] = mail_from
    msg['To'] = mail_to
    msg['Date'] = formatdate(localtime=False)
    msg['Subject'] = args.subject

    text_body = load_optional(args.text_body)
    html_body = load_optional(args.html_body)

    if html_body and text_body:
        msg.set_content(text_body)
        msg.add_alternative(html_body, subtype='html')
    elif html_body:
        msg.add_alternative(html_body, subtype='html')
    elif text_body:
        msg.set_content(text_body)
    else:
        msg.set_content('No body provided.')

    for path in args.attachment:
        if not path or not os.path.exists(path):
            continue
        ctype, encoding = mimetypes.guess_type(path)
        if ctype is None or encoding is not None:
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        with open(path, 'rb') as f:
            data = f.read()
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=os.path.basename(path))

    try:
        with smtplib.SMTP(host, port, timeout=60) as server:
            try:
                server.starttls()
            except Exception:
                pass  # If already SSL or not needed
            server.login(user, password)
            server.send_message(msg)
        print('Email sent to', mail_to)
    except Exception as e:
        print(f"Email send failed: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()