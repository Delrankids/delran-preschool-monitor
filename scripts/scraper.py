# Email
    to_addr = os.environ.get("REPORT_TO") or "robwaz@delrankids.net"

    # Always send *from the authenticated mailbox* (safest for O365/Exchange).
    # If you want replies to go elsewhere, set REPORT_FROM and we'll put it in Reply-To.
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT") or "587")
    smtp_user = os.environ.get("SMTP_USER") or os.environ.get("SMTP_USERNAME")
    smtp_password = os.environ.get("SMTP_PASS") or os.environ.get("SMTP_PASSWORD")

    # Force From to smtp_user; set Reply-To to REPORT_FROM (or MAIL_FROM) if present.
    forced_from = smtp_user or ""
    reply_to = os.environ.get("REPORT_FROM") or os.environ.get("MAIL_FROM") or None

    # Masked logging to verify values without exposing secrets
    def _mask(s: str) -> str:
        if not s:
            return ""
        if "@" in s:
            name, _, domain = s.partition("@")
            return (name[:1] + "***@" + domain) if domain else "***"
        return s[:2] + "***"

    print(
        "Email config:",
        "to=", _mask(to_addr),
        "from=", _mask(forced_from),
        "reply_to=", _mask(reply_to or ""),
        "smtp=", (smtp_host or ""),
        "port=", smtp_port,
        "user=", _mask(smtp_user),
    )

    can_send = all([to_addr, forced_from, smtp_host, smtp_port, smtp_user, smtp_password])
    if not can_send:
        raise RuntimeError(
            "Email not sent: missing one of To/From/SMTP settings. "
            "Ensure REPORT_TO and SMTP_* secrets are set. From is forced to SMTP user."
        )

    subject = (
        "Delran BOE – Preschool Mentions (Backfill " + str(datetime(2021,1,1).date())
        + " → " + str(end.date()) + ")"
        if is_backfill else
        "Delran BOE – Preschool Mentions (" + start.date().isoformat()[:7] + ") Monthly Report"
    )

    send_email(
        subject=subject,
        html_body=html_report_full,
        to_addr=to_addr,
        from_addr=forced_from,   # ← send AS the authenticated account
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        reply_to=reply_to,       # ← optional Reply‑To (your preferred address)
    )

    print(
        "Email sent to " + to_addr
        + ". Matches: " + str(sum(len(r["mentions"]) for r in results_for_email))
        + "; items: " + str(len(results_for_email))
        + "; scanned_total: " + str(len(scanned_log))
    )
