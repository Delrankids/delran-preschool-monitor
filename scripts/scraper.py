# (this is the same clean, validated version you just deployed)
# ADDITIONS near the email send section:
# - Masked printout of email config
# - Hard failure with clear reason if From/To missing

# ... everything unchanged above ...

    # Email
    to_addr = os.environ.get("REPORT_TO") or "robwaz@delrankids.net"
    from_addr = os.environ.get("REPORT_FROM") or os.environ.get("MAIL_FROM")
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT") or "587")
    smtp_user = os.environ.get("SMTP_USER") or os.environ.get("SMTP_USERNAME")
    smtp_password = os.environ.get("SMTP_PASS") or os.environ.get("SMTP_PASSWORD")

    # Masked logging (so you can see what will be used)
    def _mask(s: str) -> str:
        if not s: return ""
        if "@" in s:
            name, _, domain = s.partition("@")
            return (name[:1] + "***@" + domain) if domain else "***"
        return s[:2] + "***"

    print("Email config:",
          "to=", _mask(to_addr),
          "from=", _mask(from_addr),
          "smtp=", (smtp_host or ""), "port=", smtp_port,
          "user=", _mask(smtp_user))

    can_send = all([to_addr, from_addr, smtp_host, smtp_port, smtp_user, smtp_password])
    if not can_send:
        raise RuntimeError("Email not sent: missing one of To/From/SMTP settings. "
                           "Ensure REPORT_TO and REPORT_FROM (or MAIL_FROM) and SMTP_* secrets are set.")

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
        from_addr=from_addr,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
    )

    print(
        "Email sent to " + to_addr
        + ". Matches: " + str(sum(len(r["mentions"]) for r in results_for_email))
        + "; items: " + str(len(results_for_email))
        + "; scanned_total: " + str(len(scanned_log))
    )

# ... bottom 'if __name__ == "__main__":' block unchanged ...
