rows = []
    # Limit the list to 200 lines in email to keep it readable; full list is in scanned.csv
    MAX_EMAIL_ROWS = 200
    for i, r in enumerate(scanned_log):
        if i >= MAX_EMAIL_ROWS:
            rows.append(f'<li><em>…and {len(scanned_log) - MAX_EMAIL_ROWS} more (see scanned.csv)</em></li>')
            break
        date_html = f"{r['date']} — " if r.get("date") else ""
        url = (r.get("url") or "").replace('"', '&quot;')
        title = (r.get("title") or "Document").replace("<", "&lt;").replace(">", "&gt;")
        rows.append(
            f'<li><strong>{r["status"]}</strong> — {date_html}{title} — '
            f'<a href="{url}" target="_blank" rel="noopener noreferrer">{url}</a> '
            f'(<em>{(r.get("reason") or "").replace("<","&lt;").replace(">","&gt;")}</em>)</li>'
        )
