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
            url = r.get("url") or ""
            title = r.get("title") or "Meeting Item"
            rows.append(f"""
                <li style="margin-bottom: 20px;">
                    <p><strong>Title:</strong> {title}</p>
                    {date_html}
                    <p><strong>URL:</strong>
                        <a href="{url}" target="_blank" rel="noopener noreferrer">{url}</a>
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
