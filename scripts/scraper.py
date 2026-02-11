import html  # put this near your other imports

def html_escape(s: str) -> str:
    # html.escape handles &, <, > and, with quote=True, also quotes (")
    # It does NOT double-escape already-escaped entities.
    return html.escape(s or "", quote=True)
