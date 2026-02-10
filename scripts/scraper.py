"""
Delran BOE Preschool Monitor – Scraper (Enhanced)

What it does
------------
1) Crawls the Delran BOE meeting minutes and BOE index for PDF/DOCX/HTML items.
2) Optionally scans BoardDocs public for PDFs (limited to MAX_BOARDDOCS_FILES).
3) Extracts text and finds preschool-related mentions (via parser_utils.py).
4) Builds HTML + CSV report and emails it monthly.
5) Persists 'seen' match hashes in state.json to dedupe future runs.
6) First run does a backfill from 2021-01-01 to today; then runs monthly.
7) Writes a full audit log of *every* document seen to scanned.csv and
   appends a "Documents scanned" section to the email.

Configuration (env)
-------------------
- DELRAN_MINUTES_URL   (default: https://www.delranschools.org/b_o_e/meeting_minutes)
- DELRAN_BOE_URL       (default: https://www.delranschools.org/b_o_e)
- BOARDDOCS_PUBLIC_URL (default: https://go.boarddocs.com/nj/delranschools/Board.nsf/Public)

- REPORT_TO                -> recipient (default: robwaz@delrankids.net)
- REPORT_FROM or MAIL_FROM -> sender (one required for sending)
- SMTP_HOST
- SMTP_PORT                -> 587 (STARTTLS) or 465 (SSL)
- SMTP_USER or SMTP_USERNAME
- SMTP_PASS or SMTP_PASSWORD

- STATE_FILE            -> default: state.json
- DOC_DELAY_SECONDS     -> polite delay between document downloads (default: 2.0)
- REQUEST_TIMEOUT       -> requests timeout seconds (default: 60)
- MAX_BOARDDOCS_FILES   -> default: 50
- MIN_YEAR              -> optional int to drop items with parsed date before year
- IGNORE_DEDUPE         -> "1" to ignore dedupe for this run (default: "0")

Outputs
-------
- last_report.html  (HTML body of the email, with "Documents scanned" section)
- report.csv        (only matching rows)
- scanned.csv       (every document encountered with status and reason)
- state.json        (committed by the workflow)
"""

import os
import re
import csv
import json
import time
import hashlib
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

BASE_URL = os.environ.get(
    "DELRAN_MINUTES_URL",
    "https://www.delranschools.org/b_o_e/meeting_minutes"
)
BOE_URL = os.environ.get(
    "DELRAN_BOE_URL",
    "https://www.delranschools.org/b_o_e"
)
BOARDDOCS_PUBLIC = os.environ.get(
    "BOARDDOCS_PUBLIC_URL",
    "https://go.boarddocs.com/nj/delranschools/Board.nsf/Public"
)

STATE_FILE = os.environ.get("STATE_FILE", "state.json")

HEADERS = {
    "User-Agent": "Delran-Preschool-Agent/1.2 (+mailto:alerts@example.com)"
}

DOC_DELAY_SECONDS = float(os.environ.get("DOC_DELAY_SECONDS", "2.0"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))
MAX_BOARDDOCS_FILES = int(os.environ.get("MAX_BOARDDOCS_FILES", "50"))
MIN_YEAR = os.environ.get("MIN_YEAR")
MIN_YEAR = int(MIN_YEAR) if MIN_YEAR and str(MIN_YEAR).isdigit() else None

IGNORE_DEDUPE = os.environ.get("IGNORE_DEDUPE", "0") == "1"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ----------------------------------------------------------------------------
# State
# ----------------------------------------------------------------------------

def load_state() -> Dict:
    """
    State keys:
      - seen_hashes: list of string fingerprints of previously reported matches
      - backfill_done: bool, whether we already ran the 2021-01-01 -> today backfill
      - last_run_end: ISO timestamp of the end boundary of the last run
    """
    state = {"seen_hashes": [], "backfill_done": False, "last_run_end": None}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    state.update(loaded)
        except Exception:
            logging.warning("State file unreadable; starting fresh.")
    return state


def save_state(state: Dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def sha1_of(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", "ignore"))
    return h.hexdigest()


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ----------------------------------------------------------------------------
# Fetching helpers
# ----------------------------------------------------------------------------

def fetch_url(url: str, *, binary: bool = False) -> bytes | str:
    attempts = 3
    for i in range(1, attempts + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.content if binary else resp.text
        except Exception:
            if i == attempts:
                raise
            time.sleep(1.5 * i)  # backoff


def polite_delay():
    if DOC_DELAY_SECONDS > 0:
        time.sleep(DOC_DELAY_SECONDS)


# ----------------------------------------------------------------------------
# Link discovery
# ----------------------------------------------------------------------------

# Keep .doc to discover legacy links; they will be "no text extracted".
DOC_EXTS = (".pdf", ".docx", ".doc", ".htm", ".html")

def collect_links_from_page(page_url: str) -> List[Dict[str, str]]:
    html = fetch_url(page_url, binary=False)
    soup = BeautifulSoup(html, "lxml")

    links: List[Dict[str, str]] = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        url = urljoin(page_url, href)
        title = a.get_text(strip=True) or url
        # Include direct docs or known file handlers
        if ("DisplayFile.aspx" in url) or url.lower().endswith(DOC_EXTS):
            links.append({"title": title, "url": url, "source": "district"})
    # Dedupe by URL
    uniq, seen = [], set()
    for l in links:
        if l["url"] in seen:
            continue
        seen.add(l["url"]); uniq.append(l)
    return uniq


def get_minutes_links() -> List[Dict[str, str]]:
    items = []
    for page in (BASE_URL, BOE_URL):
        try:
            items.extend(collect_links_from_page(page))
        except Exception as e:
            logging.warning("Failed to collect links from %s: %s", page, e)
    logging.info("District links collected: %d", len(items))
    return items


def get_boarddocs_links(max_files: int) -> List[Dict[str, str]]:
    """
    Shallow crawl of BoardDocs public page to find direct PDFs under /files/.
    """
    candidates: List[Dict[str, str]] = []
    to_visit = [BOARDDOCS_PUBLIC]
    visited = set()

    while to_visit and len(to_visit) <= 8 and len(candidates) < max_files:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            html = fetch_url(url, binary=False)
        except Exception as e:
            logging.warning("BoardDocs fetch failed %s: %s", url, e)
            continue

        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(url, href)
            text = a.get_text(strip=True) or "BoardDocs File"
            if "/files/" in href and href.lower().endswith(".pdf"):
                candidates.append({"title": text, "url": full, "source": "boarddocs"})
                if len(candidates) >= max_files:
                    break
            if ("Board.nsf" in full and full.startswith("https://go.boarddocs.com")
                and full not in visited and len(to_visit) < 8):
                to_visit.append(full)

    uniq, seen = [], set()
    for it in candidates:
        if it["url"] in seen: continue
        seen.add(it["url"]); uniq.append(it)
    logging.info("BoardDocs links collected: %d", len(uniq))
    return uniq


# ----------------------------------------------------------------------------
# Extraction
# ----------------------------------------------------------------------------

def extract_text_for_url(item: Dict[str, str]) -> str:
    """
    Fetch the URL, and:
      - If HTML, try to follow inner links to PDF/DOCX; else return visible text.
      - If PDF, extract text with PyPDF2.
      - If DOCX, extract text with python-docx.
    """
    url_lower = item["url"].lower()
    path_guess = urlparse(url_lower).path.lower()
    try:
        resp = requests.get(item["url"], headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        logging.warning("Fetch failed %s: %s", item["url"], e)
        return ""

    ctype = (resp.headers.get("Content-Type") or "").lower()

    # If HTML, scan for direct file links first
    if "text/html" in ctype or path_guess.endswith((".htm", ".html")):
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if h.lower().endswith(".pdf") or "DisplayFile.aspx" in h or "/files/" in h:
                inner_url = urljoin(item["url"], h)
                polite_delay()
                try:
                    inner = requests.get(inner_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                    inner.raise_for_status()
                except Exception:
                    break
                inner_ctype = (inner.headers.get("Content-Type") or "").lower()
                if "application/pdf" in inner_ctype or inner_url.lower().endswith(".pdf"):
                    polite_delay()
                    from parser_utils import extract_text_from_pdf as _pf
                    return _pf(inner.content)
                elif inner_url.lower().endswith(".docx"):
                    polite_delay()
                    from parser_utils import extract_text_from_docx as _dx
                    return _dx(inner.content)
        # fallback: use visible page text
        return " ".join(s.strip() for s in soup.stripped_strings)

    # If PDF
    if "application/pdf" in ctype or path_guess.endswith(".pdf"):
        polite_delay()
        from parser_utils import extract_text_from_pdf as _pf
        return _pf(resp.content)

    # If DOCX
    if path_guess.endswith(".docx"):
        polite_delay()
        from parser_utils import extract_text_from_docx as _dx
        return _dx(resp.content)

    # Unknown/other
    return ""


# ----------------------------------------------------------------------------
# Date range calculation
# ----------------------------------------------------------------------------

def first_day_of_month(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def last_day_of_month(dt: datetime) -> datetime:
    first_next = (dt.replace(day=1) + timedelta(days=32)).replace(day=1)
    return first_next - timedelta(days=1)


def compute_run_range(state: Dict) -> tuple[datetime, datetime, bool]:
    """
    Returns (start, end, is_backfill).
    First run = backfill from 2021-01-01 to today.
    Subsequent runs (on last day-of-month) = current month 1st -> today.
    """
    today = datetime.utcnow()
    if not state.get("backfill_done"):
        start = datetime(2021, 1, 1)
        end = today
        return (start, end, True)
    start = first_day_of_month(today)
    end = today
    return (start, end, False)


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def within_range(iso_dt: Optional[str], start: datetime, end: datetime) -> bool:
    if not iso_dt:
        return True
    try:
        dt = dateparser.parse(iso_dt).replace(tzinfo=None)
        return start <= dt <= end
    except Exception:
        return True


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main():
    # Defer heavy imports so outer catcher writes last_report.html for import errors too
    from parser_utils import (
        find_preschool_mentions,
        guess_meeting_date,
    )
    from email_utils import send_email, render_html_report

    state = load_state()
    start, end, is_backfill = compute_run_range(state)
    logging.info("Date range: %s -> %s (backfill=%s)", start.date(), end.date(), is_backfill)

    # Gather links
    items: List[Dict[str, str]] = []
    items.extend(get_minutes_links())
    if MAX_BOARDDOCS_FILES > 0:
        items.extend(get_boarddocs_links(MAX_BOARDDOCS_FILES))

    # Audit log for every document touched
    scanned_log: List[Dict[str, str]] = []  # {date, source, title, url, status, reason}

    results_for_email: List[Dict] = []
    rows_for_csv: List[List[str]] = []

    seen_hashes = set(state.get("seen_hashes") or [])
    new_hashes = set()

    for item in items:
        title = item.get("title") or "Meeting Item"
        url = item["url"]
        source = item.get("source") or ""

        # Fetch & extract
        try:
            text = extract_text_for_url(item)
        except Exception as e:
            scanned_log.append({
                "date": "", "source": source, "title": title, "url": url,
                "status": "error", "reason": f"fetch/extract error: {e}"
            })
            continue

        if not text:
            scanned_log.append({
                "date": "", "source": source, "title": title, "url": url,
                "status": "skipped", "reason": "no text extracted"
            })
            continue

        mentions = find_preschool_mentions(text)

        # Date inference (before range filter, for logging)
        meeting_dt = guess_meeting_date(text, title=title, url=url)
        iso_date = meeting_dt.isoformat() if meeting_dt else None

        # Filter by MIN_YEAR (if set)
        if MIN_YEAR is not None and meeting_dt and meeting_dt.year < MIN_YEAR:
            scanned_log.append({
                "date": meeting_dt.date().isoformat(),
                "source": source, "title": title, "url": url,
                "status": "skipped", "reason": f"before MIN_YEAR {MIN_YEAR}"
            })
            continue

        # Range filter
        if not within_range(iso_date, start, end):
            scanned_log.append({
                "date": meeting_dt.date().isoformat() if meeting_dt else "",
                "source": source, "title": title, "url": url,
                "status": "skipped", "reason": "out of date range"
            })
            continue

        if not mentions:
            scanned_log.append({
                "date": meeting_dt.date().isoformat() if meeting_dt else "",
                "source": source, "title": title, "url": url,
                "status": "scanned", "reason": "no preschool mentions"
            })
            continue

        # Deduplicate at match-level
        kept_mentions = []
        for m in mentions:
            fp = sha1_of(url, m.get("keyword") or "", (m.get("snippet") or "")[:160])
            if IGNORE_DEDUPE or fp not in seen_hashes:
                kept_mentions.append(m)
                new_hashes.add(fp)

        if not kept_mentions:
            scanned_log.append({
                "date": meeting_dt.date().isoformat() if meeting_dt else "",
                "source": source, "title": title, "url": url,
                "status": "scanned", "reason": "only duplicates (already reported)"
            })
            continue

        # Matched item
        results_for_email.append({
            "title": title,
            "url": url,
            "date": meeting_dt.date().isoformat() if meeting_dt else "",
            "mentions": kept_mentions
        })
        for m in kept_mentions:
            rows_for_csv.append([
                meeting_dt.date().isoformat() if meeting_dt else "",
                source,
                url,
                m.get("keyword") or "",
                (m.get("snippet") or "").strip()
            ])

        scanned_log.append({
            "date": meeting_dt.date().isoformat() if meeting_dt else "",
            "source": source, "title": title, "url": url,
            "status": "matched", "reason": f"{len(kept_mentions)} new mention(s)"
        })

    # Sort results by date desc
    def sort_key(r):
        d = r.get("date")
        try:
            dt = dateparser.parse(d).date() if d else datetime(1970,1,1).date()
        except Exception:
            dt = datetime(1970,1,1).date()
        return (dt, r.get("title") or "")
    results_for_email.sort(key=sort_key, reverse=True)

    # Write matches CSV
    with open("report.csv", "w", encoding="utf-8", newline="") as cf:
        w = csv.writer(cf)
        w.writerow(["date", "source", "url", "keyword", "snippet"])
        for row in rows_for_csv:
            w.writerow(row)

    # Write scanned.csv
    with open("scanned.csv", "w", encoding="utf-8", newline="") as sf:
        w = csv.writer(sf)
        w.writerow(["date", "source", "title", "url", "status", "reason"])
        status_rank = {"matched": 0, "scanned": 1, "skipped": 2, "error": 3}
        def s_key(x):
            return (status_rank.get(x.get("status","skipped"), 9),
                    x.get("date") or "", x.get("title") or "")
        for row in sorted(scanned_log, key=s_key):
            w.writerow([
                row.get("date") or "",
                row.get("source") or "",
                row.get("title") or "",
                row.get("url") or "",
                row.get("status") or "",
                row.get("reason") or "",
            ])

    # Build main HTML (matches)
    html_report = render_html_report(results_for_email)

    # Append a "Documents scanned" section
    total_counts = {
        "matched": sum(1 for x in scanned_log if x["status"] == "matched"),
        "scanned": sum(1 for x in scanned_log if x["status"] == "scanned"),
        "skipped": sum(1 for x in scanned_log if x["status"] == "skipped"),
        "error":   sum(1 for x in scanned_log if x["status"] == "error"),
        "total":   len(scanned_log),
    }
    rows_html = []
    MAX_EMAIL_ROWS = 200  # keep email readable; full details are in scanned.csv
    for i, r in enumerate(scanned_log):
        if i >= MAX_EMAIL_ROWS:
            rows_html.append(f'<li><em>…and {len(scanned_log) - MAX_EMAIL_ROWS} more (see scanned.csv)</em></li>')
            break
        date_html = (r.get("date") + " — ") if r.get("date") else ""
        url = html_escape(r.get("url") or "")
        title = html_escape(r.get("title") or "Document")
        reason = html_escape(r.get("reason") or "")
        rows_html.append(
            f'<li><strong>{r.get("status","")}</strong> — {date_html}{title} — '
            f'<a href="{url}" target="_blank" rel="noopener noreferrer">{url}</a> '
            f'(<em>{reason}</em>)</li>'
        )
    scanned_section = f"""
    <hr>
    <details>
      <summary><strong>Documents scanned</strong> — total {total_counts['total']} (matched: {total_counts['matched']}, scanned/no-hit: {total_counts['scanned']}, skipped: {total_counts['skipped']}, error: {total_counts['error']})</summary>
      <p>Full audit log is attached as <code>scanned.csv</code> in workflow artifacts.</p>
      <ol style="margin-top: 6px;">
        {''.join(rows_html)}
      </ol>
    </details>
    """

    html_report_full = html_report.replace("</body>", scanned_section + "\n</body>")

    with open("last_report.html", "w", encoding="utf-8") as f:
        f.write(html_report_full)

    # Update state
    if not IGNORE_DEDUPE and new_hashes:
        state["seen_hashes"] = sorted(set(state.get("seen_hashes") or []) | new_hashes)
    if is_backfill:
        state["backfill_done"] = True
    state["last_run_end"] = end.isoformat()
    save_state(state)

    # Email
    to_addr = os.environ.get("REPORT_TO") or "robwaz@delrankids.net"
    from_addr = os.environ.get("REPORT_FROM") or os.environ.get("MAIL_FROM")
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT") or "587")
    smtp_user = os.environ.get("SMTP_USER") or os.environ.get("SMTP_USERNAME")
    smtp_password = os.environ.get("SMTP_PASS") or os.environ.get("SMTP_PASSWORD")

    can_send = all([to_addr, from_addr, smtp_host, smtp_port, smtp_user, smtp_password])
    if not can_send:
        logging.warning("Email not sent (missing SMTP or from/to). See artifacts for last_report.html, report.csv, and scanned.csv.")
        print(f"Report created. Matches: {sum(len(r['mentions']) for r in results_for_email)}; items: {len(results_for_email)}; scanned_total: {len(scanned_log)}")
        return

    # Subject line
    if is_backfill:
        subject = f"Delran BOE – Preschool Mentions (Backfill {datetime(2021,1,1).date()} → {end.date()})"
    else:
        subject = f"Delran BOE – Preschool Mentions ({start.date().isoformat()[:7]}) Monthly Report"

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

    print(f"Email sent to {to_addr}. Matches: {sum(len(r['mentions']) for r in results_for_email)}; items: {len(results_for_email)}; scanned_total: {len(scanned_log)}")


# ----------------------------------------------------------------------------
# Entry point with final-resort catcher
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        tb = traceback.format_exc()
        # Leave an artifact with traceback for debugging
        with open("last_report.html", "w", encoding="utf-8") as f:
            f.write(f"""<html><body>
            <h2>Delran BOE – Monitor: Unhandled Error</h2>
            <pre style="white-space: pre-wrap; font-family: monospace;">{tb}</pre>
            </body></html>""")
        print("Unhandled error; traceback written to last_report.html")
        raise
