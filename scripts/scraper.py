"""
Delran BOE Preschool Monitor – Scraper (Enhanced + Diagnostics)

What it does
------------
1) Crawls the Delran BOE Meeting Minutes and BOE pages for PDF/DOCX/HTML items.
2) Optionally scans BoardDocs Public for PDFs (limited to MAX_BOARDDOCS_FILES).
3) Extracts text and finds preschool-related mentions (via parser_utils.py).
4) Builds HTML + CSV report and emails it monthly.
5) Persists 'seen' match hashes in state.json to dedupe future runs.
6) First run does a backfill from 2021-01-01 to today; then runs monthly.
7) Writes a full audit log of *every* document seen to scanned.csv and
   appends a "Documents scanned" section to the email.
8) NEW: If discovery returns 0 links, saves raw HTML of source pages and items.json
   to .debug/ for quick diagnosis; adds verbose logging of fetch sizes/status.

Environment (via workflow or repo secrets)
------------------------------------------
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
- DEBUG_SAVE_HTML       -> "1" to force saving fetched HTML to .debug/ (default behavior
                           now saves automatically if discovery==0)

Outputs
-------
- last_report.html  (HTML body of email, includes "Documents scanned" section)
- report.csv        (only matching rows)
- scanned.csv       (every document encountered with status and reason)
- state.json        (committed by workflow)
- .debug/minutes.html, .debug/boe.html, .debug/items.json (diagnostics on zero discovery)
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
DEBUG_SAVE_HTML = os.environ.get("DEBUG_SAVE_HTML", "0") == "1"

HEADERS = {
    "User-Agent": "Delran-Preschool-Agent/1.3 (+mailto:alerts@example.com; GitHub Actions bot)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

DOC_DELAY_SECONDS = float(os.environ.get("DOC_DELAY_SECONDS", "2.0"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))
MAX_BOARDDOCS_FILES = int(os.environ.get("MAX_BOARDDOCS_FILES", "50"))
MIN_YEAR_ENV = os.environ.get("MIN_YEAR")
MIN_YEAR = int(MIN_YEAR_ENV) if (MIN_YEAR_ENV and str(MIN_YEAR_ENV).isdigit()) else None

IGNORE_DEDUPE = os.environ.get("IGNORE_DEDUPE", "0") == "1"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ----------------------------------------------------------------------------
# State
# ----------------------------------------------------------------------------

def load_state() -> Dict:
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


def ensure_debug_dir():
    os.makedirs(".debug", exist_ok=True)


# ----------------------------------------------------------------------------
# Fetch helpers
# ----------------------------------------------------------------------------

def fetch(url: str, referer: Optional[str] = None) -> requests.Response:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer
    logging.info("GET %s", url)
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    logging.info(" -> status=%s, bytes=%s", resp.status_code, len(resp.content))
    resp.raise_for_status()
    return resp


def polite_delay():
    if DOC_DELAY_SECONDS > 0:
        time.sleep(DOC_DELAY_SECONDS)


# ----------------------------------------------------------------------------
# Discovery
# ----------------------------------------------------------------------------

DOC_EXTS = (".pdf", ".docx", ".doc", ".htm", ".html")

def _collect_from_page(page_url: str, save_debug_name: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Fetches page_url, optionally saves raw HTML to .debug/save_debug_name, and
    returns discovered document-like links.
    """
    try:
        resp = fetch(page_url)
    except Exception as e:
        logging.warning("Failed to fetch %s: %s", page_url, e)
        return []

    if DEBUG_SAVE_HTML or save_debug_name:
        try:
            ensure_debug_dir()
            fname = save_debug_name or (urlparse(page_url).path.strip("/").replace("/", "_") or "page") + ".html"
            path = os.path.join(".debug", fname)
            with open(path, "wb") as f:
                f.write(resp.content)
            logging.info("Saved debug HTML to %s", path)
        except Exception as e:
            logging.warning("Could not write debug HTML for %s: %s", page_url, e)

    soup = BeautifulSoup(resp.text, "lxml")

    links: List[Dict[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"] or ""
        url = urljoin(page_url, href)
        title = a.get_text(strip=True) or url

        # Accept direct document-ish links or district file handler
        if ("DisplayFile.aspx" in url) or url.lower().endswith(DOC_EXTS):
            links.append({"title": title, "url": url, "source": "district"})

    # Dedupe
    uniq, seen = [], set()
    for it in links:
        if it["url"] in seen: 
            continue
        seen.add(it["url"])
        uniq.append(it)
    return uniq


def get_minutes_links() -> List[Dict[str, str]]:
    items = []
    items.extend(_collect_from_page(BASE_URL, save_debug_name="minutes.html"))
    items.extend(_collect_from_page(BOE_URL, save_debug_name="boe.html"))
    logging.info("District links collected: %d", len(items))
    return items


def get_boarddocs_links(max_files: int) -> List[Dict[str, str]]:
    candidates: List[Dict[str, str]] = []
    to_visit = [BOARDDOCS_PUBLIC]
    visited = set()

    while to_visit and len(to_visit) <= 8 and len(candidates) < max_files:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            resp = fetch(url)
        except Exception as e:
            logging.warning("BoardDocs fetch failed %s: %s", url, e)
            continue

        soup = BeautifulSoup(resp.text, "lxml")
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
        if it["url"] in seen: 
            continue
        seen.add(it["url"])
        uniq.append(it)
    logging.info("BoardDocs links collected: %d", len(uniq))
    return uniq


# ----------------------------------------------------------------------------
# Extraction
# ----------------------------------------------------------------------------

def extract_text_for_url(item: Dict[str, str]) -> str:
    """
    - If HTML: follow embedded doc links (PDF/DOCX) first; else return visible text.
    - If PDF: extract with PyPDF2.
    - If DOCX: extract with python-docx.
    """
    from parser_utils import extract_text_from_pdf, extract_text_from_docx  # local import

    url_lower = item["url"].lower()
    path_guess = urlparse(url_lower).path.lower()

    try:
        resp = fetch(item["url"])
    except Exception as e:
        logging.warning("Fetch failed %s: %s", item["url"], e)
        return ""

    ctype = (resp.headers.get("Content-Type") or "").lower()

    if "text/html" in ctype or path_guess.endswith((".htm", ".html")):
        soup = BeautifulSoup(resp.text, "lxml")
        # If this page points to a real doc, follow that instead.
        for a in soup.find_all("a", href=True):
            h = a["href"] or ""
            if h.lower().endswith(".pdf") or "DisplayFile.aspx" in h or "/files/" in h:
                inner_url = urljoin(item["url"], h)
                polite_delay()
                try:
                    inner = fetch(inner_url, referer=item["url"])
                except Exception:
                    break
                inner_ctype = (inner.headers.get("Content-Type") or "").lower()
                if "application/pdf" in inner_ctype or inner_url.lower().endswith(".pdf"):
                    polite_delay()
                    return extract_text_from_pdf(inner.content)
                elif inner_url.lower().endswith(".docx"):
                    polite_delay()
                    return extract_text_from_docx(inner.content)
        # Fallback to visible text
        return " ".join(s.strip() for s in soup.stripped_strings)

    if "application/pdf" in ctype or path_guess.endswith(".pdf"):
        polite_delay()
        return extract_text_from_pdf(resp.content)

    if path_guess.endswith(".docx"):
        polite_delay()
        return extract_text_from_docx(resp.content)

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
    # Defer imports so outer catcher writes last_report.html for import errors too
    from parser_utils import find_preschool_mentions, guess_meeting_date
    from email_utils import send_email, render_html_report

    state = load_state()
    start, end, is_backfill = compute_run_range(state)
    logging.info("Date range: %s -> %s (backfill=%s)", start.date(), end.date(), is_backfill)

    # Gather links
    items: List[Dict[str, str]] = []
    minutes = get_minutes_links()
    items.extend(minutes)
    if MAX_BOARDDOCS_FILES > 0:
        items.extend(get_boarddocs_links(MAX_BOARDDOCS_FILES))

    # If no items at all, save diagnostics so we can patch quickly
    if not items:
        ensure_debug_dir()
        try:
            with open(".debug/items.json", "w", encoding="utf-8") as f:
                json.dump({"minutes_count": len(minutes), "items": items}, f, indent=2)
        except Exception as e:
            logging.warning("Could not write .debug/items.json: %s", e)

    # Audit log of every document processed
    scanned_log: List[Dict[str, str]] = []

    results_for_email: List[Dict] = []
    rows_for_csv: List[List[str]] = []

    seen_hashes = set(state.get("seen_hashes") or [])
    new_hashes = set()

    for item in items:
        title = item.get("title") or "Meeting Item"
        url = item["url"]
        source = item.get("source") or ""

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

        meeting_dt = guess_meeting_date(text, title=title, url=url)
        iso_date = meeting_dt.isoformat() if meeting_dt else None

        if MIN_YEAR is not None and meeting_dt and meeting_dt.year < MIN_YEAR:
            scanned_log.append({
                "date": meeting_dt.date().isoformat(),
                "source": source, "title": title, "url": url,
                "status": "skipped", "reason": f"before MIN_YEAR {MIN_YEAR}"
            })
            continue

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
