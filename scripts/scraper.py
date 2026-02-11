"""
Delran BOE Preschool Monitor – Scraper (Enhanced + Diagnostics, clean)

What it does
------------
1) Crawls the Delran BOE Meeting Minutes and BOE pages for PDF/DOCX/HTML items.
2) Optionally scans BoardDocs Public for PDFs (limited by MAX_BOARDDOCS_FILES).
3) Extracts text and finds preschool-related mentions (via parser_utils.py).
4) Builds HTML + CSV report and emails it monthly.
5) Persists 'seen' match hashes in state.json to dedupe future runs.
6) First run does a backfill from 2021-01-01 to today; then runs monthly.
7) Writes a full audit log of *every* document seen to scanned.csv and
   appends a "Documents scanned" section to the email.
8) If discovery returns 0 links, saves the raw Minutes/BOE HTML and items.json
   under .debug/ to quickly diagnose site/layout changes.

Environment (set in workflow or repo secrets)
---------------------------------------------
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
- DOC_DELAY_SECONDS     -> polite delay between requests (default: 2.0)
- REQUEST_TIMEOUT       -> requests timeout seconds (default: 60)
- MAX_BOARDDOCS_FILES   -> default: 50
- MIN_YEAR              -> optional int to drop items with parsed date before year
- IGNORE_DEDUPE         -> "1" to ignore dedupe for this run (default: "0")
- DEBUG_SAVE_HTML       -> "1" to always save fetched HTML to .debug/

Outputs
-------
- last_report.html
- report.csv
- scanned.csv
- state.json
- .debug/minutes.html, .debug/boe.html, .debug/items.json (when discovery==0 or DEBUG_SAVE_HTML=1)
"""

import os
import csv
import json
import time
import hashlib
import logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

BASE_URL = os.environ.get(
    "DELRAN_MINUTES_URL",
    "https://www.delranschools.org/b_o_e/meeting_minutes",
)
BOE_URL = os.environ.get(
    "DELRAN_BOE_URL",
    "https://www.delranschools.org/b_o_e",
)
BOARDDOCS_PUBLIC = os.environ.get(
    "BOARDDOCS_PUBLIC_URL",
    "https://go.boarddocs.com/nj/delranschools/Board.nsf/Public",
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

_MIN_YEAR_ENV = os.environ.get("MIN_YEAR")
MIN_YEAR = int(_MIN_YEAR_ENV) if (_MIN_YEAR_ENV and str(_MIN_YEAR_ENV).isdigit()) else None

IGNORE_DEDUPE = os.environ.get("IGNORE_DEDUPE", "0") == "1"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# -----------------------------------------------------------------------------
# State helpers
# -----------------------------------------------------------------------------

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
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def ensure_debug_dir() -> None:
    os.makedirs(".debug", exist_ok=True)


# -----------------------------------------------------------------------------
# HTTP helpers
# -----------------------------------------------------------------------------

def fetch(url: str, referer: Optional[str] = None) -> requests.Response:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer
    logging.info("GET %s", url)
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    logging.info(" -> status=%s, bytes=%s", resp.status_code, len(resp.content))
    resp.raise_for_status()
    return resp


def polite_delay() -> None:
    if DOC_DELAY_SECONDS > 0:
        time.sleep(DOC_DELAY_SECONDS)


# -----------------------------------------------------------------------------
# Discovery
# -----------------------------------------------------------------------------

DOC_EXTS = (".pdf", ".docx", ".doc", ".htm", ".html")


def _collect_from_page(page_url: str, debug_name: Optional[str]) -> List[Dict[str, str]]:
    """Fetch page, optionally save raw HTML to .debug/debug_name, parse document-like links."""
    try:
        resp = fetch(page_url)
    except Exception as e:
        logging.warning("Failed to fetch %s: %s", page_url, e)
        return []

    if DEBUG_SAVE_HTML or debug_name:
        try:
            ensure_debug_dir()
            fname = debug_name or "page.html"
            with open(os.path.join(".debug", fname), "wb") as f:
                f.write(resp.content)
            logging.info("Saved debug HTML -> .debug/%s", fname)
        except Exception as e:
            logging.warning("Could not write debug HTML for %s: %s", page_url, e)

    soup = BeautifulSoup(resp.text, "lxml")

    out: List[Dict[str, str]] = []
    seen_urls = set()

    for a in soup.find_all("a", href=True):
        href = a["href"] or ""
        url = urljoin(page_url, href)
        title = a.get_text(strip=True) or url

        if ("DisplayFile.aspx" in url) or url.lower().endswith(DOC_EXTS):
            if url not in seen_urls:
                seen_urls.add(url)
                out.append({"title": title, "url": url, "source": "district"})

    return out


def get_minutes_links() -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    items.extend(_collect_from_page(BASE_URL, debug_name="minutes.html"))
    items.extend(_collect_from_page(BOE_URL, debug_name="boe.html"))
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

            if (
                "Board.nsf" in full
                and full.startswith("https://go.boarddocs.com")
                and full not in visited
                and len(to_visit) < 8
            ):
                to_visit.append(full)

    out: List[Dict[str, str]] = []
    seen = set()
    for it in candidates:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        out.append(it)

    logging.info("BoardDocs links collected: %d", len(out))
    return out


# -----------------------------------------------------------------------------
# Extraction
# -----------------------------------------------------------------------------

def extract_text_for_url(item: Dict[str, str]) -> str:
    """Follow embedded doc links (if HTML); extract text for PDF/DOCX; else visible text."""
    from parser_utils import extract_text_from_pdf, extract_text_from_docx  # local import

    url_lower = item["url"].lower()
    path_guess = urlparse(url_lower).path.lower()

    try:
        resp = fetch(item["url"])
    except Exception as e:
        logging.warning("Fetch failed %s: %s", item["url"], e)
        return ""

    ctype = (resp.headers.get("Content-Type") or "").lower()

    # If HTML, try to follow to a direct PDF/DOCX first
    if "text/html" in ctype or path_guess.endswith((".htm", ".html")):
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            h = a["href"] or ""
            if h.lower().endswith(".pdf") or "DisplayFile.aspx" in h or "/files/" in h:
                inner_url = urljoin(item["url"], h)
                polite_delay()
                try:
                    inner = fetch(inner_url, referer=item["url"])
                except Exception:
                    # Can't follow inner link; fall back to visible text below
                    break
                inner_ctype = (inner.headers.get("Content-Type") or "").lower()
                if "application/pdf" in inner_ctype or inner_url.lower().endswith(".pdf"):
                    polite_delay()
                    return extract_text_from_pdf(inner.content)
                if inner_url.lower().endswith(".docx"):
                    polite_delay()
                    return extract_text_from_docx(inner.content)
        # fallback to visible HTML text
        return " ".join(s.strip() for s in soup.stripped_strings)

    # Direct types
    if "application/pdf" in ctype or path_guess.endswith(".pdf"):
        polite_delay()
        return extract_text_from_pdf(resp.content)

    if path_guess.endswith(".docx"):
        polite_delay()
        return extract_text_from_docx(resp.content)

    # Unknown
    return ""


# -----------------------------------------------------------------------------
# Date range calculation
# -----------------------------------------------------------------------------

def first_day_of_month(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


