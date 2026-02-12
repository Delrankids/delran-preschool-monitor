# Delran BOE Preschool Monitor – Scraper (Enhanced + Diagnostics)

import os
import csv
import json
import time
import hashlib
import logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
import html as _html

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# --------------------------- Configuration ---------------------------

BASE_URL = os.environ.get("DELRAN_MINUTES_URL", "https://www.delranschools.org/b_o_e/meeting_minutes")
BOE_URL = os.environ.get("DELRAN_BOE_URL", "https://www.delranschools.org/b_o_e")
BOARDDOCS_PUBLIC = os.environ.get("BOARDDOCS_PUBLIC_URL", "https://go.boarddocs.com/nj/delranschools/Board.nsf/Public")

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


# ----------------------------- Helpers ------------------------------

def html_escape(s: str) -> str:
    return _html.escape(s or "", quote=True)

def sha1_of(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", "ignore"))
    return h.hexdigest()

def ensure_debug_dir() -> None:
    os.makedirs(".debug", exist_ok=True)

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


# ---------------------------- Discovery -----------------------------

DOC_EXTS = (".pdf", ".docx", ".doc", ".htm", ".html")

def _collect_from_page(page_url: str, debug_name: Optional[str]) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    try:
        resp = fetch(page_url)
    except Exception as e:
        logging.warning("Failed to fetch %s: %s", page_url, str(e))
        return items

    if DEBUG_SAVE_HTML or debug_name:
        try:
            ensure_debug_dir()
            name = debug_name or "page.html"
            with open(os.path.join(".debug", name), "wb") as f:
                f.write(resp.content)
            logging.info("Saved debug HTML -> .debug/%s", name)
        except Exception as e:
            logging.warning("Could not write debug HTML for %s: %s", page_url, str(e))

    soup = BeautifulSoup(resp.text, "lxml")
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"] or ""
        full = urljoin(page_url, href)
        title = a.get_text(strip=True) or full
        if ("DisplayFile.aspx" in full) or full.lower().endswith(DOC_EXTS):
            if full not in seen:
                seen.add(full)
                items.append({"title": title, "url": full, "source": "district"})
    return items

def get_minutes_links() -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    out.extend(_collect_from_page(BASE_URL, "minutes.html"))
    out.extend(_collect_from_page(BOE_URL, "boe.html"))
    logging.info("District links collected: %d", len(out))
    return out

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
          logging.warning("BoardDocs fetch failed %s: %s", url, str(e))
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
            if ("Board.nsf" in full) and full.startswith("https://go.boarddocs.com") and full not in visited and len(to_visit) < 8:
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


# ---------------------------- Extraction ----------------------------

def extract_text_for_url(item: Dict[str, str]) -> str:
    # Local import to let outer catcher show errors in last_report.html
    from parser_utils import extract_text_from_pdf, extract_text_from_docx

    url_lower = item["url"].lower()
    path_guess = urlparse(url_lower).path.lower()

    try:
        resp = fetch(item["url"])
    except Exception as e:
        logging.warning("Fetch failed %s: %s", item["url"], str(e))
        return ""

    ctype = (resp.headers.get("Content-Type") or "").lower()

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
                    break
                inner_ctype = (inner.headers.get("Content-Type") or "").lower()
                if "application/pdf" in inner_ctype or inner_url.lower().endswith(".pdf"):
                    polite_delay()
                    return extract_text_from_pdf(inner.content)
                if inner_url.lower().endswith(".docx"):
                    polite_delay()
                    return extract_text_from_docx(inner.content)
        return " ".join(s.strip() for s in soup.stripped_strings)

    if "application/pdf" in ctype or path_guess.endswith(".pdf"):
        polite_delay()
        return extract_text_from_pdf(resp.content)

    if path_guess.endswith(".docx"):
        polite_delay()
        return extract_text_from_docx(resp.content)

    return ""


# ----------------------- Date range calculation ---------------------

def first_day_of_month(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def compute_run_range(state: Dict) -> Tuple[datetime, datetime, bool]:
    today = datetime.utcnow()
    if not state.get("backfill_done"):
        return (datetime(2021, 1, 1), today, True)
    return (first_day_of_month(today), today, False)

def within_range(iso_dt: Optional[str], start: datetime, end: datetime) -> bool:
    if not iso_dt:
        return True
    try:
        dt = dateparser.parse(iso_dt).replace(tzinfo=None)
        return (start <= dt <= end)
    except Exception:
        return True


# -------------------------------- Main ------------------------------

def load_state() -> Dict:
    state = {"seen_hashes": [], "backfill_done": False, "last_run_end": None}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                state.update(data)
        except Exception:
            logging.warning("State file unreadable; starting fresh.")
    return state

def save_state(state: Dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def main() -> None:
    # Defer imports so the outer catcher can render last_report.html on import errors
    from parser_utils import find_preschool_mentions, guess_meeting_date
    from email_utils import render_html_report, _build_email_message, send_email as _send_email

    state = load_state()
    start, end, is_backfill = compute_run_range(state)
    logging.info("Date range: %s -> %s (backfill=%s)", start.date(), end.date(), is_backfill)

    # Create placeholder outputs early so artifacts always exist
    try:
        if not os.path.exists("report.csv"):
            with open("report.csv", "w", encoding="utf-8", newline="") as cf:
                w = csv.writer(cf)
                w.writerow(["date", "source", "url", "keyword", "snippet"])
        if not os.path.exists("scanned.csv"):
            with open("scanned.csv", "w", encoding="utf-8", newline="") as sf:
                w = csv.writer(sf)
                w.writerow(["date", "source", "title", "url", "status", "reason"])
        if not os.path.exists("last_report.html"):
            with open("last_report.html", "w", encoding="utf-8") as f:
                f.write("<html><body><h2>Delran BOE – Preschool Mentions</h2><p>(Initializing…)</p></body></html>")
    except Exception as _e:
        logging.warning("Could not create placeholder outputs: %s", _e)

    # Discover
    items: List[Dict[str, str]] = []
    minutes = get_minutes_links()
    items.extend(minutes)
    if MAX_BOARDDOCS_FILES > 0:
        items.extend(get_boarddocs_links(MAX_BOARDDOCS_FILES))

    if not items:
        ensure_debug_dir()
        try:
            with open(".debug/items.json", "w", encoding="utf-8") as f:
                json.dump({"minutes_count": len(minutes), "items": items}, f, indent=2)
        except Exception as e:
            logging.warning("Could not write .debug/items.json: %s", str(e))

    # Scan
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
                "date": "",
                "source": source,
                "title": title,
                "url": url,
                "status": "error",
                "reason": "fetch/extract error: " + str(e)
            })
            continue

        if not text:
            scanned_log.append({
                "date": "",
                "source": source,
                "title": title,
                "url": url,
                "status": "skipped",
                "reason": "no text extracted"
            })
            continue

        mentions = find_preschool_mentions(text)
        meeting_dt = guess_meeting_date(text, title=title, url=url)
        iso_date = meeting_dt.isoformat() if meeting_dt else None

        if (MIN_YEAR is not None) and meeting_dt and meeting_dt.year < MIN_YEAR:
            scanned_log.append({
                "date": meeting_dt.date().isoformat(),
