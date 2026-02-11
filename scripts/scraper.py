# Delran BOE Preschool Monitor – Scraper (Enhanced + Diagnostics, safe paste)

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


# ----------------------------- State --------------------------------

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


def sha1_of(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", "ignore"))
    return h.hexdigest()


def html_escape(s: str) -> str:
    s2 = s or ""
    s2 = s2.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    return s2


def ensure_debug_dir() -> None:
    os.makedirs(".debug", exist_ok=True)


# ------------------------------ HTTP --------------------------------

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
