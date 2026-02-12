# Delran BOE Preschool Monitor – Scraper (2026-02 rewrite for BoardDocs + CMS changes)
# Full file with YEAR-based backfill + subject updates included
#
# Key improvements:
# - Robust discovery for BoardDocs attachments (dynamic pages) by parsing embedded script JSON
#   and link patterns like /Board.nsf/files/<id>/(download|view) – these often lack .pdf extension.
# - Safer crawling of district CMS pages to pick up direct PDFs on Sharpschool CDN.
# - Debug artifacts (.debug/*.html, items.json) to diagnose "0 scanned" cases.
# - FORCE_FULL_RESCAN=1 support to override state.json on demand.
# - YEAR=<yyyy> support to limit a run to a single calendar year.
#
# Outputs preserved: last_report.html, report.csv, scanned.csv, to_send.eml, sent_report.eml
# Requires: parser_utils.py (extract_text_from_pdf, extract_text_from_docx, find_preschool_mentions, guess_meeting_date)
#           email_utils.py   (render_html_report, _build_email_message, send_email)

import os
import re
import csv
import json
import time
import hashlib
import logging
from typing import List, Dict, Optional, Tuple, Iterable, Set
from urllib.parse import urljoin, urlparse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
import html as _html

# --------------------------- Configuration ---------------------------

BASE_URL = os.environ.get("DELRAN_MINUTES_URL", "https://www.delranschools.org/b_o_e/meeting_minutes")
BOE_URL = os.environ.get("DELRAN_BOE_URL", "https://www.delranschools.org/b_o_e")
BOARDDOCS_PUBLIC = os.environ.get("BOARDDOCS_PUBLIC_URL", "https://go.boarddocs.com/nj/delranschools/Board.nsf/Public")

STATE_FILE = os.environ.get("STATE_FILE", "state.json")
DEBUG_SAVE_HTML = os.environ.get("DEBUG_SAVE_HTML", "1") == "1"   # default ON for easier first-run diagnosis
FORCE_FULL_RESCAN = os.environ.get("FORCE_FULL_RESCAN", "0") == "1"

HEADERS = {
    "User-Agent": "Delran-Preschool-Agent/2.0 (+mailto:alerts@example.com; GitHub Actions bot)",
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

# YEAR-based backfill (optional)
YEAR_ENV = os.environ.get("YEAR")
YEAR = int(YEAR_ENV) if YEAR_ENV and YEAR_ENV.isdigit() else None

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Discovery controls
MAX_DISTRICT_PAGES = int(os.environ.get("MAX_DISTRICT_PAGES", "25"))
MAX_CRAWL_DEPTH = int(os.environ.get("MAX_CRAWL_DEPTH", "2"))

ALLOWED_DISTRICT_DOMAINS = {
    "www.delranschools.org",
    "delranschools.org",
    "cdnsm5-ss5.sharpschool.com",  # district CDN hosting PDFs
}

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

def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def is_allowed_domain(url: str, allowed: Set[str]) -> bool:
    d = domain_of(url)
    return any((d == a) or d.endswith("." + a) for a in allowed)

def save_debug_html(name: str, content: bytes) -> None:
    if not DEBUG_SAVE_HTML:
        return
    try:
        ensure_debug_dir()
        with open(os.path.join(".debug", name), "wb") as f:
            f.write(content)
        logging.info("Saved debug HTML -> .debug/%s", name)
    except Exception as e:
        logging.warning("Could not write debug HTML %s: %s", name, str(e))

# ---------------------------- Discovery -----------------------------

DOC_EXTS = (".pdf", ".docx", ".doc", ".htm", ".html")

BOARD_DOCS_FILE_RE = re.compile(r"/Board\.nsf/files/([A-Za-z0-9]+)/(?:(?:download)|(?:view))", re.IGNORECASE)
BOARD_DOCS_JSON_URL_RE = re.compile(r'"downloadUrl"\s*:\s*"([^"]+/Board\.nsf/files/[^"]+?)"', re.IGNORECASE)
BOARD_DOCS_JSON_NAME_RE = re.compile(r'"fileName"\s*:\s*"([^"]+?)"', re.IGNORECASE)

def collect_links_from_html(page_url: str, html_text: str) -> List[Dict[str, str]]:
    """Collect direct document links from HTML anchor tags and embedded JSON."""
    soup = BeautifulSoup(html_text, "lxml")
    items: List[Dict[str, str]] = []
    seen: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        full = urljoin(page_url, href)
        title = a.get_text(strip=True) or full

        # BoardDocs file links do not always end with .pdf; capture by pattern
        if BOARD_DOCS_FILE_RE.search(full):
            if full not in seen:
                seen.add(full)
                items.append({"title": title or "BoardDocs Attachment", "url": full, "source": "boarddocs"})
            continue

        # Classic documents (PDF/DOCX) or district "DisplayFile.aspx"
        if ("DisplayFile.aspx" in full) or full.lower().endswith(DOC_EXTS):
            if full not in seen:
                seen.add(full)
                src = "district"
                # CDN may host PDFs even if navigated from district site
                if "cdnsm" in domain_of(full) or "sharpschool" in domain_of(full):
                    src = "district-cdn"
                items.append({"title": title, "url": full, "source": src})

    # Additionally parse embedded <script> JSON where BoardDocs lists files
    for script in soup.find_all("script"):
        s = script.string or script.get_text() or ""
        if not s:
            continue
        for m_url in BOARD_DOCS_JSON_URL_RE.finditer(s):
            file_url = urljoin(page_url, m_url.group(1))
            if file_url not in seen:
                seen.add(file_url)
                # Try to pull a nearby fileName from the same script block
                name_match = BOARD_DOCS_JSON_NAME_RE.search(s)
                fname = name_match.group(1) if name_match else "BoardDocs Attachment"
                items.append({"title": fname, "url": file_url, "source": "boarddocs"})

    return items

def crawl_district(start_urls: Iterable[str], allowed_domains: Set[str],
                   max_pages: int, max_depth: int) -> List[Dict[str, str]]:
    """
    Lightweight crawler for the district CMS to discover PDFs/links.
    BFS limited by domain, page count, and depth.
    """
    queue: List[Tuple[str, int]] = []
    visited: Set[str] = set()
    results: List[Dict[str, str]] = []

    for u in start_urls:
        queue.append((u, 0))

    while queue and len(visited) < max_pages:
        url, depth = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        if not is_allowed_domain(url, allowed_domains):
            continue

        try:
            resp = fetch(url)
        except Exception as e:
            logging.warning("District fetch failed %s: %s", url, e)
            continue

        save_debug_html(f"district_{len(visited):03d}.html", resp.content)

        # Collect document links on this page
        results.extend(collect_links_from_html(url, resp.text))

        # Enqueue next-level pages (only html pages)
        if depth < max_depth:
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                h = a.get("href") or ""
                nxt = urljoin(url, h)
                if (nxt not in visited
                        and is_allowed_domain(nxt, allowed_domains)
                        and (nxt.lower().endswith((".htm", ".html", "/")) or ("b_o_e" in nxt))):
                    queue.append((nxt, depth + 1))

    # Deduplicate by URL
    out, seen = [], set()
    for it in results:
        if it["url"] not in seen:
            seen.add(it["url"])
            out.append(it)
    logging.info("District links discovered: %d (pages crawled=%d)", len(out), len(visited))
    return out

def crawl_boarddocs(root_url: str, max_files: int) -> List[Dict[str, str]]:
    """
    Crawl BoardDocs public portal and discover attachments by:
      - parsing anchor tags (when present),
      - parsing embedded script JSON for downloadUrl/fileName,
      - scanning for /Board.nsf/files/<id>/(download|view) patterns.
    Limit to max_files.
    """
    if max_files <= 0:
        return []

    queue: List[str] = [root_url]
    visited: Set[str] = set()
    items: List[Dict[str, str]] = []
    page_budget = 30  # keep it modest

    while queue and page_budget > 0 and len(items) < max_files:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        page_budget -= 1

        try:
            resp = fetch(url)
        except Exception as e:
            logging.warning("BoardDocs fetch failed %s: %s", url, e)
            continue

        save_debug_html(f"boarddocs_{len(visited):03d}.html", resp.content)
        html = resp.text

        # Collect direct links & embedded JSON-based links
        new_links = collect_links_from_html(url, html)
        for it in new_links:
            if it.get("source") == "boarddocs":
                items.append(it)
                if len(items) >= max_files:
                    break
        if len(items) >= max_files:
            break

        # Also, follow more BoardDocs pages we discover (but stay under the same host)
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            h = a.get("href") or ""
            nxt = urljoin(url, h)
            if (nxt.startswith("https://go.boarddocs.com/")
                    and nxt not in visited
                    and len(queue) < 20):
                queue.append(nxt)

        # Regex search for file patterns in raw HTML as a last resort
        for m in BOARD_DOCS_FILE_RE.finditer(html):
            f_url = urljoin(url, m.group(0))
            if all(x["url"] != f_url for x in items):
                items.append({"title": "BoardDocs Attachment", "url": f_url, "source": "boarddocs"})
                if len(items) >= max_files:
                    break

    # Dedup by URL
    out, seen = [], set()
    for it in items:
        if it["url"] not in seen:
            seen.add(it["url"])
            out.append(it)
    logging.info("BoardDocs links discovered: %d (pages visited=%d)", len(out), len(visited))
    return out

def get_minutes_links() -> List[Dict[str, str]]:
    """
    Discover district-side documents (Sharpschool CMS/CDN) by crawling a few pages.
    """
    start_urls = [BASE_URL, BOE_URL]
    return crawl_district(
        start_urls=start_urls,
        allowed_domains=ALLOWED_DISTRICT_DOMAINS,
        max_pages=MAX_DISTRICT_PAGES,
        max_depth=MAX_CRAWL_DEPTH,
    )

def get_boarddocs_links(max_files: int) -> List[Dict[str, str]]:
    return crawl_boarddocs(BOARDDOCS_PUBLIC, max_files=max_files)

# ---------------------------- Extraction ----------------------------

def extract_text_for_url(item: Dict[str, str]) -> str:
    """
    Fetch the resource and extract text depending on content type.
    Supports: HTML (and follows first nested PDF/DOCX link), PDF, DOCX.
    Also treats BoardDocs download/view URLs as PDFs even if extensionless.
    """
    from parser_utils import extract_text_from_pdf, extract_text_from_docx

    url_lower = item["url"].lower()
    path_guess = urlparse(url_lower).path.lower()

    try:
        resp = fetch(item["url"])
    except Exception as e:
        logging.warning("Fetch failed %s: %s", item["url"], str(e))
        return ""

    ctype = (resp.headers.get("Content-Type") or "").lower()

    # BoardDocs attachments often serve as application/pdf or octet-stream
    if "/board.nsf/files/" in path_guess:
        polite_delay()
        try:
