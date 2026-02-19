# Delran BOE Preschool Monitor – Scraper
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
import html as _html
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth

from parser_utils import extract_text_from_pdf, extract_text_from_docx, find_preschool_mentions, guess_meeting_date, KEYWORD_REGEX
from email_utils import render_html_report, send_email

# --------------------------- Configuration ---------------------------

BASE_URL = os.environ.get("DELRAN_MINUTES_URL", "https://www.delranschools.org/b_o_e/meeting_minutes")
BOE_URL = os.environ.get("DELRAN_BOE_URL", "https://www.delranschools.org/b_o_e")
BOARDDOCS_PUBLIC = os.environ.get("BOARDDOCS_PUBLIC_URL", "https://go.boarddocs.com/nj/delranschools/Board.nsf/Public")

STATE_FILE = os.environ.get("STATE_FILE", "state.json")
DEBUG_SAVE_HTML = os.environ.get("DEBUG_SAVE_HTML", "1") == "1"
FORCE_FULL_RESCAN = os.environ.get("FORCE_FULL_RESCAN", "0") == "1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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

YEAR_ENV = os.environ.get("YEAR")
YEAR = int(YEAR_ENV) if YEAR_ENV and YEAR_ENV.isdigit() else None

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

MAX_DISTRICT_PAGES = int(os.environ.get("MAX_DISTRICT_PAGES", "50"))
MAX_CRAWL_DEPTH = int(os.environ.get("MAX_CRAWL_DEPTH", "4"))

ALLOWED_DISTRICT_DOMAINS = {
    "www.delranschools.org",
    "delranschools.org",
    "cdnsm5-ss5.sharpschool.com",
}

# ----------------------------- Helpers ------------------------------

def html_escape(s: str) -> str:
    return _html.escape(s or "", quote=True)

def sha1_of(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", "ignore"))
    return h.hexdigest()

def fetch(url: str, referer: Optional[str] = None) -> requests.Response:
    logging.info(f"Starting fetch for {url}")
    if "delranschools.org" in url.lower():
        logging.info("Using stealth Playwright for Delran page")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                    timezone_id="America/New_York",
                    bypass_csp=True,
                    ignore_https_errors=True,
                    java_script_enabled=True,
                )
                page = context.new_page()
                stealth(page)  # Correct call
                page.set_extra_http_headers(HEADERS)
                if referer:
                    page.set_extra_http_headers({"Referer": referer})
                response = page.goto(url, timeout=90000, wait_until="networkidle")
                if response is None:
                    logging.warning("No response from goto")
                else:
                    logging.info(f"Playwright response status: {response.status}")
                page.wait_for_timeout(8000)
                html = page.content()
                # After html = page.content()
                logging.info(f"Playwright HTML length: {len(html)} bytes")
                logging.info(f"Contains 'GetFile.ashx': {'getfile.ashx' in html.lower()}")
                logging.info(f"Contains 'Minutes': {'minutes' in html.lower()}")
                logging.info(f"Contains 'Cloudflare' or 'checking your browser': {'cloudflare' in html.lower() or 'checking your browser' in html.lower()}")
                logging.info(f"Page title: {soup.title.string if soup.title else 'No title'}")

                # Clean first 300 chars outside f-string
                cleaned = html[:300].replace("\n", " ").replace("\r", " ")
                logging.info(f"First 300 chars of HTML (cleaned): {cleaned}")

# Cleaned first 300 chars (no backslash in f-string)
cleaned_snippet = html[:300].replace('\n', ' ').replace('\r', ' ')
logging.info(f"First 300 chars of HTML (cleaned): {cleaned_snippet}")
                logging.info(f"First 300 chars (cleaned): {html[:300].replace('\n', ' ').replace('\r', ' ')}")
                logging.info(f"Playwright HTML length: {len(html)} bytes")
                logging.info(f"Contains 'GetFile.ashx': {'getfile.ashx' in html.lower()}")
                logging.info(f"Contains 'Minutes': {'minutes' in html.lower()}")
                logging.info(f"Contains 'Cloudflare' or 'checking your browser': {'cloudflare' in html.lower() or 'checking your browser' in html.lower()}")
                logging.info("First 300 chars of HTML: " + html[:300].replace("\n", " ").replace("\r", " "))
                logging.info(f"Page title: {soup.title.string if soup.title else 'No title'}")
                logging.info(f"HTML snippet (first 500 chars): {html[:500]}")
                logging.info(f"Contains 'Cloudflare' or 'checking your browser': {'cloudflare' in html.lower() or 'checking your browser' in html.lower()}")
                logging.info(f"Contains 'GetFile.ashx': {'getfile.ashx' in html.lower()}")
                logging.info(f"Page title: {soup.title.string if soup.title else 'No title'}")
                logging.info(f"First 500 chars of HTML: {html[:500]}")
                logging.info(f"Does HTML contain 'GetFile.ashx'? {'getfile.ashx' in html.lower()}")
                logging.info(f"Does HTML contain 'Minutes'? {'minutes' in html.lower()}")
                logging.info(f"Number of <a> tags: {len(soup.find_all('a'))}")
                browser.close()
                logging.info(f"Stealth Playwright fetch success: {len(html)} bytes")
                class FakeResponse:
                    def __init__(self, text):
                        self.text = text
                        self.content = text.encode('utf-8')
                        self.status_code = 200 if len(text) > 5000 else 403
                    def raise_for_status(self):
                        if self.status_code != 200:
                            raise requests.exceptions.HTTPError(f"Status {self.status_code}")
                return FakeResponse(html)
        except Exception as e:
            logging.error(f"Stealth Playwright fetch failed: {str(e)}")
            raise
    else:
        headers = dict(HEADERS)
        if referer:
            headers["Referer"] = referer
        logging.info(f"Using requests for {url}")
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        logging.info(f"requests fetch: status={resp.status_code}, bytes={len(resp.content)}")
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
    soup = BeautifulSoup(html_text, "lxml")
    items: List[Dict[str, str]] = []
    seen: Set[str] = set()

    logging.info(f"Collecting links from {page_url}")

    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        full = urljoin(page_url, href)
        title = a.get_text(strip=True) or full
        lower_full = full.lower()
        lower_title = title.lower()

        if BOARD_DOCS_FILE_RE.search(full):
            if full not in seen:
                seen.add(full)
                items.append({"title": title or "BoardDocs Attachment", "url": full, "source": "boarddocs"})
                logging.info(f"Found BoardDocs: {full}")
            continue

        if 'getfile.ashx' in lower_full or 'displayfile.aspx' in lower_full or any(word in lower_title for word in ['minutes', 'agenda', 'boe', 'board', 'reorganization', 're-organization', 'session', 'meeting']):
            if full not in seen:
                seen.add(full)
                items.append({
                    "title": title or "Delran Meeting Document",
                    "url": full,
                    "source": "district"
                })
                logging.info(f"FOUND DELRAN DOCUMENT: {full} ({title})")

    for script in soup.find_all("script"):
        s = script.string or script.get_text() or ""
        if not s:
            continue
        for m_url in BOARD_DOCS_JSON_URL_RE.finditer(s):
            file_url = urljoin(page_url, m_url.group(1))
            if file_url not in seen:
                seen.add(file_url)
                name_match = BOARD_DOCS_JSON_NAME_RE.search(s)
                fname = name_match.group(1) if name_match else "BoardDocs Attachment"
                items.append({"title": fname, "url": file_url, "source": "boarddocs"})
                logging.info(f"Found BoardDocs JSON: {file_url}")

    logging.info(f"Collected {len(items)} links from {page_url}")
    return items

# ... (keep the rest of the file unchanged: crawl_district, crawl_boarddocs, get_minutes_links, load_state, save_state, process_document, write_report_csv, write_scanned_csv, main)

# (Paste the rest from your previous version - I omitted it to save space, but keep crawl_district, crawl_boarddocs, etc. as they are in your last successful version)










