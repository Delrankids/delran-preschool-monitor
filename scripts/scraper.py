# Delran BOE Preschool Monitor – Scraper (2026-02 rewrite for BoardDocs + CMS changes)
# Full file with YEAR-based backfill + subject updates + keyword highlighting
#
# Key improvements:
# - Robust discovery for BoardDocs attachments (dynamic pages) by parsing embedded script JSON
#   and link patterns like /Board.nsf/files/<id>/(download|view) – these often lack .pdf extension.
# - Safer crawling of district CMS pages to pick up direct PDFs on Sharpschool CDN.
# - Debug artifacts (.debug/*.html, items.json) to diagnose "0 scanned" cases.
# - FORCE_FULL_RESCAN=1 support to override state.json on demand.
# - YEAR=<yyyy> support to limit a run to a single calendar year.
# - Keyword highlighting in the final HTML via <mark>.
#
# Outputs preserved: last_report.html, report.csv, scanned.csv, to_send.eml, sent_report.eml
# Requires: parser_utils.py (extract_text_from_pdf, extract_text_from_docx, find_preschool_mentions, guess_meeting_date, KEYWORD_REGEX)
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
            # Prefer to treat as PDF first; fall back to HTML parse if not PDF-ish
            if "pdf" in ctype or resp.content[:4] == b"%PDF":
                return extract_text_from_pdf(resp.content)
        except Exception:
            pass
        # If not a PDF, try to parse HTML and follow any nested links
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            h = a.get("href") or ""
            if "/Board.nsf/files/" in h or h.lower().endswith(".pdf"):
                inner_url = urljoin(item["url"], h)
                try:
                    inner = fetch(inner_url, referer=item["url"])
                except Exception as e:
                    logging.warning("Inner fetch failed %s: %s", inner_url, e)
                    continue
                if (inner.headers.get("Content-Type") or "").lower().find("pdf") >= 0 or inner.content[:4] == b"%PDF":
                    polite_delay()
                    return extract_text_from_pdf(inner.content)
        # Last resort: text from page
        return " ".join(s.strip() for s in soup.stripped_strings)

    if "text/html" in ctype or path_guess.endswith((".htm", ".html")):
        soup = BeautifulSoup(resp.text, "lxml")
        # Follow a likely inner document if present
        for a in soup.find_all("a", href=True):
            h = a.get("href") or ""
            if not h:
                continue
            if h.lower().endswith(".pdf") or "DisplayFile.aspx" in h or "/files/" in h or "/Board.nsf/files/" in h or h.lower().endswith(".docx"):
                inner_url = urljoin(item["url"], h)
                polite_delay()
                try:
                    inner = fetch(inner_url, referer=item["url"])
                except Exception as e:
                    logging.warning("Inner fetch failed %s: %s", inner_url, e)
                    continue
                inner_ctype = (inner.headers.get("Content-Type") or "").lower()
                if "application/pdf" in inner_ctype or inner_url.lower().endswith(".pdf") or inner.content[:4] == b"%PDF":
                    polite_delay()
                    return extract_text_from_pdf(inner.content)
                if inner_url.lower().endswith(".docx"):
                    polite_delay()
                    return extract_text_from_docx(inner.content)
        # Fallback: join visible text
        return " ".join(s.strip() for s in soup.stripped_strings)

    if "application/pdf" in ctype or path_guess.endswith(".pdf") or resp.content[:4] == b"%PDF":
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
    if YEAR is not None:
        # Jan 1 .. Dec 31 of the requested year
        start = datetime(YEAR, 1, 1)
        end = datetime(YEAR, 12, 31, 23, 59, 59)
        return (start, end, True)
    if FORCE_FULL_RESCAN:
        return (datetime(2021, 1, 1), today, True)
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

# ------------------------- Highlighting ------------------------------

# Reuse the same regex the detector uses to avoid drift.
try:
    from parser_utils import KEYWORD_REGEX as KW_RE
except Exception:
    # Fallback minimal set if import fails for any reason
    KW_RE = re.compile(r"\b(preschool|pre[\s\-]?k|prek|upk|early\s+childhood|child[\s\-]?care|childcare|day[\s\-]?care|wrap[\s\-]?around|before\s+care|after\s+care|extended\s+day)\b", re.IGNORECASE)

def _highlight_text_node(soup: BeautifulSoup, node, regex: re.Pattern):
    """Replace matches in a NavigableString with <mark> elements."""
    text = str(node)
    parts = []
    last = 0
    for m in regex.finditer(text):
        if m.start() > last:
            parts.append(text[last:m.start()])
        mark = soup.new_tag("mark")
        # pale yellow background, subtle padding; inline so it survives email clients
        mark["style"] = "background:#fff3cd;padding:0 2px 0 2px;border-radius:2px;"
        mark.string = m.group(0)
        parts.append(mark)
        last = m.end()
    if not parts:
        return
    if last < len(text):
        parts.append(text[last:])
    # Replace node with the sequence
    node.replace_with(*parts)

def highlight_keywords_in_html(html: str, regex: re.Pattern) -> str:
    """Traverse HTML and wrap keyword matches in <mark>, skipping SCRIPT/STYLE."""
    soup = BeautifulSoup(html, "lxml")
    for tn in list(soup.find_all(string=True)):
        parent = tn.parent
        if not parent or getattr(parent, "name", "").lower() in ("script", "style"):
            continue
        # Only mutate visible text nodes
        _highlight_text_node(soup, tn, regex)
    return str(soup)

# -------------------------------- Main ------------------------------

def main() -> None:
    # Defer imports so the outer catcher can render last_report.html on import errors
    from parser_utils import find_preschool_mentions, guess_meeting_date
    from email_utils import render_html_report, _build_email_message, send_email as _send_email

    state = load_state()
    start, end, is_backfill = compute_run_range(state)
    logging.info("Date range: %s -> %s (backfill=%s, FORCE_FULL_RESCAN=%s, YEAR=%s)",
                 start.date(), end.date(), is_backfill, FORCE_FULL_RESCAN, YEAR)

    # Create placeholders early so artifacts always exist
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
    items.extend(get_boarddocs_links(MAX_BOARDDOCS_FILES))

    if not items:
        ensure_debug_dir()
        try:
            with open(".debug/items.json", "w", encoding="utf-8") as f:
                json.dump({
                    "minutes_count": len(minutes),
                    "items": items,
                    "notes": "No items discovered. Inspect .debug/district_*.html and .debug/boarddocs_*.html."
                }, f, indent=2)
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

        # Fetch/Extract safely
        try:
            text = extract_text_for_url(item)
        except Exception as e:
            scanned_log.append({
                "date": "",
                "source": source,
                "title": title,
                "url": url,
                "status": "error",
                "reason": f"fetch/extract error: {e}"
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
                "source": source,
                "title": title,
                "url": url,
                "status": "skipped",
                "reason": f"before MIN_YEAR {MIN_YEAR}"
            })
            continue

        if not within_range(iso_date, start, end):
            scanned_log.append({
                "date": meeting_dt.date().isoformat() if meeting_dt else "",
                "source": source,
                "title": title,
                "url": url,
                "status": "skipped",
                "reason": "out of date range"
            })
            continue

        if not mentions:
            scanned_log.append({
                "date": meeting_dt.date().isoformat() if meeting_dt else "",
                "source": source,
                "title": title,
                "url": url,
                "status": "scanned",
                "reason": "no preschool mentions"
            })
            continue

        # Dedup within state
        kept: List[Dict] = []
        for m in mentions:
            fp = sha1_of(url, m.get("keyword") or "", (m.get("snippet") or "")[:160])
            if IGNORE_DEDUPE or fp not in seen_hashes:
                kept.append(m)
                new_hashes.add(fp)

        if not kept:
            scanned_log.append({
                "date": meeting_dt.date().isoformat() if meeting_dt else "",
                "source": source,
                "title": title,
                "url": url,
                "status": "scanned",
                "reason": "only duplicates (already reported)"
            })
            continue

        results_for_email.append({
            "title": title,
            "url": url,
            "date": meeting_dt.date().isoformat() if meeting_dt else "",
            "mentions": kept
        })

        for m in kept:
            rows_for_csv.append([
                meeting_dt.date().isoformat() if meeting_dt else "",
                source,
                url,
                m.get("keyword") or "",
                (m.get("snippet") or "").strip()
            ])

        scanned_log.append({
            "date": meeting_dt.date().isoformat() if meeting_dt else "",
            "source": source,
            "title": title,
            "url": url,
            "status": "matched",
            "reason": f"{len(kept)} new mention(s)"
        })

    # Sort & write CSVs
    def sort_key(r: Dict) -> Tuple[datetime, str]:
        d = r.get("date")
        try:
            dt = dateparser.parse(d).date() if d else datetime(1970, 1, 1).date()
        except Exception:
            dt = datetime(1970, 1, 1).date()
        return (dt, r.get("title") or "")
    results_for_email.sort(key=sort_key, reverse=True)

    with open("report.csv", "w", encoding="utf-8", newline="") as cf:
        w = csv.writer(cf)
        w.writerow(["date", "source", "url", "keyword", "snippet"])
        for row in rows_for_csv:
            w.writerow(row)

    with open("scanned.csv", "w", encoding="utf-8", newline="") as sf:
        w = csv.writer(sf)
        w.writerow(["date", "source", "title", "url", "status", "reason"])
        status_rank = {"matched": 0, "scanned": 1, "skipped": 2, "error": 3}
        def s_key(x: Dict) -> Tuple[int, str, str]:
            return (status_rank.get(x.get("status", "skipped"), 9), x.get("date") or "", x.get("title") or "")
        for row in sorted(scanned_log, key=s_key):
            w.writerow([
                row.get("date") or "",
                row.get("source") or "",
                row.get("title") or "",
                row.get("url") or "",
                row.get("status") or "",
                row.get("reason") or ""
            ])

    # Build HTML
    from email_utils import render_html_report  # re-import to avoid linter complaints
    html_report = render_html_report(results_for_email)

    # Append audited "Documents scanned" section
    totals = {
        "matched": sum(1 for x in scanned_log if x.get("status") == "matched"),
        "scanned": sum(1 for x in scanned_log if x.get("status") == "scanned"),
        "skipped": sum(1 for x in scanned_log if x.get("status") == "skipped"),
        "error": sum(1 for x in scanned_log if x.get("status") == "error"),
        "total": len(scanned_log)
    }

    rows_html: List[str] = []
    MAX_EMAIL_ROWS = 200
    for i, r in enumerate(scanned_log):
        if i >= MAX_EMAIL_ROWS:
            rows_html.append("<li><em>…and " + str(len(scanned_log) - MAX_EMAIL_ROWS) + " more (see scanned.csv)</em></li>")
            break
        dt_html = (r.get("date") + " — ") if r.get("date") else ""
        url_html = html_escape(r.get("url") or "")
        title_html = html_escape(r.get("title") or "Document")
        reason_html = html_escape(r.get("reason") or "")
        rows_html.append(
            "<li><strong>" + html_escape(r.get("status") or "") + "</strong> — "
            + dt_html + title_html + " — "
            + "<a href=\"" + url_html + "\" target=\"_blank\" rel=\"noopener noreferrer\">" + url_html + "</a> "
            + "(<em>" + reason_html + "</em>)</li>"
        )

    scanned_section = (
        "<hr><details><summary><strong>Documents scanned</strong> — total "
        + str(totals["total"])
        + " (matched: " + str(totals["matched"])
        + ", scanned/no-hit: " + str(totals["scanned"])
        + ", skipped: " + str(totals["skipped"])
        + ", error: " + str(totals["error"]) + ")"
        + "</summary><p>Full audit log is attached as <code>scanned.csv</code> in workflow artifacts.</p>"
        + "<ol style=\"margin-top: 6px;\">" + "".join(rows_html) + "</ol></details>"
    )

    if "</body>" in html_report:
        html_report_full = html_report.replace("</body>", scanned_section + "\n</body>")
    else:
        html_report_full = html_report + scanned_section

    # >>> Highlight keywords across the entire HTML <<<
    html_report_full = highlight_keywords_in_html(html_report_full, KW_RE)

    with open("last_report.html", "w", encoding="utf-8") as f:
        f.write(html_report_full)

    # Update state
    if not IGNORE_DEDUPE and new_hashes:
        state["seen_hashes"] = sorted(set(state.get("seen_hashes") or []) | new_hashes)
    if is_backfill:
        state["backfill_done"] = True
    state["last_run_end"] = end.isoformat()
    save_state(state)

    # ------------------------------ Email ------------------------------
    # Force From to the authenticated mailbox (same as TEST email).
    to_addr = os.environ.get("REPORT_TO") or "robwaz@delrankids.net"
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT") or "587")
    smtp_user = os.environ.get("SMTP_USER") or os.environ.get("SMTP_USERNAME")
    smtp_password = os.environ.get("SMTP_PASS") or os.environ.get("SMTP_PASSWORD")

    forced_from = smtp_user or ""
    reply_to = os.environ.get("REPORT_FROM") or os.environ.get("MAIL_FROM") or None

    def _mask(s: str) -> str:
        if not s:
            return ""
        if "@" in s:
            name, _, domain = s.partition("@")
            return (name[:1] + "***@" + domain) if domain else "***"
        return s[:2] + "***"

    print("=== EMAIL PREP ===")
    print("Email config:", "to=", _mask(to_addr), "from=", _mask(forced_from),
          "reply_to=", _mask(reply_to or ""), "smtp=", smtp_host or "",
          "port=", smtp_port, "user=", _mask(smtp_user))

    can_send = all([to_addr, forced_from, smtp_host, smtp_port, smtp_user, smtp_password])
    if not can_send:
        raise RuntimeError("Email not sent: missing To/From/SMTP settings. "
                           "Set REPORT_TO and SMTP_* secrets. From is forced to SMTP user.")

    # Subject reflects YEAR/backfill/monthly modes
    if YEAR is not None:
        subject = f"Delran BOE – Preschool Mentions (Backfill {YEAR}-01-01 → {YEAR}-12-31)"
    elif is_backfill:
        subject = ("Delran BOE – Preschool Mentions (Backfill "
                   + str(datetime(2021, 1, 1).date()) + " → " + str(datetime.utcnow().date()) + ")")
    else:
        subject = ("Delran BOE – Preschool Mentions (" + start.date().isoformat()[:7] + ") Monthly Report")

    # Pre-build .eml (pre-send) for diagnostics
    try:
        from email_utils import _build_email_message
        msg = _build_email_message(
            subject=subject,
            html_body=html_report_full,
            to_addr=to_addr,
            from_addr=forced_from,
            reply_to=reply_to,
        )
        with open("to_send.eml", "wb") as pf:
            pf.write(msg.as_bytes())
        print("Saved to_send.eml (pre-send).")
    except Exception as _pre:
        print("!!! WARNING: Could not prebuild .eml:", _pre)

    # Send and save final eml
    from email_utils import send_email as _send_email
    eml_bytes = _send_email(
        subject=subject,
        html_body=html_report_full,
        to_addr=to_addr,
        from_addr=forced_from,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        reply_to=reply_to,
    )
    with open("sent_report.eml", "wb") as ef:
        ef.write(eml_bytes)

    print("=== EMAIL SENT === to " + to_addr
          + " | Matches: " + str(sum(len(r["mentions"]) for r in results_for_email))
          + " | Items: " + str(len(results_for_email))
          + " | Scanned total: " + str(len(scanned_log)))


# --------------------------- Entry point ----------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        tb = traceback.format_exc()
        with open("last_report.html", "w", encoding="utf-8") as f:
            f.write(
                "<html><body>"
                "<h2>Delran BOE – Monitor: Unhandled Error</h2>"
                "<pre style=\"white-space: pre-wrap; font-family: monospace;\">"
                + html_escape(tb) +
                "</pre>"
                "</body></html>"
            )
        print("Unhandled error; traceback written to last_report.html")
        raise
