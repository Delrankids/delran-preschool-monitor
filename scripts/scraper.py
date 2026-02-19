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

# Import utils
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

def ensure_debug_dir() -> None:
    os.makedirs(".debug", exist_ok=True)

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
                stealth(page)
                page.set_extra_http_headers(HEADERS)
                if referer:
                    page.set_extra_http_headers({"Referer": referer})
                response = page.goto(url, timeout=90000, wait_until="networkidle")
                if response is None:
                    logging.warning("No response from goto")
                else:
                    logging.info(f"Playwright response status: {response.status}")

                # Attempt to close alert pop-up
                try:
                    page.wait_for_timeout(5000)
                    page.click('button[aria-label="close"], button.close, [class*="close"], [id*="close"], [title="Close"], .alert-dismissible button', timeout=10000)
                    logging.info("Attempted to close alert pop-up")
                except Exception as e:
                    logging.info(f"No pop-up close button found or failed to click: {e}")

                page.wait_for_timeout(8000)
                html = page.content()
                browser.close()

                logging.info(f"Stealth Playwright fetch success: {len(html)} bytes")

                # Debug what was fetched
                logging.info(f"Contains 'GetFile.ashx': {'getfile.ashx' in html.lower()}")
                logging.info(f"Contains 'Minutes': {'minutes' in html.lower()}")
                logging.info(f"Contains 'Cloudflare' or 'checking your browser': {'cloudflare' in html.lower() or 'checking your browser' in html.lower()}")
                cleaned = html[:300].replace("\n", " ").replace("\r", " ")
                logging.info(f"First 300 chars of HTML (cleaned): {cleaned}")
                soup = BeautifulSoup(html, "lxml")
                logging.info(f"Page title: {soup.title.string if soup.title else 'No title'}")

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

        # Broad match for Delran minutes / file handlers
        if 'getfile.ashx' in lower_full or 'displayfile.aspx' in lower_full or \
           any(word in lower_title for word in ['minutes', 'agenda', 'boe', 'board', 'reorganization', 're-organ', 'session', 'meeting', 'work session']):
            if full not in seen:
                seen.add(full)
                items.append({
                    "title": title or "Delran Meeting Document",
                    "url": full,
                    "source": "district"
                })
                logging.info(f"FOUND DELRAN DOCUMENT: {full} ({title})")

    # BoardDocs JSON in scripts
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

def crawl_district(start_urls: Iterable[str], allowed_domains: Set[str],
                   max_pages: int, max_depth: int) -> List[Dict[str, str]]:
    queue: List[Tuple[str, int]] = [(u, 0) for u in start_urls]
    visited: Set[str] = set()
    results: List[Dict[str, str]] = []

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

        results.extend(collect_links_from_html(url, resp.text))

        if depth < max_depth:
            soup = BeautifulSoup(resp.text, "lxml")

            pagination_patterns = re.compile(r'(next|>|»|more|\.{3}|page\s*\d+|pg=|p=)', re.IGNORECASE)
            next_links = (
                soup.find_all('a', string=pagination_patterns) +
                soup.find_all('a', href=re.compile(r'(page|pg|p)=', re.IGNORECASE))
            )

            for a in next_links:
                h = a.get('href') or ''
                nxt = urljoin(url, h)
                if nxt not in visited and is_allowed_domain(nxt, allowed_domains) and nxt != url:
                    queue.append((nxt, depth + 1))
                    logging.info(f"Queued pagination link: {nxt}")

            for a in soup.find_all("a", href=True):
                h = a.get("href") or ""
                nxt = urljoin(url, h)
                if (nxt not in visited and
                    is_allowed_domain(nxt, allowed_domains) and
                    any(kw in nxt.lower() for kw in ['minutes', 'boe', 'board', 'meeting', 'agenda', 'getfile', 'displayfile'])):
                    queue.append((nxt, depth + 1))
                    logging.info(f"Queued related minutes link: {nxt}")

    out, seen = [], set()
    for it in results:
        if it["url"] not in seen:
            seen.add(it["url"])
            out.append(it)
    logging.info("District links discovered: %d (pages crawled=%d)", len(out), len(visited))
    return out

def crawl_boarddocs(root_url: str, max_files: int) -> List[Dict[str, str]]:
    if max_files <= 0:
        return []

    queue: List[str] = [root_url]
    visited: Set[str] = set()
    items: List[Dict[str, str]] = []
    page_budget = 30

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

        new_links = collect_links_from_html(url, html)
        for it in new_links:
            if it.get("source") == "boarddocs":
                items.append(it)
                if len(items) >= max_files:
                    break
        if len(items) >= max_files:
            break

        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            h = a.get("href") or ""
            nxt = urljoin(url, h)
            if (nxt.startswith("https://go.boarddocs.com/")
                    and nxt not in visited
                    and len(queue) < 20):
                queue.append(nxt)

        for m in BOARD_DOCS_FILE_RE.finditer(html):
            f_url = urljoin(url, m.group(0))
            if all(x["url"] != f_url for x in items):
                items.append({"title": "BoardDocs Attachment", "url": f_url, "source": "boarddocs"})
                if len(items) >= max_files:
                    break

    out, seen = [], set()
    for it in items:
        if it["url"] not in seen:
            seen.add(it["url"])
            out.append(it)
    logging.info("BoardDocs links discovered: %d (pages visited=%d)", len(out), len(visited))
    return out

def get_minutes_links() -> List[Dict[str, str]]:
    start_urls = [BASE_URL, BOE_URL]
    district_links = crawl_district(start_urls, ALLOWED_DISTRICT_DOMAINS, MAX_DISTRICT_PAGES, MAX_CRAWL_DEPTH)
    boarddocs_links = crawl_boarddocs(BOARDDOCS_PUBLIC, MAX_BOARDDOCS_FILES)
    all_links = district_links + boarddocs_links
    if YEAR:
        all_links = [link for link in all_links if str(YEAR) in link["url"] or str(YEAR) in link["title"]]
    logging.info(f"Total minutes links discovered: {len(all_links)}")
    return all_links

# ---------------------------- State Management ------------------------------

def load_state() -> Dict:
    if FORCE_FULL_RESCAN or not os.path.exists(STATE_FILE):
        return {"seen_hashes": [], "seen_urls": [], "backfill_done": False, "last_run_end": None}
    with open(STATE_FILE, 'r') as f:
        return json.load(f)

def save_state(state: Dict) -> None:
    state["last_run_end"] = datetime.utcnow().isoformat()
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ---------------------------- Processing ------------------------------

def process_document(link: Dict[str, str], state: Dict) -> Optional[Dict]:
    url = link["url"]
    title = link["title"]

    hash_key = sha1_of(url, title)
    if not IGNORE_DEDUPE and hash_key in state["seen_hashes"] and not FORCE_FULL_RESCAN:
        logging.info("Skipping seen: %s", url)
        return None

    polite_delay()
    try:
        resp = fetch(url)
    except Exception as e:
        logging.warning("Doc fetch failed %s: %s", url, e)
        return None

    content = resp.content
    ext = url.lower().split('.')[-1] if '.' in url else ""

    if ext == "pdf":
        text = extract_text_from_pdf(content)
    elif ext in ("docx", "doc"):
        text = extract_text_from_docx(content)
    elif ext in ("htm", "html") or 'getfile.ashx' in url.lower() or 'displayfile' in url.lower():
        soup = BeautifulSoup(content, "lxml")
        text = soup.get_text(separator="\n", strip=True)
    else:
        logging.warning("Unsupported format: %s", url)
        return None

    mentions = find_preschool_mentions(text)
    if not mentions:
        return None

    date_dt = guess_meeting_date(text, title=title, url=url)
    date_str = date_dt.strftime("%Y-%m-%d") if date_dt else ""

    if MIN_YEAR and date_dt and date_dt.year < MIN_YEAR:
        return None

    result = {
        "url": url,
        "title": title,
        "date": date_str,
        "mentions": mentions
    }

    state["seen_hashes"].append(hash_key)
    state["seen_urls"].append(url)
    return result

# ---------------------------- Reporting ------------------------------

def write_report_csv(results: List[Dict]) -> None:
    with open("report.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "title", "date", "keyword", "snippet"])
        writer.writeheader()
        for r in results:
            for m in r.get("mentions", []):
                writer.writerow({
                    "url": r["url"],
                    "title": r["title"],
                    "date": r["date"],
                    "keyword": m["keyword"],
                    "snippet": m["snippet"]
                })

def write_scanned_csv(links: List[Dict[str, str]]) -> None:
    with open("scanned.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "title", "source"])
        writer.writeheader()
        for link in links:
            writer.writerow(link)

# ---------------------------- Main ------------------------------

def main():
    state = load_state()

    links = get_minutes_links()
    write_scanned_csv(links)

    results: List[Dict] = []
    for link in links:
        res = process_document(link, state)
        if res:
            results.append(res)

    if results:
        html_body = render_html_report(results)
        with open("last_report.html", "w", encoding="utf-8") as f:
            f.write(html_body)

        write_report_csv(results)

        to_addr = os.environ.get("REPORT_TO")
        from_addr = os.environ.get("MAIL_FROM") or os.environ.get("REPORT_FROM")
        reply_to = os.environ.get("REPORT_FROM")
        smtp_host = os.environ.get("SMTP_HOST")
        smtp_port = int(os.environ.get("SMTP_PORT") or 587)
        smtp_user = os.environ.get("SMTP_USERNAME") or os.environ.get("SMTP_USER")
        smtp_pass = os.environ.get("SMTP_PASSWORD") or os.environ.get("SMTP_PASS")

        if all([to_addr, from_addr, smtp_host, smtp_user, smtp_pass]):
            try:
                eml_bytes = send_email(
                    subject="Delran BOE Preschool Report",
                    html_body=html_body,
                    to_addr=to_addr,
                    from_addr=from_addr,
                    smtp_host=smtp_host,
                    smtp_port=smtp_port,
                    smtp_user=smtp_user,
                    smtp_password=smtp_pass,
                    reply_to=reply_to
                )
                with open("sent_report.eml", "wb") as f:
                    f.write(eml_bytes)
            except Exception as e:
                logging.error("Email send failed: %s", e)
                from email.message import EmailMessage
                msg = EmailMessage()
                msg['Subject'] = "Delran BOE Preschool Report"
                msg.set_content("No body provided.")
                msg.add_alternative(html_body, subtype='html')
                with open("to_send.eml", "wb") as f:
                    f.write(msg.as_bytes())
        else:
            logging.warning("Missing email env vars; skipping send.")

    save_state(state)

if __name__ == "__main__":
    main()
