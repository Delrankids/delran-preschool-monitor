"""
Delran BOE Preschool Monitor – Scraper

What it does
------------
1) Crawls the Delran BOE meeting minutes page (and subpages) for PDF/DOCX links.
2) Extracts text and finds preschool-related mentions (via parser_utils.py).
3) Builds an HTML report and emails it (via email_utils.py).
4) Persists 'seen' URLs in state.json so future runs only process new docs.

Configuration
-------------
SMTP and reporting are provided via environment variables (with fallbacks):
- REPORT_TO                -> recipient (default: robwaz@delrankids.net)
- REPORT_FROM or MAIL_FROM -> sender
- SMTP_HOST
- SMTP_PORT                -> 587 (STARTTLS) or 465 (implicit SSL)
- SMTP_USER or SMTP_USERNAME
- SMTP_PASS or SMTP_PASSWORD

Usage
-----
Run directly (e.g., GitHub Actions step):
    python scripts/scraper.py
"""

import os
import json
import time
import logging
from typing import List, Dict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime

from parser_utils import (
    extract_text_from_pdf,
    extract_text_from_docx,
    find_preschool_mentions,
)
from email_utils import send_email, render_html_report

# ---- Settings ---------------------------------------------------------------

BASE_URL = os.environ.get(
    "DELRAN_MINUTES_URL",
    "https://www.delranschools.org/b_o_e/meeting_minutes"
)

STATE_FILE = os.environ.get("STATE_FILE", "state.json")

# Identify ourselves politely to the district site
HEADERS = {
    "User-Agent": "Delran-Preschool-Agent/1.0 (+mailto:alerts@example.com)"
}

# How long to pause between document downloads (be polite)
DOC_DELAY_SECONDS = float(os.environ.get("DOC_DELAY_SECONDS", "2.0"))

# Requests timeouts
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))

# ----------------------------------------------------------------------------


def load_state() -> Dict:
    """Load crawler state."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            # Corrupt or unreadable state; start fresh
            return {"seen_urls": []}
    return {"seen_urls": []}


def save_state(state: Dict) -> None:
    """Persist crawler state."""
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def fetch_url(url: str, *, binary: bool = False) -> bytes | str:
    """
    Fetch a URL with a simple retry (3 attempts).
    Returns bytes if binary=True, else text.
    """
    attempts = 3
    for i in range(1, attempts + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.content if binary else resp.text
        except Exception as e:
            if i == attempts:
                raise
            time.sleep(1.5 * i)  # basic backoff


def get_minutes_links() -> List[Dict[str, str]]:
    """
    Crawl the main minutes page, and expand subpages to collect PDF/DOCX links.
    Returns: list of dicts {title, url}
    """
    html = fetch_url(BASE_URL, binary=False)
    soup = BeautifulSoup(html, "lxml")

    links: List[Dict[str, str]] = []

    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        url = urljoin(BASE_URL, href)
        title = a.get_text(strip=True) or url


