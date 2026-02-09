#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Delran BOE monitor: scan district Meeting Minutes and BoardDocs public files
for mentions of: 'preschool', 'free preschool', 'preschool expansion'.

Features
- Sources:
  * District Meeting Minutes: https://www.delranschools.org/b_o_e/meeting_minutes
  * BOE index (fallback):   https://www.delranschools.org/b_o_e
  * BoardDocs public:       https://go.boarddocs.com/nj/delranschools/Board.nsf/Public
- Extracts text from PDF/HTML/DOCX
- For PDFs: per-page keyword hits and deep links (url#page=N)
- Labels items as agenda/minutes/packet/unknown based on URL/text
- Dedupe across runs using .data/seen.json (unless --ignore-dedupe)
- Attaches matched PDFs (up to 10, ≤ 8 MB each) and also writes report.csv
- Optional controls:
  * --ignore-dedupe
  * --min-year YYYY (drop items with parsed dates before this year)
  * --max-boarddocs-files N (cap how many BoardDocs PDFs to process; default 50)
"""
import os, re, csv, json, time, hashlib, logging, argparse
from datetime import datetime
from urllib.parse import urljoin, urlparse
from io import BytesIO

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from pdfminer.high_level import extract_pages, extract_text
from pdfminer.layout import LTTextContainer, LTTextBox, LTTextLine

BASE = 'https://www.delranschools.org'
MINUTES_URL = 'https://www.delranschools.org/b_o_e/meeting_minutes'
BOE_URL = 'https://www.delranschools.org/b_o_e'
BOARDDOCS_PUBLIC = 'https://go.boarddocs.com/nj/delranschools/Board.nsf/Public'

KEYWORDS = ['preschool','free preschool','preschool expansion']
HEADERS = { 'User-Agent': 'DelranMinutesBot/1.2 (+https://github.com/actions)' }

STATE_PATH = '.data/seen.json'
ATTACH_DIR = 'attachments'
MAX_ATTACHMENTS = 10
MAX_ATTACH_SIZE = 8 * 1024 * 1024  # 8 MB

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def fetch(url: str) -> requests.Response:
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return resp

def ensure_dirs():
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    os.makedirs(ATTACH_DIR, exist_ok=True)

def load_state() -> set:
    try:
        with open(STATE_PATH, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_state(seen: set):
    with open(STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(sorted(seen), f, indent=2)

def gather_district_links() -> list[dict]:
    links = []
    for page_url in [MINUTES_URL, BOE_URL]:
        try:
            resp = fetch(page_url)
        except Exception as e:
            logging.warning('Failed to load %s: %s', page_url, e)
            continue
        soup = BeautifulSoup(resp.text, 'lxml')
