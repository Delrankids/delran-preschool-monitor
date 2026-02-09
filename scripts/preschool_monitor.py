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
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = ' '.join((a.get_text() or '').split())
            full = urljoin(page_url, href)
            if 'DisplayFile.aspx' in href or any(href.lower().endswith(ext) for ext in ['.pdf', '.docx', '.doc', '.html', '.htm']):
                links.append({'url': full, 'text': text, 'source': 'district'})
    out, seen = [], set()
    for it in links:
        if it['url'] in seen: continue
        seen.add(it['url']); out.append(it)
    logging.info('District links: %d', len(out))
    return out

def gather_boarddocs_links(max_files: int = 50) -> list[dict]:
    candidates = []
    to_visit = [BOARDDOCS_PUBLIC]
    visited = set()

    while to_visit and len(to_visit) <= 8 and len(candidates) < max_files:
        url = to_visit.pop(0)
        if url in visited: continue
        visited.add(url)
        try:
            resp = fetch(url)
        except Exception as e:
            logging.warning('BoardDocs fetch failed %s: %s', url, e)
            continue
        soup = BeautifulSoup(resp.text, 'lxml')
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = ' '.join((a.get_text() or '').split())
            full = urljoin(url, href)
            if '/files/' in href and href.lower().endswith('.pdf'):
                candidates.append({'url': full, 'text': text or 'BoardDocs File', 'source': 'boarddocs'})
                if len(candidates) >= max_files: break
            if 'Board.nsf' in full and full.startswith('https://go.boarddocs.com') and full not in visited and len(to_visit) < 8:
                to_visit.append(full)

    out, seen = [], set()
    for it in candidates:
        if it['url'] in seen: continue
        seen.add(it['url']); out.append(it)
    logging.info('BoardDocs links: %d', len(out))
    return out

def guess_date_from_text(text: str):
    pats = [
        r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b',
        r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',
        r'\b\d{4}-\d{2}-\d{2}\b',
    ]
    for pat in pats:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            try: return dateparser.parse(m.group(0), dayfirst=False, fuzzy=True)
            except: pass
    return None

def pdf_per_page_hits(pdf_bytes: bytes, keywords: list[str]):
    results = []
    try:
        for page_num, layout in enumerate(extract_pages(BytesIO(pdf_bytes)), start=1):
            page_text_parts = []
            for element in layout:
                if isinstance(element, (LTTextContainer, LTTextBox, LTTextLine)):
                    page_text_parts.append(element.get_text())
            page_text = ' '.join(' '.join(page_text_parts).split())
            page_lower = page_text.lower()
            for kw in keywords:
                k = kw.lower(); start = 0
                while True:
                    idx = page_lower.find(k, start)
                    if idx == -1: break
                    left = max(0, idx-100); right = min(len(page_text), idx+len(k)+100)
                    snippet = page_text[left:right]
                    results.append({'page': page_num, 'keyword': kw, 'snippet': snippet})
                    start = idx + len(k)
    except Exception as e:
        logging.warning('PDF scan failed: %s', e)
    return results

def text_hits(text: str, keywords: list[str]):
    res = []; lower = text.lower()
    for kw in keywords:
        k = kw.lower(); start = 0
        while True:
            idx = lower.find(k, start)
            if idx == -1: break
            left = max(0, idx-80); right = min(len(text), idx+len(kw)+80)
            snippet = text[left:right].replace('\n',' ')
            res.append({'keyword': kw, 'snippet': snippet, 'page': None})
            start = idx + len(k)
    return res

def fetch_and_extract(item: dict):
    url = item['url']
    try:
        resp = fetch(url)
    except Exception as e:
        logging.warning('Fetch failed %s: %s', url, e)
        return None

    ctype = resp.headers.get('Content-Type','').lower()
    text=''; hits=[]; meeting_date=None; attachments=[]

    if 'application/pdf' in ctype or url.lower().endswith('.pdf'):
        pdf_bytes = resp.content
        hits = pdf_per_page_hits(pdf_bytes, KEYWORDS)
        try: text = extract_text(BytesIO(pdf_bytes)) or ''
        except: text=''
        meeting_date = guess_date_from_text(text)
        if hits:
            fname = os.path.basename(urlparse(url).path) or f'file-{int(time.time())}.pdf'
            out_path = os.path.join(ATTACH_DIR, fname)
            base,ext = os.path.splitext(out_path); i=1
            while os.path.exists(out_path):
                out_path=f"{base}-{i}{ext}"; i+=1
            if len(pdf_bytes) <= MAX_ATTACH_SIZE:
                with open(out_path,'wb') as f: f.write(pdf_bytes)
                attachments.append(out_path)
        deep_links = [f"{url}#page={h['page']}" for h in hits if h.get('page')]
    else:
        if 'text/html' in ctype or url.lower().endswith(('.htm','.html')):
            soup = BeautifulSoup(resp.text,'lxml')
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.lower().endswith('.pdf') or 'DisplayFile.aspx' in href or '/files/' in href:
                    return fetch_and_extract({'url': urljoin(url,href),'text':item.get('text'),'source':item.get('source')})
            text=' '.join(soup.stripped_strings)
            hits=text_hits(text,KEYWORDS); meeting_date=guess_date_from_text(text)
            deep_links=[url]
        elif url.lower().endswith('.docx'):
            try:
                import zipfile
                zf=zipfile.ZipFile(BytesIO(resp.content))
                xml=zf.read('word/document.xml').decode('utf-8','ignore')
                soup=BeautifulSoup(xml,'xml')
                text='\n'.join([t.get_text(strip=True) for t in soup.find_all(['w:t'])])
            except: text=''
            hits=text_hits(text,KEYWORDS); meeting_date=guess_date_from_text(text); deep_links=[url]
        else:
            text=''; hits=[]; meeting_date=None; deep_links=[url]

    uniq=[]; seen=set()
    for h in hits:
        sig=(h.get('page'),h['keyword'],h['snippet'])
        if sig in seen: continue
        seen.add(sig); uniq.append(h)

    title_l=((item.get('text') or '')+' '+url).lower()
    kind='unknown'
    if 'agenda' in title_l: kind='agenda'
    elif 'minute' in title_l or 'minutes' in title_l: kind='minutes'
    elif 'packet' in title_l: kind='packet'

    return {
        'url': url,
        'title': (item.get('text') or '').strip() or 'Meeting Item',
        'source': item.get('source') or 'unknown',
        'date': meeting_date.isoformat() if meeting_date else None,
        'matches': uniq,
        'deep_links': deep_links,
        'attachments': attachments,
        'kind': kind,
    }

def within_range(iso_date,start,end):
    if not iso_date: return True
    dt=dateparser.parse(iso_date).replace(tzinfo=None)
    return start<=dt<=end

def fingerprint(entry_url,match):
    h=hashlib.sha1()
    h.update(entry_url.encode())
    h.update((match.get('keyword') or '').encode())
    h.update((match.get('snippet') or '')[:120].encode())
    h.update(str(match.get('page') or '').encode())
    return h.hexdigest()

def build_reports(items,start,end,seen):
    attachment_paths=[]; new_seen=set(seen)
    per=[]

    for e in items:
        if not within_range(e.get('date'),start,end): continue
        for m in e.get('matches',[]):
            fp=fingerprint(e['url'],m)
            if fp in new_seen: continue
            per.append((e,m,fp))

    def sort_key(t):
        e,m,fp=t
        d=e.get('date')
        dt=dateparser.parse(d).replace(tzinfo=None) if d else datetime(1970,1,1)
        return(dt,e.get('source'))

    per.sort(key=sort_key,reverse=True)

    md=[f"# Delran BOE Preschool Mentions — New Findings",f"**Date range:** {start.date()} to {end.date()}\n"]
    html=['<html><body>',f'<h2>Delran BOE Preschool Mentions — New Findings</h2>',f'<p><strong>Date range:</strong> {start.date()} to {end.date()}</p>']

    if not per:
        md.append("No new findings in this period.")
        html.append("<p>No new findings in this period.</p>")
    else:
        current=None
        for e,m,fp in per:
            date_str=dateparser.parse(e['date']).date().isoformat() if e.get('date') else 'Unknown date'
            if e['url']!=current:
                if current: md.append('')
                md.append(f"## {date_str} — {e['title']} ({e['kind']}, {e['source']})")
                md.append(f"Source: {e['url']}")
                if e.get('deep_links'):
                    md.append("Deep links:")
                    for dl in sorted(set(e['deep_links'])): md.append(f"- {dl}")
                html.append(f"<h3>{date_str} — {e['title']} ({e['kind']}, {e['source']})</h3>")
                html.append(f"<p>Source: {e['url']}</p>")
                if e.get('deep_links'):
                    html.append("<p>Deep links:</p><ul>")
                    for dl in sorted(set(e['deep_links'])): html.append(f"<li>{dl}</li>")
                    html.append("</ul>")
                current=e['url']
            snip=(m.get('snippet') or '').strip()
            if m.get('page'):
                md.append(f"- **{m['keyword']}** (page {m['page']}) … {snip} …")
                html.append(f"<p><strong>{m['keyword']}</strong> (page {m['page']}) … {snip} …</p>")
            else:
                md.append(f"- **{m['keyword']}** … {snip} …")
                html.append(f"<p><strong>{m['keyword']}</strong> … {snip} …</p>")
            new_seen.add(fp)

        for e,m,fp in per:
            for ap in e.get('attachments',[]):
                if len(attachment_paths)>=MAX_ATTACHMENTS: break
                if os.path.exists(ap) and os.path.getsize(ap)<=MAX_ATTACH_SIZE and ap not in attachment_paths:
                    attachment_paths.append(ap)

    md_text='\n'.join(md)+'\n'
    html.append("</body></html>")
    html_text='\n'.join(html)

    new_items={}
    for e,m,fp in per:
        new_items.setdefault(e['url'],{'entry':e,'matches':[]})['matches'].append(m)

    return new_items,md_text,html_text,new_seen,attachment_paths

def main():
    ensure_dirs()
    ap=argparse.ArgumentParser()
    ap.add_argument('--start',required=True)
    ap.add_argument('--end',required=True)
    ap.add_argument('--out-md',default='report.md')
    ap.add_argument('--out-html',default='report.html')
    ap.add_argument('--out-csv',default='report.csv')
    ap.add_argument('--ignore-dedupe',action='store_true')
    ap.add_argument('--min-year',type=int,default=None)
    ap.add_argument('--max-boarddocs-files',type=int,default=50)
    args=ap.parse_args()

    start=dateparser.parse(args.start).replace(tzinfo=None)
    end=dateparser.parse(args.end).replace(tzinfo=None)

    seen=set() if args.ignore_dedupe else load_state()

    district=gather_district_links()
    boarddocs=gather_boarddocs_links(max_files=args.max_boarddocs_files)

    items=[]
    for item in district+boarddocs:
        res=fetch_and_extract(item)
        if res and res.get('matches'):
            items.append(res)

    if args.min_year is not None:
        fl=[]
        for e in items:
            ok=True
            if e.get('date'):
                try:
                    if dateparser.parse(e['date']).year < args.min_year: ok=False
                except: ok=True
            if ok: fl.append(e)
        items=fl

    new_items,md,html,new_seen,attachment_paths=build_reports(items,start,end,seen)

    with open(args.out_md,'w',encoding='utf-8') as f: f.write(md)
    with open(args.out_html,'w',encoding='utf-8') as f: f.write(html)

    total=0
    with open(args.out_csv,'w',encoding='utf-8',newline='') as cf:
        w=csv.writer(cf)
        w.writerow(['date','kind','source','url','page','keyword','snippet'])
        for url,blob in new_items.items():
            e=blob['entry']
            for m in blob['matches']:
                d=e.get('date')
                fmt=dateparser.parse(d).date().isoformat() if d else ''
                w.writerow([fmt,e.get('kind') or '',e.get('source') or '',e.get('url') or '',m.get('page') or '',m.get('keyword') or '',(m.get('snippet') or '').strip()])
                total+=1

    if not args.ignore_dedupe:
        save_state(new_seen)

    attachments=attachment_paths[:]
    if os.path.exists(args.out_csv):
        attachments.append(args.out_csv)
    with open('.data/attachments.json','w',encoding='utf-8') as f:
        json.dump(attachments,f)

    print(f"Report written with {sum(len(v['matches']) for v in new_items.values())} new matches across {len(new_items)} items; CSV rows={total}.")

if __name__=='__main__':
    main()
