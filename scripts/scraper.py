import os, json, time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime

from parser_utils import extract_text_from_pdf, extract_text_from_docx, find_preschool_mentions
from email_utils import send_email, render_html_report

BASE_URL = "https://www.delranschools.org/b_o_e/meeting_minutes"
STATE_FILE = "state.json"
HEADERS = {"User-Agent": "Delran-Preschool-Agent/1.0 (+mailto:alerts@example.com)"}

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"seen_urls": []}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def get_minutes_links():
    resp = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    links = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        url = urljoin(BASE_URL, href)
        title = a.get_text(strip=True) or url

        if any(url.lower().endswith(ext) for ext in [".pdf", ".docx", ".doc"]):
            links.append({"title": title, "url": url})
        elif "meeting" in url.lower() or "minute" in url.lower() or "board" in url.lower():
            # Explore subpages for documents
            try:
                links.extend(expand_subpage(url))
            except Exception:
                pass

    # Deduplicate while preserving order
    seen = set()
    uniq = []
    for it in links:
        if it["url"] not in seen:
            uniq.append(it)
            seen.add(it["url"])
    return uniq

def expand_subpage(url: str):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    out = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        u = urljoin(url, href)
        title = a.get_text(strip=True) or u
        if any(u.lower().endswith(ext) for ext in [".pdf", ".docx", ".doc"]):
            out.append({"title": title, "url": u})
    return out

def download(url: str) -> bytes:
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.content

def extract_text_from_bytes(url: str, content: bytes) -> str:
    lower = url.lower()
    if lower.endswith(".pdf"):
        return extract_text_from_pdf(content)
    if lower.endswith(".docx"):
        return extract_text_from_docx(content)
    return ""  # .doc best-effort skipped here

def main():
    state = load_state()
    seen = set(state.get("seen_urls", []))

    top_links = get_minutes_links()
    # Only process new docs
    new_docs = [d for d in top_links if d["url"] not in seen]

    results = []
    for d in tqdm(new_docs, desc="Scanning documents"):
        try:
            content = download(d["url"])
            text = extract_text_from_bytes(d["url"], content)
            if not text.strip():
                continue
            mentions = find_preschool_mentions(text)
            if mentions:
                results.append({
                    "title": d["title"],
                    "url": d["url"],
                    "mentions": mentions
                })
            time.sleep(2)  # politeness
        except Exception as e:
            print(f"Error processing {d['url']}: {e}")

    # Update state to include everything seen (so we don’t reprocess)
    all_urls = list(seen.union({d["url"] for d in top_links}))
    state["seen_urls"] = all_urls
    save_state(state)

    subject = f"Delran BOE – Preschool Mentions ({datetime.now().strftime('%B %Y')})"
    html = render_html_report(results)

    # SMTP config from env vars / repo secrets
    to_addr   = os.environ.get("REPORT_TO", "robwaz@delrankids.net")
    from_addr = os.environ.get("REPORT_FROM")          # e.g., notifications@yourdomain.com
    smtp_host = os.environ.get("SMTP_HOST")            # e.g., smtp.office365.com
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))# 587 for STARTTLS
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASS")

    if not all([from_addr, smtp_host, smtp_user, smtp_pass]):
        print("Missing SMTP config. Write HTML to last_report.html for debug.")
        with open("last_report.html", "w", encoding="utf-8") as f:
            f.write(html)
        return

    send_email(
        subject=subject,
        html_body=html,
        to_addr=to_addr,
        from_addr=from_addr,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_pass,
    )
    print("Report emailed.")

if __name__ == "__main__":
    main()
