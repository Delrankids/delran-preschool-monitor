import os
import json
import time
import logging
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime

from parser_utils import extract_text_from_pdf, extract_text_from_docx, find_preschool_mentions
from email_utils import send_email, render_html_report

BASE_URL = "https://www.delranschools.org/b_o_e/meeting_minutes"
STATE_FILE = "state.json"

HEADERS = {
    "User-Agent": "Delran-Preschool-Agent/1.0 (+mailto:alerts@example.com)"
}

DOC_DELAY_SECONDS = 2.0
REQUEST_TIMEOUT = 60


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except:
            return {"seen_urls": []}
    return {"seen_urls": []}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def fetch_url(url, binary=False):
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.content if binary else resp.text
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))


def get_minutes_links():
    html = fetch_url(BASE_URL, binary=False)
    soup = BeautifulSoup(html, "lxml")

    links = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        url = urljoin(BASE_URL, href)
        title = a.get_text(strip=True) or url

        if any(url.lower().endswith(ext) for ext in [".pdf", ".docx", ".doc"]):
            links.append({"title": title, "url": url})
        elif any(word in url.lower() for word in ["meeting", "minute", "board"]):
            try:
                links.extend(expand_subpage(url))
            except Exception as e:
                logging.warning(f"Subpage failed: {url} - {e}")

    # Deduplicate
    seen = set()
    unique = []
    for x in links:
        if x["url"] not in seen:
            unique.append(x)
            seen.add(x["url"])
    return unique


def expand_subpage(url):
    html = fetch_url(url, binary=False)
    soup = BeautifulSoup(html, "lxml")
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


def download(url):
    return fetch_url(url, binary=True)


def extract_text(url, data):
    lower = url.lower()
    if lower.endswith(".pdf"):
        return extract_text_from_pdf(data)
    if lower.endswith(".docx"):
        return extract_text_from_docx(data)
    return ""


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    state = load_state()
    # Touch file early so workflow always finds it
    save_state(state)

    already_seen = set(state.get("seen_urls", []))

    links = get_minutes_links()
    new_docs = [x for x in links if x["url"] not in already_seen]

    results = []

    for d in tqdm(new_docs, desc="Scanning docs"):
        try:
            data = download(d["url"])
            text = extract_text(d["url"], data)
            if not text.strip():
                time.sleep(DOC_DELAY_SECONDS)
                continue

            mentions = find_preschool_mentions(text)
            if mentions:
                results.append({
                    "title": d["title"],
                    "url": d["url"],
                    "mentions": mentions
                })

            time.sleep(DOC_DELAY_SECONDS)

        except Exception as e:
            logging.error(f"Error processing {d['url']}: {e}")

    # Update state
    all_seen = sorted(set(already_seen).union({x["url"] for x in links}))
    state["seen_urls"] = all_seen
    save_state(state)

    subject = f"Delran BOE – Preschool Mentions ({datetime.now().strftime('%B %Y')})"
    html = render_html_report(results)

    to_addr = os.getenv("REPORT_TO", "robwaz@delrankids.net")

    from_addr = os.getenv("REPORT_FROM") or os.getenv("MAIL_FROM")
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT") or "587")
    smtp_user = os.getenv("SMTP_USER") or os.getenv("SMTP_USERNAME")
    smtp_pass = os.getenv("SMTP_PASS") or os.getenv("SMTP_PASSWORD")

    if not all([from_addr, smtp_host, smtp_user, smtp_pass]):
        logging.warning("Missing SMTP config. Saving last_report.html")
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

    logging.info("Report emailed successfully.")


if __name__ == "__main__":
    main()
