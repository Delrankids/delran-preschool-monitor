# ==================== REPLACE THE ENTIRE collect_links_from_html FUNCTION ====================

def collect_links_from_html(page_url: str, html_text: str) -> List[Dict[str, str]]:
    """
    Collect document links from HTML page, including special handling for Delran's minutes list.
    """
    soup = BeautifulSoup(html_text, "lxml")
    items: List[Dict[str, str]] = []
    seen: Set[str] = set()

    # General link collection (BoardDocs patterns, DisplayFile, extensions)
    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        full = urljoin(page_url, href)
        title = a.get_text(strip=True) or full

        if BOARD_DOCS_FILE_RE.search(full):
            if full not in seen:
                seen.add(full)
                items.append({"title": title or "BoardDocs Attachment", "url": full, "source": "boarddocs"})
            continue

        if ("DisplayFile.aspx" in full) or full.lower().endswith(DOC_EXTS):
            if full not in seen:
                seen.add(full)
                src = "district"
                if "cdnsm" in domain_of(full) or "sharpschool" in domain_of(full):
                    src = "district-cdn"
                items.append({"title": title, "url": full, "source": src})

    # Embedded BoardDocs JSON in <script> tags
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

    # Delran-specific: SharpSchool minutes list (ul.file-list or general table rows)
    # First try modern selector (ul.file-list)
    for li in soup.select('ul.file-list li, .file-list li'):
        link_tag = li.find('a', href=True)
        if link_tag:
            href = link_tag.get('href', '')
            full_url = urljoin(page_url, href)
            title = link_tag.get_text(strip=True)
            if any(ext in full_url.lower() for ext in ['.pdf', '.doc', '.docx', 'getfile.ashx', 'displayfile.aspx']):
                if full_url not in seen:
                    seen.add(full_url)
                    items.append({
                        "title": title or "Meeting Minutes",
                        "url": full_url,
                        "source": "district"
                    })
                    logging.info(f"Found Delran minutes link (file-list): {full_url} ({title})")

    # Fallback: scan table rows if the site uses <tr> instead
    for row in soup.find_all('tr'):
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 2:
            link_tag = cells[0].find('a', href=True)
            if link_tag:
                href = link_tag.get('href', '')
                full_url = urljoin(page_url, href)
                title = link_tag.get_text(strip=True)
                if any(ext in full_url.lower() for ext in ['.pdf', '.doc', '.docx', 'getfile.ashx', 'displayfile.aspx']):
                    if full_url not in seen:
                        seen.add(full_url)
                        items.append({
                            "title": title or "Meeting Minutes",
                            "url": full_url,
                            "source": "district"
                        })
                        logging.info(f"Found Delran minutes link (table row): {full_url} ({title})")

    return items


# ==================== REPLACE THE PAGINATION/FOLLOW BLOCK INSIDE crawl_district ====================

# Inside the crawl_district function, find this block:
# if depth < max_depth:
#     soup = BeautifulSoup(resp.text, "lxml")
#     for a in soup.find_all("a", href=True):
#         ... your original follow logic ...

# REPLACE the entire if depth < max_depth: block with this improved version:

        if depth < max_depth:
            soup = BeautifulSoup(resp.text, "lxml")

            # Pagination detection: look for "next", ">", "Page 2", ?page= etc.
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

            # Follow any promising internal links (minutes, BOE, agendas, file handlers)
            for a in soup.find_all("a", href=True):
                h = a.get("href") or ""
                nxt = urljoin(url, h)
                if (nxt not in visited and
                    is_allowed_domain(nxt, allowed_domains) and
                    any(kw in nxt.lower() for kw in ['minutes', 'boe', 'board', 'meeting', 'agenda', 'getfile', 'displayfile'])):
                    queue.append((nxt, depth + 1))
                    logging.info(f"Queued related minutes link: {nxt}")

