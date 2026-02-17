# ==================== REPLACE THE ENTIRE collect_links_from_html FUNCTION ====================

def collect_links_from_html(page_url: str, html_text: str) -> List[Dict[str, str]]:
    """
    Collect document links from HTML page, including special handling for Delran's minutes table.
    """
    soup = BeautifulSoup(html_text, "lxml")
    items: List[Dict[str, str]] = []
    seen: Set[str] = set()

    # === YOUR ORIGINAL LINK COLLECTION CODE SHOULD STAY HERE ===
    # (the for a in soup.find_all("a", href=True): loop, BoardDocs JSON handling, etc.)
    # Do NOT delete it — add the new block AFTER it

    # Special handling for Delran minutes table (SharpSchool style)
    for row in soup.find_all('tr'):
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 2:
            link_tag = cells[0].find('a', href=True)
            if link_tag:
                href = link_tag.get('href', '')
                full_url = urljoin(page_url, href)
                title = link_tag.get_text(strip=True)
                # Match common Delran file patterns
                if any(ext in full_url.lower() for ext in ['.pdf', '.doc', '.docx', 'getfile.ashx', 'displayfile.aspx']):
                    if full_url not in seen:
                        seen.add(full_url)
                        items.append({
                            "title": title or "Meeting Minutes",
                            "url": full_url,
                            "source": "district"
                        })
                        logging.info(f"Found Delran minutes link: {full_url} ({title})")

    # === YOUR EXISTING CODE CONTINUES HERE ===
    # (Embedded JSON handling for BoardDocs scripts, etc.)

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

            # Better pagination detection for SharpSchool-style pages
            # Look for links with text like "next", ">", "Page 2", or query params like ?page=2
            pagination_patterns = re.compile(r'(next|>|»|more|\.{3}|page\s*\d+|pg=|p=)', re.IGNORECASE)
            next_links = soup.find_all('a', string=pagination_patterns) + \
                         soup.find_all('a', href=re.compile(r'(page|pg|p)=', re.IGNORECASE))

            for a in next_links:
                h = a.get('href') or ''
                nxt = urljoin(url, h)
                if nxt not in visited and is_allowed_domain(nxt, allowed_domains) and nxt != url:
                    queue.append((nxt, depth + 1))
                    logging.info(f"Queued pagination link: {nxt}")

            # Also follow any promising internal links (minutes, agendas, BOE-related)
            for a in soup.find_all("a", href=True):
                h = a.get("href") or ""
                nxt = urljoin(url, h)
                if (nxt not in visited and
                    is_allowed_domain(nxt, allowed_domains) and
                    any(kw in nxt.lower() for kw in ['minutes', 'boe', 'board', 'meeting', 'agenda', 'getfile', 'displayfile'])):
                    queue.append((nxt, depth + 1))
                    logging.info(f"Queued related link: {nxt}")
