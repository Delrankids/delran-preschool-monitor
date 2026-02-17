# In collect_links_from_html(page_url: str, html_text: str) -> List[Dict[str, str]]:
# ... existing code ...

    # Special handling for Delran minutes table (SharpSchool style)
    for row in soup.find_all('tr'):  # Look for table rows
        cells = row.find_all('td')
        if len(cells) >= 2:  # File Name + Size columns
            link_tag = cells[0].find('a', href=True)
            if link_tag:
                href = link_tag['href']
                full_url = urljoin(page_url, href)
                title = link_tag.get_text(strip=True)
                if full_url.lower().endswith(('.pdf', '.doc', '.docx')) or 'DisplayFile.aspx' in full_url or 'GetFile.ashx' in full_url:
                    items.append({"title": title or "Meeting Minutes", "url": full_url, "source": "district"})

# In crawl_district(...):
# ... existing queue/visited ...

        # Better pagination/follow logic
        if depth < max_depth:
            soup = BeautifulSoup(resp.text, "lxml")
            # Find "next" pagination links (common SharpSchool patterns)
            next_links = soup.find_all('a', text=re.compile(r'(next|>|»|\.{3}|more|page\s*\d+)', re.I))
            for a in next_links + soup.find_all('a', href=re.compile(r'(page|pg|p)=', re.I)):
                h = a.get('href') or ''
                nxt = urljoin(url, h)
                if nxt not in visited and is_allowed_domain(nxt, allowed_domains):
                    queue.append((nxt, depth + 1))
                    logging.info(f"Queued pagination/follow link: {nxt}")

            # Also follow any other minutes-related links
            for a in soup.find_all("a", href=True):
                h = a.get("href") or ""
                nxt = urljoin(url, h)
                if (nxt not in visited
                    and is_allowed_domain(nxt, allowed_domains)
                    and any(kw in nxt.lower() for kw in ['minutes', 'boe', 'board', 'meeting', 'agenda'])):
                    queue.append((nxt, depth + 1))
