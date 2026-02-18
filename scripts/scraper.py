def fetch(url: str, referer: Optional[str] = None) -> requests.Response:
    logging.info(f"Starting fetch for {url}")
    if "delranschools.org" in url.lower():
        logging.info("Using Playwright for Delran page")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, timeout=60000)
                page.wait_for_load_state("networkidle", timeout=30000)
                html = page.content()
                browser.close()
                logging.info(f"Playwright fetch success: {len(html)} bytes")
                class FakeResponse:
                    def __init__(self, text):
                        self.text = text
                        self.content = text.encode('utf-8')
                        self.status_code = 200
                    def raise_for_status(self):
                        pass
                return FakeResponse(html)
        except Exception as e:
            logging.error(f"Playwright fetch failed: {e}")
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
