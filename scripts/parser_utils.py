import re
from io import BytesIO
from typing import List, Dict, Optional
from PyPDF2 import PdfReader
from docx import Document
from datetime import datetime
from dateutil import parser as dateparser

# Expanded patterns to better catch UPK/expansion topics
PRESCHOOL_PATTERNS = [
    r"\bpreschool\b",
    r"\bpre[\s-]?k\b",
    r"\bprek\b",
    r"\bpk\b",
    r"\buniversal\s+pre[\s-]?k\b",
    r"\buniversal\s+preschool\b",
    r"\bUPK\b",
    r"\bearly\s+childhood\b",
    r"\b3[\s-]?year[\s-]?old(?:s)?\b",
    r"\b4[\s-]?year[\s-]?old(?:s)?\b",
    r"\bpreschool\s+expan(?:sion|d)\w*\b",
    r"\bexpan(?:sion|d)\w*\s+of\s+preschool\b",
    r"\btuition(?:-free)?\b",
    r"\blottery\b",
    r"\benrollment\b",
    r"\bPEEA\b",  # NJ Preschool Expansion Aid acronym
]
KEYWORD_REGEX = re.compile("|".join(PRESCHOOL_PATTERNS), re.IGNORECASE)


def extract_text_from_pdf(content: bytes) -> str:
    """
    Extracts concatenated text from all pages of a PDF.
    """
    reader = PdfReader(BytesIO(content))
    texts = []
    for page in reader.pages:
        try:
            t = page.extract_text() or ""
            texts.append(t)
        except Exception:
            continue
    return "\n".join(texts)


def extract_text_from_docx(content: bytes) -> str:
    """
    Extract text from a .docx file.
    """
    doc = Document(BytesIO(content))
    return "\n".join(p.text for p in doc.paragraphs)


def find_preschool_mentions(text: str, context_chars: int = 160) -> List[Dict]:
    """
    Returns a list of {"keyword": <str>, "snippet": <str>}.
    """
    mentions = []
    if not text:
        return mentions
    for m in KEYWORD_REGEX.finditer(text):
        start = max(m.start() - context_chars, 0)
        end = min(m.end() + context_chars, len(text))
        snippet = text[start:end].replace("\r", " ").replace("\n", " ")
        mentions.append({
            "keyword": m.group(0),
            "snippet": snippet.strip()
        })
    return mentions


_DATE_PATTERNS = [
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+\d{1,2},\s+\d{4}\b",
]
_DATE_REGEXES = [re.compile(p, re.IGNORECASE) for p in _DATE_PATTERNS]

def guess_meeting_date(text: str, title: str = "", url: str = "") -> Optional[datetime]:
    """
    Best-effort date inference from document text/title/url.
    """
    srcs = [text or "", title or "", url or ""]
    for source in srcs:
        for rx in _DATE_REGEXES:
            m = rx.search(source)
            if m:
                try:
                    return dateparser.parse(m.group(0), dayfirst=False, fuzzy=True)
                except Exception:
                    continue
    # As a fallback, look for year in URL (e.g., /2023/09/)
    m = re.search(r"/(20\d{2})/(\d{1,2})/", url)
    if m:
        try:
            y, mo = int(m.group(1)), int(m.group(2))
            return datetime(y, mo, 1)
        except Exception:
            pass
    return None
``
