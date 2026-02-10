import re
from io import BytesIO
from typing import List, Dict, Optional
from PyPDF2 import PdfReader
from docx import Document
from datetime import datetime
from dateutil import parser as dateparser

# --------------------------------------------------------------------
# Preschool-related keyword patterns
# --------------------------------------------------------------------

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
    r"\bPEEA\b"
]

KEYWORD_REGEX = re.compile("|".join(PRESCHOOL_PATTERNS), re.IGNORECASE)


# --------------------------------------------------------------------
# PDF & DOCX text extraction
# --------------------------------------------------------------------

def extract_text_from_pdf(content: bytes) -> str:
    """
    Extract concatenated text from all pages of a PDF file.
    Safe fallback: returns empty string for unreadable pages.
    """
    reader = PdfReader(BytesIO(content))
    texts = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
            texts.append(text)
        except Exception:
            continue
    return "\n".join(texts)


def extract_text_from_docx(content: bytes) -> str:
    """
    Extract text from a .docx file.
    """
    doc = Document(BytesIO(content))
    return "\n".join(p.text for p in doc.paragraphs)


# --------------------------------------------------------------------
# Keyword detection with snippet context
# --------------------------------------------------------------------

def find_preschool_mentions(text: str, context_chars: int = 160) -> List[Dict]:
    """
    Find all preschool-related keyword hits in text.

    Returns a list of:
    {
        "keyword": "string matched",
        "snippet": "surrounding context"
    }
    """
    mentions = []
    if not text:
        return mentions

    for match in KEYWORD_REGEX.finditer(text):
        start = max(match.start() - context_chars, 0)
        end = min(match.end() + context_chars, len(text))
        snippet = text[start:end].replace("\r", " ").replace("\n", " ")

        mentions.append({
            "keyword": match.group(0),
            "snippet": snippet.strip()
        })

    return mentions


# --------------------------------------------------------------------
# Meeting date detection
# --------------------------------------------------------------------

DATE_PATTERNS = [
    # October 21, 2024
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",

    # 10/21/2024 or 10/21/24
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",

    # 2024-10-21
    r"\b\d{4}-\d{2}-\d{2}\b",

    # Oct 21, 2024
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+\d{1,2},\s+\d{4}\b",
]

DATE_REGEXES = [re.compile(p, re.IGNORECASE) for p in DATE_PATTERNS]


def guess_meeting_date(text: str, title: str = "", url: str = "") -> Optional[datetime]:
    """
    Best-effort extraction of a meeting date from:
    - document text
    - title
    - URL patterns

    Returns datetime or None.
    """
    candidates = [text or "", title or "", url or ""]

    # Try explicit date patterns first
    for source in candidates:
        for rx in DATE_REGEXES:
            match = rx.search(source)
            if match:
                try:
                    return dateparser.parse(match.group(0), dayfirst=False, fuzzy=True)
                except Exception:
                    continue

    # URL fallback: /2023/09/ patterns
    fallback = re.search(r"/(20\d{2})/(\d{1,2})/", url)
    if fallback:
        try:
            y = int(fallback.group(1))
            m = int(fallback.group(2))
            return datetime(y, m, 1)
        except Exception:
            pass

    return None
