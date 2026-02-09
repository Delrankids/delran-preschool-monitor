import re
from io import BytesIO
from typing import List, Dict
from PyPDF2 import PdfReader
from docx import Document

PRESCHOOL_PATTERNS = [
    r"\bpreschool\b",
    r"\bpre[-\s]?k\b",
    r"\bprek\b",
    r"\bpk\b",
    r"\bearly\s+childhood\b",
    r"\b3[-\s]?year[-\s]?old(s)?\b",
    r"\b4[-\s]?year[-\s]?old(s)?\b",
    r"\buniversal\s+preschool\b",
    r"\btuition\b",
    r"\blottery\b",
]
KEYWORD_REGEX = re.compile("|".join(PRESCHOOL_PATTERNS), re.IGNORECASE)

def extract_text_from_pdf(content: bytes) -> str:
    reader = PdfReader(BytesIO(content))
    texts = []
    for page in reader.pages:
        try:
            texts.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(texts)

def extract_text_from_docx(content: bytes) -> str:
    doc = Document(BytesIO(content))
    return "\n".join(p.text for p in doc.paragraphs)

def find_preschool_mentions(text: str, context_chars: int = 160) -> List[Dict]:
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
