import re
from io import BytesIO
from typing import List, Dict, Optional, Iterable, Tuple
from datetime import datetime, date

from PyPDF2 import PdfReader
from docx import Document
from dateutil import parser as dateparser

# --------------------------------------------------------------------
# Keyword patterns (expanded)
# --------------------------------------------------------------------

_PRESCHOOL_TERMS = [
    r"\bpreschool\b",
    r"\bpre[\s\-]?school\b",
    r"\bpre[\s\-]?k\b",
    r"\bprek\b",
    r"\bpre[\s\-]?k3\b",
    r"\bpre[\s\-]?k4\b",
    r"\bpk\b",
    r"\buniversal\s+pre[\s\-]?k\b",
    r"\buniversal\s+preschool\b",
    r"\bUPK\b",
    r"\bearly\s+childhood\b",
]

_CHILDCARE_TERMS = [
    r"\bchild[\s\-]?care\b",
    r"\bchildcare\b",
    r"\bday[\s\-]?care\b",
    r"\bwrap[\s\-]?around\b",
    r"\bbefore\s+care\b",
    r"\bafter\s+care\b",
    r"\bextended\s+day\b",
]

_PROGRAM_CONTEXT_TERMS = [
    r"\btuition(?:\s*preschool)?\b",
    r"\btuition[\s\-]?free\b",
    r"\blottery\b",
    r"\benrollment\b",
    r"\bPEEA\b",
]

PRESCHOOL_PATTERNS = _PRESCHOOL_TERMS + _CHILDCARE_TERMS + _PROGRAM_CONTEXT_TERMS
KEYWORD_REGEX = re.compile("|".join(PRESCHOOL_PATTERNS), re.IGNORECASE)

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

_WHITESPACE_RE = re.compile(r"[ \t\r\f\v]+")

def _normalize_space(s: str) -> str:
    s = s.replace("\u00A0", " ")
    s = _WHITESPACE_RE.sub(" ", s)
    return s.strip()

def _split_sentences(text: str) -> List[str]:
    """
    Lightweight heuristic: split on sentence punctuation or double line breaks.
    """
    text = text.replace("\r", "\n")
    text = _normalize_space(text)
    parts = re.split(r'(?<=[\.\?!])\s+|(?:\n{2,})', text)
    parts = [p.strip() for p in parts if p and p.strip()]
    return parts or [text]

def _bounded_context(text: str, match_span: Tuple[int, int], target_len: int = 220) -> str:
    """
    Return a cleaned snippet containing the match, ideally centered within
    nearby sentences, and clipped to a reasonable max length.
    """
    if not text:
        return ""
    text_norm = _normalize_space(text)
    start, end = match_span
    start = max(0, start)
    end = min(len(text_norm), end)

    sentences = _split_sentences(text_norm)
    abs_pos = 0
    idx = 0
    for i, s in enumerate(sentences):
        next_pos = abs_pos + len(s)
        if abs_pos <= start < next_pos or abs_pos < end <= next_pos or (start <= abs_pos and next_pos <= end):
            idx = i
            break
        abs_pos = next_pos + 1

    chosen = sentences[idx:idx + 1]
    if chosen and len(" ".join(chosen)) < target_len // 2:
        if idx > 0:
            chosen.insert(0, sentences[idx - 1])
        if idx + 1 < len(sentences):
            chosen.append(sentences[idx + 1])

    snippet = _normalize_space(" ".join(chosen))

    if len(snippet) > target_len:
        mid = (start + end) // 2
        left = max(0, mid - target_len // 2)
        right = min(len(text_norm), left + target_len)
        snippet = text_norm[left:right].strip()
        if left > 0:
            snippet = "…" + snippet
        if right < len(text_norm):
            snippet = snippet + "…"

    return snippet

# --------------------------------------------------------------------
# PDF & DOCX text extraction
# --------------------------------------------------------------------

def extract_text_from_pdf(content: bytes) -> str:
    """
    Extract text from every PDF page, skipping unreadable pages.
    """
    try:
        reader = PdfReader(BytesIO(content))
    except Exception:
        return ""
    texts: List[str] = []
    for page in getattr(reader, "pages", []):
        try:
            txt = page.extract_text() or ""
        except Exception:
            txt = ""
        if txt:
            texts.append(txt)
    return "\n".join(texts)

def extract_text_from_docx(content: bytes) -> str:
    """
    Extract text from .docx paragraphs safely.
    """
    try:
        doc = Document(BytesIO(content))
    except Exception:
        return ""
    return "\n".join(_normalize_space(p.text) for p in doc.paragraphs if p.text)

# --------------------------------------------------------------------
# Keyword detection with snippet context
# --------------------------------------------------------------------

def find_preschool_mentions(text: str, context_chars: int = 220) -> List[Dict]:
    """
    Detect all relevant preschool/UPK/childcare mentions.

    Returns:
      [
        {"keyword": "...", "snippet": "..."},
        ...
      ]
    """
    mentions: List[Dict] = []
    if not text:
        return mentions

    seen: set = set()   # de‑duplicate identical (keyword, snippet)

    for m in KEYWORD_REGEX.finditer(text):
        span = (m.start(), m.end())
        snippet = _bounded_context(text, span, target_len=context_chars)
        key = (m.group(0).lower(), snippet.lower())
        if key in seen:
            continue
        seen.add(key)
        mentions.append({
            "keyword": m.group(0),
            "snippet": snippet
        })
    return mentions

# --------------------------------------------------------------------
# Meeting date detection (hardened)
# --------------------------------------------------------------------

DATE_PATTERNS = [
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+\d{1,2},\s+\d{4}\b",
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b(20\d{2})[-_/]?(0?[1-9]|1[0-2])[-_/]?(0?[1-9]|[12]\d|3[01])\b",
]
DATE_REGEXES = [re.compile(p, re.IGNORECASE) for p in DATE_PATTERNS]

DATE_HINT_WINDOW_RE = re.compile(
    r"(Board of Education|BOE|Meeting Minutes|Regular Meeting|Special Meeting|Workshop Meeting|Agenda)",
    re.IGNORECASE
)

def _parse_candidates_from_text(source: str) -> List[datetime]:
    cands: List[datetime] = []
    for rx in DATE_REGEXES:
        for m in rx.finditer(source or ""):
            token = m.group(0)
            try:
                dt = dateparser.parse(token, dayfirst=False, fuzzy=True)
                if 2015 <= dt.year <= datetime.utcnow().year + 1:
                    cands.append(dt)
            except Exception:
                continue
    return cands

def _score_date(dt: datetime, *, origin: str, today: date) -> float:
    score = 0.0
    if dt.date() > today:
        score += 10.0
    if origin == "title":
        score -= 1.0
    elif origin == "url":
        score -= 0.5
    age_days = (today - min(dt.date(), today)).days
    score += min(age_days / 365.0, 10.0)
    return score

def _best_candidate(cands: List[Tuple[datetime, str]]) -> Optional[datetime]:
    if not cands:
        return None
    today = datetime.utcnow().date()
    ranked = [(dt, _score_date(dt, origin=o, today=today)) for dt, o in cands]
    ranked.sort(key=lambda x: (x[1], -x[0].timestamp()))
    return ranked[0][0] if ranked else None

def guess_meeting_date(text: str, title: str = "", url: str = "") -> Optional[datetime]:
    """
    Multi‑source date inference:
      - explicit dates in title/URL
      - text windows around BOE-related hint phrases
      - global text fallback
    """
    candidates: List[Tuple[datetime, str]] = []

    for origin, chunk in (("title", title or ""), ("url", url or "")):
        for dt in _parse_candidates_from_text(chunk):
            candidates.append((dt, origin))

    if text:
        tnorm = _normalize_space(text)
        for m in DATE_HINT_WINDOW_RE.finditer(tnorm):
            start = max(0, m.start() - 200)
            end = min(len(tnorm), m.end() + 200)
            window = tnorm[start:end]
            for dt in _parse_candidates_from_text(window):
                candidates.append((dt, "hint-window"))

    if not candidates and text:
        for dt in _parse_candidates_from_text(text):
            candidates.append((dt, "body"))

    return _best_candidate(candidates)
