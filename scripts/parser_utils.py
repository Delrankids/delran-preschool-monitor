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
# Notes:
# - We normalize many common variants of "pre-k" and childcare terms.
# - Keep patterns conservative to avoid false positives, but include
#   realistic program phrases used in BOE minutes.
# - Regexes are case-insensitive; we compile once below.

_PRESCHOOL_TERMS = [
    r"\bpreschool\b",
    r"\bpre[\s\-]?school\b",
    r"\bpre[\s\-]?k\b",
    r"\bprek\b",
    r"\bpre[\s\-]?k3\b", r"\bpre[\s\-]?k4\b", r"\bpk\b",
    r"\buniversal\s+pre[\s\-]?k\b",
    r"\buniversal\s+preschool\b",
    r"\bUPK\b",
    r"\bearly\s+childhood\b",
]

_CHILDCARE_TERMS = [
    r"\bchild[\s\-]?care\b", r"\bchildcare\b",
    r"\bday[\s\-]?care\b",
    r"\bwrap[\s\-]?around\b",
    r"\bbefore\s+care\b", r"\bafter\s+care\b",
    r"\bextended\s+day\b",
]

_PROGRAM_CONTEXT_TERMS = [
    r"\btuition(?:\s*preschool)?\b",
    r"\btuition[\s\-]?free\b",
    r"\blottery\b",
    r"\benrollment\b",
    r"\bPEEA\b",  # Preschool Expansion/Education Aid (NJ)
]

PRESCHOOL_PATTERNS = _PRESCHOOL_TERMS + _CHILDCARE_TERMS + _PROGRAM_CONTEXT_TERMS
KEYWORD_REGEX = re.compile("|".join(PRESCHOOL_PATTERNS), re.IGNORECASE)

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

_SENTENCE_SPLIT_RE = re.compile(
    r"(?<!\b[A-Z])[\.!?](?:\s+|\s*$)|\n{2,}", re.MULTILINE
)
_WHITESPACE_RE = re.compile(r"[ \t\r\f\v]+")


def _normalize_space(s: str) -> str:
    s = s.replace("\u00A0", " ")
    s = _WHITESPACE_RE.sub(" ", s)
    return s.strip()


def _split_sentences(text: str) -> List[str]:
    # Light heuristic sentence splitter; avoids over-splitting on initials.
    text = text.replace("\r", "\n")
    parts: List[str] = []
    start = 0
    for m in _SENTENCE_SPLIT_RE.finditer(text):
        end = m.end()
        chunk = text[start:end].strip()
        if chunk:
            parts.append(chunk)
        start = end
    tail = text[start:].strip()
    if tail:
        parts.append(tail)
    return parts or [text]


def _bounded_context(
    text: str, match_span: Tuple[int, int], target_len: int = 220
) -> str:
    """
    Build a snippet around the match using nearby sentences; keep it compact.
    """
    if not text:
        return ""
    text_norm = _normalize_space(text)
    start, end = match_span
    start = max(0, start)
    end = min(len(text_norm), end)

    # Prefer sentence-aware slice
    # Find sentence boundaries:
    sentences = _split_sentences(text_norm)
    # Walk through sentences to find where the match sits
    pos = 0
    idx = 0
    for i, s in enumerate(sentences):
        nxt = pos + len(s)
        if pos <= start < nxt or pos < end <= nxt or (start <= pos and nxt <= end):
            idx = i
            break
        pos = nxt + 1  # +1 for the split char

    # Build snippet from the target sentence +/- one neighbor if needed
    chosen = sentences[idx:idx + 1]
    if chosen and len(_normalize_space(" ".join(chosen))) < target_len // 2:
        if idx > 0:
            chosen.insert(0, sentences[idx - 1])
        if idx + 1 < len(sentences):
            chosen.append(sentences[idx + 1])

    snippet = _normalize_space(" ".join(chosen))
    if len(snippet) > target_len:
        # Keep keyword centered
        mid = (start + end) // 2
        left = max(0, mid - target_len // 2)
        right = min(len(text_norm), left + target_len)
        snippet = text_norm[left:right].strip()
        # Ellipsize if trimmed
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
    Extract concatenated text from all pages of a PDF file.
    Safe fallback: returns empty string for unreadable pages.
    """
    try:
        reader = PdfReader(BytesIO(content))
    except Exception:
        return ""
    texts: List[str] = []
    for page in getattr(reader, "pages", []):
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text:
            texts.append(text)
    return "\n".join(texts)


def extract_text_from_docx(content: bytes) -> str:
    """
    Extract text from a .docx file.
    """
    try:
        doc = Document(BytesIO(content))
    except Exception:
        return ""
    return "\n".join(_normalize_space(p.text) for p in doc.paragraphs if p.text)


# --------------------------------------------------------------------
# Keyword detection with snippet context (improved)
# --------------------------------------------------------------------

def find_preschool_mentions(text: str, context_chars: int = 220) -> List[Dict]:
    """
    Find all preschool/UPK/childcare-related keyword hits in text.

    Returns a list of dicts:
    {
        "keyword": "string matched",
        "snippet": "surrounding context"
    }
    """
    mentions: List[Dict] = []
    if not text:
        return mentions

    # We’ll dedupe repeated identical snippets to avoid spammy results.
    seen_snips = set()

    for m in KEYWORD_REGEX.finditer(text):
        span = (m.start(), m.end())
        snippet = _bounded_context(text, span, target_len=context_chars)
        key = (m.group(0).lower(), snippet.lower())
        if key in seen_snips:
            continue
        seen_snips.add(key)
        mentions.append({
            "keyword": m.group(0),
            "snippet": snippet
        })

    return mentions


# --------------------------------------------------------------------
# Meeting date detection (hardened)
# --------------------------------------------------------------------
# We consider multiple candidate sources:
#  1) title and URL (often most reliable).
#  2) text near phrases like “Meeting Minutes”, “Board of Education”, etc.
#  3) global date patterns in the document.
#
# We prefer:
#   - dates <= today
#   - dates in/near title/URL
#   - full dates (Month D, YYYY or MM/DD/YYYY)
#   - years >= 2015 to avoid OCR noise

# Common date formats
DATE_PATTERNS = [
    # October 21, 2024
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
    # Oct 21, 2024 / Sept. 8, 2023
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+\d{1,2},\s+\d{4}\b",
    # 10/21/2024 or 10/21/24
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    # 2024-10-21
    r"\b\d{4}-\d{2}-\d{2}\b",
    # 20241021 or 2024_10_21
    r"\b(20\d{2})[-_/]?(0?[1-9]|1[0-2])[-_/]?(0?[1-9]|[12]\d|3[01])\b",
    # 10-21-2024
    r"\b(0?[1-9]|1[0-2])[-_.](0?[1-9]|[12]\d|3[01])[-_.](20\d{2})\b",
]
DATE_REGEXES = [re.compile(p, re.IGNORECASE) for p in DATE_PATTERNS]

# Phrases that hint where a meeting date tends to sit
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
                # Filter out silly years from OCR
                if 2015 <= dt.year <= datetime.utcnow().year + 1:
                    cands.append(dt)
            except Exception:
                continue
    return cands

def _score_date(dt: datetime, *, origin: str, today: date) -> float:
    """
    Lower score is better.
    Prefer dates not in the future, and those from title/url slightly.
    """
    score = 0.0
    # Penalize future dates
    if dt.date() > today:
        score += 10.0
    # Preference by origin
    if origin == "title":
        score -= 1.0
    elif origin == "url":
        score -= 0.5
    # Prefer more recent (but not future)
    age_days = (today - min(dt.date(), today)).days
    score += min(age_days / 365.0, 10.0)
    return score

def _best_candidate(cands: List[Tuple[datetime, str]]) -> Optional[datetime]:
    if not cands:
        return None
    today = datetime.utcnow().date()
    scored = [(dt, _score_date(dt, origin=src, today=today)) for dt, src in cands]
    scored.sort(key=lambda x: (x[1], -x[0].timestamp()))
    return scored[0][0] if scored else None

def guess_meeting_date(text: str, title: str = "", url: str = "") -> Optional[datetime]:
    """
    Best-effort extraction of a meeting date from:
      - title / url
      - document text (with hints)
    Returns datetime or None.
    """
    candidates: List[Tuple[datetime, str]] = []

    # 1) Title and URL first (often very reliable)
    for src, chunk in (("title", title or ""), ("url", url or "")):
        for dt in _parse_candidates_from_text(chunk):
            candidates.append((dt, src))

    # 2) Nearby hints in text: find small windows around hint phrases
    if text:
        text_norm = _normalize_space(text)
        for m in DATE_HINT_WINDOW_RE.finditer(text_norm):
            start = max(0, m.start() - 200)
            end = min(len(text_norm), m.end() + 200)
            window = text_norm[start:end]
            for dt in _parse_candidates_from_text(window):
                candidates.append((dt, "hint-window"))

    # 3) Fallback: any date in the whole text (but this is noisier)
    if not candidates and text:
        for dt in _parse_candidates_from_text(text):
            candidates.append((dt, "body"))

    return _best_candidate(candidates)
