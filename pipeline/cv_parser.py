"""
Extract structured text from a PDF CV.
Returns a dict with raw text + key sections detected.
"""

import re
import pdfplumber


def parse_cv(pdf_path: str) -> dict:
    """Extract text and key info from a CV PDF."""
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            text += page_text + "\n"

    text = re.sub(r'\n{3,}', '\n\n', text).strip()

    name    = _extract_name(text)
    phone   = _extract_phone(text)
    linkedin = _extract_linkedin(text)

    return {
        "raw_text": text[:4000],   # cap to avoid overwhelming the LLM
        "name":     name,
        "phone":    phone,
        "linkedin": linkedin,
    }


def _extract_name(text: str) -> str:
    # Usually the first non-empty line is the name
    for line in text.splitlines():
        line = line.strip()
        if line and len(line.split()) in (2, 3) and line[0].isupper():
            if not any(w in line.lower() for w in ("curriculum", "vitae", "resume", "cv", "profile")):
                return line
    return ""


def _extract_phone(text: str) -> str:
    m = re.search(r'(\+?\d[\d\s\-().]{7,}\d)', text)
    return m.group(1).strip() if m else ""


def _extract_linkedin(text: str) -> str:
    m = re.search(r'linkedin\.com/in/[\w\-]+', text, re.I)
    return m.group(0) if m else ""
