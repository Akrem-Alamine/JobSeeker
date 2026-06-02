import sys
from pathlib import Path

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

# Reuse the pipeline LLM client
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from pipeline.llm_client import llm_generate_json
except ImportError:
    llm_generate_json = None


def extract_text(filepath: str) -> str:
    if pdfplumber is None:
        return ""
    try:
        with pdfplumber.open(filepath) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception:
        return ""


def parse_fields(text: str) -> dict:
    if not llm_generate_json or not text.strip():
        return {}
    prompt = (
        "Extract the following fields from this CV. Return ONLY a JSON object:\n"
        '{"name": "Full Name", "email": "email@example.com", '
        '"university": "University Name", "graduation": "Month Year or Year", '
        '"internship_company": "Company Name", "internship_role": "Job Title"}\n\n'
        f"CV TEXT:\n{text[:3500]}\n\n"
        "Use null for any field not found. Do not invent."
    )
    return llm_generate_json(prompt, max_tokens=250, temperature=0.0)
