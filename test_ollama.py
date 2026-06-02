"""
test_ollama.py — Generate 3 letters via Ollama qwen2.5:1.5b, no DB save.

Usage:
  python3 test_ollama.py
"""

import sys
import time
import requests
sys.path.insert(0, '.')

from pipeline.cv_parser        import parse_cv
from pipeline.letter_generator import generate_letter
from pathlib import Path

OLLAMA_URL   = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen2.5:1.5b"

# Monkey-patch llm_client to force Ollama qwen2.5:1.5b for this test
import pipeline.llm_client as _lc
_orig = _lc.llm_generate

def _ollama_qwen(prompt, max_tokens=400, temperature=0.3):
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model":   OLLAMA_MODEL,
                "prompt":  prompt,
                "stream":  False,
                "options": {"temperature": temperature, "num_predict": max_tokens},
            },
            timeout=120,
        )
        return resp.json().get("response", "").strip()
    except Exception as e:
        return f"[ERROR: {e}]"

_lc.llm_generate = _ollama_qwen

CONTACTS = [
    {"id": 3,  "first_name": "Daniel", "last_name": "Gellert",
     "title": "Tech Lead Data Platform",         "company": "Procure Ai",
     "company_domain": "procure.ai",             "email": "daniel.gellert@procure.ai"},
    {"id": 5,  "first_name": "Felix",  "last_name": "Mueller",
     "title": "Co-Founder, Business Development","company": "plus10",
     "company_domain": "plus10.de",              "email": "felix.mueller@plus10.de"},
    {"id": 8,  "first_name": "Jasper", "last_name": "Vries",
     "title": "Co-Founder and CEO",              "company": "Coolgradient",
     "company_domain": "coolgradient.nl",        "email": "jasper.de.vries@coolgradient.nl"},
]

CV_PATH = Path("output/uploads/cv.pdf")


def main():
    if not CV_PATH.exists():
        print("ERROR: cv.pdf not found at output/uploads/cv.pdf")
        sys.exit(1)

    cv = parse_cv(str(CV_PATH))
    print(f"CV loaded: {cv.get('name', '?')}\n")
    print(f"Model: {OLLAMA_MODEL}")
    print("=" * 70)

    for i, contact in enumerate(CONTACTS, 1):
        print(f"\n[{i}/3] {contact['first_name']} {contact['last_name']} "
              f"— {contact['title']} @ {contact['company']}")
        print("-" * 70)
        t0     = time.time()
        letter = generate_letter(contact, cv)
        elapsed = time.time() - t0

        if not letter:
            print("  ✗ Generation returned None")
            continue

        print(letter)
        print("-" * 70)
        words     = len(letter.split())
        valid     = letter.lstrip().lower().startswith("dear") and len(letter) >= 300
        print(f"⏱ {elapsed:.1f}s  |  {words} words  |  {'✓ valid' if valid else '✗ invalid'}")

    print("\n(Nothing saved to DB)")


if __name__ == "__main__":
    main()
