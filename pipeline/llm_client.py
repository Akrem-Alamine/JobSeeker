"""
Shared LLM client — auto-selects backend based on available API keys.

Priority:
  1. Groq   (if GROQ_KEY in .env)   — free, fast, Llama 3.1 70B
  2. Gemini (if GEMINI_KEY in .env) — free via AI Studio, Gemini 2.0 Flash
  3. Ollama (local fallback)         — phi3:mini, no API key needed

Usage:
  from pipeline.llm_client import llm_generate, LLM_BACKEND

  text = llm_generate(prompt, max_tokens=150, temperature=0.05)
"""

import os
import re
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(".env"))

GROQ_KEY   = os.getenv("GROQ_KEY", "")
GEMINI_KEY = os.getenv("GEMINI_KEY", "")

GROQ_URL      = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL    = "llama-3.1-8b-instant"   # fast + free; swap to llama-3.1-70b-versatile for quality

GEMINI_MODEL    = "gemini-2.0-flash"
GEMINI_ENDPOINT = (
    f"https://generativelanguage.googleapis.com/v1/models"
    f"/{GEMINI_MODEL}:generateContent"
)

OLLAMA_URL   = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "phi3:mini"

if GROQ_KEY:
    LLM_BACKEND = "groq"
elif GEMINI_KEY:
    LLM_BACKEND = "gemini"
else:
    LLM_BACKEND = "ollama"


def llm_generate(prompt: str, max_tokens: int = 300, temperature: float = 0.1) -> str:
    """
    Generate text from a prompt using the available LLM backend.
    Returns the raw response string, or '' on failure.
    """
    if LLM_BACKEND == "groq":
        return _groq(prompt, max_tokens, temperature)
    if LLM_BACKEND == "gemini":
        return _gemini(prompt, max_tokens, temperature)
    return _ollama(prompt, max_tokens, temperature)


def llm_generate_ollama(prompt: str, model: str = "qwen2.5:1.5b",
                         max_tokens: int = 400, temperature: float = 0.3,
                         url: str = OLLAMA_URL) -> str:
    """Direct Ollama call — bypasses the auto-selected backend."""
    try:
        resp = requests.post(
            url,
            json={
                "model":   model,
                "prompt":  prompt,
                "stream":  False,
                "options": {"temperature": temperature, "num_predict": max_tokens},
            },
            timeout=120,
        )
        return resp.json().get("response", "").strip()
    except Exception:
        return ""


def llm_generate_json(prompt: str, max_tokens: int = 200, temperature: float = 0.05) -> dict:
    """
    Generate and parse a JSON response. Returns {} on failure or invalid JSON.
    """
    text  = llm_generate(prompt, max_tokens=max_tokens, temperature=temperature)
    match = re.search(r'\{[^{}]+\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass
    return {}


def llm_generate_json_array(prompt: str, expected: int, max_tokens: int = 1500, temperature: float = 0.0) -> list[dict]:
    """
    Generate and parse a JSON array response. Returns a list of `expected` dicts
    (padded with {} for missing entries). Used for batch LLM calls.
    """
    text = llm_generate(prompt, max_tokens=max_tokens, temperature=temperature)
    # Try to extract a JSON array from the response
    match = re.search(r'\[.*\]', text, re.DOTALL)
    if match:
        try:
            result = json.loads(match.group())
            if isinstance(result, list):
                # Pad or trim to exactly `expected` entries
                result = list(result) + [{}] * expected
                return result[:expected]
        except Exception:
            pass
    # Fallback: try to extract individual objects
    objects = re.findall(r'\{[^{}]+\}', text, re.DOTALL)
    parsed = []
    for obj in objects:
        try:
            parsed.append(json.loads(obj))
        except Exception:
            parsed.append({})
    parsed += [{}] * expected
    return parsed[:expected]


# ── Groq ──────────────────────────────────────────────────────────────────────

def _groq(prompt: str, max_tokens: int, temperature: float) -> str:
    headers = {
        "Authorization": f"Bearer {GROQ_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":       GROQ_MODEL,
        "messages":    [{"role": "user", "content": prompt}],
        "max_tokens":  max_tokens,
        "temperature": temperature,
    }
    for attempt in range(4):
        try:
            resp = requests.post(GROQ_URL, headers=headers, json=payload, timeout=30)
            if resp.status_code == 429:
                # Respect retry-after header if present, else exponential backoff
                retry_after = float(resp.headers.get("retry-after", 2 ** attempt))
                time.sleep(min(retry_after, 60))
                continue
            return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception:
            if attempt < 3:
                time.sleep(2 ** attempt)
    return ""


# ── Gemini ────────────────────────────────────────────────────────────────────

def _gemini(prompt: str, max_tokens: int, temperature: float) -> str:
    try:
        resp = requests.post(
            GEMINI_ENDPOINT,
            params={"key": GEMINI_KEY},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": max_tokens,
                    "temperature":     temperature,
                },
            },
            timeout=30,
        )
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        return ""


# ── Ollama ────────────────────────────────────────────────────────────────────

def _ollama(prompt: str, max_tokens: int, temperature: float) -> str:
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model":   OLLAMA_MODEL,
                "prompt":  prompt,
                "stream":  False,
                "options": {"temperature": temperature, "num_predict": max_tokens},
            },
            timeout=60,
        )
        return resp.json().get("response", "").strip()
    except Exception:
        return ""
