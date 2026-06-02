"""
Free unlimited email verifier — no API key needed.

Steps per address:
  1. Syntax check
  2. DNS MX lookup  (cached per domain — very fast)
  3. SMTP handshake (optional, --smtp flag)

Output files (output/):
  verified_emails.csv   — full results
  deliverable.txt       — passed all checks
  undeliverable.txt     — failed (bad syntax / no MX / mailbox rejected)
  unknown.txt           — MX exists but SMTP inconclusive (catch-all / timeout)
  risky.txt             — disposable / role-based addresses

Usage:
    python verify_emails.py                  # DNS only (fast, ~2min for 23k)
    python verify_emails.py --smtp           # DNS + SMTP (slower, more accurate)
    python verify_emails.py --smtp --workers 20
"""

import argparse
import csv
import json
import re
import smtplib
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import dns.resolver
import dns.exception
from tqdm import tqdm

INPUT_FILE = Path("output/all_recipients.txt")
OUT_DIR    = Path("output")
PROGRESS   = OUT_DIR / "verification_progress.json"
CSV_OUT    = OUT_DIR / "verified_emails.csv"

EMAIL_RE = re.compile(r'^[\w.+%\-]+@[\w.\-]+\.[a-zA-Z]{2,}$', re.IGNORECASE)

# Role-based prefixes → risky
ROLE_PREFIXES = {
    "info", "contact", "admin", "support", "hello", "help", "sales",
    "marketing", "noreply", "no-reply", "team", "office", "mail",
    "enquiries", "enquiry", "hr", "jobs", "careers", "press", "media",
}

# Common disposable email domains
DISPOSABLE_DOMAINS = {
    "mailinator.com", "guerrillamail.com", "tempmail.com", "throwam.com",
    "yopmail.com", "trashmail.com", "fakeinbox.com", "sharklasers.com",
    "maildrop.cc", "dispostable.com", "spamgourmet.com", "getairmail.com",
}

# DNS resolver with short timeout
resolver = dns.resolver.Resolver()
resolver.timeout = 5
resolver.lifetime = 5

# Domain MX cache — avoids re-querying same domain for 1000s of emails
_mx_cache: dict[str, list[str]] = {}


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_syntax(email: str) -> bool:
    return bool(EMAIL_RE.match(email))


def get_mx(domain: str) -> list[str]:
    if domain in _mx_cache:
        return _mx_cache[domain]
    try:
        records = resolver.resolve(domain, "MX")
        mx_list = sorted(records, key=lambda r: r.preference)
        result = [str(r.exchange).rstrip(".") for r in mx_list]
    except (dns.exception.DNSException, Exception):
        result = []
    _mx_cache[domain] = result
    return result


def check_smtp(email: str, mx_hosts: list[str],
               sender: str = "verify@check.local",
               timeout: int = 10) -> str:
    """
    Returns: 'deliverable' | 'undeliverable' | 'unknown'
    """
    for mx in mx_hosts[:2]:  # try top 2 MX records
        try:
            with smtplib.SMTP(timeout=timeout) as smtp:
                smtp.connect(mx, 25)
                smtp.ehlo_or_helo_if_needed()
                smtp.mail(sender)
                code, _ = smtp.rcpt(email)
                smtp.quit()
                if code == 250:
                    return "deliverable"
                elif code in (550, 551, 553, 554):
                    return "undeliverable"
                else:
                    return "unknown"
        except smtplib.SMTPConnectError:
            continue
        except smtplib.SMTPServerDisconnected:
            return "unknown"
        except (socket.timeout, socket.gaierror, OSError):
            continue
        except Exception:
            return "unknown"
    return "unknown"


def verify_one(email: str, do_smtp: bool) -> dict:
    email = email.strip().lower()
    result = {
        "email":          email,
        "classification": "",
        "status":         "",
    }

    # 1. Syntax
    if not check_syntax(email):
        result["classification"] = "Undeliverable"
        result["status"] = "InvalidSyntax"
        return result

    local, domain = email.rsplit("@", 1)

    # 2. Disposable
    if domain in DISPOSABLE_DOMAINS:
        result["classification"] = "Risky"
        result["status"] = "DisposableDomain"
        return result

    # 3. Role-based
    if local.split("+")[0] in ROLE_PREFIXES:
        result["classification"] = "Risky"
        result["status"] = "RoleAddress"
        return result

    # 4. DNS MX
    mx_hosts = get_mx(domain)
    if not mx_hosts:
        result["classification"] = "Undeliverable"
        result["status"] = "DomainNoMX"
        return result

    # 5. SMTP (optional)
    if do_smtp:
        smtp_result = check_smtp(email, mx_hosts)
        if smtp_result == "deliverable":
            result["classification"] = "Deliverable"
            result["status"] = "Success"
        elif smtp_result == "undeliverable":
            result["classification"] = "Undeliverable"
            result["status"] = "MailboxDoesNotExist"
        else:
            result["classification"] = "Unknown"
            result["status"] = "SMTPUnreachable"
    else:
        result["classification"] = "Deliverable"
        result["status"] = "MXFound"

    return result


# ---------------------------------------------------------------------------
# Progress checkpoint
# ---------------------------------------------------------------------------

def load_progress() -> dict:
    if PROGRESS.exists():
        return json.loads(PROGRESS.read_text())
    return {"done": [], "results": []}


def save_progress(state: dict):
    PROGRESS.write_text(json.dumps(state))


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_outputs(results: list[dict]):
    OUT_DIR.mkdir(exist_ok=True)

    with CSV_OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["email", "classification", "status"])
        w.writeheader()
        w.writerows(results)

    buckets = {"Deliverable": [], "Undeliverable": [], "Risky": [], "Unknown": []}
    for r in results:
        cls = r["classification"]
        if cls in buckets:
            buckets[cls].append(r["email"])

    file_map = {
        "Deliverable":   OUT_DIR / "deliverable.txt",
        "Undeliverable": OUT_DIR / "undeliverable.txt",
        "Risky":         OUT_DIR / "risky.txt",
        "Unknown":       OUT_DIR / "unknown.txt",
    }
    for cls, path in file_map.items():
        path.write_text("\n".join(sorted(buckets[cls])))

    print(f"\nResults saved to {OUT_DIR}/")
    print(f"  verified_emails.csv  : {len(results)} rows")
    total = len(results)
    for cls in ("Deliverable", "Undeliverable", "Risky", "Unknown"):
        n = len(buckets[cls])
        pct = 100 * n / total if total else 0
        print(f"  {cls:<15}: {n:>6}  ({pct:.1f}%)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--smtp",    action="store_true", help="Enable SMTP mailbox check (slower)")
    parser.add_argument("--workers", type=int, default=40, help="Parallel workers (default 40)")
    parser.add_argument("--input",   default=str(INPUT_FILE), help="Input file (one email per line)")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: {input_path} not found.")
        return

    addresses = [
        line.strip().lower()
        for line in input_path.read_text().splitlines()
        if line.strip() and "@" in line
    ]
    print(f"Addresses   : {len(addresses)}")
    print(f"SMTP check  : {'yes' if args.smtp else 'no (DNS only)'}")
    print(f"Workers     : {args.workers}")
    if not args.smtp:
        print("  Tip: add --smtp for mailbox-level verification (slower)\n")

    state = load_progress()
    done_set = set(state["done"])
    results  = state["results"]

    pending = [a for a in addresses if a not in done_set]
    print(f"Remaining   : {len(pending)}  ({len(done_set)} already done)\n")

    checkpoint_every = 500

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(verify_one, addr, args.smtp): addr for addr in pending}
        batch = []

        with tqdm(total=len(pending), unit="email") as bar:
            for future in as_completed(futures):
                r = future.result()
                results.append(r)
                done_set.add(r["email"])
                batch.append(r["email"])
                bar.update(1)
                bar.set_postfix({"last": r["classification"][:4]})

                if len(batch) >= checkpoint_every:
                    state["done"]    = list(done_set)
                    state["results"] = results
                    save_progress(state)
                    batch.clear()

    # Final save
    state["done"]    = list(done_set)
    state["results"] = results
    save_progress(state)

    write_outputs(results)

    if len(done_set) >= len(addresses):
        PROGRESS.unlink(missing_ok=True)
        print("\nComplete — progress file removed.")


if __name__ == "__main__":
    main()
