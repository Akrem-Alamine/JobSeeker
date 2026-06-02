# Email Database Cleaner — Project Plan

## Goal
Filter and refactor an email database by cross-referencing it with sent email history
from Gmail, identifying invalid/bounced addresses, and producing a clean list.

## Phases

### Phase 1 — Gmail Connection & Sent Email Extraction ✅
- Connect to Gmail via MCP tools (already authenticated)
- Query the SENT folder for a user-defined date range
- Paginate through all results and collect every unique recipient address

### Phase 2 — Bounce / Delivery Failure Detection
- Search the INBOX for bounce-back messages (from MAILER-DAEMON, postmaster, etc.)
- Parse the failed recipient addresses out of those threads
- Build a `bounced_emails.txt` list

### Phase 3 — Database Ingestion
- Accept the user's email database (CSV / TXT / Excel)
- Parse and normalize all email addresses in it

### Phase 4 — Cross-Reference & Filtering
- Match database addresses against:
  - Sent list   → was this address ever contacted?
  - Bounce list → did delivery fail for this address?
- Produce output files:
  - `valid_emails.csv`   — sent successfully (no bounce)
  - `bounced_emails.csv` — confirmed delivery failure
  - `unsent_emails.csv`  — in DB but never contacted

### Phase 5 — Report
- Summary counts per category
- Optional: flag duplicates, malformed addresses

## Output Files (under `output/`)
| File | Contents |
|------|----------|
| `sent_addresses.txt` | All unique addresses from Sent folder |
| `bounced_addresses.txt` | Addresses that bounced |
| `valid_emails.csv` | Clean, deliverable contacts |
| `bounced_emails.csv` | Confirmed bad addresses |
| `unsent_emails.csv` | Never contacted |

## Tech Stack
- Gmail: Claude Code MCP Gmail tools
- Data processing: Python (pandas)
- Input DB: TBD (user to provide)
