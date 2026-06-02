# Setup Guide — IMAP (no Google Cloud needed)

## Step 1 — Enable IMAP in Gmail / Workspace

**Gmail personal:**
Settings (gear) → See all settings → Forwarding and POP/IMAP → Enable IMAP → Save

**Google Workspace:**
Your admin may need to enable IMAP for the org, or it may already be on.

## Step 2 — Create an App Password

Go to: https://myaccount.google.com/apppasswords
(Requires 2-Step Verification to be enabled)

1. Select app: **Mail** / Other (name it anything)
2. Click **Generate**
3. Copy the 16-character password shown (spaces don't matter)

> If your Workspace org uses SSO/SAML, ask your admin to allow app passwords,
> or use the admin-generated password they provide.

## Step 3 — Configure .env

```bash
cp .env.example .env
# then edit .env with your address and app password
```

## Step 4 — Install dependencies

```bash
pip install -r requirements.txt
```

## Step 5 — Run

```bash
# Fetch sent emails + all follow-ups / bounces for 2024
python fetch_threads.py --after 2024-01-01 --before 2025-01-01

# Sent emails only (faster, skip follow-up scan)
python fetch_threads.py --after 2024-01-01 --before 2025-01-01 --no-followups
```

## Output (in output/)

| File | Contents |
|------|----------|
| `emails.db` | SQLite database (queryable) |
| `sent_emails.csv` | Every sent email in the period |
| `followups.csv` | All replies / bounces linked to sent emails |
| `bounced_emails.csv` | Bounce messages only |
| `bounced_addresses.txt` | One failed address per line |

## Next step — filter your database

Once the above files exist, drop your contacts file in `data/` and run:

```bash
python filter_database.py --db data/mylist.csv --email-column Email
```
