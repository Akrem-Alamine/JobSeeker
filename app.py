"""
Lead Pipeline — Flask Dashboard
Run: .venv/bin/python app.py
"""

import csv
import functools
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, Response, jsonify, redirect, render_template, request, session, stream_with_context

app = Flask(__name__)
app.secret_key = os.urandom(24)
DB_PATH        = "output/leads.db"
UPLOAD_DIR     = Path("output/uploads")
SESSION_FILE   = Path("output/session_state.json")
UPLOAD_DIR.mkdir(exist_ok=True)
BATCH_SIZE      = 1500
SESSION_LIMIT   = 1500   # emails per session before cooldown
COOLDOWN_SECS   = 86400  # 24h cooldown once limit is reached
RUNNER_INTERVAL = 15 * 60  # response auto-scanner interval (15 min)
_stop_flag      = False

# ── Response auto-runner state ─────────────────────────────────────────────────
_runner_lock  = threading.Lock()
_runner_state: dict = {
    'running':     False,
    'last_run':    None,
    'last_result': None,
    'next_run':    None,
    'error':       '',
    'total_runs':  0,
}

# ── Prepare-letters background thread ─────────────────────────────────────────
_prepare_stop        = False
_prepare_thread: threading.Thread | None = None
_prepare_lock        = threading.Lock()
_prepare_extra_procs: list = []   # extra Ollama processes spawned per run
_prepare_status: dict = {
    'running': False, 'done': 0, 'total': 0,
    'ready': 0, 'failed': 0, 'current': '', 'currents': {}, 'error': '',
}


def _letter_looks_valid(text: str) -> tuple[bool, str]:
    if not text or len(text) < 200:
        return False, "too short"
    if not text.lstrip().lower().startswith("dear"):
        return False, "missing greeting"
    if re.search(r'^#{1,3} |^\*\*|^- |^\d+\. ', text, re.MULTILINE):
        return False, "contains markdown"
    # Must contain the fixed CV paragraph (proves template was used)
    if "Q-Leap Networks GmbH" not in text:
        return False, "missing fixed CV paragraph"
    return True, ""


def _wait_ollama_ready(port: int, timeout: int = 30) -> bool:
    """Return True once Ollama on given port responds to a health ping."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"http://localhost:{port}/api/tags", timeout=2)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def _run_prepare(num_workers: int = 1):
    global _prepare_stop, _prepare_status, _prepare_extra_procs
    import queue as _queue
    import subprocess as _subprocess
    from pipeline.cv_parser        import parse_cv
    from pipeline.letter_generator import generate_letter
    from pipeline.llm_client       import llm_generate_ollama

    cv_path = UPLOAD_DIR / "cv.pdf"
    if not cv_path.exists():
        with _prepare_lock:
            _prepare_status.update({'running': False, 'error': 'No CV uploaded'})
        return

    cv      = parse_cv(str(cv_path))
    conn    = get_conn()
    contacts = conn.execute(f"""
        SELECT ct.id, ct.first_name, ct.last_name, ct.title,
               ct.company, ct.company_domain, ct.email
        FROM contacts ct
        WHERE {OUTREACH_FILTER}
          AND NOT EXISTS (
              SELECT 1 FROM outreach o
              WHERE o.contact_id = ct.id AND o.status IN ('sent', 'draft')
          )
        ORDER BY
            CASE WHEN ct.first_name IS NOT NULL AND ct.first_name != '' THEN 0 ELSE 1 END,
            ct.id
    """).fetchall()
    conn.close()

    total    = len(contacts)
    _db_lock = threading.Lock()

    with _prepare_lock:
        _prepare_status.update({
            'total': total, 'done': 0, 'ready': 0,
            'failed': 0, 'error': '', 'current': '',
        })

    # ── Spin up extra Ollama instances (one per extra worker) ─────────────────
    BASE_PORT   = 11434
    extra_procs = []
    ports       = [BASE_PORT]
    _prepare_extra_procs.clear()

    for i in range(1, num_workers):
        port = BASE_PORT + i
        env  = os.environ.copy()
        env['OLLAMA_HOST']   = f'127.0.0.1:{port}'
        env['OLLAMA_MODELS'] = '/usr/share/ollama/.ollama/models'
        proc = _subprocess.Popen(
            ['/usr/local/bin/ollama', 'serve'],
            env=env,
            stdout=_subprocess.DEVNULL,
            stderr=_subprocess.DEVNULL,
            start_new_session=True,  # own process group so killpg kills serve + runner children
        )
        extra_procs.append(proc)
        _prepare_extra_procs.append(proc)
        ports.append(port)

    # Wait for each extra instance to be reachable
    for port in ports[1:]:
        _wait_ollama_ready(port)

    # Warm up model on every instance (loads weights into RAM)
    def _warmup(port):
        try:
            requests.post(
                f"http://localhost:{port}/api/generate",
                json={"model": "qwen2.5:1.5b", "prompt": "hi", "stream": False,
                      "options": {"num_predict": 1}},
                timeout=60,
            )
        except Exception:
            pass

    warmup_threads = [threading.Thread(target=_warmup, args=(p,), daemon=True) for p in ports]
    for t in warmup_threads:
        t.start()
    for t in warmup_threads:
        t.join()

    # Pool of (port, callable) tuples — each worker grabs one exclusively
    ollama_pool: _queue.Queue = _queue.Queue()
    for port in ports:
        fn = functools.partial(
            llm_generate_ollama,
            model="qwen2.5:1.5b",
            url=f"http://localhost:{port}/api/generate",
        )
        ollama_pool.put((port, fn))

    with _prepare_lock:
        _prepare_status['currents'] = {}

    def process(row):
        if _prepare_stop:
            return False
        contact        = dict(row)
        port, ollama_fn = ollama_pool.get()
        label          = contact.get('company') or contact['email']
        try:
            with _prepare_lock:
                _prepare_status['current']            = label
                _prepare_status['currents'][port]     = label

            letter = generate_letter(contact, cv, llm_fn=ollama_fn)
            ok, _  = _letter_looks_valid(letter)

            if ok:
                try:
                    with _db_lock:
                        wconn = get_conn()
                        wconn.execute(
                            "INSERT INTO outreach "
                            "(contact_id, email, company, domain, letter, status) "
                            "VALUES (?,?,?,?,?,'draft')",
                            (contact['id'], contact['email'],
                             contact['company'], contact['company_domain'], letter),
                        )
                        wconn.commit()
                        wconn.close()
                    with _prepare_lock:
                        _prepare_status['ready'] += 1
                except Exception:
                    with _prepare_lock:
                        _prepare_status['failed'] += 1
            else:
                with _prepare_lock:
                    _prepare_status['failed'] += 1
        finally:
            ollama_pool.put((port, ollama_fn))
            with _prepare_lock:
                _prepare_status['currents'].pop(port, None)
                _prepare_status['done'] += 1
        return True

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = {pool.submit(process, row): row for row in contacts}
            for fut in as_completed(futures):
                if _prepare_stop:
                    pool.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    fut.result()
                except Exception:
                    with _prepare_lock:
                        _prepare_status['failed'] += 1
                        _prepare_status['done']   += 1
    finally:
        import signal
        for proc in extra_procs:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass
        _prepare_extra_procs.clear()

    with _prepare_lock:
        _prepare_status.update({'running': False, 'current': ''})


def _load_session() -> dict:
    try:
        if SESSION_FILE.exists():
            return json.loads(SESSION_FILE.read_text())
    except Exception:
        pass
    return {"cooldown_start": None}


def _save_session(state: dict):
    SESSION_FILE.write_text(json.dumps(state))


def _sent_today() -> int:
    """Count emails successfully sent today (calendar day, UTC)."""
    try:
        conn = get_conn()
        n = conn.execute(
            "SELECT COUNT(*) FROM outreach WHERE status='sent' AND DATE(sent_at)=DATE('now')"
        ).fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0


def _session_stats() -> dict:
    """Return {sent, remaining, cooldown_secs_left}."""
    state    = _load_session()
    sent     = _sent_today()
    cd_start = state.get("cooldown_start")

    cooldown_left = 0
    if cd_start:
        elapsed = time.time() - cd_start
        cooldown_left = max(0, int(COOLDOWN_SECS - elapsed))
        if cooldown_left == 0:
            state = {"cooldown_start": None}
            _save_session(state)

    remaining = max(0, SESSION_LIMIT - sent)
    return {"sent": sent, "remaining": remaining, "cooldown_secs_left": cooldown_left}


def _cooldown_remaining() -> int:
    return _session_stats()["cooldown_secs_left"]


def _record_sent(count: int):
    """Trigger cooldown if today's sent total just reached the limit."""
    state = _load_session()
    if state.get("cooldown_start"):
        return
    if _sent_today() >= SESSION_LIMIT:
        state["cooldown_start"] = time.time()
        _save_session(state)


def _load_dotenv() -> dict:
    env: dict = {}
    try:
        for line in Path('.env').read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, _, v = line.partition('=')
                env[k.strip()] = v.strip().strip('"').strip("'")
    except Exception:
        pass
    return env


def _run_response_loop():
    from pipeline.response_collector import connect_imap, collect_and_process, init_responses_table
    while True:
        with _runner_lock:
            _runner_state['running'] = True
            _runner_state['error']   = ''
        try:
            env      = _load_dotenv()
            email    = env.get('EMAIL_ADDRESS', '').strip()
            password = env.get('EMAIL_PASSWORD', '').strip().replace(' ', '')
            if not email or not password:
                raise ValueError('No credentials found in .env')
            conn = get_conn()
            init_responses_table(conn)
            try:
                mail   = connect_imap(email, password)
                result = collect_and_process(mail, email, conn)
                try:
                    mail.logout()
                except Exception:
                    pass
                with _runner_lock:
                    _runner_state['last_result'] = result
                    _runner_state['total_runs'] += 1
            finally:
                conn.close()
        except Exception as exc:
            with _runner_lock:
                _runner_state['error'] = str(exc)
        now = datetime.now(timezone.utc)
        with _runner_lock:
            _runner_state['running']  = False
            _runner_state['last_run'] = now.isoformat()
            _runner_state['next_run'] = (now + timedelta(seconds=RUNNER_INTERVAL)).isoformat()
        time.sleep(RUNNER_INTERVAL)


# ── Jinja filter ──────────────────────────────────────────────────────────────

@app.template_filter("format_num")
def format_num(v):
    try:
        return f"{int(v):,}"
    except Exception:
        return v


# ── DB helper ─────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.route("/")
def overview():
    conn = get_conn()
    total_co  = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    hit       = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=1").fetchone()[0]
    exhausted = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=2").fetchone()[0]
    pending   = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=0").fetchone()[0]
    total_ct  = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    deliverable = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='deliverable'").fetchone()[0]
    unknown     = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='unknown'").fetchone()[0]
    by_status = conn.execute(
        "SELECT email_status, COUNT(*) FROM contacts GROUP BY email_status ORDER BY COUNT(*) DESC"
    ).fetchall()
    by_source = conn.execute(
        "SELECT source, COUNT(*) FROM contacts GROUP BY source ORDER BY COUNT(*) DESC LIMIT 10"
    ).fetchall()
    conn.close()
    processed = hit + exhausted
    pct = round(processed / total_co * 100, 1) if total_co else 0
    return render_template("overview.html", active="overview", title="Overview", stats={
        "total_companies": total_co, "hit": hit, "exhausted": exhausted,
        "pending": pending, "processed": processed, "scrape_pct": pct,
        "total_contacts": total_ct, "deliverable": deliverable, "unknown": unknown,
        "by_status": by_status, "by_source": by_source,
    })


@app.route("/companies")
def companies():
    conn = get_conn()
    total     = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    success   = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=1").fetchone()[0]
    exhausted = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=2").fetchone()[0]
    pending   = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=0").fetchone()[0]
    pct       = round((success + exhausted) / total * 100, 1) if total else 0
    country_count = conn.execute(
        "SELECT COUNT(DISTINCT country) FROM companies WHERE country != '' AND country IS NOT NULL"
    ).fetchone()[0]
    countries = [r[0] for r in conn.execute(
        "SELECT DISTINCT country FROM companies WHERE country != '' ORDER BY country"
    ).fetchall()]
    conn.close()
    return render_template("companies.html", active="companies", title="Companies",
                           total=total, success=success, exhausted=exhausted,
                           pending=pending, pct=pct,
                           country_count=country_count, countries=countries)


@app.route("/contacts/verified")
def contacts_verified():
    conn = get_conn()
    total       = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status IN ('deliverable','unknown')").fetchone()[0]
    deliverable = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='deliverable'").fetchone()[0]
    unknown     = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='unknown'").fetchone()[0]
    sources     = [r[0] for r in conn.execute(
        "SELECT DISTINCT source FROM contacts WHERE source != '' ORDER BY source"
    ).fetchall()]
    conn.close()
    return render_template("contacts.html", active="verified", title="Verified Contacts",
                           mode="verified", total=total, deliverable=deliverable,
                           unknown=unknown, sources=sources)



@app.route("/pipeline")
def pipeline():
    steps = [
        {"id": "discover",  "name": "Discover Companies", "icon": "🔎",
         "desc": "Find new tech companies globally via Wikidata, HN Hiring, GitHub, DDG.", "has_limit": False},
        {"id": "wikidata",  "name": "Wikidata Executives", "icon": "📖",
         "desc": "Bulk SPARQL query — fetch CEO/Founder/COO for all known companies. Free.", "has_limit": False},
        {"id": "enrich",   "name": "DDG Enrich",          "icon": "🦆",
         "desc": "DuckDuckGo Instant API — enrich companies that yielded 0 contacts.", "has_limit": False},
        {"id": "scrape",    "name": "Scrape Companies",   "icon": "🕷",
         "desc": "Requests + Playwright + JSON-LD + Schema.org + DuckDuckGo per company.", "has_limit": True},
        {"id": "people",    "name": "Find People",        "icon": "👥",
         "desc": "LinkedIn, conferences, ProductHunt, Wellfound.", "has_limit": False},
        {"id": "apis",      "name": "API Sources",        "icon": "🔑",
         "desc": "GitHub, Crunchbase, Hunter, Apollo (requires API keys).", "has_limit": False},
        {"id": "reverse-enrich", "name": "Reverse Email Lookup", "icon": "🪪",
         "desc": "Free — Gravatar + GitHub: fill missing name, country, LinkedIn/GitHub URL from email address.",
         "has_limit": False},
        {"id": "ceo",       "name": "CEO Finder",            "icon": "🎯",
         "desc": "DDG + Ollama — finds CEO/Founder for companies with no executive contact yet.", "has_limit": False},
        {"id": "emails",    "name": "Email Finder (Web)",    "icon": "📧",
         "desc": "DDG search — finds real emails for contacts stored without one.", "has_limit": False},
        {"id": "search-db", "name": "Web Search Enrichment", "icon": "🔍",
         "desc": "Search DuckDuckGo for every contact, extract clean name/title/company/country via local Ollama LLM.",
         "has_limit": False, "has_workers": True},
    ]
    return render_template("pipeline.html", active="pipeline", title="Run Pipeline", steps=steps)


@app.route("/clean")
def clean():
    conn = get_conn()
    status_counts = conn.execute(
        "SELECT email_status, COUNT(*) FROM contacts GROUP BY email_status ORDER BY COUNT(*) DESC"
    ).fetchall()
    undeliv = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='undeliverable'").fetchone()[0]
    risky   = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='risky'").fetchone()[0]
    noemail = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='no_email' OR email LIKE '_noemail_%'").fetchone()[0]
    conn.close()
    delete_options = [
        ("Undeliverable", "undeliverable", undeliv, "danger"),
        ("Risky",         "risky",         risky,   "warning"),
        ("No Email",      "no_email",      noemail, "secondary"),
    ]
    return render_template("clean.html", active="clean", title="Verify & Clean",
                           status_counts=status_counts, delete_options=delete_options)


# ── API endpoints for DataTables (server-side) ────────────────────────────────

def _dt_response(conn, base_query, count_query, params, columns, request):
    draw   = int(request.args.get("draw", 1))
    start  = int(request.args.get("start", 0))
    length = int(request.args.get("length", 25))
    search = request.args.get("search[value]", "").strip()

    total = conn.execute(count_query, params).fetchone()[0]

    order_col   = int(request.args.get("order[0][column]", 0))
    order_dir   = "DESC" if request.args.get("order[0][dir]", "desc") == "desc" else "ASC"
    order_field = columns[order_col] if order_col < len(columns) else columns[0]
    order_field = order_field or columns[0]

    # Wrap in subquery so SELECT aliases are referenceable in WHERE/ORDER BY
    # without ambiguity across JOINed tables.
    sub = f"SELECT * FROM ({base_query}) AS _sub"

    search_params = list(params)
    where_extra   = ""
    if search:
        clauses     = " OR ".join(f"{c} LIKE ?" for c in columns if c)
        where_extra = f" WHERE ({clauses})"
        search_params += [f"%{search}%"] * sum(1 for c in columns if c)

    query    = f"{sub}{where_extra} ORDER BY {order_field} {order_dir} LIMIT ? OFFSET ?"
    rows     = conn.execute(query, search_params + [length, start]).fetchall()
    filtered = conn.execute(f"SELECT COUNT(*) FROM ({sub}{where_extra})", search_params).fetchone()[0]

    return {"draw": draw, "recordsTotal": total, "recordsFiltered": filtered,
            "data": [dict(r) for r in rows]}


@app.route("/api/companies")
def api_companies():
    conn   = get_conn()
    status  = request.args.get("status", "")
    country = request.args.get("country", "")

    where  = []
    params = []
    if status != "":
        where.append("scraped=?"); params.append(int(status))
    if country:
        where.append("country=?"); params.append(country)

    w = ("WHERE " + " AND ".join(where)) if where else ""
    base  = f"SELECT name, domain, country, source, scraped, substr(added_at,1,10) as added_at FROM companies {w}"
    count = f"SELECT COUNT(*) FROM companies {w}"
    cols  = ["name", "domain", "country", "source", "scraped", "added_at"]

    result = _dt_response(conn, base, count, params, cols, request)
    conn.close()
    return jsonify(result)


@app.route("/api/contacts/<mode>")
def api_contacts(mode):
    conn   = get_conn()
    status = request.args.get("status", "")
    source = request.args.get("source", "")

    if mode == "verified":
        base_where = "WHERE ct.email_status IN ('deliverable','unknown')"
    else:
        base_where = "WHERE ct.email_status IN ('unverified','confirmed') AND ct.email NOT LIKE '_noemail_%'"

    search_st = request.args.get("search_status", "")
    params = []
    if status:
        base_where += " AND ct.email_status=?"; params.append(status)
    if source:
        base_where += " AND ct.source=?"; params.append(source)
    if search_st == "enriched":
        base_where += " AND ct.search_status='enriched'"
    elif search_st == "not_found":
        base_where += " AND ct.search_status='not_found'"
    elif search_st == "pending":
        base_where += " AND ct.search_status IS NULL"

    base = f"""
        SELECT
            ct.first_name || ' ' || ct.last_name AS name,
            ct.title AS title,
            ct.company AS company,
            ct.company_domain AS domain,
            co.country AS country,
            ct.email AS email,
            ct.email_status AS status,
            ct.source AS source,
            substr(ct.created_at,1,10) AS added,
            ct.search_status AS search_status
        FROM contacts ct
        LEFT JOIN companies co ON ct.company_id = co.id
        {base_where}
    """
    count = f"SELECT COUNT(*) FROM contacts ct LEFT JOIN companies co ON ct.company_id=co.id {base_where}"
    cols  = ["name", "title", "company", "domain", "country", "email", "status", "source", "added", "search_status"]

    result = _dt_response(conn, base, count, params, cols, request)
    conn.close()
    return jsonify(result)


# ── Streaming run endpoints ────────────────────────────────────────────────────

def _stream_cmd(cmd):
    def generate():
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, bufsize=1)
        for line in proc.stdout:
            yield f"data: {line.rstrip()}\n\n"
        proc.wait()
        yield "data: __done__\n\n"
    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/run/<step>")
def run_step(step):
    python = sys.executable
    valid  = {"discover", "wikidata", "enrich", "scrape", "people", "apis"}

    if step == "verify-db":
        smtp = request.args.get("smtp", "true") == "true"
        cmd  = [python, "run_pipeline.py", "--verify-db"]
        if smtp:
            cmd.append("--smtp")
        return _stream_cmd(cmd)

    if step == "search-db":
        workers = request.args.get("workers", "3")
        return _stream_cmd([python, "run_pipeline.py", "--search-db", "--workers", workers])

    if step == "reverse-enrich":
        return _stream_cmd([python, "run_pipeline.py", "--reverse-enrich"])

    if step not in valid:
        return "Invalid step", 400

    cmd = [python, "run_pipeline.py", "--step", step]
    if step == "scrape":
        limit   = request.args.get("limit", "1000")
        workers = request.args.get("workers", "30")
        cmd += ["--limit", limit, "--workers", workers]

    return _stream_cmd(cmd)


# ── Clean actions ─────────────────────────────────────────────────────────────

@app.route("/clean/delete", methods=["POST"])
def clean_delete():
    status = request.form.get("status", "")
    if status not in ("undeliverable", "risky", "no_email"):
        return "Invalid", 400
    conn = get_conn()
    if status == "no_email":
        conn.execute("DELETE FROM contacts WHERE email_status='no_email' OR email LIKE '_noemail_%'")
    else:
        conn.execute("DELETE FROM contacts WHERE email_status=?", (status,))
    conn.commit(); conn.close()
    return redirect("/clean")


@app.route("/clean/delete-all", methods=["POST"])
def clean_delete_all():
    conn = get_conn()
    conn.execute("""
        DELETE FROM contacts
        WHERE email_status IN ('undeliverable','risky','no_email')
           OR email LIKE '_noemail_%'
    """)
    conn.commit(); conn.close()
    return redirect("/clean")


# ── Export ────────────────────────────────────────────────────────────────────

@app.route("/export/<mode>")
def export_csv(mode):
    conn = get_conn()
    if mode == "verified":
        where = "WHERE ct.email_status IN ('deliverable','unknown')"
    else:
        where = "WHERE ct.email_status IN ('unverified','confirmed') AND ct.email NOT LIKE '_noemail_%'"

    rows = conn.execute(f"""
        SELECT ct.first_name, ct.last_name, ct.title, ct.company, ct.company_domain,
               co.country, ct.email, ct.email_status, ct.source
        FROM contacts ct
        LEFT JOIN companies co ON ct.company_id = co.id
        {where}
        ORDER BY ct.created_at DESC
    """).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["First Name","Last Name","Title","Company","Domain","Country","Email","Status","Source"])
    writer.writerows(rows)

    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=contacts_{mode}.csv"})


# ── Outreach ──────────────────────────────────────────────────────────────────

OUTREACH_FILTER = """
    email NOT LIKE '_noemail_%'
    AND email != ''
    AND company IS NOT NULL AND company != ''
"""

@app.route("/responses")
def responses_page():
    from pipeline.response_collector import init_responses_table
    conn = get_conn()
    init_responses_table(conn)
    total_bounces = conn.execute(
        "SELECT COUNT(*) FROM responses WHERE type='delivery_failure' AND processed=1"
    ).fetchone()[0]
    total_autos = conn.execute(
        "SELECT COUNT(*) FROM responses WHERE type='auto_reply' AND processed=1"
    ).fetchone()[0]
    contacts_removed = conn.execute(
        "SELECT COUNT(*) FROM responses WHERE type='delivery_failure' AND processed=1 AND contact_id IS NOT NULL"
    ).fetchone()[0]
    pending_count = conn.execute(
        "SELECT COUNT(*) FROM responses WHERE processed=0"
    ).fetchone()[0]
    recent = [dict(r) for r in conn.execute("""
        SELECT r.id, r.type, r.from_email, r.new_email, r.subject,
               r.received_at, r.processed_at, c.company
        FROM responses r
        LEFT JOIN contacts c ON c.id = r.contact_id
        WHERE r.processed = 1
        ORDER BY r.id DESC
        LIMIT 30
    """).fetchall()]
    conn.close()
    with _runner_lock:
        runner = dict(_runner_state)
    return render_template("responses.html", active="responses", title="Responses",
                           total_bounces=total_bounces,
                           total_autos=total_autos,
                           contacts_removed=contacts_removed,
                           pending_count=pending_count,
                           recent=recent,
                           runner=runner)


@app.route("/responses/runner-status")
def responses_runner_status():
    with _runner_lock:
        state = dict(_runner_state)
    return jsonify(state)


@app.route("/responses/collect", methods=["POST"])
def responses_collect():
    from pipeline.response_collector import connect_imap, collect, init_responses_table
    data       = request.get_json()
    gmail_user = data.get("email", "").strip()
    password   = data.get("password", "").strip()

    def generate():
        if not gmail_user or not password:
            yield f"data: {json.dumps({'error': 'Credentials missing'})}\n\n"
            return
        yield f"data: {json.dumps({'step': 'connecting'})}\n\n"
        try:
            conn = get_conn()
            init_responses_table(conn)
            mail = connect_imap(gmail_user, password)
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            return
        yield f"data: {json.dumps({'step': 'scanning'})}\n\n"
        try:
            saved = collect(mail, gmail_user, conn)
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            return
        finally:
            try: mail.logout()
            except: pass
            conn.close()
        yield f"data: {json.dumps({'step': 'done', 'count': len(saved), 'items': saved})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/responses/process", methods=["POST"])
def responses_process():
    from pipeline.response_collector import connect_imap, process_one, init_responses_table
    data       = request.get_json()
    gmail_user = data.get("email", "").strip()
    password   = data.get("password", "").strip()

    def generate():
        if not gmail_user or not password:
            yield f"data: {json.dumps({'error': 'Credentials missing'})}\n\n"
            return
        conn = get_conn()
        init_responses_table(conn)
        pending = [dict(r) for r in conn.execute(
            "SELECT * FROM responses WHERE processed=0 ORDER BY type, id"
        ).fetchall()]
        if not pending:
            yield f"data: {json.dumps({'step': 'done', 'total': 0})}\n\n"
            conn.close()
            return
        try:
            mail = connect_imap(gmail_user, password)
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            conn.close()
            return
        yield f"data: {json.dumps({'step': 'start', 'total': len(pending)})}\n\n"
        for i, resp in enumerate(pending, 1):
            result = process_one(mail, resp, conn)
            yield f"data: {json.dumps({'step': 'item', 'index': i, 'total': len(pending), 'type': resp['type'], 'email': resp['from_email'], 'result': result})}\n\n"
        try: mail.logout()
        except: pass
        conn.close()
        yield f"data: {json.dumps({'step': 'done', 'total': len(pending)})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/responses/process-one/<int:resp_id>", methods=["POST"])
def responses_process_one(resp_id):
    from pipeline.response_collector import connect_imap, process_one, init_responses_table
    data       = request.get_json()
    gmail_user = data.get("email", "").strip()
    password   = data.get("password", "").strip()
    if not gmail_user or not password:
        return json.dumps({"error": "Credentials missing"}), 400
    conn = get_conn()
    init_responses_table(conn)
    row = conn.execute("SELECT * FROM responses WHERE id=?", (resp_id,)).fetchone()
    if not row:
        conn.close()
        return json.dumps({"error": "Not found"}), 404
    try:
        mail = connect_imap(gmail_user, password)
        result = process_one(mail, dict(row), conn)
        try: mail.logout()
        except: pass
    except Exception as e:
        conn.close()
        return json.dumps({"error": str(e)}), 500
    conn.close()
    return json.dumps({"result": result})


@app.route("/responses/list-folders", methods=["POST"])
def responses_list_folders():
    from pipeline.response_collector import connect_imap
    data       = request.get_json()
    gmail_user = data.get("email", "").strip()
    password   = data.get("password", "").strip()
    if not gmail_user or not password:
        return json.dumps({"error": "Credentials missing"}), 400
    try:
        mail    = connect_imap(gmail_user, password)
        _, data = mail.list()
        folders = []
        for item in data:
            if item:
                parts = item.decode() if isinstance(item, bytes) else item
                folders.append(parts)
        mail.logout()
        return json.dumps({"folders": folders})
    except Exception as e:
        return json.dumps({"error": str(e)}), 500


@app.route("/responses/reclassify/<int:resp_id>", methods=["POST"])
def responses_reclassify(resp_id):
    data     = request.get_json()
    new_type = data.get("type", "").strip()
    if new_type not in ("human_reply", "auto_reply", "delivery_failure"):
        return json.dumps({"error": "Invalid type"}), 400
    conn = get_conn()
    conn.execute("UPDATE responses SET type=?, processed=0 WHERE id=?", (new_type, resp_id))
    conn.commit()
    conn.close()
    return json.dumps({"ok": True})




@app.route("/outreach")
def outreach():
    conn = get_conn()
    total  = conn.execute(
        f"SELECT COUNT(*) FROM contacts WHERE {OUTREACH_FILTER}"
    ).fetchone()[0]
    sent   = conn.execute("SELECT COUNT(*) FROM outreach WHERE status='sent'").fetchone()[0]
    failed = conn.execute("SELECT COUNT(*) FROM outreach WHERE status='failed'").fetchone()[0]
    drafts = conn.execute("SELECT COUNT(*) FROM outreach WHERE status='draft'").fetchone()[0]
    countries = [r[0] for r in conn.execute(
        f"SELECT DISTINCT country FROM contacts WHERE {OUTREACH_FILTER} "
        "AND country IS NOT NULL AND country!='' ORDER BY country"
    ).fetchall()]
    conn.close()
    cv_ready = (UPLOAD_DIR / "cv.pdf").exists()
    return render_template("outreach.html", active="outreach", title="Outreach",
                           total=total, sent=sent, failed=failed, drafts=drafts,
                           countries=countries, cv_ready=cv_ready)


@app.route("/outreach/upload-cv", methods=["POST"])
def outreach_upload_cv():
    f = request.files.get("cv")
    if not f or not f.filename.endswith(".pdf"):
        return jsonify({"ok": False, "error": "Please upload a PDF file"}), 400
    f.save(UPLOAD_DIR / "cv.pdf")
    return jsonify({"ok": True})


@app.route("/outreach/test-email", methods=["POST"])
def outreach_test_email():
    from pipeline.email_sender import test_connection
    data     = request.get_json()
    email    = data.get("email", "").strip()
    password = data.get("password", "").strip()
    if not email or not password:
        return jsonify({"ok": False, "error": "Email and password required"})
    ok, err = test_connection(email, password)
    return jsonify({"ok": ok, "error": err})


@app.route("/api/outreach/contacts")
def api_outreach_contacts():
    conn    = get_conn()
    country = request.args.get("country", "")
    status_f = request.args.get("status", "")   # all / sent / unsent

    base_where = f"""
        WHERE {OUTREACH_FILTER}
    """
    params = []
    if country:
        base_where += " AND ct.country=?"; params.append(country)
    if status_f == "sent":
        base_where += " AND EXISTS (SELECT 1 FROM outreach o WHERE o.contact_id=ct.id AND o.status='sent')"
    elif status_f == "unsent":
        base_where += " AND NOT EXISTS (SELECT 1 FROM outreach o WHERE o.contact_id=ct.id AND o.status='sent')"

    base = f"""
        SELECT
            ct.id,
            TRIM(ct.first_name || ' ' || COALESCE(ct.last_name,'')) AS name,
            ct.title,
            ct.company,
            ct.company_domain AS domain,
            ct.country,
            ct.email,
            ct.email_status AS status,
            CASE WHEN EXISTS (SELECT 1 FROM outreach o WHERE o.contact_id=ct.id AND o.status='sent')
                 THEN 'sent' ELSE 'unsent' END AS outreach_status
        FROM contacts ct
        {base_where}
    """
    count = f"SELECT COUNT(*) FROM contacts ct {base_where}"
    cols  = ["ct.id", "name", "ct.title", "ct.company", "domain", "ct.country", "ct.email", "status", "outreach_status"]

    result = _dt_response(conn, base, count, params, cols, request)
    conn.close()
    return jsonify(result)


@app.route("/outreach/preview/<int:contact_id>")
def outreach_preview(contact_id):
    """Generate and return a sample letter for a contact (used by modal preview)."""
    from pipeline.cv_parser       import parse_cv
    from pipeline.letter_generator import generate_letter
    cv_path = UPLOAD_DIR / "cv.pdf"
    if not cv_path.exists():
        return jsonify({"ok": False, "error": "No CV uploaded yet"})
    conn = get_conn()
    row  = conn.execute(
        "SELECT id, first_name, last_name, title, company, company_domain, email "
        "FROM contacts WHERE id=?", (contact_id,)
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"ok": False, "error": "Contact not found"})
    from pipeline.letter_generator import letter_tier
    cv      = parse_cv(str(cv_path))
    contact = dict(row)
    letter  = generate_letter(contact, cv)
    tier    = letter_tier(contact)
    subject = f"Speculative Application – {cv.get('name') or 'Candidate'}"
    return jsonify({"ok": True, "letter": letter, "subject": subject,
                    "to": row["email"], "company": row["company"], "tier": tier})


@app.route("/outreach/send", methods=["POST"])
def outreach_send():
    """SSE stream — generates letter and sends email for each selected contact."""
    from pipeline.cv_parser      import parse_cv
    from pipeline.letter_generator import generate_letter
    from pipeline.email_sender   import send_application

    data        = request.get_json()
    sender      = data.get("email", "").strip()
    password    = data.get("password", "").strip()
    reply_to    = data.get("reply_to", "").strip()
    contact_ids = data.get("contact_ids", [])

    cv_path = UPLOAD_DIR / "cv.pdf"

    def generate():
        if not cv_path.exists():
            yield f"data: {json.dumps({'error': 'No CV uploaded'})}\n\n"
            return
        if not sender or not password:
            yield f"data: {json.dumps({'error': 'Email credentials missing'})}\n\n"
            return
        if not contact_ids:
            yield f"data: {json.dumps({'error': 'No contacts selected'})}\n\n"
            return

        cv = parse_cv(str(cv_path))
        conn = get_conn()

        for i, cid in enumerate(contact_ids):
            row = conn.execute(
                "SELECT ct.id, ct.first_name, ct.last_name, ct.title, ct.company, ct.company_domain, ct.email "
                "FROM contacts ct WHERE ct.id=?", (cid,)
            ).fetchone()
            if not row:
                continue

            contact = dict(row)
            yield f"data: {json.dumps({'step': 'generating', 'index': i+1, 'total': len(contact_ids), 'company': contact['company']})}\n\n"

            letter = generate_letter(contact, cv)

            yield f"data: {json.dumps({'step': 'sending', 'index': i+1, 'company': contact['company']})}\n\n"

            ok, err = send_application(
                sender_email    = sender,
                app_password    = password,
                recipient_email = contact["email"],
                recipient_name  = f"{contact['first_name']} {contact['last_name']}".strip(),
                company_name    = contact["company"],
                candidate_name  = cv.get("name") or sender.split("@")[0],
                letter_body     = letter,
                cv_path         = str(cv_path),
                reply_to        = reply_to,
            )

            status = "sent" if ok else "failed"
            conn.execute(
                "INSERT INTO outreach (contact_id, email, company, domain, letter, sent_at, status, error) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (contact["id"], contact["email"], contact["company"],
                 contact["company_domain"], letter,
                 datetime.now(timezone.utc).isoformat(), status, err)
            )
            conn.commit()

            yield f"data: {json.dumps({'step': 'done', 'index': i+1, 'company': contact['company'], 'status': status, 'error': err})}\n\n"
            time.sleep(2)   # avoid Gmail rate limit

        conn.close()
        yield f"data: {json.dumps({'step': 'finished', 'total': len(contact_ids)})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/outreach/cooldown")
def outreach_cooldown():
    stats = _session_stats()
    return jsonify({
        "remaining_secs": stats["cooldown_secs_left"],
        "sent":           stats["sent"],
        "remaining":      stats["remaining"],
    })


@app.route("/outreach/apply-signature", methods=["POST"])
def outreach_apply_signature():
    """Rewrite all draft letters to use the standard signature."""
    from pipeline.letter_generator import apply_signature
    conn  = get_conn()
    rows  = conn.execute("SELECT id, letter FROM outreach WHERE status='draft'").fetchall()
    updated = 0
    for row in rows:
        new_letter = apply_signature(row['letter'])
        if new_letter != row['letter']:
            conn.execute("UPDATE outreach SET letter=? WHERE id=?", (new_letter, row['id']))
            updated += 1
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "updated": updated})


@app.route("/outreach/prepare/start", methods=["POST"])
def outreach_prepare_start():
    global _prepare_thread, _prepare_stop
    data    = request.get_json() or {}
    workers = max(1, min(int(data.get('workers', 1)), 8))
    with _prepare_lock:
        if _prepare_status.get('running'):
            return jsonify({"ok": False, "error": "Already running"})
        _prepare_stop = False
        _prepare_status['running'] = True
        _prepare_status['workers'] = workers
        _prepare_status['error']   = ''
    _prepare_thread = threading.Thread(
        target=_run_prepare, args=(workers,), daemon=True
    )
    _prepare_thread.start()
    return jsonify({"ok": True, "workers": workers})


@app.route("/outreach/prepare/stop", methods=["POST"])
def outreach_prepare_stop():
    global _prepare_stop
    import signal, subprocess as _sp
    _prepare_stop = True
    for proc in _prepare_extra_procs:
        try:
            # Kill the whole process group so child runner processes die too
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass
    _prepare_extra_procs.clear()
    return jsonify({"ok": True})


@app.route("/outreach/prepare/status")
def outreach_prepare_status():
    conn        = get_conn()
    ready_count = conn.execute("SELECT COUNT(*) FROM outreach WHERE status='draft'").fetchone()[0]
    conn.close()
    with _prepare_lock:
        status = dict(_prepare_status)
    status['ready_count'] = ready_count
    return jsonify(status)


@app.route("/outreach/send-batch/stop", methods=["POST"])
def outreach_send_stop():
    global _stop_flag
    _stop_flag = True
    return jsonify({"ok": True})


@app.route("/outreach/send-batch", methods=["POST"])
def outreach_send_batch():
    """SSE — sends up to `limit` pre-prepared draft letters. No LLM calls."""
    from pipeline.cv_parser    import parse_cv
    from pipeline.email_sender import open_smtp, send_with_server

    data     = request.get_json()
    sender   = data.get("email", "").strip()
    password = data.get("password", "").strip()
    reply_to = data.get("reply_to", "").strip()
    limit    = int(data.get("limit", BATCH_SIZE))
    cv_path  = UPLOAD_DIR / "cv.pdf"

    def generate():
        stats = _session_stats()
        if stats["cooldown_secs_left"] > 0:
            yield f"data: {json.dumps({'error': 'cooldown', 'remaining_secs': stats['cooldown_secs_left']})}\n\n"
            return
        if stats["remaining"] == 0:
            yield f"data: {json.dumps({'error': 'cooldown', 'remaining_secs': 1})}\n\n"
            return
        if not cv_path.exists():
            yield f"data: {json.dumps({'error': 'No CV uploaded'})}\n\n"
            return
        if not sender or not password:
            yield f"data: {json.dumps({'error': 'Email credentials missing'})}\n\n"
            return

        cv   = parse_cv(str(cv_path))
        conn = get_conn()

        batch = conn.execute("""
            SELECT o.id, o.contact_id, o.email, o.company, o.domain, o.letter,
                   ct.first_name, ct.last_name
            FROM outreach o
            JOIN contacts ct ON ct.id = o.contact_id
            WHERE o.status = 'draft'
            ORDER BY o.id
            LIMIT ?
        """, (limit,)).fetchall()

        total = len(batch)
        if total == 0:
            yield f"data: {json.dumps({'error': 'No prepared letters ready — run Prepare first.'})}\n\n"
            conn.close()
            return

        # Open one persistent SMTP connection for the whole batch
        try:
            smtp_server = open_smtp(sender, password)
        except Exception as e:
            yield f"data: {json.dumps({'error': f'SMTP connection failed: {e}'})}\n\n"
            conn.close()
            return

        global _stop_flag
        _stop_flag   = False
        smtp_limited = False
        # Cap batch size to what's left in this session
        session_remaining = _session_stats()["remaining"]
        batch = batch[:session_remaining]
        total = len(batch)
        if total == 0:
            yield f"data: {json.dumps({'error': 'No prepared letters ready — run Prepare first.'})}\n\n"
            conn.close()
            return
        yield f"data: {json.dumps({'step': 'start', 'total': total})}\n\n"

        candidate_name = cv.get('name') or sender.split('@')[0]
        sent_count     = 0

        try:
            for i, row in enumerate(batch):
                if _stop_flag:
                    yield f"data: {json.dumps({'step': 'stopped', 'index': i, 'total': total})}\n\n"
                    break

                contact = dict(row)
                yield f"data: {json.dumps({'step': 'sending', 'index': i+1, 'total': total, 'company': contact['company']})}\n\n"

                ok, err, smtp_server = send_with_server(
                    server          = smtp_server,
                    sender_email    = sender,
                    app_password    = password,
                    recipient_email = contact['email'],
                    candidate_name  = candidate_name,
                    letter_body     = contact['letter'],
                    cv_path         = str(cv_path),
                    reply_to        = reply_to,
                )

                # Gmail SMTP rate limit — stop the batch, don't lock cooldown
                if err == 'DAILY_LIMIT_EXCEEDED':
                    yield f"data: {json.dumps({'step': 'smtp_limited', 'index': i+1, 'sent': sent_count})}\n\n"
                    smtp_limited = True
                    break

                status = 'sent' if ok else 'failed'
                conn.execute(
                    "UPDATE outreach SET status=?, sent_at=?, error=? WHERE id=?",
                    (status, datetime.now(timezone.utc).isoformat(), err, contact['id']),
                )
                conn.commit()
                if ok:
                    sent_count += 1
                    _record_sent(1)

                yield f"data: {json.dumps({'step': 'done', 'index': i+1, 'total': total, 'company': contact['company'], 'status': status, 'error': err})}\n\n"

                # Keep-alive NOOP every 20 emails to prevent idle timeout
                if ok and sent_count % 20 == 0:
                    try:
                        smtp_server.noop()
                    except Exception:
                        try:
                            smtp_server = open_smtp(sender, password)
                        except Exception:
                            pass

                time.sleep(2)
        finally:
            try:
                smtp_server.quit()
            except Exception:
                pass
            conn.close()

        if not smtp_limited:
            yield f"data: {json.dumps({'step': 'finished', 'total': total, 'sent': sent_count})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# Start response auto-scanner background thread
threading.Thread(target=_run_response_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(debug=True, port=5000, threaded=True)
