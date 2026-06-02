import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template, request, Response, stream_with_context

sys.path.insert(0, str(Path(__file__).parent.parent))

from sender.config    import load as load_config, save as save_config
from sender.cv_parser import extract_text, parse_fields
from sender.mailer    import start_batch, get_progress, render

UPLOAD_DIR = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

DB_PATH = Path(__file__).parent.parent / "output" / "leads.db"

app = Flask(__name__, template_folder="templates")


def _migrate():
    conn = sqlite3.connect(str(DB_PATH))
    cols = {r[1] for r in conn.execute("PRAGMA table_info(contacts)").fetchall()}
    if "outreach_sent_at" not in cols:
        conn.execute("ALTER TABLE contacts ADD COLUMN outreach_sent_at TEXT")
        conn.commit()
    conn.close()


_migrate()


def _stats() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    sendable  = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE email NOT LIKE '_noemail_%' AND email_status != 'undeliverable'"
    ).fetchone()[0]
    sent      = conn.execute("SELECT COUNT(*) FROM contacts WHERE outreach_status = 'sent'").fetchone()[0]
    failed    = conn.execute("SELECT COUNT(*) FROM contacts WHERE outreach_status = 'failed'").fetchone()[0]
    remaining = sendable - sent - failed
    conn.close()
    return {"sendable": sendable, "sent": sent, "failed": failed, "remaining": max(remaining, 0)}


def _cooldown_secs(config: dict) -> int:
    last = config.get("last_batch_at")
    if not last:
        return 0
    try:
        elapsed = time.time() - datetime.fromisoformat(last).timestamp()
        return max(0, int(86400 - elapsed))
    except Exception:
        return 0


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state")
def api_state():
    config = load_config()
    return jsonify({
        "config":       config,
        "stats":        _stats(),
        "cooldown_secs": _cooldown_secs(config),
        "progress":     get_progress(),
    })


@app.route("/api/config", methods=["POST"])
def api_save_config():
    data   = request.get_json() or {}
    config = load_config()
    allowed = {
        "your_name", "your_email", "gmail_app_password",
        "your_university", "your_graduation", "your_internship_company",
        "template_subject", "template_body", "batch_size",
    }
    for k, v in data.items():
        if k in allowed:
            config[k] = v
    save_config(config)
    return jsonify({"ok": True})


@app.route("/api/upload-cv", methods=["POST"])
def api_upload_cv():
    f = request.files.get("cv")
    if not f:
        return jsonify({"error": "No file uploaded"}), 400
    path = UPLOAD_DIR / "cv.pdf"
    f.save(str(path))
    text   = extract_text(str(path))
    fields = parse_fields(text)
    return jsonify({"fields": fields, "preview": text[:600]})


@app.route("/api/preview")
def api_preview():
    config = load_config()
    # Show two examples: one with name, one without
    with_name    = {"first_name": "Daniel",  "company": "TechCorp GmbH"}
    without_name = {"first_name": "",        "company": "Qleap Networks"}
    return jsonify({
        "with_name": {
            "subject": render(config["template_subject"], with_name,    config),
            "body":    render(config["template_body"],    with_name,    config),
        },
        "without_name": {
            "subject": render(config["template_subject"], without_name, config),
            "body":    render(config["template_body"],    without_name, config),
        },
    })


@app.route("/api/send", methods=["POST"])
def api_send():
    config = load_config()

    if not config.get("your_email") or not config.get("gmail_app_password"):
        return jsonify({"error": "Gmail credentials not set"}), 400
    if not config.get("your_name"):
        return jsonify({"error": "Your name is not set"}), 400

    remaining = _cooldown_secs(config)
    if remaining > 0:
        return jsonify({"error": "cooldown", "remaining_secs": remaining}), 429

    prog = get_progress()
    if prog.get("running"):
        return jsonify({"error": "A batch is already running"}), 409

    config["last_batch_at"] = datetime.now(timezone.utc).isoformat()
    save_config(config)

    start_batch(config)
    return jsonify({"ok": True})


@app.route("/api/progress-stream")
def api_progress_stream():
    def generate():
        while True:
            prog = get_progress()
            yield f"data: {json.dumps(prog)}\n\n"
            if not prog.get("running"):
                break
            time.sleep(1)
    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    app.run(debug=True, port=5051)
