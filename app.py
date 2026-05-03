"""
app.py  —  Hardcover ↔ CWA ↔ Shelfmark sync daemon + web dashboard
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask, render_template, jsonify, request, redirect, url_for
from flask_socketio import SocketIO, emit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from sync import run_sync, HARDCOVER_STATUS_LABELS

# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("hardcover_sync.app")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change_me")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ─────────────────────────────────────────────────────────────────────────────
# Shared state (in-process; survives restarts only if you persist to JSON)
# ─────────────────────────────────────────────────────────────────────────────

state = {
    "running": False,
    "last_sync_start": None,
    "last_sync_end": None,
    "last_sync_result": None,
    "last_sync_books": [],
    "log": [],
    "next_run": None,
}


def get_config() -> dict:
    """Build config dict from current env (re-read on each sync so .env edits are picked up)."""
    load_dotenv(override=True)

    raw_statuses = os.getenv("SYNC_STATUSES", "1,2,3")
    try:
        status_ids = [int(s.strip()) for s in raw_statuses.split(",") if s.strip()]
    except ValueError:
        status_ids = [1, 2, 3]

    return {
        "hardcover_token": os.getenv("HARDCOVER_API_TOKEN", ""),
        "hardcover_api_url": os.getenv("HARDCOVER_API_URL", "https://api.hardcover.app/v1/graphql"),
        "cwa_db_path": os.getenv("CWA_DB_PATH", "/books/metadata.db"),
        "cwa_url": os.getenv("CWA_URL", "http://localhost:8083"),
        "shelfmark_url": os.getenv("SHELFMARK_URL", "http://localhost:8084"),
        "shelfmark_api_key": os.getenv("SHELFMARK_API_KEY", ""),
        "shelfmark_format": os.getenv("SHELFMARK_FORMAT", "epub"),
        "shelfmark_language": os.getenv("SHELFMARK_LANGUAGE", "en"),
        "kobo_db_path": os.getenv("KOBO_DB_PATH", ""),
        "kobo_api_url": os.getenv("KOBO_API_URL", ""),
        "kobo_email": os.getenv("KOBO_EMAIL", ""),
        "kobo_password": os.getenv("KOBO_PASSWORD", ""),
        "llm_base_url": os.getenv("LLM_BASE_URL", "http://localhost:11434/v1"),
        "llm_api_key": os.getenv("LLM_API_KEY", ""),
        "llm_model": os.getenv("LLM_MODEL", "llama3.2:3b"),
        "sync_statuses": status_ids,
        "auto_download": os.getenv("AUTO_DOWNLOAD", "true").lower() == "true",
        "auto_fix_series": os.getenv("AUTO_FIX_SERIES", "true").lower() == "true",
        "sync_reading_progress": os.getenv("SYNC_READING_PROGRESS", "true").lower() == "true",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Sync job
# ─────────────────────────────────────────────────────────────────────────────

def emit_log_entry(entry: dict):
    """Push a log entry to all connected WebSocket clients."""
    socketio.emit("log_entry", entry, namespace="/")


def do_sync():
    """Called by scheduler and manual trigger."""
    if state["running"]:
        logger.info("Sync already running, skipping.")
        return
    config = get_config()
    try:
        run_sync(config, state, emit_log=emit_log_entry)
        socketio.emit("sync_complete", state["last_sync_result"], namespace="/")
    except Exception as e:
        logger.exception("Unhandled error during sync: %s", e)
        state["running"] = False
        state["log"].append({
            "time": datetime.now().strftime("%H:%M:%S"),
            "level": "error",
            "msg": f"Unhandled sync error: {e}",
        })


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler
# ─────────────────────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler(daemon=True)


def start_scheduler():
    interval = int(os.getenv("SYNC_INTERVAL_MINUTES", "15"))
    scheduler.add_job(
        do_sync,
        trigger=IntervalTrigger(minutes=interval),
        id="sync_job",
        replace_existing=True,
        next_run_time=None,  # Don't run immediately on startup
    )
    scheduler.start()
    logger.info("Scheduler started — sync every %d minute(s)", interval)


def _update_next_run():
    job = scheduler.get_job("sync_job")
    if job and job.next_run_time:
        state["next_run"] = job.next_run_time.strftime("%H:%M:%S")
    else:
        state["next_run"] = None


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    config = get_config()
    interval = int(os.getenv("SYNC_INTERVAL_MINUTES", "15"))
    _update_next_run()
    return render_template(
        "index.html",
        state=state,
        config=config,
        interval=interval,
        status_labels=HARDCOVER_STATUS_LABELS,
    )


@app.route("/api/status")
def api_status():
    _update_next_run()
    return jsonify({
        "running": state["running"],
        "last_sync_start": state["last_sync_start"],
        "last_sync_end": state["last_sync_end"],
        "last_sync_result": state["last_sync_result"],
        "next_run": state["next_run"],
        "book_count": len(state["last_sync_books"]),
    })


@app.route("/api/books")
def api_books():
    return jsonify(state["last_sync_books"])


@app.route("/api/logs")
def api_logs():
    limit = int(request.args.get("limit", 200))
    return jsonify(state["log"][-limit:])


@app.route("/api/sync", methods=["POST"])
def api_sync_now():
    """Manual sync trigger via API or UI button."""
    if state["running"]:
        return jsonify({"ok": False, "message": "Sync already running"}), 409
    socketio.start_background_task(do_sync)
    return jsonify({"ok": True, "message": "Sync started"})


@app.route("/api/config", methods=["GET"])
def api_config_get():
    """Return sanitised config (no secrets)."""
    config = get_config()
    safe = {k: v for k, v in config.items() if "token" not in k and "password" not in k and "key" not in k}
    return jsonify(safe)


@app.route("/api/suggestions")
def api_suggestions():
    config = get_config()
    # Get read books from state
    read_books = [book for book in state.get("last_sync_books", []) if book.get("status_id") == 3]
    suggestions = get_ai_suggestions(read_books, config)
    return jsonify(suggestions)


@app.route("/api/add_want_to_read", methods=["POST"])
def api_add_want_to_read():
    from sync import add_hardcover_want_to_read
    data = request.json or {}
    book_id = data.get("book_id")
    if not book_id:
        return jsonify({"ok": False, "message": "Book ID required"}), 400

    config = get_config()
    success = add_hardcover_want_to_read(int(book_id), config["hardcover_token"], config["hardcover_api_url"])
    if success:
        return jsonify({"ok": True})
    else:
        return jsonify({"ok": False, "message": "Failed to add book"}), 500


@app.route("/api/import_goodreads", methods=["POST"])
def api_import_goodreads():
    from sync import import_goodreads_to_hardcover
    data = request.json or {}
    rss_url = data.get("rss_url")
    status_id = data.get("status_id", 3)  # Default to Read

    if not rss_url:
        return jsonify({"ok": False, "message": "RSS URL required"}), 400

    config = get_config()
    result = import_goodreads_to_hardcover(rss_url, config["hardcover_token"], config["hardcover_api_url"], status_id)
    return jsonify({"ok": True, "result": result})


@app.route("/api/fix_series")
def api_fix_series():
    from sync import fix_missing_series_books
    config = get_config()
    result = fix_missing_series_books(config["hardcover_token"], config["hardcover_api_url"])
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket events
# ─────────────────────────────────────────────────────────────────────────────

@socketio.on("connect")
def ws_connect():
    # Send current log history to newly connected client
    _update_next_run()
    emit("init", {
        "log": state["log"][-100:],
        "running": state["running"],
        "next_run": state["next_run"],
    })


@socketio.on("sync_now")
def ws_sync_now():
    if not state["running"]:
        socketio.start_background_task(do_sync)
        emit("ack", {"message": "Sync started"})
    else:
        emit("ack", {"message": "Already running"})


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    start_scheduler()
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", "5055"))
    logger.info("Starting Hardcover Sync dashboard on http://%s:%d", host, port)
    socketio.run(app, host=host, port=port, debug=False, allow_unsafe_werkzeug=True)
