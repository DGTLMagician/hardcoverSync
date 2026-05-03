# Hardcover Sync

Bidirectional sync daemon + web dashboard connecting **Hardcover** → **Calibre Web Automated (CWA)** → **Shelfmark**.

```
Hardcover (your shelves)
    ↕  GraphQL API
Hardcover Sync  ──→  CWA library check (SQLite)
    └──→  Shelfmark (if book missing, trigger download)
```

## What it does

Every N minutes (default: 15) the sync daemon:

1. **Fetches** your Hardcover want-to-read / currently reading / read list via GraphQL
2. **Checks** the CWA Calibre library (`metadata.db`) for each book — by ISBN-13 first, then title + author fuzzy match
3. **Triggers a Shelfmark download request** for any book not found in CWA
4. **Streams logs** in real-time to the web dashboard via WebSocket

---

## Quick start

### 1 — Configure

```bash
cp .env.example .env
nano .env
```

Minimum required settings:

```env
HARDCOVER_API_TOKEN=your_token_from_hardcover_settings
CWA_DB_PATH=/path/to/your/calibre/library/metadata.db
SHELFMARK_URL=http://localhost:8084
```

Get your Hardcover token at: https://hardcover.app/account/api

### 2a — Run with Python (no Docker)

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Open http://localhost:5055

### 2b — Run with Docker Compose

```bash
# Edit CWA_LIBRARY_PATH in docker-compose.yml first
docker compose up -d
```

---

## Configuration reference

| Variable | Default | Description |
|---|---|---|
| `HARDCOVER_API_TOKEN` | — | **Required.** From Hardcover → Settings → API |
| `HARDCOVER_API_URL` | `https://api.hardcover.app/v1/graphql` | GraphQL endpoint |
| `CWA_DB_PATH` | `/books/metadata.db` | Path to Calibre `metadata.db` |
| `CWA_URL` | `http://localhost:8083` | CWA base URL |
| `SHELFMARK_URL` | `http://localhost:8084` | Shelfmark base URL |
| `SHELFMARK_API_KEY` | — | API key if Shelfmark auth is enabled |
| `SHELFMARK_FORMAT` | `epub` | Preferred download format: epub/mobi/pdf/azw3 |
| `SHELFMARK_LANGUAGE` | `en` | Preferred language code |
| `SYNC_INTERVAL_MINUTES` | `15` | Background sync interval |
| `SYNC_STATUSES` | `1,2,3` | Hardcover status IDs: 1=want 2=reading 3=read 5=dnf |
| `AUTO_DOWNLOAD` | `true` | Trigger Shelfmark for missing books |
| `SYNC_READING_PROGRESS` | `true` | Push Calibre read data back to Hardcover |
| `WEB_HOST` | `0.0.0.0` | Dashboard listen address |
| `WEB_PORT` | `5055` | Dashboard port |
| `SECRET_KEY` | — | Flask session secret — change this! |

All settings are also editable live from the **Configuration** tab in the dashboard (written back to `.env`).

---

## Hardcover status IDs

| ID | Label |
|---|---|
| 1 | Want to Read |
| 2 | Currently Reading |
| 3 | Read |
| 5 | Did Not Finish |

---

## CWA library matching

Books are matched against the Calibre library in this order:

1. **ISBN-13** — exact match via the `identifiers` table (most reliable)
2. **Title + Author** — normalised lowercase fuzzy match

If a book is not found → Shelfmark is called with the book's ISBN-13 (if available) or title+author as the search term.

---

## Shelfmark integration

Shelfmark's built-in API is minimal by design (it is a manual tool). The sync daemon tries:

1. `GET /api/search?query=<isbn or title>` → pick first result → `POST /api/download`
2. Fallback: `POST /api/request` with title/author/isbn body

Point your Shelfmark download folder to your CWA ingest folder (`/cwa-book-ingest`) so downloaded books are automatically imported.

---

## Web dashboard

| Route | Description |
|---|---|
| `GET /` | Dashboard (Books / Live Log / Configuration) |
| `POST /api/sync` | Trigger manual sync |
| `GET /api/status` | JSON status |
| `GET /api/books` | Last sync book list |
| `GET /api/logs` | Log entries |
| `GET /api/config` | Sanitised config |
| `POST /api/config` | Update config |

WebSocket (`socket.io`) is used for real-time log streaming.

---

## Architecture

```
app.py          Flask + SocketIO web server + APScheduler
sync.py         Core sync logic (Hardcover, CWA, Shelfmark)
templates/      Jinja2 HTML dashboard
.env            All configuration
```
