"""
cwa_kobo_sync.py
Synchronizes Calibre-Web-Automated (CWA) Kobo reading state to Hardcover.
"""

import os
import sqlite3
import json
import logging
import argparse
import requests
import hashlib
from datetime import datetime, timezone
from typing import Optional, Any
from dotenv import load_dotenv

from sync import _hc_query, search_hardcover_books, _normalise_isbn, _normalise, _extract_authors, _best_isbn13

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("cwa_kobo_sync")

class CwaKoboClient:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.session = requests.Session()

    def get_library_sync(self) -> list[dict]:
        url = f"{self.base_url}/kobo/{self.token}/v1/library/sync"
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 401:
            raise PermissionError(
                f"CWA Kobo sync returned 401 Unauthorized. "
                f"Check that CWA_USER in your .env is the Kobo Sync Token "
                f"(found in CWA Admin > Users > <user> > Kobo Sync Token), "
                f"not the CWA username/password."
            )
        resp.raise_for_status()
        data = resp.json()
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            for k in ["sync", "books", "items"]:
                if isinstance(data.get(k), list):
                    return data[k]
            if isinstance(data, list):
                return data
        return [data] if isinstance(data, dict) else []

    def get_book_state(self, uuid: str) -> dict:
        url = f"{self.base_url}/kobo/{self.token}/v1/library/{uuid}/state"
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 401:
                logger.error(f"CWA Kobo 401 Unauthorized fetching state for {uuid}. Check CWA_USER token.")
        except requests.RequestException as e:
            logger.error(f"Failed to fetch state for {uuid}: {e}")
        return {}

    def parse_state(self, uuid: str, library_entry: dict, state_entry: dict) -> dict:
        title = library_entry.get("Title") or library_entry.get("title") or ""
        author = library_entry.get("Author") or library_entry.get("author") or ""
        authors = [author] if author else []
        
        isbn13 = library_entry.get("ISBN") or library_entry.get("isbn") or ""
        isbn13 = _normalise_isbn(isbn13)
        
        identifiers = library_entry.get("Identifiers", {})
        if not isinstance(identifiers, dict):
            identifiers = {}
            
        hc_book_id = identifiers.get("hardcover-id") or identifiers.get("hardcover")
        hc_edition_id = identifiers.get("hardcover-edition")
        
        try:
            hc_book_id = int(hc_book_id) if hc_book_id else None
        except ValueError:
            hc_book_id = None
            
        try:
            hc_edition_id = int(hc_edition_id) if hc_edition_id else None
        except ValueError:
            hc_edition_id = None

        page_count = library_entry.get("PageCount") or state_entry.get("PageCount")
        try:
            page_count = int(page_count) if page_count else None
        except ValueError:
            page_count = None

        progress_percent = None
        percent_keys = ["PercentRead", "percentRead", "progress", "Progress", "readingProgress"]
        for k in percent_keys:
            if k in state_entry:
                try:
                    val = float(state_entry[k])
                    if val > 1.0:
                        val = val / 100.0
                    progress_percent = min(max(val, 0.0), 1.0)
                    break
                except ValueError:
                    continue

        completed = False
        status_str = str(state_entry.get("ReadingStatus") or state_entry.get("readingStatus") or state_entry.get("Status") or "").lower()
        if status_str in ["finished", "read", "completed"]:
            completed = True
        elif progress_percent is not None and progress_percent >= 0.98:
            completed = True

        progress_pages = None
        if progress_percent is not None and page_count and not completed:
            progress_pages = int(round(page_count * progress_percent))

        return {
            "source": "cwa-kobo",
            "sourceBookUuid": uuid,
            "title": title,
            "authors": authors,
            "isbn13": isbn13,
            "hardcoverBookId": hc_book_id,
            "hardcoverEditionId": hc_edition_id,
            "progressPercent": progress_percent,
            "progressPages": progress_pages,
            "completed": completed,
            "pageCount": page_count,
            "rawState": state_entry
        }

class HardcoverClient:
    def __init__(self, token: str, api_url: str = "https://api.hardcover.app/v1/graphql"):
        self.token = token
        self.api_url = api_url

    def query(self, query_str: str, variables: dict = None) -> dict:
        return _hc_query(query_str, variables, token=self.token, url=self.api_url)

    def get_user_book(self, book_id: int):
        q = """
        query GetUserBook($bookId: Int!) {
          me {
            user_books(where: {book_id: {_eq: $bookId}}) {
              id
              status_id
              edition_id
              privacy_id
              user_book_reads(limit: 1, order_by: {started_at: desc_nulls_last}) {
                id
                started_at
                finished_at
                progress
                progress_pages
              }
            }
          }
        }
        """
        res = self.query(q, {"bookId": book_id})
        if res and res.get("me"):
            me = res["me"]
            if isinstance(me, list) and me: me = me[0]
            if isinstance(me, dict):
                ub = me.get("user_books", [])
                if ub:
                    return ub[0]
        return None

    def insert_user_book(self, book_id: int, status_id: int, edition_id: int = None, privacy_setting_id: int = 1):
        q = """
        mutation InsertUserBook($bookId: Int!, $statusId: Int!, $editionId: Int, $privacyId: Int) {
          insert_user_book(object: {book_id: $bookId, status_id: $statusId, edition_id: $editionId, privacy_id: $privacyId}) {
            id
            status_id
          }
        }
        """
        return self.query(q, {"bookId": book_id, "statusId": status_id, "editionId": edition_id, "privacyId": privacy_setting_id})

    def update_user_book(self, user_book_id: int, status_id: int, edition_id: int = None):
        q = """
        mutation UpdateUserBook($id: Int!, $statusId: Int!, $editionId: Int) {
          update_user_book_by_pk(pk_columns: {id: $id}, _set: {status_id: $statusId, edition_id: $editionId}) {
            id
            status_id
          }
        }
        """
        # Hardcover's API uses update_user_book, not update_user_book_by_pk in older versions?
        # Let's use update_user_book with where if by_pk fails, but by_pk is standard Hasura.
        # Wait, sync.py uses:
        # update_user_book(id: $id, object: {status_id: $statusId}) { id }
        q_alt = """
        mutation UpdateUserBook($id: Int!, $statusId: Int) {
          update_user_book(id: $id, object: {status_id: $statusId}) {
            id
            status_id
          }
        }
        """
        return self.query(q_alt, {"id": user_book_id, "statusId": status_id})

    def insert_user_book_read(self, user_book_id: int, progress: float = None, progress_pages: int = None, completed: bool = False):
        q = """
        mutation InsertUserBookRead($userBookId: Int!, $progress: numeric, $progressPages: Int, $finishedAt: timestamptz) {
          insert_user_book_read(object: {
            user_book_id: $userBookId,
            progress: $progress,
            progress_pages: $progressPages,
            finished_at: $finishedAt
          }) {
            id
          }
        }
        """
        vars = {
            "userBookId": user_book_id,
            "progress": progress,
            "progressPages": progress_pages,
            "finishedAt": datetime.now(timezone.utc).isoformat() if completed else None
        }
        return self.query(q, vars)
        
    def update_user_book_read(self, read_id: int, progress: float = None, progress_pages: int = None, completed: bool = False):
        # Similarly, update_user_book_read
        q = """
        mutation UpdateUserBookRead($id: Int!, $progress: numeric, $progressPages: Int, $finishedAt: timestamptz) {
          update_user_book_read(id: $id, object: {
            progress: $progress,
            progress_pages: $progressPages,
            finished_at: $finishedAt
          }) {
            id
          }
        }
        """
        vars = {
            "id": read_id,
            "progress": progress,
            "progressPages": progress_pages,
            "finishedAt": datetime.now(timezone.utc).isoformat() if completed else None
        }
        return self.query(q, vars)

class LocalSyncState:
    def __init__(self, db_path: str = "cwa_hardcover_sync_state.db"):
        self.db_path = db_path
        self.setup()

    def setup(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS state (
                cwa_uuid TEXT PRIMARY KEY,
                hardcover_book_id INTEGER,
                hardcover_edition_id INTEGER,
                hardcover_user_book_id INTEGER,
                last_cwa_state_hash TEXT,
                last_cwa_updated_at TEXT,
                last_synced_at TEXT,
                last_sync_status TEXT,
                last_error TEXT,
                manual_match_required BOOLEAN
            )
        ''')
        conn.commit()
        conn.close()

    def get_state(self, cwa_uuid: str) -> dict:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM state WHERE cwa_uuid = ?", (cwa_uuid,))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else {}

    def update_state(self, cwa_uuid: str, updates: dict):
        current = self.get_state(cwa_uuid)
        merged = {**current, **updates, "cwa_uuid": cwa_uuid}
        
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            INSERT OR REPLACE INTO state (
                cwa_uuid, hardcover_book_id, hardcover_edition_id, hardcover_user_book_id,
                last_cwa_state_hash, last_cwa_updated_at, last_synced_at, last_sync_status,
                last_error, manual_match_required
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            merged.get("cwa_uuid"),
            merged.get("hardcover_book_id"),
            merged.get("hardcover_edition_id"),
            merged.get("hardcover_user_book_id"),
            merged.get("last_cwa_state_hash"),
            merged.get("last_cwa_updated_at"),
            merged.get("last_synced_at"),
            merged.get("last_sync_status"),
            merged.get("last_error"),
            merged.get("manual_match_required", False)
        ))
        conn.commit()
        conn.close()

class SyncManager:
    def __init__(self, cwa_client: CwaKoboClient, hc_client: HardcoverClient, local_state: LocalSyncState, config: dict = None, log_func=None):
        self.cwa_client = cwa_client
        self.hc_client = hc_client
        self.local_state = local_state
        self.config = config or {}
        self.log_func = log_func

    def log(self, msg: str, level: str = "info"):
        if self.log_func:
            self.log_func(msg, level)
        else:
            if level == "error":
                logger.error(msg)
            elif level == "warning":
                logger.warning(msg)
            else:
                logger.info(msg)

    def hash_state(self, state: dict) -> str:
        s = f"{state.get('progressPercent')}_{state.get('progressPages')}_{state.get('completed')}"
        return hashlib.md5(s.encode()).hexdigest()

    def run(self, dry_run: bool = False):
        prefix = "[DRY RUN] " if dry_run else ""
        self.log(f"Starting {prefix}CWA-Kobo to Hardcover sync...")
        
        library = self.cwa_client.get_library_sync()
        if not library:
            self.log("No books returned from CWA Kobo sync.", "warning")
            return

        items = []
        for item in library:
            if "BookID" in item or "Id" in item or "uuid" in item:
                items.append(item)
            elif "SyncItem" in item:
                items.append(item["SyncItem"])
                
        if not items:
            items = library

        self.log(f"Found {len(items)} items in CWA Kobo library.")
        stats = {"seen": 0, "matched": 0, "skipped": 0, "updated": 0, "completed": 0, "manual_match_required": 0}

        for item in items:
            uuid = item.get("BookID") or item.get("Id") or item.get("uuid")
            if not uuid: continue

            stats["seen"] += 1
            raw_state = self.cwa_client.get_book_state(uuid)
            parsed = self.cwa_client.parse_state(uuid, item, raw_state)
            
            if not parsed["completed"] and (parsed["progressPercent"] is None or parsed["progressPercent"] == 0):
                stats["skipped"] += 1
                continue

            state_hash = self.hash_state(parsed)
            local_entry = self.local_state.get_state(uuid)

            if local_entry.get("last_cwa_state_hash") == state_hash and not local_entry.get("manual_match_required"):
                stats["skipped"] += 1
                continue

            hc_book_id = local_entry.get("hardcover_book_id") or parsed["hardcoverBookId"]
            
            if not hc_book_id:
                isbn = parsed.get("isbn13")
                query = isbn if isbn else parsed.get("title")
                self.log(f"Matching '{parsed['title']}'...")
                search_results = search_hardcover_books(query, self.hc_client.token, self.hc_client.api_url)
                if search_results:
                    hc_book_id = search_results[0]["id"]
                    self.local_state.update_state(uuid, {"hardcover_book_id": hc_book_id})
                else:
                    self.log(f"Could not find match for '{parsed['title']}'", "warning")
                    self.local_state.update_state(uuid, {"manual_match_required": 1})
                    stats["manual_match_required"] += 1
                    continue

            stats["matched"] += 1
            target_status_id = 3 if parsed["completed"] else 2
            
            try:
                existing = self.hc_client.get_user_book(hc_book_id)
                user_book_id = None

                if not existing:
                    self.log(f"  → {prefix}Inserting user_book for '{parsed['title']}' status {target_status_id}")
                    if not dry_run:
                        ins = self.hc_client.insert_user_book(hc_book_id, target_status_id, parsed["hardcoverEditionId"], 1)
                        if ins and "insert_user_book" in ins:
                            user_book_id = ins["insert_user_book"]["id"]
                else:
                    user_book_id = existing["id"]
                    hc_status_id = existing.get("status_id")
                    if hc_status_id != target_status_id and not (hc_status_id == 3 and target_status_id == 2):
                        self.log(f"  → {prefix}Updating status for '{parsed['title']}' to {target_status_id}")
                        if not dry_run: self.hc_client.update_user_book(user_book_id, target_status_id)

                if user_book_id:
                    reads = existing.get("user_book_reads", []) if existing else []
                    if reads:
                        self.log(f"  → {prefix}Updating progress for '{parsed['title']}': {parsed['progressPercent']*100:.1f}%")
                        if not dry_run: self.hc_client.update_user_book_read(reads[0]["id"], parsed["progressPercent"], parsed["progressPages"], parsed["completed"])
                    else:
                        self.log(f"  → {prefix}Inserting progress for '{parsed['title']}': {parsed['progressPercent']*100:.1f}%")
                        if not dry_run: self.hc_client.insert_user_book_read(user_book_id, parsed["progressPercent"], parsed["progressPages"], parsed["completed"])

                self.local_state.update_state(uuid, {
                    "hardcover_book_id": hc_book_id,
                    "last_cwa_state_hash": state_hash,
                    "manual_match_required": False,
                    "last_synced_at": datetime.now(timezone.utc).isoformat(),
                    "last_error": None
                })
                stats["updated"] += 1
                if parsed["completed"]: stats["completed"] += 1
            except Exception as e:
                self.log(f"Error syncing {parsed['title']}: {e}", "error")
                self.local_state.update_state(uuid, {"last_error": str(e)})

        self.log(f"CWA Kobo Sync finished. Stats: {stats}")

def run_cwa_kobo_sync(config: dict, log_func=None, dry_run: bool = False):
    cwa_url = config.get("cwa_url")
    cwa_user = config.get("cwa_user")
    hc_token = config.get("hardcover_token")
    
    if not all([cwa_url, cwa_user, hc_token]):
        if log_func:
            log_func("CWA_USER, CWA_URL or HARDCOVER_API_TOKEN is missing. Skipping CWA Kobo sync.", "warning")
        return
        
    cwa_client = CwaKoboClient(cwa_url, cwa_user)
    hc_client = HardcoverClient(hc_token, config.get("hardcover_api_url", "https://api.hardcover.app/v1/graphql"))
    local_state = LocalSyncState()
    
    manager = SyncManager(cwa_client, hc_client, local_state, config, log_func)
    manager.run(dry_run=dry_run)
