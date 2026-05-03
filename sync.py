"""
hardcover_sync/sync.py
Core sync logic between Hardcover ↔ CWA ↔ Shelfmark
"""

import sqlite3
import logging
import requests
import os
import json
from datetime import datetime, timezone
from typing import Optional, Any
from openai import OpenAI
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

logger = logging.getLogger("hardcover_sync")


# ─────────────────────────────────────────────────────────────────────────────
# Hardcover GraphQL helpers
# ─────────────────────────────────────────────────────────────────────────────

HARDCOVER_STATUS_NAMES = {
    1: "want_to_read",
    2: "currently_reading",
    3: "read",
    5: "did_not_finish",
}

HARDCOVER_STATUS_LABELS = {
    1: "Want to Read",
    2: "Currently Reading",
    3: "Read",
    5: "Did Not Finish",
}

CWA_STATUS_LABELS = {
    0: "Unknown",
    1: "Want to Read",
    2: "Currently Reading",
    3: "Read",
    5: "Did Not Finish",
}

CWA_STATUS_TAG_ALIASES = {
    1: ["want to read", "to read", "to-read", "toread", "want", "wishlist"],
    2: ["currently reading", "reading", "in progress", "reading now"],
    3: ["read", "finished", "completed"],
    5: ["did not finish", "did not finish", "dnf", "abandoned", "dropped"],
}

CWA_STATUS_READ_TAGS = set(x for tags in CWA_STATUS_TAG_ALIASES.values() for x in tags)


def _cwa_status_from_tags(tags: list[str]) -> int:
    """Infer a CWA reading status from Calibre/CWA tags."""
    if not tags:
        return 0

    normalized = {_normalise(str(tag)) for tag in tags if tag}

    if any(alias in normalized for alias in CWA_STATUS_TAG_ALIASES[3]):
        return 3
    if any(alias in normalized for alias in CWA_STATUS_TAG_ALIASES[2]):
        return 2
    if any(alias in normalized for alias in CWA_STATUS_TAG_ALIASES[5]):
        return 5
    if any(alias in normalized for alias in CWA_STATUS_TAG_ALIASES[1]) or "unread" in normalized:
        return 1

    return 0


def _cwa_status_tag_name(status_id: int) -> str:
    """Return the canonical CWA tag name for a given status."""
    return CWA_STATUS_LABELS.get(status_id, "Unknown")


def _cwa_status_tag_ids(conn: sqlite3.Connection) -> dict[int, int]:
    """Fetch or create tag IDs for all known CWA status tags."""
    cursor = conn.cursor()
    tag_ids: dict[int, int] = {}

    for status_id, aliases in CWA_STATUS_TAG_ALIASES.items():
        for alias in aliases:
            cursor.execute("SELECT id FROM tags WHERE lower(name) = ?", (_normalise(alias),))
            row = cursor.fetchone()
            if row:
                tag_ids[status_id] = row[0]
                break
        if status_id not in tag_ids:
            canonical = _cwa_status_tag_name(status_id)
            cursor.execute("INSERT INTO tags(name) VALUES(?)", (canonical,))
            tag_ids[status_id] = cursor.lastrowid

    return tag_ids


def update_cwa_book_status(db_path: str, book_id: int, status_id: int) -> bool:
    """Apply a status tag to a CWA/Calibre book directly in the metadata.db."""
    if status_id not in CWA_STATUS_LABELS:
        return False

    if not os.path.exists(db_path):
        logger.warning("Cannot update CWA status because metadata.db does not exist: %s", db_path)
        return False

    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT id, name FROM tags")
        tags = {row["id"]: row["name"] for row in cursor.fetchall()}
        status_tag_ids = _cwa_status_tag_ids(conn)

        target_tag_id = status_tag_ids[status_id]
        all_status_aliases = {alias for aliases in CWA_STATUS_TAG_ALIASES.values() for alias in aliases}

        # Remove existing status-related tags for this book.
        placeholder = ",".join("?" for _ in all_status_aliases)
        cursor.execute(
            f"SELECT id, name FROM tags WHERE lower(name) IN ({placeholder})",
            tuple(_normalise(alias) for alias in all_status_aliases),
        )
        existing_status_tag_ids = [row["id"] for row in cursor.fetchall()]
        if existing_status_tag_ids:
            placeholder_ids = ",".join("?" for _ in existing_status_tag_ids)
            cursor.execute(
                f"DELETE FROM books_tags_link WHERE book = ? AND tag IN ({placeholder_ids})",
                (book_id, *existing_status_tag_ids),
            )

        cursor.execute(
            "INSERT OR IGNORE INTO books_tags_link(book, tag) VALUES(?, ?)" ,
            (book_id, target_tag_id),
        )

        conn.commit()
        return True

    except sqlite3.DatabaseError as e:
        logger.error("Failed to write CWA status tag: %s", e)
        return False

    finally:
        if conn is not None:
            conn.close()


def lookup_kobo_library(db_path: str = None) -> list[dict]:
    """Read read-book metadata from a Kobo local database or JSON export path."""
    if not db_path:
        return []

    if not os.path.exists(db_path):
        logger.warning("Kobo DB path does not exist: %s", db_path)
        return []

    conn = None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        table_names = [row[0] for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        candidate_tables = [name for name in table_names if any(key in name.lower() for key in ("content", "book", "item"))]

        for table in candidate_tables:
            cols = [row[1].lower() for row in cursor.execute(f"PRAGMA table_info('{table}')")]
            if "title" not in cols or not any(x in cols for x in ("isbn", "isbn13", "isbn_13")):
                continue

            title_col = next((c for c in cols if c == "title"), None)
            author_col = next((c for c in cols if c in ("author", "creator", "authors")), None)
            isbn_col = next((c for c in cols if c in ("isbn13", "isbn_13", "isbn")), None)
            read_col = next((c for c in cols if c in ("readstate", "read_state", "isread", "progress", "readposition")), None)

            if not title_col or not isbn_col or not author_col or not read_col:
                continue

            rows = cursor.execute(
                f"SELECT \"{title_col}\" AS title, \"{author_col}\" AS author, \"{isbn_col}\" AS isbn13, \"{read_col}\" AS read_state FROM \"{table}\""
            ).fetchall()

            books = []
            for row in rows:
                if row["read_state"] is None:
                    continue
                value = row["read_state"]
                if isinstance(value, str):
                    if value.strip().lower() not in ("1", "true", "yes", "read", "finished", "completed"):
                        continue
                elif isinstance(value, (int, float)):
                    if float(value) <= 0:
                        continue
                else:
                    continue

                isbn13 = _normalise_isbn(row["isbn13"])
                if not isbn13:
                    continue

                books.append({
                    "title": row["title"],
                    "authors": [row["author"]] if row["author"] else [],
                    "isbn13": isbn13,
                    "status_id": 3,
                    "status": "Read",
                })

            if books:
                logger.info("Found %d read Kobo book(s) in table %s", len(books), table)
                return books

        logger.warning("Unable to interpret Kobo DB schema in %s; no Kobo books loaded.", db_path)
        return []

    except sqlite3.DatabaseError as e:
        logger.error("Error reading Kobo DB: %s", e)
        return []

    finally:
        if conn is not None:
            conn.close()


def _json_maybe(value: Any, fallback: Any):
    """
    Return JSON-ish values as Python objects.

    Hardcover jsonb fields may come back as dict/list already, or as JSON strings
    depending on the client/server serialization path.
    """
    if value is None or value == "":
        return fallback

    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return fallback

    return value


def _extract_cover_url(cached_image: Any) -> Optional[str]:
    """Extract a usable cover URL from Hardcover cached_image JSON."""
    image = _json_maybe(cached_image, {})

    if isinstance(image, dict):
        # Common shape: {"url": "..."}
        url = image.get("url")
        if isinstance(url, str) and url.strip():
            return url.strip()

        # Be defensive for alternative shapes.
        for key in ("image_url", "src", "cover_url"):
            val = image.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

    if isinstance(image, str) and image.strip().startswith(("http://", "https://")):
        return image.strip()

    return None


def _extract_authors(cached_contributors: Any) -> list[str]:
    """Extract author names from Hardcover cached_contributors JSON.
    
    Hardcover's cached_contributors field is a JSONB column that can be:
    - A JSON string: '[{"name": "...", "contribution": "Author"}, ...]'
    - A Python list already parsed
    - Entries may have contribution=None, "Author", "author", or other roles
    """
    contributors = _json_maybe(cached_contributors, [])

    # If it's a nested dict (e.g. {"author": [...]}) try to unwrap
    if isinstance(contributors, dict):
        for key in ("author", "authors", "contributors"):
            if isinstance(contributors.get(key), list):
                contributors = contributors[key]
                break
        else:
            contributors = []

    if not isinstance(contributors, list):
        return []

    authors: list[str] = []
    # Non-author roles to explicitly exclude
    non_author_roles = {
        "illustrator", "editor", "translator", "foreword", "introduction",
        "narrator", "cover artist", "cover design", "photographer",
    }

    for contributor in contributors:
        if not isinstance(contributor, dict):
            # Could be a bare string
            if isinstance(contributor, str) and contributor.strip():
                authors.append(contributor.strip())
            continue

        name = contributor.get("name") or contributor.get("full_name")
        if not name:
            continue

        contribution = (contributor.get("contribution") or "").lower().strip()
        
        # Include if contribution is empty/None, or is explicitly an author role
        if contribution == "" or contribution in ("author", "write", "writer"):
            authors.append(str(name).strip())
        elif contribution not in non_author_roles:
            # For unknown roles, include as author if no explicit authors found yet
            # (we'll trim later if needed)
            authors.append(str(name).strip())

    return [a for a in authors if a]


def _best_isbn13(editions: Any) -> Optional[str]:
    """Return the first available isbn_13 from the Hardcover editions array."""
    if not isinstance(editions, list):
        return None

    for edition in editions:
        if not isinstance(edition, dict):
            continue

        isbn13 = edition.get("isbn_13")
        if isbn13:
            return str(isbn13).replace("-", "").strip()

    return None


def _hc_query(query: str, variables: dict = None, token: str = None, url: str = None):
    """Execute a Hardcover GraphQL query."""
    if not token:
        logger.error("Hardcover token is missing")
        return None

    if not url:
        logger.error("Hardcover API URL is missing")
        return None

    token_value = str(token).strip()
    if token_value.lower().startswith("bearer "):
        token_value = token_value.split(" ", 1)[1].strip()
    if token_value.lower().startswith("token "):
        token_value = token_value.split(" ", 1)[1].strip()

    if not token_value:
        logger.error("Hardcover token is empty after normalization")
        return None

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "hardcover-sync/1.0",
        "Authorization": f"Bearer {token_value}",
    }

    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    response = None
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        try:
            data = response.json()
        except ValueError as e:
            logger.error("Hardcover returned non-JSON response: %s", e)
            return None

        if "errors" in data:
            logger.error("GraphQL errors: %s", data["errors"])
            return None

        return data.get("data")

    except requests.RequestException as e:
        if response is not None and response.status_code == 403:
            logger.error("Hardcover request forbidden (%s): %s", response.status_code, response.text[:300])
        else:
            logger.error("Hardcover request failed: %s", e)
        return None


def fetch_hardcover_books(token: str, url: str, status_ids: list[int]) -> list[dict]:
    """Fetch user books from Hardcover with given status IDs."""
    query = """
    query GetUserBooks($statusIds: [Int!]!) {
      me {
        user_books(where: {status_id: {_in: $statusIds}}) {
          id
          status_id
          rating
          date_added
          user_book_reads(limit: 1, order_by: {started_at: desc_nulls_last}) {
            started_at
            finished_at
            progress
          }
          book {
            id
            title
            slug
            cached_contributors
            cached_image
            editions(limit: 5, where: {isbn_13: {_is_null: false}}) {
              isbn_13
              isbn_10
            }
          }
        }
      }
    }
    """

    data = _hc_query(query, {"statusIds": status_ids}, token=token, url=url)
    if not data:
        return []

    me = data.get("me")

    # Hardcover/Hasura can return me as a list, commonly with one current user.
    if isinstance(me, list):
        if not me:
            return []
        me = me[0]

    if not isinstance(me, dict):
        logger.error("Unexpected Hardcover 'me' response shape: %r", me)
        return []

    user_books = me.get("user_books", [])
    if not isinstance(user_books, list):
        logger.error("Unexpected Hardcover 'user_books' response shape: %r", user_books)
        return []

    return user_books

def update_hardcover_status(
    token: str,
    url: str,
    user_book_id: int,
    status_id: int,
    rating: float = None,
) -> bool:
    """
    Update a user_book status/rating on Hardcover.

    Dates (started_at/finished_at) zitten op user_book_reads, niet op user_books,
    en worden hier niet gesynchroniseerd.

    Belangrijk:
    Als rating None is, sturen we geen rating-field mee. Anders kan een API
    dit interpreteren als rating=null en bestaande ratings wissen.
    """
    if rating is None:
        mutation = """
        mutation UpdateUserBook(
          $id: Int!,
          $statusId: Int
        ) {
          update_user_book(
            id: $id,
            object: {
              status_id: $statusId
            }
          ) {
            id
            status_id
          }
        }
        """
        variables = {
            "id": user_book_id,
            "statusId": status_id,
        }
    else:
        mutation = """
        mutation UpdateUserBook(
          $id: Int!,
          $statusId: Int,
          $rating: numeric
        ) {
          update_user_book(
            id: $id,
            object: {
              status_id: $statusId,
              rating: $rating
            }
          ) {
            id
            status_id
          }
        }
        """
        variables = {
            "id": user_book_id,
            "statusId": status_id,
            "rating": rating,
        }

    data = _hc_query(mutation, variables, token=token, url=url)
    return data is not None


# ─────────────────────────────────────────────────────────────────────────────
# CWA / Calibre SQLite library helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalise(s: str) -> str:
    """Lowercase + strip for fuzzy matching."""
    return s.lower().strip() if s else ""


def _normalise_isbn(isbn: Optional[str]) -> Optional[str]:
    """Normalize ISBN strings for comparison."""
    if not isbn:
        return None

    cleaned = str(isbn).replace("-", "").replace(" ", "").strip()
    return cleaned or None


def lookup_cwa_library(db_path: str) -> list[dict]:
    """
    Read all books from Calibre's metadata.db.
    Returns list of dicts with id, title, authors, isbn, tags, and identifiers.
    """
    if not db_path:
        logger.warning("CWA metadata.db path is empty")
        return []

    if not os.path.exists(db_path):
        logger.warning("CWA metadata.db not found at %s", db_path)
        return []

    con = None

    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        try:
            cur.execute("SELECT id, title, pubdate, last_modified, uuid, series_index FROM books")
        except sqlite3.OperationalError as e:
            logger.error("CWA DB query error: %s", e)
            return []

        books_raw = cur.fetchall()
        books = []

        for row in books_raw:
            book_id = row["id"]

            # Authors
            cur.execute(
                "SELECT authors.name FROM books_authors_link "
                "JOIN authors ON authors.id = books_authors_link.author "
                "WHERE books_authors_link.book = ?",
                (book_id,),
            )
            authors = [r[0] for r in cur.fetchall()]

            # Identifiers (isbn, isbn13, asin, goodreads, etc.)
            cur.execute(
                "SELECT type, val FROM identifiers WHERE book = ?",
                (book_id,),
            )
            identifiers = {r["type"]: r["val"] for r in cur.fetchall()}

            # Calibre usually stores ISBN under "isbn"; keep isbn13 fallback too.
            isbn13 = (
                identifiers.get("isbn13")
                or identifiers.get("isbn_13")
                or identifiers.get("isbn")
            )

            # Tags
            cur.execute(
                "SELECT tags.name FROM books_tags_link "
                "JOIN tags ON tags.id = books_tags_link.tag "
                "WHERE books_tags_link.book = ?",
                (book_id,),
            )
            tags = [r[0] for r in cur.fetchall()]

            # Series — series_index lives on the books table, not books_series_link
            cur.execute(
                "SELECT series.name FROM books_series_link "
                "JOIN series ON series.id = books_series_link.series "
                "WHERE books_series_link.book = ?",
                (book_id,),
            )
            series_rows = cur.fetchall()
            series_name = series_rows[0][0] if series_rows else None
            series_index = row["series_index"] if series_rows else None

            status_id = _cwa_status_from_tags(tags)
            books.append(
                {
                    "id": book_id,
                    "title": row["title"],
                    "authors": authors,
                    "isbn13": _normalise_isbn(isbn13),
                    "identifiers": identifiers,
                    "tags": tags,
                    "series": series_name,
                    "series_index": series_index,
                    "status_id": status_id,
                    "status": CWA_STATUS_LABELS.get(status_id, "Unknown"),
                    "last_modified": row["last_modified"],
                }
            )

        return books

    except Exception as e:
        logger.error("CWA library read error: %s", e)
        return []

    finally:
        if con is not None:
            con.close()


def find_book_in_cwa(
    cwa_books: list[dict],
    title: str,
    authors: list[str],
    isbn13: str = None,
) -> Optional[dict]:
    """Search CWA library for a book by ISBN first, then title+author fuzzy match."""
    isbn13_norm = _normalise_isbn(isbn13)

    # ISBN match is most reliable.
    if isbn13_norm:
        for book in cwa_books:
            cwa_isbn = _normalise_isbn(book.get("isbn13"))
            if cwa_isbn and cwa_isbn == isbn13_norm:
                return book

    # Title + author fuzzy match.
    norm_title = _normalise(title)
    norm_authors = [_normalise(a) for a in (authors or []) if a]

    for book in cwa_books:
        if _normalise(book.get("title", "")) != norm_title:
            continue

        if not norm_authors:
            return book

        book_authors = [_normalise(a) for a in book.get("authors", [])]
        joined_book_authors = " ".join(book_authors)

        if any(author in joined_book_authors for author in norm_authors if author):
            return book

    return None


def find_book_in_hc(
    hc_books: list[dict],
    title: str,
    authors: list[str],
    isbn13: str = None,
) -> Optional[dict]:
    """Search Hardcover user library for a book by ISBN first, then title+author fuzzy match."""
    isbn13_norm = _normalise_isbn(isbn13)

    # ISBN match is most reliable.
    if isbn13_norm:
        for user_book in hc_books:
            book = user_book.get("book") or {}
            hc_isbn = _best_isbn13(book.get("editions", []))
            if hc_isbn and hc_isbn == isbn13_norm:
                return user_book

    # Title + author fuzzy match.
    norm_title = _normalise(title)
    norm_authors = [_normalise(a) for a in (authors or []) if a]

    for user_book in hc_books:
        book = user_book.get("book") or {}
        if _normalise(book.get("title", "")) != norm_title:
            continue

        if not norm_authors:
            return user_book

        hc_authors = [_normalise(a) for a in _extract_authors(book.get("cached_contributors", []))]
        joined_hc_authors = " ".join(hc_authors)

        if any(author in joined_hc_authors for author in norm_authors if author):
            return user_book

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Shelfmark downloader trigger
# ─────────────────────────────────────────────────────────────────────────────

def _shelfmark_api_base(shelfmark_url: str) -> str:
    """
    Return normalized Shelfmark API base.

    Accepts either:
      http://host:port
      http://host:port/
      http://host:port/api
    """
    base = shelfmark_url.rstrip("/")
    if base.endswith("/api"):
        return base
    return f"{base}/api"


def _shelfmark_json_error(response: requests.Response) -> str:
    """Return a useful error message from a Shelfmark response."""
    try:
        data = response.json()
        if isinstance(data, dict):
            return (
                data.get("message")
                or data.get("error")
                or response.text[:300]
            )
    except Exception:
        pass

    return response.text[:300]


def _shelfmark_login_if_configured(
    session: requests.Session,
    api_base: str,
    username: str = None,
    password: str = None,
) -> Optional[str]:
    """
    Login to Shelfmark if credentials are configured.

    Shelfmark frontend uses cookie/session auth with credentials included.
    If your Shelfmark has auth disabled, this function safely does nothing.
    """
    if not username or not password:
        return None

    login_url = f"{api_base}/auth/login"

    response = session.post(
        login_url,
        json={
            "username": username,
            "password": password,
        },
        timeout=30,
    )

    if response.status_code in (200, 201):
        return None

    return f"Shelfmark login failed HTTP {response.status_code}: {_shelfmark_json_error(response)}"


def _shelfmark_pick_release(
    releases: list[dict],
    preferred_format: str = "epub",
    language: str = "en",
) -> Optional[dict]:
    """
    Pick the best release from Shelfmark results.

    Preference order:
      1. preferred format + requested language
      2. preferred format
      3. first release
    """
    if not releases:
        return None

    preferred_format_norm = (preferred_format or "").lower().strip()
    language_norm = (language or "").lower().strip()

    def release_format(release: dict) -> str:
        return str(release.get("format") or "").lower().strip()

    def release_language(release: dict) -> str:
        raw = (
            release.get("language")
            or release.get("languages")
            or release.get("lang")
            or ""
        )

        if isinstance(raw, list):
            return ",".join(str(x).lower().strip() for x in raw)

        return str(raw).lower().strip()

    if preferred_format_norm and language_norm:
        for release in releases:
            if not isinstance(release, dict):
                continue

            if (
                release_format(release) == preferred_format_norm
                and language_norm in release_language(release)
            ):
                return release

    if preferred_format_norm:
        for release in releases:
            if not isinstance(release, dict):
                continue

            if release_format(release) == preferred_format_norm:
                return release

    for release in releases:
        if isinstance(release, dict):
            return release

    return None


def trigger_shelfmark_search(
    shelfmark_url: str,
    api_key: str,
    title: str,
    author: str,
    isbn13: str = None,
    preferred_format: str = "epub",
    language: str = "en",
    username: str = None,
    password: str = None,
    release_source: str = "direct_download",
) -> dict:
    """
    Search Shelfmark releases and queue the best matching release for download.

    Current Shelfmark flow:
      GET  /api/releases?source=direct_download&query=...
      POST /api/releases/download

    This replaces the old/non-existing:
      /api/search
      /api/download
      /api/request
    """
    if not shelfmark_url:
        return {"success": False, "message": "Shelfmark URL is empty"}

    api_base = _shelfmark_api_base(shelfmark_url)

    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
    })

    # Keep this for reverse proxies or future token setups. Current Shelfmark
    # normally uses cookie/session auth, but this does not hurt if ignored.
    if api_key:
        session.headers.update({"Authorization": f"Bearer {api_key}"})

    login_error = _shelfmark_login_if_configured(
        session=session,
        api_base=api_base,
        username=username,
        password=password,
    )
    if login_error:
        return {"success": False, "message": login_error}

    search_terms = []
    title_author = f"{title} {author}".strip()
    if isbn13:
        search_terms.append(isbn13)
    if title_author and title_author not in search_terms:
        search_terms.append(title_author)
    if title and title not in search_terms:
        search_terms.append(title)

    if not search_terms:
        return {"success": False, "message": "No title/author/ISBN available for Shelfmark search"}

    try:
        releases_url = f"{api_base}/releases"
        releases = []
        last_query = None

        for query_text in search_terms:
            last_query = query_text
            const_params = {
                "source": release_source or "direct_download",
                "query": query_text,
                "content_type": "ebook",
            }
            if language:
                const_params["languages"] = language

            response = session.get(
                releases_url,
                params=const_params,
                timeout=120,
            )

            if response.status_code == 401:
                return {
                    "success": False,
                    "message": (
                        "Shelfmark requires authentication. Set shelfmark_username "
                        "and shelfmark_password in your config, or disable auth for this integration."
                    ),
                }

            if response.status_code == 404:
                return {
                    "success": False,
                    "message": (
                        f"Shelfmark releases endpoint not found at {releases_url}. "
                        "Check whether shelfmark_url points to the Shelfmark root URL, not the frontend path."
                    ),
                }

            response.raise_for_status()

            try:
                data = response.json()
            except ValueError:
                return {
                    "success": False,
                    "message": f"Shelfmark returned non-JSON release search response: {response.text[:300]}",
                }

            releases = data.get("releases", []) if isinstance(data, dict) else []
            if not isinstance(releases, list):
                return {
                    "success": False,
                    "message": f"Unexpected Shelfmark releases response shape: {type(releases).__name__}",
                }

            if releases:
                search_query = query_text
                break

            if query_text != search_terms[-1]:
                logger.info("Shelfmark search returned no results for query '%s'; retrying with next term", query_text)

        if not releases:
            return {
                "success": False,
                "message": f"No Shelfmark releases found for queries: {search_terms!r}",
            }

        release = _shelfmark_pick_release(
            releases,
            preferred_format=preferred_format,
            language=language,
        )

        if not release:
            return {
                "success": False,
                "message": f"No usable Shelfmark release found for query '{search_query}'",
            }

        # Shelfmark download endpoint expects a release-like payload.
        # Fill title/author/content_type defensively because some direct/manual
        # results contain limited metadata.
        download_payload = dict(release)
        download_payload.setdefault("title", title)
        download_payload.setdefault("author", author)
        download_payload.setdefault("format", preferred_format)
        download_payload.setdefault("content_type", "ebook")

        # These are expected by the frontend's downloadRelease payload.
        if "source" not in download_payload:
            download_payload["source"] = release_source or "direct_download"

        if "source_id" not in download_payload:
            source_id = (
                release.get("source_id")
                or release.get("id")
                or release.get("book_id")
            )
            if source_id:
                download_payload["source_id"] = str(source_id)

        if not download_payload.get("source_id"):
            return {
                "success": False,
                "message": (
                    "Shelfmark found a release, but it has no source_id/id, "
                    "so it cannot be queued for download."
                ),
            }

        download_url = f"{api_base}/releases/download"
        dl_response = session.post(
            download_url,
            json=download_payload,
            timeout=60,
        )

        if dl_response.status_code == 401:
            return {
                "success": False,
                "message": "Shelfmark download requires authentication; login/session was not accepted.",
            }

        if dl_response.status_code not in (200, 201, 202, 204):
            return {
                "success": False,
                "message": (
                    f"Shelfmark download queue failed HTTP {dl_response.status_code}: "
                    f"{_shelfmark_json_error(dl_response)}"
                ),
            }

        picked_title = download_payload.get("title") or title
        picked_format = download_payload.get("format") or preferred_format
        picked_source = download_payload.get("source") or release_source

        return {
            "success": True,
            "message": (
                f"Shelfmark download queued: '{picked_title}' "
                f"({picked_format}, source={picked_source}, source_id={download_payload.get('source_id')})"
            ),
        }

    except requests.exceptions.ConnectionError:
        return {"success": False, "message": f"Cannot reach Shelfmark at {shelfmark_url}"}
    except requests.exceptions.Timeout:
        return {"success": False, "message": f"Shelfmark request timed out at {shelfmark_url}"}
    except requests.RequestException as e:
        return {"success": False, "message": f"Shelfmark HTTP error: {e}"}
    except Exception as e:
        return {"success": False, "message": f"Shelfmark error: {e}"}


# ─────────────────────────────────────────────────────────────────────────────
# AI Suggestions
# ─────────────────────────────────────────────────────────────────────────────

def generate_ai_suggestions(
    read_books: list[dict],
    llm_base_url: str,
    llm_api_key: str,
    llm_model: str,
    already_in_library: list[str] = None,
) -> list[dict]:
    """Generate book recommendations with justifications and hard-filtering of existing books."""
    import re

    if not read_books:
        return []

    book_list = "\n".join(
        f"- {book['title']} by {book['author']}" for book in read_books[:20]
    )

    # Normalize titles for matching (lowercase, alphanumeric only)
    def normalize_title(t):
        if not t: return ""
        # Remove subtitles after colon or dash to match more broadly
        t = re.split(r"[:\-]", t)[0]
        return re.sub(r"[^a-z0-9]", "", t.lower())

    excluded_normalized = set()
    if already_in_library:
        excluded_normalized = {normalize_title(t) for t in already_in_library}

    exclusion_section = ""
    if already_in_library:
        # Give the LLM some examples of what to avoid, but we'll hard-filter later too
        exclude_titles = "\n".join(f"- {t}" for t in already_in_library[:40])
        exclusion_section = (
            f"\nDo NOT suggest any of these books (the user already has them):\n{exclude_titles}\n"
        )

    system_msg = (
        "You are a professional book recommendation engine. "
        "You MUST respond with a list of 12 book recommendations. "
        "Each recommendation MUST follow this EXACT format on a single line:\n"
        "TITLE by AUTHOR | JUSTIFICATION\n"
        "CRITICAL RULES:\n"
        "1. Do NOT suggest books the user has already read.\n"
        "2. Do NOT include ANY text other than the list.\n"
        "3. Do NOT include <think> tags or reasoning steps.\n"
        "4. Use the pipe character '|' to separate the book from the justification."
    )

    user_msg = (
        f"The user has read these books:\n{book_list}\n"
        f"{exclusion_section}\n"
        "Recommend 12 new books in the format: 'Title by Author | Justification'. "
    )

    try:
        client = OpenAI(
            base_url=llm_base_url,
            api_key=llm_api_key or "not-needed",
        )

        response = client.chat.completions.create(
            model=llm_model,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=2000,
            temperature=0.8, # Slightly higher temperature for more variety
        )

        raw = response.choices[0].message.content or ""
        raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()

        suggestions = []
        for line in raw.splitlines():
            line = line.strip()
            if not line or "|" not in line:
                continue
            
            line = re.sub(r"^[\d]+[.)]\s*|^[-*]\s*", "", line).strip()
            parts = line.split("|", 1)
            if len(parts) == 2:
                sug_text = parts[0].strip()
                just_text = parts[1].strip()
                
                if " by " in sug_text.lower():
                    # --- HARD FILTERING IN PYTHON ---
                    # Split title and author
                    title_part = re.split(r"\s+by\s+", sug_text, flags=re.IGNORECASE)[0].strip()
                    norm_sug = normalize_title(title_part)
                    
                    if norm_sug in excluded_normalized:
                        logger.info("AI suggested already-read book, filtering out: %s", title_part)
                        continue

                    suggestions.append({
                        "suggestion": sug_text,
                        "justification": just_text
                    })

        return suggestions[:8]

    except Exception as e:
        logger.error("Failed to generate AI suggestions: %s", e)
        return []



def search_hardcover_books(query: str, token: str, url: str) -> list[dict]:
    """Search for books on Hardcover using a query string.
    
    Uses a 3-tier strategy:
    1. Exact title match
    2. Case-insensitive ILIKE on title (handles subtitle differences)
    3. Author-aware ILIKE (title contains first words of query)
    """
    if not query or not query.strip():
        return []

    isbn_query = _normalise_isbn(query)
    title_query = query.strip()

    def _parse_results(books_raw: list) -> list[dict]:
        results = []
        for book in (books_raw or []):
            if not isinstance(book, dict):
                continue
            authors = _extract_authors(book.get("cached_contributors", []))
            isbn13 = _best_isbn13(book.get("editions", []))
            cover_url = _extract_cover_url(book.get("cached_image"))
            results.append({
                "id": book.get("id"),
                "title": book.get("title"),
                "slug": book.get("slug"),
                "authors": authors,
                "author": authors[0] if authors else "",
                "isbn13": isbn13,
                "cover_url": cover_url,
            })
        return results

    # Tier 1: exact match or ISBN
    q1 = """
    query SearchBooksExact($title: String!, $isbn: String) {
      books(
        limit: 5,
        where: {
          _or: [
            {title: {_eq: $title}}
            {slug: {_eq: $title}}
            {editions: {isbn_13: {_eq: $isbn}}}
          ]
        }
      ) {
        id title slug cached_contributors cached_image
        editions(limit: 1) { isbn_13 }
      }
    }
    """
    data = _hc_query(q1, {"title": title_query, "isbn": isbn_query}, token=token, url=url)
    results = _parse_results((data or {}).get("books", []))
    if results:
        return results

    # Tier 2: case-insensitive ILIKE — catches "Title: Subtitle" vs "Title"
    q2 = """
    query SearchBooksIlike($pattern: String!) {
      books(
        limit: 5,
        order_by: {users_count: desc_nulls_last},
        where: {title: {_ilike: $pattern}}
      ) {
        id title slug cached_contributors cached_image
        editions(limit: 1) { isbn_13 }
      }
    }
    """
    # Use first 4 significant words to avoid over-matching
    words = [w for w in title_query.split() if len(w) > 2][:4]
    pattern = "%" + " ".join(words) + "%" if words else f"%{title_query}%"
    data = _hc_query(q2, {"pattern": pattern}, token=token, url=url)
    results = _parse_results((data or {}).get("books", []))
    if results:
        return results

    # Tier 3: broader fuzzy — just first keyword
    if words:
        pattern3 = f"%{words[0]}%"
        data = _hc_query(q2, {"pattern": pattern3}, token=token, url=url)
        results = _parse_results((data or {}).get("books", []))
        if results:
            return results

    return []




def add_hardcover_want_to_read(book_id: int, token: str, url: str) -> bool:
    """Add a book to Hardcover as Want to Read."""
    mutation = """
    mutation AddUserBook($bookId: Int!) {
      insert_user_book(object: {book_id: $bookId, status_id: 1}) {
        id
      }
    }
    """
    data = _hc_query(mutation, {"bookId": book_id}, token=token, url=url)
    return data is not None


def add_hardcover_book_status(book_id: int, status_id: int, token: str, url: str) -> bool:
    """Add a book to Hardcover with a specific status."""
    mutation = """
    mutation AddUserBook($bookId: Int!, $statusId: Int!) {
      insert_user_book(object: {book_id: $bookId, status_id: $statusId}) {
        id
      }
    }
    """
    data = _hc_query(mutation, {"bookId": book_id, "statusId": status_id}, token=token, url=url)
    return data is not None


def get_hardcover_series_books(token: str, url: str, series_id: int) -> list[dict]:
    """Get all books in a series from Hardcover."""
    query = """
    query GetSeriesBooks($seriesId: Int!) {
      series_by_pk(id: $seriesId) {
        books(order_by: {position: asc}) {
          id
          position
          book {
            id
            title
            slug
            cached_contributors
            editions(limit: 1) {
              isbn_13
            }
          }
        }
      }
    }
    """

    data = _hc_query(query, {"seriesId": series_id}, token=token, url=url)
    if not data or not data.get("series_by_pk"):
        return []

    series_books = data["series_by_pk"].get("books", [])
    results = []
    for sb in series_books:
        book = sb.get("book", {})
        authors = _extract_authors(book.get("cached_contributors", []))
        isbn13 = _best_isbn13(book.get("editions", []))
        results.append({
            "id": book.get("id"),
            "title": book.get("title"),
            "authors": authors,
            "isbn13": isbn13,
            "position": sb.get("position"),
        })

    return results


def get_hardcover_user_series(token: str, url: str) -> list[dict]:
    """Get user's series with book counts."""
    query = """
    query GetUserSeries {
      me {
        user_books(where: {status_id: {_in: [1, 2, 3]}}) {
          book {
            book_series {
              series {
                id
                name
                books_count
              }
            }
          }
        }
      }
    }
    """

    data = _hc_query(query, {}, token=token, url=url)
    if not data or not data.get("me"):
        return []

    user_books = data["me"].get("user_books", [])
    series_map = {}

    for ub in user_books:
        book = ub.get("book") or {}
        for sb in book.get("book_series", []):
            series = sb.get("series") or {}
            series_id = series.get("id")
            if series_id:
                if series_id not in series_map:
                    series_map[series_id] = {
                        "id": series_id,
                        "name": series.get("name", ""),
                        "total_books": series.get("books_count", 0),
                        "user_books": [],
                    }
                series_map[series_id]["user_books"].append(book.get("id"))

    return list(series_map.values())


def get_hardcover_series_id_for_book(book_id: int, token: str, url: str) -> Optional[int]:
    """Fetch the primary series ID for a given Hardcover book."""
    query = """
    query GetBookSeries($bookId: Int!) {
      books_by_pk(id: $bookId) {
        book_series(limit: 1) {
          series_id
        }
      }
    }
    """
    data = _hc_query(query, {"bookId": book_id}, token=token, url=url)
    if data and data.get("books_by_pk"):
        book_series = data["books_by_pk"].get("book_series", [])
        if book_series:
            return book_series[0].get("series_id")
    return None

def download_missing_cwa_series_books(cwa_books: list[dict], config: dict, log_func) -> dict:
    """Find series that exist in CWA, look them up on Hardcover, and trigger Shelfmark for missing books."""
    result = {"series_checked": 0, "books_downloaded": 0, "errors": 0}
    
    # Group CWA books by series name
    cwa_series_groups = {}
    cwa_isbns = set(b.get("isbn13") for b in cwa_books if b.get("isbn13"))
    cwa_hc_ids = set(int(b["identifiers"]["hardcover-id"]) for b in cwa_books if b.get("identifiers", {}).get("hardcover-id"))
    
    for book in cwa_books:
        if book.get("series"):
            if book["series"] not in cwa_series_groups:
                cwa_series_groups[book["series"]] = []
            cwa_series_groups[book["series"]].append(book)
            
    result["series_checked"] = len(cwa_series_groups)
    
    token = config["hardcover_token"]
    url = config["hardcover_api_url"]
    
    for series_name, books_in_series in cwa_series_groups.items():
        # Find a Hardcover series ID by checking the books we have
        hc_series_id = None
        for book in books_in_series:
            hc_id = book.get("identifiers", {}).get("hardcover-id")
            if hc_id:
                try:
                    hc_series_id = get_hardcover_series_id_for_book(int(hc_id), token, url)
                    if hc_series_id:
                        break
                except ValueError:
                    continue
            else:
                # Fallback: search Hardcover for the book to get its ID, then get the series
                search_res = search_hardcover_books(book.get("isbn13") or book["title"], token, url)
                if search_res and search_res[0].get("id"):
                    hc_series_id = get_hardcover_series_id_for_book(search_res[0]["id"], token, url)
                    if hc_series_id:
                        break
                        
        if not hc_series_id:
            log_func(f"Could not find Hardcover Series ID for CWA series '{series_name}'", "warning")
            continue
            
        hc_series_books = get_hardcover_series_books(token, url, hc_series_id)
        
        for hc_book in hc_series_books:
            # Check if we have this book in CWA
            has_book = False
            if hc_book.get("id") in cwa_hc_ids:
                has_book = True
            elif hc_book.get("isbn13") and hc_book.get("isbn13") in cwa_isbns:
                has_book = True
            elif find_book_in_cwa(cwa_books, hc_book.get("title"), hc_book.get("authors", []), hc_book.get("isbn13")):
                has_book = True
                
            if not has_book and config.get("auto_download", False):
                log_func(f"CWA Series '{series_name}' missing book: '{hc_book.get('title')}'. Triggering Shelfmark...")
                dl_result = trigger_shelfmark_search(
                    shelfmark_url=config["shelfmark_url"],
                    api_key=config.get("shelfmark_api_key", ""),
                    title=hc_book.get("title"),
                    author=hc_book.get("authors")[0] if hc_book.get("authors") else "",
                    isbn13=hc_book.get("isbn13"),
                    preferred_format=config.get("shelfmark_format", "epub"),
                    language=config.get("shelfmark_language", "en")
                )
                if dl_result.get("success"):
                    result["books_downloaded"] += 1
                    log_func(f"  ✓ {dl_result.get('message', 'Download requested')}")
                else:
                    result["errors"] += 1
                    log_func(f"  ✗ {dl_result.get('message', 'Unknown Shelfmark error')}", "error")

    return result

def fix_missing_series_books(token: str, url: str) -> dict:
    """Find series with missing books and add them to Want to Read."""
    result = {"checked_series": 0, "missing_books_added": 0, "errors": 0}

    try:
        series_list = get_hardcover_user_series(token, url)
        result["checked_series"] = len(series_list)

        for series in series_list:
            if series["total_books"] <= len(series["user_books"]):
                continue  # Series is complete

            # Get all books in the series
            series_books = get_hardcover_series_books(token, url, series["id"])
            user_book_ids = set(series["user_books"])

            missing_books = [b for b in series_books if b["id"] not in user_book_ids]

            for book in missing_books:
                if add_hardcover_want_to_read(book["id"], token, url):
                    result["missing_books_added"] += 1
                    logger.info(f"Added missing series book: {book['title']} from series {series['name']}")
                else:
                    result["errors"] += 1
                    logger.error(f"Failed to add missing series book: {book['title']}")

    except Exception as e:
        logger.error(f"Error fixing missing series books: {e}")
        result["errors"] += 1

    return result


def parse_goodreads_title(full_title: str) -> str:
    """Parse Goodreads RSS title to extract clean book title."""
    # Remove everything after : or |
    title = full_title.split(':')[0].split('|')[0].strip()
    # If ends with ), remove the last parentheses block
    if title.endswith(')'):
        paren_start = title.rfind('(')
        if paren_start > 0:
            title = title[:paren_start].strip()
    return title


def parse_goodreads_rss(rss_url: str) -> list[dict]:
    """Parse Goodreads RSS feed and extract book information."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.1",
            "Accept-Language": "en-US,en;q=0.9",
        }
        response = requests.get(rss_url, timeout=30, headers=headers)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        books = []

        # Find all item elements (books)
        for item in root.findall('.//item'):
            title_elem = item.find('title')
            author_elem = item.find('{http://www.goodreads.com/rss/}author_name')
            isbn_elem = item.find('{http://www.goodreads.com/rss}isbn13')
            rating_elem = item.find('{http://www.goodreads.com/rss}user_rating')
            review_elem = item.find('{http://www.goodreads.com/rss}user_review')

            if title_elem is not None and title_elem.text:
                book = {
                    "title": parse_goodreads_title(title_elem.text.strip()),
                    "author": author_elem.text.strip() if author_elem is not None and author_elem.text else "",
                    "isbn13": isbn_elem.text.strip() if isbn_elem is not None and isbn_elem.text else "",
                    "rating": int(rating_elem.text) if rating_elem is not None and rating_elem.text else None,
                    "review": review_elem.text.strip() if review_elem is not None and review_elem.text else "",
                }
                books.append(book)

        return books

    except Exception as e:
        logger.error(f"Error parsing Goodreads RSS: {e}")
        return []


def import_goodreads_to_hardcover(rss_url: str, token: str, url: str, status_id: int = 3) -> dict:
    """Import books from Goodreads RSS to Hardcover."""
    result = {"books_found": 0, "books_added": 0, "books_skipped": 0, "errors": 0}

    try:
        books = parse_goodreads_rss(rss_url)
        result["books_found"] = len(books)

        for book in books:
            # Search for the book on Hardcover by title or ISBN
            search_query = book['title']
            if book['isbn13']:
                search_query = book['isbn13']  # Prefer ISBN if available
            search_results = search_hardcover_books(search_query, token, url)

            if not search_results:
                logger.warning(f"Book not found on Hardcover: {book['title']} by {book['author']}")
                result["books_skipped"] += 1
                continue

            # Use the first search result
            hardcover_book = search_results[0]

            # Add to user books with the specified status
            mutation = f"""
            mutation AddUserBook($bookId: Int!, $statusId: Int!) {{
              insert_user_book(object: {{book_id: $bookId, status_id: $statusId}}) {{
                id
              }}
            }}
            """

            data = _hc_query(mutation, {"bookId": hardcover_book["id"], "statusId": status_id}, token=token, url=url)

            if data:
                result["books_added"] += 1
                logger.info(f"Added book to Hardcover: {book['title']} by {book['author']}")
            else:
                result["errors"] += 1
                logger.error(f"Failed to add book: {book['title']} by {book['author']}")

    except Exception as e:
        logger.error(f"Error importing Goodreads to Hardcover: {e}")
        result["errors"] += 1

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Main sync orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def run_sync(config: dict, state: dict, emit_log=None) -> dict:
    """
    Main sync function. Returns a result dict with counts and log entries.
    state is a shared mutable dict updated in place.
    emit_log(msg, level) is an optional callback for real-time log streaming.
    """
    state.setdefault("log", [])

    result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "books_checked": 0,
        "books_in_cwa": 0,
        "books_downloaded": 0,
        "books_skipped": 0,
        "books_synced_to_cwa": 0,
        "errors": 0,
    }

    def log(msg: str, level: str = "info"):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = {"time": ts, "level": level, "msg": msg}

        state.setdefault("log", [])
        state["log"].append(entry)

        if len(state["log"]) > 500:
            state["log"] = state["log"][-500:]

        log_func = getattr(logger, level, logger.info)
        log_func(msg)

        if emit_log:
            emit_log(entry)

    state["running"] = True
    state["last_sync_start"] = datetime.now().isoformat()

    try:
        log("─── Sync started ───")

        # 1. Fetch books from Hardcover
        log("Fetching books from Hardcover…")

        status_ids = config.get("sync_statuses", [1, 2, 3])
        if not isinstance(status_ids, list):
            status_ids = [1, 2, 3]

        # Always include status 3 (Read) so we can push it back to CWA,
        # even if the user's sync_statuses config doesn't include it.
        fetch_ids = sorted(set(status_ids) | {3})
        hc_books = fetch_hardcover_books(
            token=config["hardcover_token"],
            url=config["hardcover_api_url"],
            status_ids=fetch_ids,
        )

        if not hc_books:
            log(
                "No books returned from Hardcover. This can mean: no matching statuses, invalid token, or a GraphQL/API error.",
                "warning",
            )
            result["finished_at"] = datetime.now(timezone.utc).isoformat()
            state["last_sync_result"] = result
            state["last_sync_end"] = datetime.now().isoformat()
            return result

        status_labels = ", ".join(
            HARDCOVER_STATUS_LABELS.get(status_id, str(status_id))
            for status_id in status_ids
        )
        log(f"Found {len(hc_books)} book(s) on Hardcover with status: {status_labels}")
        result["books_checked"] = len(hc_books)

        # 2. Load CWA library
        log("Reading Calibre/CWA library…")
        cwa_books = lookup_cwa_library(config["cwa_db_path"])
        state["cwa_books"] = cwa_books
        log(f"CWA library contains {len(cwa_books)} book(s)")

        kobo_books = lookup_kobo_library(config.get("kobo_db_path", ""))
        log(f"Kobo sync: found {len(kobo_books)} read book(s) from Kobo")

        sync_results = []

        # 3. For each Hardcover book, compare Hardcover / CWA / Kobo status and sync accordingly
        for user_book in hc_books:
            book = user_book.get("book") or {}

            title = book.get("title") or "Unknown Title"
            authors = _extract_authors(book.get("cached_contributors"))
            author_str = authors[0] if authors else ""

            isbn13 = _best_isbn13(book.get("editions", []))
            cover_url = _extract_cover_url(book.get("cached_image"))

            status_id = user_book.get("status_id", 0)
            status_label = HARDCOVER_STATUS_LABELS.get(status_id, "Unknown")

            cwa_match = find_book_in_cwa(cwa_books, title, authors, isbn13)
            cwa_status_id = cwa_match.get("status_id") if cwa_match else 0
            kobo_match = find_book_in_cwa(kobo_books, title, authors, isbn13)
            kobo_status_id = 3 if kobo_match else 0

            book_entry = {
                "hc_id": book.get("id"),
                "ub_id": user_book.get("id"),
                "title": title,
                "author": author_str,
                "isbn13": isbn13,
                "status": status_label,
                "status_id": status_id,
                "cwa_status_id": cwa_status_id,
                "cwa_status": CWA_STATUS_LABELS.get(cwa_status_id, "Unknown"),
                "kobo_status_id": kobo_status_id,
                "kobo_status": "Read" if kobo_status_id == 3 else "Unknown",
                "in_cwa": False,
                "download_triggered": False,
                "download_result": None,
                "cover_url": cover_url,
            }

            if cwa_match:
                result["books_in_cwa"] += 1
                book_entry["in_cwa"] = True
                log(f"✓ '{title}' — found in CWA (book_id={cwa_match['id']}, status={book_entry['cwa_status']})")

            read_wins = status_id == 3 or cwa_status_id == 3 or kobo_status_id == 3

            # ── Hardcover → CWA status sync ──────────────────────────────
            if cwa_match and status_id in (1, 2, 3) and cwa_status_id != status_id:
                # Hardcover Read always wins
                target_cwa = 3 if read_wins else status_id
                if target_cwa != cwa_status_id:
                    updated = update_cwa_book_status(config["cwa_db_path"], cwa_match["id"], target_cwa)
                    if updated:
                        result["books_synced_to_cwa"] += 1
                        book_entry["cwa_status_id"] = target_cwa
                        book_entry["cwa_status"] = CWA_STATUS_LABELS.get(target_cwa, "Unknown")
                        log(f"  → CWA status updated to '{CWA_STATUS_LABELS.get(target_cwa)}' for '{title}'")
                    else:
                        result["errors"] += 1
                        log(f"  ✗ Failed to update CWA status for '{title}'", "error")

            if read_wins and status_id != 3:
                if update_hardcover_status(
                    token=config["hardcover_token"],
                    url=config["hardcover_api_url"],
                    user_book_id=user_book.get("id"),
                    status_id=3,
                ):
                    status_id = 3
                    status_label = HARDCOVER_STATUS_LABELS[3]
                    book_entry["status_id"] = 3
                    book_entry["status"] = status_label
                    log(f"  → Marked '{title}' as Read in Hardcover")
                else:
                    result["errors"] += 1
                    log(f"  ✗ Failed to mark '{title}' as Read in Hardcover", "error")

            elif cwa_match and cwa_status_id != 3 and status_id != 3:
                if status_id != 1:
                    if update_hardcover_status(
                        token=config["hardcover_token"],
                        url=config["hardcover_api_url"],
                        user_book_id=user_book.get("id"),
                        status_id=1,
                    ):
                        status_id = 1
                        status_label = HARDCOVER_STATUS_LABELS[1]
                        book_entry["status_id"] = 1
                        book_entry["status"] = status_label
                        log(f"  → Marked '{title}' as Want to Read in Hardcover")
                    else:
                        result["errors"] += 1
                        log(f"  ✗ Failed to update Hardcover status for '{title}'", "error")

            if not cwa_match:
                log(f"✗ '{title}' — not in CWA", "warning")

                if config.get("auto_download") and status_id in (1, 2, 3):
                    log(f"  → Triggering Shelfmark for '{title}' by {author_str or '?'}")

                    dl_result = trigger_shelfmark_search(
                        shelfmark_url=config["shelfmark_url"],
                        api_key=config.get("shelfmark_api_key", ""),
                        title=title,
                        author=author_str,
                        isbn13=isbn13,
                        preferred_format=config.get("shelfmark_format", "epub"),
                        language=config.get("shelfmark_language", "en"),
                    )

                    book_entry["download_triggered"] = True
                    book_entry["download_result"] = dl_result

                    if dl_result.get("success"):
                        result["books_downloaded"] += 1
                        log(f"  ✓ {dl_result.get('message', 'Download requested')}")
                    else:
                        result["errors"] += 1
                        log(f"  ✗ {dl_result.get('message', 'Unknown Shelfmark error')}", "error")

                else:
                    result["books_skipped"] += 1
            else:
                book_entry["download_triggered"] = False

            sync_results.append(book_entry)

        if kobo_books:
            log("Syncing Kobo read books into CWA where missing…")
            for kobo_book in kobo_books:
                if not find_book_in_cwa(cwa_books, kobo_book["title"], kobo_book["authors"], kobo_book["isbn13"]):
                    log(f"✗ Kobo read book '{kobo_book['title']}' is missing in CWA", "warning")
                    if config.get("auto_download"):
                        log(f"  → Triggering Shelfmark for Kobo book '{kobo_book['title']}'")
                        dl_result = trigger_shelfmark_search(
                            shelfmark_url=config["shelfmark_url"],
                            api_key=config.get("shelfmark_api_key", ""),
                            title=kobo_book["title"],
                            author=(kobo_book["authors"][0] if kobo_book["authors"] else ""),
                            isbn13=kobo_book["isbn13"],
                            preferred_format=config.get("shelfmark_format", "epub"),
                            language=config.get("shelfmark_language", "en"),
                        )
                        if dl_result.get("success"):
                            result["books_downloaded"] += 1
                            log(f"  ✓ {dl_result.get('message', 'Download requested')}")
                        else:
                            result["errors"] += 1
                            log(f"  ✗ {dl_result.get('message', 'Unknown Shelfmark error')}", "error")
                    else:
                        result["books_skipped"] += 1

        if kobo_books:
            log("Syncing Kobo read books into Hardcover where missing…")
            for kobo_book in kobo_books:
                if not find_book_in_hc(hc_books, kobo_book["title"], kobo_book["authors"], kobo_book["isbn13"]):
                    log(f"✗ Kobo read book '{kobo_book['title']}' is missing in Hardcover", "warning")
                    search_res = search_hardcover_books(
                        kobo_book["isbn13"] or kobo_book["title"],
                        config["hardcover_token"],
                        config["hardcover_api_url"]
                    )
                    if search_res:
                        hc_id = search_res[0].get("id")
                        if hc_id:
                            log(f"  → Found on Hardcover, adding as Read")
                            added = add_hardcover_book_status(
                                book_id=hc_id,
                                status_id=3,
                                token=config["hardcover_token"],
                                url=config["hardcover_api_url"]
                            )
                            if added:
                                log(f"  ✓ Added '{kobo_book['title']}' to Hardcover")
                                # Add a dummy entry so we don't re-add if it's also in CWA
                                hc_books.append({"book": {"title": kobo_book["title"], "id": hc_id}, "status_id": 3})
                            else:
                                log(f"  ✗ Failed to add '{kobo_book['title']}' to Hardcover", "error")
                                result["errors"] += 1
                    else:
                        log(f"  ✗ Could not find '{kobo_book['title']}' on Hardcover search", "warning")

        log("Syncing CWA books into Hardcover where missing…")
        for cwa_book in cwa_books:
            if not find_book_in_hc(hc_books, cwa_book["title"], cwa_book["authors"], cwa_book["isbn13"]):
                target_status = cwa_book.get("status_id", 0)
                if target_status not in [1, 2, 3, 5]:
                    target_status = 1  # Default to Want to Read if unknown
                
                log(f"✗ CWA book '{cwa_book['title']}' is missing in Hardcover", "warning")
                search_res = search_hardcover_books(
                    cwa_book["isbn13"] or cwa_book["title"],
                    config["hardcover_token"],
                    config["hardcover_api_url"]
                )
                if search_res:
                    hc_id = search_res[0].get("id")
                    if hc_id:
                        log(f"  → Found on Hardcover, adding with status {HARDCOVER_STATUS_LABELS.get(target_status, target_status)}")
                        added = add_hardcover_book_status(
                            book_id=hc_id,
                            status_id=target_status,
                            token=config["hardcover_token"],
                            url=config["hardcover_api_url"]
                        )
                        if added:
                            log(f"  ✓ Added '{cwa_book['title']}' to Hardcover")
                            hc_books.append({"book": {"title": cwa_book["title"], "id": hc_id}, "status_id": target_status})
                        else:
                            log(f"  ✗ Failed to add '{cwa_book['title']}' to Hardcover", "error")
                            result["errors"] += 1
                else:
                    log(f"  ✗ Could not find '{cwa_book['title']}' on Hardcover search", "warning")

        # 4. Fix missing series books
        if config.get("auto_fix_series", True):
            log("Checking for missing books in series…")
            series_result = fix_missing_series_books(config["hardcover_token"], config["hardcover_api_url"])
            log(f"Hardcover Series check: {series_result['checked_series']} series checked, {series_result['missing_books_added']} missing books added to Want To Read.")
            
            try:
                cwa_series_res = download_missing_cwa_series_books(cwa_books, config, log)
                log(f"CWA Series check: {cwa_series_res['series_checked']} series checked, {cwa_series_res['books_downloaded']} missing books triggered for download.")
            except Exception as e:
                log(f"Error checking CWA series completion: {e}", "error")

        # 4.5. Sync CWA Kobo Reading progress to Hardcover
        try:
            from cwa_kobo_sync import run_cwa_kobo_sync
            log("Running CWA Kobo reading progress sync to Hardcover...")
            run_cwa_kobo_sync(config, log_func=log)
        except Exception as e:
            log(f"Error during CWA Kobo sync: {e}", "error")


        # 5. Update state
        state["last_sync_books"] = sync_results
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        state["last_sync_result"] = result
        state["last_sync_end"] = datetime.now().isoformat()

        log(
            f"─── Sync complete: {result['books_checked']} checked, "
            f"{result['books_in_cwa']} in CWA, "
            f"{result['books_downloaded']} download(s) triggered, "
            f"{result['books_skipped']} skipped, "
            f"{result['errors']} error(s) ───"
        )

        return result

    except KeyError as e:
        result["errors"] += 1
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        state["last_sync_result"] = result
        state["last_sync_end"] = datetime.now().isoformat()
        log(f"Missing required config key: {e}", "error")
        return result

    except Exception as e:
        result["errors"] += 1
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        state["last_sync_result"] = result
        state["last_sync_end"] = datetime.now().isoformat()
        log(f"Unhandled sync error: {e}", "error")
        return result

    finally:
        state["running"] = False

