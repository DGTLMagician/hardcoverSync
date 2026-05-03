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
    """Extract author names from Hardcover cached_contributors JSON."""
    contributors = _json_maybe(cached_contributors, [])

    if not isinstance(contributors, list):
        return []

    authors: list[str] = []

    for contributor in contributors:
        if not isinstance(contributor, dict):
            continue

        name = contributor.get("name")
        contribution = contributor.get("contribution")

        if not name:
            continue

        # Hardcover usually uses "Author", but keep it tolerant.
        if contribution in (None, "Author", "author"):
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

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    payload = {"query": query}
    if variables:
        payload["variables"] = variables

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
            cur.execute("SELECT id, title, pubdate, last_modified, uuid FROM books")
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

            books.append(
                {
                    "id": book_id,
                    "title": row["title"],
                    "authors": authors,
                    "isbn13": _normalise_isbn(isbn13),
                    "identifiers": identifiers,
                    "tags": tags,
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

    search_query = isbn13 if isbn13 else f"{title} {author}".strip()
    if not search_query:
        return {"success": False, "message": "No title/author/ISBN available for Shelfmark search"}

    try:
        releases_url = f"{api_base}/releases"

        params = {
            "source": release_source or "direct_download",
            "query": search_query,
            "content_type": "ebook",
        }

        # Shelfmark supports language filters on release searches in the frontend flow.
        if language:
            params["languages"] = language

        response = session.get(
            releases_url,
            params=params,
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

        if not releases:
            return {
                "success": False,
                "message": f"No Shelfmark releases found for query '{search_query}'",
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
        return {
            "success": False,
            "message": f"Cannot reach Shelfmark at {shelfmark_url}",
        }
    except requests.exceptions.Timeout:
        return {
            "success": False,
            "message": f"Shelfmark request timed out at {shelfmark_url}",
        }
    except requests.RequestException as e:
        return {
            "success": False,
            "message": f"Shelfmark HTTP error: {e}",
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Shelfmark error: {e}",
        }

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

        hc_books = fetch_hardcover_books(
            token=config["hardcover_token"],
            url=config["hardcover_api_url"],
            status_ids=status_ids,
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
        log(f"CWA library contains {len(cwa_books)} book(s)")

        sync_results = []

        # 3. For each Hardcover book, check CWA and optionally trigger Shelfmark
        for user_book in hc_books:
            book = user_book.get("book") or {}

            title = book.get("title") or "Unknown Title"
            authors = _extract_authors(book.get("cached_contributors"))
            author_str = authors[0] if authors else ""

            isbn13 = _best_isbn13(book.get("editions", []))
            cover_url = _extract_cover_url(book.get("cached_image"))

            status_id = user_book.get("status_id", 0)
            status_label = HARDCOVER_STATUS_LABELS.get(status_id, "Unknown")

            book_entry = {
                "hc_id": book.get("id"),
                "ub_id": user_book.get("id"),
                "title": title,
                "author": author_str,
                "isbn13": isbn13,
                "status": status_label,
                "status_id": status_id,
                "in_cwa": False,
                "download_triggered": False,
                "download_result": None,
                "cover_url": cover_url,
            }

            cwa_match = find_book_in_cwa(cwa_books, title, authors, isbn13)

            if cwa_match:
                result["books_in_cwa"] += 1
                book_entry["in_cwa"] = True
                log(f"✓ '{title}' — found in CWA (book_id={cwa_match['id']})")

            else:
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

            sync_results.append(book_entry)

        # 4. Update state
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

