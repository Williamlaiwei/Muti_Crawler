import argparse
import csv
import os
import random
import re
import sqlite3
import time
import threading
import sys
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


SCRIBD_HOME = "https://www.scribd.com/"
DOCUMENT_RE = re.compile(
    r"https?://(?:www\.)?scribd\.com/(?:document|doc)/(\d+)(?:/[^?#]*)?",
    re.IGNORECASE,
)


# ============================================================
# KEYWORD POOL
# ============================================================

TOPICS = [
    "computer science", "artificial intelligence", "machine learning",
    "deep learning", "data science", "data mining", "cyber security",
    "network security", "software engineering", "computer networks",
    "database systems", "cloud computing", "distributed systems",
    "operating systems", "computer vision", "natural language processing",
    "information systems", "web development", "mobile development",
    "programming", "python", "java", "c++", "javascript",
    "civil engineering", "mechanical engineering", "electrical engineering",
    "chemical engineering", "environmental engineering", "structural engineering",
    "geotechnical engineering", "transport engineering", "construction engineering",
    "industrial engineering", "aerospace engineering", "biomedical engineering",
    "materials engineering", "architecture", "urban planning",
    "business", "management", "marketing", "finance", "accounting",
    "economics", "international business", "human resource management",
    "operations management", "project management", "supply chain management",
    "entrepreneurship", "business strategy", "corporate finance",
    "biology", "chemistry", "physics", "mathematics", "statistics",
    "astronomy", "geology", "ecology", "environmental science",
    "biochemistry", "microbiology", "genetics", "molecular biology",
    "neuroscience", "psychology", "sociology", "political science",
    "anthropology", "education", "linguistics", "communication",
    "international relations", "public administration", "history",
    "philosophy", "literature", "language", "religion", "art history",
    "cultural studies", "media studies", "medicine", "nursing",
    "public health", "pharmacy", "dentistry", "nutrition", "physiology",
    "anatomy", "pathology", "epidemiology", "law", "international law",
    "business law", "criminal law", "constitutional law", "commercial law",
    "human rights law",
]

DOCUMENT_TYPES = [
    "book", "ebook", "textbook", "report", "research report",
    "technical report", "annual report", "paper", "research paper",
    "conference paper", "working paper", "thesis", "master thesis",
    "phd thesis", "dissertation", "lecture", "lecture notes", "class notes",
    "course notes", "manual", "training manual", "user manual",
    "technical manual", "handbook", "guide", "study guide", "case study",
    "assignment", "project", "project report", "presentation", "slides",
    "review", "literature review", "journal", "article", "chapter",
    "summary", "exam", "exam notes", "practice questions", "tutorial",
    "worksheet",
]

MODIFIERS = [
    "introduction", "fundamentals", "basic", "beginner", "advanced",
    "principles", "concepts", "theory", "analysis", "methods", "methodology",
    "applications", "examples", "overview", "complete", "comprehensive",
    "practical", "professional", "modern", "contemporary", "research",
    "academic", "design", "development", "management", "strategy",
    "practice", "techniques", "case",
]

COUNTRIES = [
    "Australia", "United States", "United Kingdom", "Canada", "China",
    "Taiwan", "Japan", "Korea", "India", "Singapore", "Malaysia",
    "Indonesia", "Thailand", "Vietnam", "Philippines", "Germany", "France",
    "Italy", "Spain", "Netherlands", "Brazil", "Mexico", "South Africa",
    "Nigeria", "Kenya", "Saudi Arabia", "UAE",
]

LEVELS = [
    "university", "college", "undergraduate", "graduate", "postgraduate",
    "master", "doctoral", "phd", "high school", "first year", "second year",
    "third year", "final year",
]

YEARS = [str(y) for y in range(1990, 2027)]

TEMPLATES = [
    "{topic}",
    "{topic} {type}",
    "{modifier} {topic}",
    "{topic} {modifier}",
    "{topic} {year}",
    "{topic} {type} {year}",
    "{topic} {modifier} {type}",
    "{modifier} {topic} {type}",
    "{level} {topic}",
    "{level} {topic} {type}",
    "{topic} {country}",
    "{topic} {type} {country}",
    "{topic} {country} {year}",
    "{topic} {modifier} {year}",
    "{level} {topic} {year}",
    "{topic} {type} {country} {year}",
]


def generate_keyword():
    template = random.choice(TEMPLATES)
    keyword = template.format(
        topic=random.choice(TOPICS),
        type=random.choice(DOCUMENT_TYPES),
        modifier=random.choice(MODIFIERS),
        country=random.choice(COUNTRIES),
        level=random.choice(LEVELS),
        year=random.choice(YEARS),
    )
    return " ".join(keyword.split())


# ============================================================
# DATABASE
# ============================================================

def connect_db(db_path):
    con = sqlite3.connect(
        db_path,
        timeout=30,
        check_same_thread=False,
    )
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def init_db(db_path):
    con = connect_db(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS queries (
            keyword TEXT PRIMARY KEY,
            searched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL DEFAULT 'done',
            result_count INTEGER NOT NULL DEFAULT 0,
            new_document_count INTEGER NOT NULL DEFAULT 0,
            error TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            title TEXT,
            url TEXT NOT NULL,
            found_by_keyword TEXT,
            first_seen TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS downloads (
            document_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'queued',
            file_path TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(document_id) REFERENCES documents(document_id)
        )
        """
    )
    con.commit()
    con.close()


def keyword_used(db_path, keyword):
    con = connect_db(db_path)
    row = con.execute(
        "SELECT 1 FROM queries WHERE keyword = ? LIMIT 1",
        (keyword,),
    ).fetchone()
    con.close()
    return row is not None


def generate_unused_keyword(db_path, max_attempts=10000):
    for _ in range(max_attempts):
        keyword = generate_keyword()
        if not keyword_used(db_path, keyword):
            return keyword
    raise RuntimeError("Could not generate an unused keyword.")


def claim_keyword(db_path, keyword):
    """
    Atomically reserve one keyword for a search worker.

    Returns True only for the worker that successfully inserts it.
    """
    con = connect_db(db_path)
    try:
        cursor = con.execute(
            """
            INSERT OR IGNORE INTO queries
            (
                keyword,
                searched_at,
                status,
                result_count,
                new_document_count,
                error
            )
            VALUES (?, CURRENT_TIMESTAMP, 'running', 0, 0, NULL)
            """,
            (keyword,),
        )
        con.commit()
        return cursor.rowcount > 0
    finally:
        con.close()


def claim_generated_keyword(db_path, db_lock, max_attempts=10000):
    """
    Generate and atomically reserve an unused keyword.
    """
    for _ in range(max_attempts):
        keyword = generate_keyword()

        # Serializing only this tiny DB claim avoids duplicate keywords.
        with db_lock:
            if claim_keyword(db_path, keyword):
                return keyword

    raise RuntimeError("Could not generate and claim an unused keyword.")


def fallback_title_from_url(url):
    parts = [p for p in urlsplit(url).path.split("/") if p]
    if len(parts) >= 3:
        return parts[2].replace("-", " ").replace("_", " ").strip()
    return ""


def save_search_results(db_path, keyword, results):
    con = connect_db(db_path)
    new_count = 0

    for item in results:
        doc_id = item.get("id", "").strip()
        url = item.get("url", "").strip()
        title = item.get("title", "").strip() or fallback_title_from_url(url)

        if not doc_id or not url:
            continue

        cursor = con.execute(
            """
            INSERT OR IGNORE INTO documents
            (document_id, title, url, found_by_keyword)
            VALUES (?, ?, ?, ?)
            """,
            (doc_id, title, url, keyword),
        )

        if cursor.rowcount > 0:
            new_count += 1
            con.execute(
                """
                INSERT OR IGNORE INTO downloads(document_id, status)
                VALUES (?, 'queued')
                """,
                (doc_id,),
            )
        elif title:
            # Fill title later if the first sighting had a blank title.
            con.execute(
                """
                UPDATE documents
                SET title = CASE
                    WHEN title IS NULL OR title = '' THEN ?
                    ELSE title
                END
                WHERE document_id = ?
                """,
                (title, doc_id),
            )

    con.execute(
        """
        INSERT OR REPLACE INTO queries
        (keyword, searched_at, status, result_count, new_document_count, error)
        VALUES (?, CURRENT_TIMESTAMP, 'done', ?, ?, NULL)
        """,
        (keyword, len(results), new_count),
    )

    con.commit()
    con.close()
    return new_count


def save_query_error(db_path, keyword, error):
    con = connect_db(db_path)
    con.execute(
        """
        INSERT OR REPLACE INTO queries
        (keyword, searched_at, status, result_count, new_document_count, error)
        VALUES (?, CURRENT_TIMESTAMP, 'error', 0, 0, ?)
        """,
        (keyword, str(error)[:2000]),
    )
    con.commit()
    con.close()


def get_counts(db_path):
    con = connect_db(db_path)
    total_docs = con.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    queued = con.execute(
        "SELECT COUNT(*) FROM downloads WHERE status = 'queued'"
    ).fetchone()[0]
    searched = con.execute("SELECT COUNT(*) FROM queries").fetchone()[0]
    con.close()
    return total_docs, queued, searched


def export_queue(db_path, output_path, limit=None):
    con = connect_db(db_path)
    sql = """
        SELECT d.url
        FROM documents d
        JOIN downloads q ON q.document_id = d.document_id
        WHERE q.status = 'queued'
        ORDER BY d.first_seen, d.document_id
    """
    params = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (limit,)

    rows = con.execute(sql, params).fetchall()
    con.close()

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as f:
        for (url,) in rows:
            f.write(url + "\n")
    return len(rows)


def export_metadata_csv(db_path, output_path):
    con = connect_db(db_path)
    rows = con.execute(
        """
        SELECT document_id, title, url, found_by_keyword, first_seen
        FROM documents
        ORDER BY first_seen, document_id
        """
    ).fetchall()
    con.close()

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "document_id", "title", "url", "found_by_keyword", "first_seen"
        ])
        writer.writerows(rows)
    return len(rows)


# ============================================================
# CHROME
# ============================================================

def build_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1440,1200")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--lang=en-US")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


# ============================================================
# SEARCH
# ============================================================

def dismiss_popups(driver):
    texts = [
        "Accept", "Accept All", "Accept all", "I agree", "Got it",
        "Close", "No thanks", "Not now",
    ]
    for text in texts:
        try:
            elements = driver.find_elements(
                By.XPATH,
                f"//button[contains(normalize-space(.), '{text}')]",
            )
            for element in elements:
                if element.is_displayed():
                    try:
                        element.click()
                        time.sleep(0.5)
                        return
                    except Exception:
                        pass
        except Exception:
            pass


def find_search_input(driver):
    selectors = [
        (By.CSS_SELECTOR, "input[type='search']"),
        (By.CSS_SELECTOR, "input[placeholder*='Search' i]"),
        (By.CSS_SELECTOR, "input[aria-label*='Search' i]"),
        (
            By.XPATH,
            "//input[contains(translate(@placeholder,"
            "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'search')]",
        ),
    ]

    for by, selector in selectors:
        try:
            element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((by, selector))
            )
            if element:
                return element
        except TimeoutException:
            continue
    return None


def build_search_url(keyword, page=1):
    """
    Build a Scribd search URL with the 1-3 page filter and an explicit page.

    Example page 3:
      https://www.scribd.com/search?query=tea&ct_lang=0&
      filters=%7B%22num_pages%22%3A%221-3%22%7D&page=3
    """
    params = {
        "query": keyword,
        "ct_lang": "0",
        "filters": '{"num_pages":"1-3"}',
        "page": str(page),
    }
    return "https://www.scribd.com/search?" + urlencode(params)


def search_scribd(
    driver,
    keyword,
    page=1,
    result_wait_seconds=60.0,
    stable_checks=3,
    stable_interval_seconds=0.5,
    stop_event=None,
    progress_callback=None,
):
    """
    Open one filtered Scribd result page exactly once.

    No refresh/retry loop is used. After navigation, poll the number of visible
    Scribd document links until the positive count is stable for
    `stable_checks` consecutive observations, or until `result_wait_seconds`
    expires. A zero count is never considered "stable"; zero waits all the way
    to the timeout before the caller treats the page as empty.
    """
    search_url = build_search_url(keyword, page=page)
    driver.get(search_url)

    # Basic document readiness first.
    try:
        WebDriverWait(
            driver,
            min(max(result_wait_seconds, 1.0), 60.0),
        ).until(
            lambda d: d.execute_script("return document.readyState")
            in ("interactive", "complete")
        )
    except TimeoutException:
        pass

    dismiss_popups(driver)

    stable_checks = max(1, int(stable_checks))
    stable_interval_seconds = max(0.1, float(stable_interval_seconds))
    deadline = time.monotonic() + max(1.0, float(result_wait_seconds))

    last_positive_count = None
    stable_run = 0
    observed_count = 0

    while time.monotonic() < deadline:
        if stop_event is not None and stop_event.is_set():
            return driver.current_url, observed_count

        try:
            observed_count = len(
                driver.find_elements(
                    By.CSS_SELECTOR,
                    "a[href*='/document/'], a[href*='/doc/']",
                )
            )
        except Exception:
            observed_count = 0

        if progress_callback:
            progress_callback(
                page=page,
                observed_count=observed_count,
                status="waiting",
            )

        if observed_count > 0:
            if observed_count == last_positive_count:
                stable_run += 1
            else:
                last_positive_count = observed_count
                stable_run = 1

            if stable_run >= stable_checks:
                if progress_callback:
                    progress_callback(
                        page=page,
                        observed_count=observed_count,
                        status="stable",
                    )
                break
        else:
            # Zero is not accepted as "stable". Keep waiting until timeout.
            last_positive_count = None
            stable_run = 0

        if stop_event is not None:
            if stop_event.wait(stable_interval_seconds):
                break
        else:
            time.sleep(stable_interval_seconds)

    return driver.current_url, observed_count


# ============================================================
# RESULT EXTRACTION
# ============================================================

def normalize_document_url(url):
    if not url:
        return None
    match = DOCUMENT_RE.search(url)
    if not match:
        return None

    doc_id = match.group(1)
    parts = urlsplit(url)
    path_parts = [part for part in parts.path.split("/") if part]
    if len(path_parts) < 2:
        return None

    if len(path_parts) >= 3:
        slug = path_parts[2]
        return f"https://www.scribd.com/document/{doc_id}/{slug}"
    return f"https://www.scribd.com/document/{doc_id}"


def get_document_id(url):
    match = DOCUMENT_RE.search(url)
    return match.group(1) if match else ""


def collect_visible_documents(driver):
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
    results = []

    for anchor in anchors:
        try:
            href = anchor.get_attribute("href")
            normalized = normalize_document_url(href)
            if not normalized:
                continue

            try:
                title = (
                    anchor.get_attribute("aria-label")
                    or anchor.get_attribute("title")
                    or anchor.text
                    or ""
                ).strip()
            except Exception:
                title = ""

            results.append({
                "id": get_document_id(normalized),
                "url": normalized,
                "title": title,
            })
        except Exception:
            continue

    return results


def collect_one_result_page(
    driver,
    keyword,
    page_number,
    stop_event=None,
    result_wait_seconds=60.0,
    stable_checks=3,
    stable_interval_seconds=0.5,
    progress_callback=None,
):
    """
    Load one result page once, wait for its result count to stabilize, then
    perform exactly one DOM scan. The page is never refreshed by this function.
    """
    if stop_event is not None and stop_event.is_set():
        return []

    if progress_callback:
        progress_callback(
            page=page_number,
            page_unique=None,
            observed_count=0,
            status="loading",
        )

    search_scribd(
        driver,
        keyword,
        page=page_number,
        result_wait_seconds=result_wait_seconds,
        stable_checks=stable_checks,
        stable_interval_seconds=stable_interval_seconds,
        stop_event=stop_event,
        progress_callback=progress_callback,
    )

    if stop_event is not None and stop_event.is_set():
        return []

    page_documents = {}
    visible = collect_visible_documents(driver)

    for item in visible:
        doc_id = item["id"]
        if not doc_id:
            continue

        if doc_id not in page_documents:
            page_documents[doc_id] = item
        elif (
            not page_documents[doc_id]["title"]
            and item["title"]
        ):
            page_documents[doc_id]["title"] = item["title"]

    if progress_callback:
        progress_callback(
            page=page_number,
            page_unique=len(page_documents),
            observed_count=len(page_documents),
            page_document_ids=list(page_documents.keys()),
            status=("loaded" if page_documents else "empty"),
        )

    return list(page_documents.values())


def collect_search_results(
    driver,
    keyword,
    max_results=None,
    max_pages=20,
    stop_event=None,
    result_wait_seconds=60.0,
    stable_checks=3,
    stable_interval_seconds=0.5,
    page_delay_seconds=2.0,
    progress_callback=None,
):
    """
    Visit result pages until one of these conditions is met:

      - max_pages > 0 and that page limit is reached;
      - max_results is not None and that per-keyword document cap is reached;
      - two consecutive pages add no new document IDs;
      - Ctrl+C / stop_event interrupts the run.

    max_pages == 0 means automatic/unlimited paging until exhaustion is
    detected by consecutive no-new pages.
    """
    documents = {}
    empty_or_duplicate_pages = 0
    page_number = 1

    while True:
        if stop_event is not None and stop_event.is_set():
            break

        if max_pages > 0 and page_number > max_pages:
            break

        if max_results is not None and len(documents) >= max_results:
            break

        before_count = len(documents)

        page_results = collect_one_result_page(
            driver,
            keyword,
            page_number,
            stop_event=stop_event,
            result_wait_seconds=result_wait_seconds,
            stable_checks=stable_checks,
            stable_interval_seconds=stable_interval_seconds,
            progress_callback=progress_callback,
        )

        for item in page_results:
            doc_id = item["id"]
            if not doc_id:
                continue

            if doc_id not in documents:
                documents[doc_id] = item
            elif (
                not documents[doc_id]["title"]
                and item["title"]
            ):
                documents[doc_id]["title"] = item["title"]

        new_on_page = len(documents) - before_count

        if progress_callback:
            progress_callback(
                page=page_number,
                page_unique=len(page_results),
                new_on_page=new_on_page,
                keyword_total=len(documents),
                keyword_target=max_results,
                status="page done",
            )

        if new_on_page == 0:
            empty_or_duplicate_pages += 1
        else:
            empty_or_duplicate_pages = 0

        if empty_or_duplicate_pages >= 2:
            if progress_callback:
                progress_callback(
                    page=page_number,
                    page_unique=len(page_results),
                    new_on_page=new_on_page,
                    keyword_total=len(documents),
                    keyword_target=max_results,
                    status="keyword done",
                )
            break

        if max_results is not None and len(documents) >= max_results:
            break

        if max_pages > 0 and page_number >= max_pages:
            break

        if page_delay_seconds > 0:
            if stop_event is not None:
                if stop_event.wait(page_delay_seconds):
                    break
            else:
                time.sleep(page_delay_seconds)

        page_number += 1

    result_list = list(documents.values())
    if max_results is not None:
        result_list = result_list[:max_results]

    return result_list


# ============================================================
# PIPELINE
# ============================================================

def run_pipeline(
    db_path,
    target_new,
    max_per_query,
    queue_output,
    metadata_output,
    headless,
    one_keyword=None,
    pause_between_queries=2.0,
    pages_per_keyword=3,
    search_workers=1,
    result_wait_seconds=60.0,
    stable_checks=3,
    stable_interval_seconds=0.5,
    page_delay_seconds=2.0,
):
    """
    Search Scribd with 1-64 concurrent workers.

    Target semantics:
      - Once the live NEW unique count reaches target_new, no NEW keyword is
        assigned.
      - Workers that already started a keyword are allowed to finish that
        keyword normally and save all of its results.
      - Therefore the final count may intentionally exceed target_new.

    Paging semantics:
      - pages_per_keyword > 0: hard page limit per keyword.
      - pages_per_keyword == 0: automatic/unlimited paging until exhaustion.
    """
    init_db(db_path)

    if not 1 <= search_workers <= 64:
        raise ValueError("search_workers must be between 1 and 64")

    if pages_per_keyword < 0:
        raise ValueError("pages_per_keyword cannot be negative")

    if one_keyword and search_workers != 1:
        print(
            "[INFO] --keyword is single-keyword test mode; "
            "forcing search_workers=1."
        )
        search_workers = 1

    counter_lock = threading.Lock()
    db_lock = threading.Lock()
    drivers_lock = threading.Lock()
    state_lock = threading.Lock()
    stop_event = threading.Event()          # user interrupt / fatal stop only
    target_reached_event = threading.Event()  # stop assigning NEW keywords
    dashboard_stop = threading.Event()

    active_drivers = {}

    state = {
        "new_this_run": 0,
        "live_new_this_run": 0,
        "queries_finished": 0,
    }

    live_seen_ids = set()
    live_lock = threading.Lock()

    worker_states = {
        worker_id: {
            "keyword": "starting...",
            "page": 0,
            "max_pages": pages_per_keyword,
            "page_unique": None,
            "observed_count": 0,
            "keyword_total": 0,
            "status": "starting",
        }
        for worker_id in range(1, search_workers + 1)
    }

    def short_keyword(value, width=32):
        value = value or ""
        return value if len(value) <= width else value[: width - 1] + "…"

    def progress_bar(current, total, width=18):
        if total is None or total <= 0:
            return "░" * width

        total = max(int(total), 1)
        current = max(0, min(int(current or 0), total))
        filled = int(width * current / total)
        return "█" * filled + "░" * (width - filled)

    def page_label(page, max_pages):
        if max_pages == 0:
            return f"{page:>2}/∞"
        return f"{page:>2}/{max_pages:<2}"

    def update_worker(worker_id, **kwargs):
        with state_lock:
            worker_states[worker_id].update(kwargs)

    def add_live_document_ids(document_ids):
        ids = {str(x).strip() for x in (document_ids or []) if str(x).strip()}
        if not ids:
            return

        with live_lock:
            candidates = ids - live_seen_ids
            if not candidates:
                return

            live_seen_ids.update(candidates)

            placeholders = ",".join("?" for _ in candidates)
            con = connect_db(db_path)
            try:
                rows = con.execute(
                    f"""
                    SELECT document_id
                    FROM documents
                    WHERE document_id IN ({placeholders})
                    """,
                    tuple(candidates),
                ).fetchall()
            finally:
                con.close()

            existing = {row[0] for row in rows}
            fresh = candidates - existing

            if fresh:
                with counter_lock:
                    state["live_new_this_run"] += len(fresh)
                    live_new = state["live_new_this_run"]

                if live_new >= target_new:
                    target_reached_event.set()

    def dashboard_loop():
        first_draw = True

        while not dashboard_stop.is_set():
            with state_lock:
                snapshot = {
                    wid: dict(values)
                    for wid, values in worker_states.items()
                }

            with counter_lock:
                confirmed_new = state["new_this_run"]
                live_new = state["live_new_this_run"]
                queries_finished = state["queries_finished"]

            target_state = (
                " | target reached; finishing active keywords"
                if target_reached_event.is_set()
                else ""
            )

            lines = [
                (
                    f"TOTAL {progress_bar(live_new, target_new, 26)} "
                    f"{live_new}/{target_new} live | "
                    f"confirmed {confirmed_new} | "
                    f"queries {queries_finished}"
                    f"{target_state}"
                )
            ]

            for wid in sorted(snapshot):
                ws = snapshot[wid]

                detail = ""
                if ws["page_unique"] is not None:
                    detail += f" | page {ws['page_unique']:>2}"
                elif ws["observed_count"]:
                    detail += f" | seen {ws['observed_count']:>2}"

                lines.append(
                    f"W{wid:02d} "
                    f"{progress_bar(ws['page'], ws['max_pages'])} "
                    f"{page_label(ws['page'], ws['max_pages']):>5} | "
                    f"{short_keyword(ws['keyword']):<32} | "
                    f"{ws['status']:<12} | "
                    f"found {ws['keyword_total']:<4}"
                    f"{detail}"
                )

            if first_draw:
                print("\n".join(lines), flush=True)
                first_draw = False
            else:
                sys.stdout.write(f"\033[{len(lines)}F")
                for line in lines:
                    sys.stdout.write("\033[2K" + line + "\n")
                sys.stdout.flush()

            dashboard_stop.wait(0.5)

    def register_driver(worker_id, driver):
        with drivers_lock:
            active_drivers[worker_id] = driver

    def unregister_driver(worker_id):
        with drivers_lock:
            active_drivers.pop(worker_id, None)

    def close_driver(worker_id, driver):
        if driver is None:
            return
        try:
            driver.quit()
        except Exception:
            pass
        unregister_driver(worker_id)

    def force_stop_active_drivers():
        with drivers_lock:
            snapshot = list(active_drivers.items())

        for _, driver in snapshot:
            try:
                service = getattr(driver, "service", None)
                process = getattr(service, "process", None)

                if process is not None and process.poll() is None:
                    process.terminate()
            except Exception:
                pass

    def make_driver(worker_id):
        driver = build_driver(headless=headless)
        register_driver(worker_id, driver)
        return driver

    def process_worker(worker_id):
        driver = None

        def progress_callback(**kwargs):
            mapped = {}

            if kwargs.get("page") is not None:
                mapped["page"] = kwargs["page"]

            if "page_unique" in kwargs:
                mapped["page_unique"] = kwargs["page_unique"]

            if kwargs.get("observed_count") is not None:
                mapped["observed_count"] = kwargs["observed_count"]

            page_document_ids = kwargs.get("page_document_ids")
            if page_document_ids:
                add_live_document_ids(page_document_ids)

            if kwargs.get("keyword_total") is not None:
                mapped["keyword_total"] = kwargs["keyword_total"]

            if kwargs.get("status"):
                mapped["status"] = kwargs["status"]

            update_worker(worker_id, **mapped)

        try:
            driver = make_driver(worker_id)

            while not stop_event.is_set():
                # IMPORTANT: reaching the target stops only NEW keyword
                # assignment. It never cuts off the keyword already in progress.
                if target_reached_event.is_set() and not one_keyword:
                    update_worker(worker_id, status="target done")
                    break

                if one_keyword:
                    keyword = one_keyword.strip()

                    with db_lock:
                        claimed = claim_keyword(db_path, keyword)

                    if not claimed:
                        update_worker(
                            worker_id,
                            keyword=keyword,
                            status="already used",
                        )
                        break
                else:
                    keyword = claim_generated_keyword(
                        db_path,
                        db_lock,
                    )

                if stop_event.is_set():
                    break

                update_worker(
                    worker_id,
                    keyword=keyword,
                    page=0,
                    max_pages=pages_per_keyword,
                    page_unique=None,
                    observed_count=0,
                    keyword_total=0,
                    status="starting",
                )

                driver_failed = False

                try:
                    results = collect_search_results(
                        driver,
                        keyword=keyword,
                        max_results=max_per_query,
                        max_pages=pages_per_keyword,
                        stop_event=stop_event,
                        result_wait_seconds=result_wait_seconds,
                        stable_checks=stable_checks,
                        stable_interval_seconds=stable_interval_seconds,
                        page_delay_seconds=page_delay_seconds,
                        progress_callback=progress_callback,
                    )

                    # Only a real user/fatal stop discards an unfinished keyword.
                    # Reaching the target does NOT set stop_event, so active
                    # keywords always get saved.
                    if stop_event.is_set():
                        break

                    with db_lock:
                        new_count = save_search_results(
                            db_path,
                            keyword,
                            results,
                        )

                    with counter_lock:
                        state["new_this_run"] += new_count
                        state["queries_finished"] += 1
                        run_new = state["new_this_run"]

                        if state["live_new_this_run"] < run_new:
                            state["live_new_this_run"] = run_new

                        live_new = state["live_new_this_run"]

                    if live_new >= target_new or run_new >= target_new:
                        target_reached_event.set()

                    update_worker(
                        worker_id,
                        status=f"saved +{new_count}",
                        keyword_total=len(results),
                    )

                except Exception as error:
                    if stop_event.is_set():
                        break

                    with db_lock:
                        save_query_error(
                            db_path,
                            keyword,
                            error,
                        )

                    update_worker(
                        worker_id,
                        status=f"error {type(error).__name__}",
                    )
                    driver_failed = True

                if driver_failed and not stop_event.is_set():
                    close_driver(worker_id, driver)
                    driver = None

                    try:
                        update_worker(worker_id, status="restarting")
                        driver = make_driver(worker_id)
                    except Exception:
                        stop_event.set()
                        break

                if one_keyword:
                    break

                if target_reached_event.is_set():
                    update_worker(worker_id, status="target done")
                    break

                if pause_between_queries > 0:
                    update_worker(worker_id, status="waiting")
                    if stop_event.wait(pause_between_queries):
                        break

        finally:
            update_worker(worker_id, status="stopped")
            close_driver(worker_id, driver)

    pages_label = (
        "auto / until exhausted"
        if pages_per_keyword == 0
        else str(pages_per_keyword)
    )
    max_docs_label = (
        "unlimited"
        if max_per_query is None
        else str(max_per_query)
    )

    print(
        "\n========== SEARCH CONFIG (v14) ==========\n"
        f"Search workers       : {search_workers}\n"
        f"Target new documents : {target_new}\n"
        f"Pages per keyword    : {pages_label}\n"
        f"Max docs per keyword : {max_docs_label}\n"
        f"Result max wait      : {result_wait_seconds}s\n"
        f"Stable checks        : {stable_checks}\n"
        f"Stable interval      : {stable_interval_seconds}s\n"
        f"Refresh/retry        : disabled\n"
        f"Delay between pages  : {page_delay_seconds}s\n"
        f"=========================================\n"
    )

    dashboard_thread = threading.Thread(
        target=dashboard_loop,
        name="search-dashboard",
        daemon=True,
    )
    dashboard_thread.start()

    executor = ThreadPoolExecutor(
        max_workers=search_workers,
        thread_name_prefix="scribd-search",
    )

    futures = [
        executor.submit(process_worker, worker_id)
        for worker_id in range(1, search_workers + 1)
    ]

    interrupted = False

    try:
        pending = set(futures)

        while pending:
            done, pending = wait(
                pending,
                timeout=0.25,
                return_when=FIRST_COMPLETED,
            )

            for future in done:
                future.result()

    except KeyboardInterrupt:
        interrupted = True
        stop_event.set()
        force_stop_active_drivers()

        for future in futures:
            future.cancel()

    finally:
        executor.shutdown(
            wait=not interrupted,
            cancel_futures=True,
        )

        dashboard_stop.set()
        dashboard_thread.join(timeout=1.5)
        print()

    queue_count = export_queue(db_path, queue_output)
    metadata_count = export_metadata_csv(db_path, metadata_output)

    print("\n========== SEARCH SUMMARY ==========")
    print(f"Search workers          : {search_workers}")
    print(f"Live unique observed    : {state['live_new_this_run']}")
    print(f"New documents confirmed : {state['new_this_run']}")
    print(f"Queries finished        : {state['queries_finished']}")
    print(f"Queued URLs             : {queue_count}")
    print(f"Metadata rows           : {metadata_count}")
    print(f"Database                : {db_path}")
    print(f"Queue file              : {queue_output}")
    print(f"Metadata CSV            : {metadata_output}")
    if target_reached_event.is_set() and not interrupted:
        print("Target behavior         : no new keywords; active keywords finished")
    if interrupted:
        print("Status                  : interrupted by user")
    print("====================================")

    return {
        "interrupted": interrupted,
        "live_new": state["live_new_this_run"],
        "confirmed_new": state["new_this_run"],
        "queries_finished": state["queries_finished"],
        "queued_urls": queue_count,
        "metadata_rows": metadata_count,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Generate unused keywords, search Scribd, deduplicate documents, "
            "and build a persistent batch-download queue."
        )
    )
    parser.add_argument(
        "--target-new",
        type=int,
        default=1000,
        help=(
            "Once this many live NEW unique documents are observed, stop "
            "assigning new keywords. Already-running keywords finish normally."
        ),
    )
    parser.add_argument(
        "--search-workers",
        type=int,
        default=1,
        help=(
            "Concurrent Scribd search workers / Chrome instances. "
            "Allowed range: 1-64. Default: 1."
        ),
    )
    parser.add_argument(
        "--pages-per-keyword",
        type=int,
        default=3,
        help=(
            "Maximum result pages per keyword. Use 0 for automatic/unlimited "
            "paging until the keyword is exhausted."
        ),
    )
    parser.add_argument(
        "--max-per-query",
        type=int,
        default=None,
        help=(
            "Optional unique-document cap per keyword. With a positive page "
            "limit, the default is pages-per-keyword * 40. In auto page mode "
            "(--pages-per-keyword 0), omitted means unlimited."
        ),
    )
    parser.add_argument("--db", default="scribd_state.db")
    parser.add_argument(
        "--queue-output",
        default="scribd_download_queue.txt",
    )
    parser.add_argument(
        "--metadata-output",
        default="scribd_documents.csv",
    )
    parser.add_argument(
        "--keyword",
        default=None,
        help="Optional one-keyword test mode.",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=2.0,
        help="Pause in seconds between keyword searches.",
    )
    parser.add_argument(
        "--result-wait",
        type=float,
        default=60.0,
        help=(
            "Maximum seconds to wait on the SAME result page before treating "
            "a still-zero page as empty. No refresh is performed. Default: 60."
        ),
    )
    parser.add_argument(
        "--stable-checks",
        type=int,
        default=3,
        help=(
            "Positive result count must remain unchanged for this many checks "
            "before scanning. Default: 3."
        ),
    )
    parser.add_argument(
        "--stable-interval",
        type=float,
        default=0.5,
        help="Seconds between result-count stability checks. Default: 0.5.",
    )
    # Backward-compatible old options. They are accepted so previous commands
    # do not fail, but v14 intentionally does not refresh/retry the same page.
    parser.add_argument(
        "--render-settle",
        type=float,
        default=0.0,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--empty-retries",
        type=int,
        default=0,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--page-delay",
        type=float,
        default=2.0,
        help="Seconds between result pages for the same keyword. Default: 2.",
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Show Chrome instead of headless mode.",
    )
    args = parser.parse_args()

    if args.target_new <= 0:
        raise SystemExit("--target-new must be greater than 0")
    if not 1 <= args.search_workers <= 64:
        raise SystemExit("--search-workers must be between 1 and 64")
    if args.pages_per_keyword < 0:
        raise SystemExit("--pages-per-keyword cannot be negative")
    if args.max_per_query is not None and args.max_per_query <= 0:
        raise SystemExit("--max-per-query must be greater than 0")
    if args.result_wait <= 0:
        raise SystemExit("--result-wait must be greater than 0")
    if args.stable_checks <= 0:
        raise SystemExit("--stable-checks must be greater than 0")
    if args.stable_interval <= 0:
        raise SystemExit("--stable-interval must be greater than 0")
    if args.page_delay < 0:
        raise SystemExit("--page-delay cannot be negative")

    max_per_query = (
        args.max_per_query
        if args.max_per_query is not None
        else (
            None
            if args.pages_per_keyword == 0
            else args.pages_per_keyword * 40
        )
    )

    run_pipeline(
        db_path=args.db,
        target_new=args.target_new,
        max_per_query=max_per_query,
        queue_output=args.queue_output,
        metadata_output=args.metadata_output,
        headless=not args.show_browser,
        one_keyword=args.keyword,
        pause_between_queries=max(args.pause, 0),
        pages_per_keyword=args.pages_per_keyword,
        search_workers=args.search_workers,
        result_wait_seconds=args.result_wait,
        stable_checks=args.stable_checks,
        stable_interval_seconds=args.stable_interval,
        page_delay_seconds=args.page_delay,
    )
