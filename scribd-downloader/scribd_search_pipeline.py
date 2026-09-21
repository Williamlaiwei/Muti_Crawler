# Scribd Search Pipeline v16.2 — v16 search optimizations + separate diagnostics
import argparse
import csv
import json
import os
import random
import re
import sqlite3
import time
import threading
import sys
from collections import deque
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
from urllib.parse import urlencode, urlsplit, parse_qs


try:
    import psutil  # optional; enables system/Chrome resource telemetry
except Exception:
    psutil = None

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
            started_at TEXT,
            last_attempt_at TEXT,
            heartbeat_at TEXT,
            finished_at TEXT,
            failed_at TEXT,
            recovered_at TEXT,
            FOREIGN KEY(document_id) REFERENCES documents(document_id)
        )
        """
    )
    # Existing databases created by v14 and earlier are migrated in-place.
    existing_columns = {
        row[1]
        for row in con.execute("PRAGMA table_info(downloads)").fetchall()
    }
    monitoring_columns = {
        "started_at": "TEXT",
        "last_attempt_at": "TEXT",
        "heartbeat_at": "TEXT",
        "finished_at": "TEXT",
        "failed_at": "TEXT",
        "recovered_at": "TEXT",
    }
    for column_name, column_type in monitoring_columns.items():
        if column_name not in existing_columns:
            con.execute(
                f"ALTER TABLE downloads ADD COLUMN {column_name} {column_type}"
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

def build_driver(
    headless=True,
    block_images=True,
    page_load_strategy="eager",
):
    """Build a search browser using conservative v16 optimizations.

    ``eager`` lets Selenium return once the DOM is ready instead of waiting for
    every image/subresource. The existing result-count stability checks still
    decide when the Scribd results are ready to scan.

    Search-result images can be disabled because the crawler only needs links
    and text metadata. Both optimizations can be switched off for A/B tests.
    """
    options = webdriver.ChromeOptions()

    strategy = str(page_load_strategy or "eager").lower()
    if strategy not in {"normal", "eager", "none"}:
        raise ValueError(
            "page_load_strategy must be one of: normal, eager, none"
        )
    options.page_load_strategy = strategy

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1440,1200")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--lang=en-US")

    if block_images:
        options.add_experimental_option(
            "prefs",
            {"profile.managed_default_content_settings.images": 2},
        )

    # Linux / Colab containers normally require these flags. They are added
    # only on Linux so the Windows local configuration remains unchanged.
    if sys.platform.startswith("linux"):
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=0")

        for chrome_binary in (
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
        ):
            if os.path.exists(chrome_binary):
                options.binary_location = chrome_binary
                break

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

    v16.1 diagnostic behavior:
      - keeps the original waiting/stability rule unchanged;
      - records navigation time, result-stability wait time, observed-count
        trace, requested URL and actual URL;
      - returns diagnostics to the caller so page 0 / duplicate-page cases can
        be explained without changing the crawler's search behavior.

    A zero count is still NEVER considered stable. It waits until
    `result_wait_seconds` expires, exactly like v16.
    """
    search_url = build_search_url(keyword, page=page)

    navigation_started = time.perf_counter()
    driver.get(search_url)
    navigation_seconds = time.perf_counter() - navigation_started

    ready_started = time.perf_counter()
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
    ready_wait_seconds = time.perf_counter() - ready_started

    dismiss_popups(driver)

    stable_checks = max(1, int(stable_checks))
    stable_interval_seconds = max(0.1, float(stable_interval_seconds))
    deadline = time.monotonic() + max(1.0, float(result_wait_seconds))

    last_positive_count = None
    stable_run = 0
    observed_count = 0
    stable_reached = False
    observation_trace = []
    poll_started = time.perf_counter()

    while time.monotonic() < deadline:
        if stop_event is not None and stop_event.is_set():
            break

        try:
            observed_count = len(
                driver.find_elements(
                    By.CSS_SELECTOR,
                    "a[href*='/document/'], a[href*='/doc/']",
                )
            )
        except Exception:
            observed_count = 0

        trace_elapsed = time.perf_counter() - poll_started
        observation_trace.append((round(trace_elapsed, 2), int(observed_count)))
        # Keep diagnostics compact even with very short polling intervals.
        if len(observation_trace) > 12:
            observation_trace = observation_trace[-12:]

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
                stable_reached = True
                if progress_callback:
                    progress_callback(
                        page=page,
                        observed_count=observed_count,
                        status="stable",
                    )
                break
        else:
            # Preserve v16 behavior: zero waits to the full result timeout.
            last_positive_count = None
            stable_run = 0

        if stop_event is not None:
            if stop_event.wait(stable_interval_seconds):
                break
        else:
            time.sleep(stable_interval_seconds)

    stable_wait_seconds = time.perf_counter() - poll_started

    try:
        current_url = driver.current_url or ""
    except Exception:
        current_url = ""

    try:
        page_title = driver.title or ""
    except Exception:
        page_title = ""

    try:
        ready_state = driver.execute_script("return document.readyState") or ""
    except Exception:
        ready_state = ""

    try:
        actual_page_values = parse_qs(urlsplit(current_url).query).get("page", [])
        actual_page = actual_page_values[0] if actual_page_values else ""
    except Exception:
        actual_page = ""

    perf_info = {}
    try:
        perf_info = driver.execute_script(
            """
            const nav = performance.getEntriesByType('navigation')[0] || {};
            const res = performance.getEntriesByType('resource') || [];
            let transfer = 0;
            for (const r of res) transfer += Number(r.transferSize || 0);
            return {
              resource_count: res.length,
              transfer_bytes: transfer,
              dom_content_loaded_ms: Number(nav.domContentLoadedEventEnd || 0),
              load_event_ms: Number(nav.loadEventEnd || 0),
              navigator_online: navigator.onLine
            };
            """
        ) or {}
    except Exception:
        perf_info = {}

    diagnostics = {
        "requested_url": search_url,
        "current_url": current_url,
        "requested_page": page,
        "actual_page": actual_page,
        "page_title": page_title,
        "ready_state": ready_state,
        "navigation_seconds": navigation_seconds,
        "ready_wait_seconds": ready_wait_seconds,
        "stable_wait_seconds": stable_wait_seconds,
        "observed_count": observed_count,
        "stable_reached": stable_reached,
        "stable_run": stable_run,
        "observation_trace": observation_trace,
        "resource_count": perf_info.get("resource_count"),
        "transfer_bytes": perf_info.get("transfer_bytes"),
        "dom_content_loaded_ms": perf_info.get("dom_content_loaded_ms"),
        "load_event_ms": perf_info.get("load_event_ms"),
        "navigator_online": perf_info.get("navigator_online"),
    }

    return current_url, observed_count, diagnostics


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
    anchors = driver.find_elements(
        By.CSS_SELECTOR,
        "a[href*='/document/'], a[href*='/doc/']",
    )
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
        return [], {}

    if progress_callback:
        progress_callback(
            page=page_number,
            page_unique=None,
            observed_count=0,
            status="loading",
        )

    _current_url, _observed_count, search_diagnostics = search_scribd(
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
        return [], search_diagnostics

    page_documents = {}
    extraction_started = time.perf_counter()
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

    extraction_seconds = time.perf_counter() - extraction_started

    # Extra context is collected only for empty pages, where it is useful for
    # diagnosing redirects/challenges/no-results without changing behavior.
    body_snippet = ""
    body_text_length = None
    all_anchor_count = None
    if not page_documents:
        try:
            all_anchor_count = len(driver.find_elements(By.CSS_SELECTOR, "a[href]"))
        except Exception:
            all_anchor_count = None
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text or ""
            body_text_length = len(body_text)
            body_snippet = " ".join(body_text.split())[:240]
        except Exception:
            body_snippet = ""

    search_diagnostics["extraction_seconds"] = extraction_seconds
    search_diagnostics["body_snippet"] = body_snippet
    search_diagnostics["body_text_length"] = body_text_length
    search_diagnostics["all_anchor_count"] = all_anchor_count

    if progress_callback:
        progress_callback(
            page=page_number,
            page_unique=len(page_documents),
            observed_count=len(page_documents),
            page_document_ids=list(page_documents.keys()),
            status=("loaded" if page_documents else "empty"),
        )

    return list(page_documents.values()), search_diagnostics


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

        page_started_at = datetime.now().astimezone().isoformat(timespec="seconds")
        page_started_perf = time.perf_counter()

        page_results, page_diagnostics = collect_one_result_page(
            driver,
            keyword,
            page_number,
            stop_event=stop_event,
            result_wait_seconds=result_wait_seconds,
            stable_checks=stable_checks,
            stable_interval_seconds=stable_interval_seconds,
            progress_callback=progress_callback,
        )

        page_read_seconds = time.perf_counter() - page_started_perf
        page_finished_at = datetime.now().astimezone().isoformat(timespec="seconds")

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
                timing_event=True,
                timing_keyword=keyword,
                page_started_at=page_started_at,
                page_finished_at=page_finished_at,
                page_read_seconds=page_read_seconds,
                debug_event=True,
                observed_count=page_diagnostics.get("observed_count", len(page_results)),
                navigation_seconds=page_diagnostics.get("navigation_seconds", 0.0),
                ready_wait_seconds=page_diagnostics.get("ready_wait_seconds", 0.0),
                stable_wait_seconds=page_diagnostics.get("stable_wait_seconds", 0.0),
                extraction_seconds=page_diagnostics.get("extraction_seconds", 0.0),
                stable_reached=page_diagnostics.get("stable_reached", False),
                stable_run=page_diagnostics.get("stable_run", 0),
                observation_trace=page_diagnostics.get("observation_trace", []),
                requested_page=page_diagnostics.get("requested_page", page_number),
                actual_page=page_diagnostics.get("actual_page", ""),
                requested_url=page_diagnostics.get("requested_url", ""),
                current_url=page_diagnostics.get("current_url", ""),
                page_title=page_diagnostics.get("page_title", ""),
                ready_state=page_diagnostics.get("ready_state", ""),
                body_snippet=page_diagnostics.get("body_snippet", ""),
                all_anchor_count=page_diagnostics.get("all_anchor_count"),
                body_text_length=page_diagnostics.get("body_text_length"),
                resource_count=page_diagnostics.get("resource_count"),
                transfer_bytes=page_diagnostics.get("transfer_bytes"),
                dom_content_loaded_ms=page_diagnostics.get("dom_content_loaded_ms"),
                load_event_ms=page_diagnostics.get("load_event_ms"),
                navigator_online=page_diagnostics.get("navigator_online"),
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
    run_id=None,
    page_log_output=None,
    device_id="",
    metrics_output="scribd_metrics.csv",
    metrics_interval_hours=1.0,
    block_images=True,
    page_load_strategy="eager",
    debug_page_log=True,
    debug_page_lines=8,
    debug_page_output="scribd_page_debug.log",
    diagnostic_output="scribd_diagnostic.jsonl",
    diagnostic_interval_seconds=5.0,
    diagnostic_system=True,
):
    """
    Search Scribd with 1-64 concurrent workers.

    Target semantics:
      - Once the confirmed NEW unique count committed to SQLite reaches target_new,
        no NEW keyword is assigned.
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

    debug_page_log = bool(debug_page_log)
    debug_page_lines = max(1, int(debug_page_lines or 8))  # kept for CLI compatibility
    debug_events = deque(maxlen=debug_page_lines)
    debug_lock = threading.Lock()

    # v16.2: diagnostics are written to files only. The main TOTAL/Wxx
    # dashboard is deliberately left unchanged. A second terminal can run
    # watch_scribd_debug.py against diagnostic_output.
    diagnostic_stop = threading.Event()
    diagnostic_write_lock = threading.Lock()
    diagnostic_interval_seconds = max(1.0, float(diagnostic_interval_seconds or 5.0))
    diagnostic_system = bool(diagnostic_system)

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

    # Timing telemetry is separate from stdout so the existing TOTAL/Wxx
    # dashboard stays unchanged. v16 stores aggregated metrics every N hours.
    # A raw per-page CSV is still available when --page-log is explicitly set.
    timing_lock = threading.Lock()
    page_read_times = []

    interval_lock = threading.Lock()
    interval_started_wall = datetime.now().astimezone()
    interval_started_perf = time.monotonic()
    interval_page_times = []
    interval_page_unique = 0
    interval_new_on_page = 0
    interval_queries_finished = 0
    interval_confirmed_new = 0

    metrics_interval_seconds = max(
        1.0,
        float(metrics_interval_hours or 1.0) * 3600.0,
    )

    def _summary(values):
        values = [float(v) for v in values]
        if not values:
            return {
                "count": 0,
                "avg": 0.0,
                "median": 0.0,
                "p95": 0.0,
                "min": 0.0,
                "max": 0.0,
            }
        ordered = sorted(values)
        n = len(ordered)
        m = n // 2
        median = ordered[m] if n % 2 else (ordered[m - 1] + ordered[m]) / 2.0
        p95_index = min(n - 1, max(0, int((n - 1) * 0.95 + 0.999999)))
        return {
            "count": n,
            "avg": sum(ordered) / n,
            "median": median,
            "p95": ordered[p95_index],
            "min": ordered[0],
            "max": ordered[-1],
        }

    def _append_csv_row(path, fields, row):
        if not path:
            return
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        needs_header = not output.exists() or output.stat().st_size == 0
        with output.open("a", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            if needs_header:
                writer.writeheader()
            writer.writerow({key: row.get(key, "") for key in fields})

    def _flush_interval_locked(partial=False, force=False):
        nonlocal interval_started_wall, interval_started_perf
        nonlocal interval_page_times, interval_page_unique, interval_new_on_page
        nonlocal interval_queries_finished, interval_confirmed_new

        now_perf = time.monotonic()
        elapsed = now_perf - interval_started_perf
        has_data = bool(
            interval_page_times
            or interval_queries_finished
            or interval_confirmed_new
        )

        if not force and elapsed < metrics_interval_seconds:
            return False
        if not has_data:
            if force or elapsed >= metrics_interval_seconds:
                interval_started_wall = datetime.now().astimezone()
                interval_started_perf = now_perf
            return False

        ended_wall = datetime.now().astimezone()
        summary = _summary(interval_page_times)
        hours = max(elapsed / 3600.0, 1e-9)

        fields = [
            "device_id", "run_id", "interval_started_at", "interval_ended_at",
            "interval_seconds", "partial", "search_workers", "pages_per_keyword",
            "result_wait", "stable_checks", "stable_interval", "page_delay",
            "page_load_strategy", "images_blocked", "pages_read",
            "avg_page_seconds", "median_page_seconds", "p95_page_seconds",
            "min_page_seconds", "max_page_seconds", "page_unique_sum",
            "new_on_page_sum", "queries_finished", "confirmed_new",
            "pages_per_hour", "confirmed_per_hour",
        ]
        row = {
            "device_id": device_id,
            "run_id": "" if run_id is None else run_id,
            "interval_started_at": interval_started_wall.isoformat(timespec="seconds"),
            "interval_ended_at": ended_wall.isoformat(timespec="seconds"),
            "interval_seconds": f"{elapsed:.3f}",
            "partial": "yes" if partial else "no",
            "search_workers": search_workers,
            "pages_per_keyword": pages_per_keyword,
            "result_wait": result_wait_seconds,
            "stable_checks": stable_checks,
            "stable_interval": stable_interval_seconds,
            "page_delay": page_delay_seconds,
            "page_load_strategy": page_load_strategy,
            "images_blocked": "yes" if block_images else "no",
            "pages_read": summary["count"],
            "avg_page_seconds": f"{summary['avg']:.3f}",
            "median_page_seconds": f"{summary['median']:.3f}",
            "p95_page_seconds": f"{summary['p95']:.3f}",
            "min_page_seconds": f"{summary['min']:.3f}",
            "max_page_seconds": f"{summary['max']:.3f}",
            "page_unique_sum": interval_page_unique,
            "new_on_page_sum": interval_new_on_page,
            "queries_finished": interval_queries_finished,
            "confirmed_new": interval_confirmed_new,
            "pages_per_hour": f"{summary['count'] / hours:.2f}",
            "confirmed_per_hour": f"{interval_confirmed_new / hours:.2f}",
        }
        _append_csv_row(metrics_output, fields, row)

        interval_started_wall = ended_wall
        interval_started_perf = now_perf
        interval_page_times = []
        interval_page_unique = 0
        interval_new_on_page = 0
        interval_queries_finished = 0
        interval_confirmed_new = 0
        return True

    def _write_diagnostic(event):
        if not diagnostic_output:
            return
        payload = dict(event or {})
        payload.setdefault("timestamp", datetime.now().astimezone().isoformat(timespec="seconds"))
        payload.setdefault("device_id", device_id)
        payload.setdefault("run_id", "" if run_id is None else run_id)
        output = Path(diagnostic_output)
        output.parent.mkdir(parents=True, exist_ok=True)
        with diagnostic_write_lock:
            with output.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

    def _classify_page(payload):
        page_unique = int(payload.get("page_unique") or 0)
        observed = int(payload.get("observed_count") or 0)
        if page_unique > 0:
            return "ok"
        requested_page = str(payload.get("requested_page") or payload.get("page") or "")
        actual_page = str(payload.get("actual_page") or "")
        current_url = str(payload.get("current_url") or "")
        title = str(payload.get("page_title") or "").lower()
        body = str(payload.get("body_snippet") or "").lower()
        ready = str(payload.get("ready_state") or "").lower()
        combined = f"{title} {body}"
        challenge_terms = (
            "captcha", "verify you are human", "unusual traffic", "access denied",
            "security check", "robot", "blocked", "too many requests", "429",
        )
        no_result_terms = (
            "no results", "no documents found", "we couldn't find", "we could not find",
        )
        if requested_page and actual_page and requested_page != actual_page:
            return "page_mismatch"
        if current_url and "/search" not in current_url:
            return "redirected"
        if any(term in combined for term in challenge_terms):
            return "challenge_or_block_text"
        if any(term in combined for term in no_result_terms):
            return "explicit_no_results"
        if ready not in ("interactive", "complete", ""):
            return "page_not_ready"
        if observed > 0:
            return "anchors_seen_but_no_unique_docs"
        return "zero_unknown"

    _net_prev = {"time": None, "sent": None, "recv": None}

    def _system_snapshot():
        result = {
            "psutil_available": bool(psutil is not None),
            "cpu_percent": None,
            "ram_percent": None,
            "ram_available_gb": None,
            "python_rss_gb": None,
            "browser_processes": None,
            "browser_rss_gb": None,
            "net_rx_mbps": None,
            "net_tx_mbps": None,
        }
        if psutil is None or not diagnostic_system:
            return result
        try:
            result["cpu_percent"] = round(float(psutil.cpu_percent(interval=None)), 1)
            vm = psutil.virtual_memory()
            result["ram_percent"] = round(float(vm.percent), 1)
            result["ram_available_gb"] = round(float(vm.available) / (1024 ** 3), 2)
            proc = psutil.Process(os.getpid())
            result["python_rss_gb"] = round(float(proc.memory_info().rss) / (1024 ** 3), 3)
            children = proc.children(recursive=True)
            browser_count = 0
            browser_rss = 0
            for child in children:
                try:
                    name = (child.name() or "").lower()
                    if "chrome" in name or "chromedriver" in name or "chromium" in name:
                        browser_count += 1
                        browser_rss += child.memory_info().rss
                except Exception:
                    continue
            result["browser_processes"] = browser_count
            result["browser_rss_gb"] = round(float(browser_rss) / (1024 ** 3), 3)
            net = psutil.net_io_counters()
            now = time.monotonic()
            if _net_prev["time"] is not None:
                elapsed = max(now - _net_prev["time"], 1e-6)
                result["net_rx_mbps"] = round((net.bytes_recv - _net_prev["recv"]) * 8 / elapsed / 1_000_000, 3)
                result["net_tx_mbps"] = round((net.bytes_sent - _net_prev["sent"]) * 8 / elapsed / 1_000_000, 3)
            _net_prev.update(time=now, sent=net.bytes_sent, recv=net.bytes_recv)
        except Exception as exc:
            result["system_error"] = f"{type(exc).__name__}: {exc}"
        return result

    def diagnostic_monitor_loop():
        # Prime psutil CPU/net counters so later samples are meaningful.
        if psutil is not None and diagnostic_system:
            try:
                psutil.cpu_percent(interval=None)
                net = psutil.net_io_counters()
                _net_prev.update(time=time.monotonic(), sent=net.bytes_sent, recv=net.bytes_recv)
            except Exception:
                pass

        while not diagnostic_stop.is_set():
            with state_lock:
                snapshot = {wid: dict(values) for wid, values in worker_states.items()}
            active = []
            zero_waiting = []
            positive_waiting = []
            statuses = {}
            for wid, ws in snapshot.items():
                status = str(ws.get("status") or "")
                statuses[status] = statuses.get(status, 0) + 1
                if status not in {"stopped", "target done"}:
                    active.append(wid)
                if status in {"loading", "waiting", "stable"}:
                    if int(ws.get("observed_count") or 0) > 0:
                        positive_waiting.append(wid)
                    else:
                        zero_waiting.append(wid)

            zero_ratio = (len(zero_waiting) / len(active)) if active else 0.0
            severity = "ok"
            if len(active) >= 4 and zero_ratio >= 0.8:
                severity = "zero_storm"
            elif len(active) >= 4 and zero_ratio >= 0.5:
                severity = "many_zero_workers"

            event = {
                "type": "monitor",
                "severity": severity,
                "configured_workers": search_workers,
                "result_wait_seconds": result_wait_seconds,
                "stable_checks": stable_checks,
                "stable_interval_seconds": stable_interval_seconds,
                "page_delay_seconds": page_delay_seconds,
                "page_load_strategy": page_load_strategy,
                "images_blocked": bool(block_images),
                "active_workers": len(active),
                "zero_waiting_workers": len(zero_waiting),
                "positive_waiting_workers": len(positive_waiting),
                "zero_waiting_ids": zero_waiting,
                "positive_waiting_ids": positive_waiting,
                "status_counts": statuses,
                "workers": {
                    str(wid): {
                        "page": ws.get("page"),
                        "status": ws.get("status"),
                        "observed_count": ws.get("observed_count"),
                        "page_unique": ws.get("page_unique"),
                        "keyword_total": ws.get("keyword_total"),
                        "keyword": ws.get("keyword"),
                    }
                    for wid, ws in snapshot.items()
                },
            }
            event.update(_system_snapshot())
            _write_diagnostic(event)
            diagnostic_stop.wait(diagnostic_interval_seconds)

    def _short_debug_text(value, limit=44):
        text = " ".join(str(value or "").split())
        if len(text) <= limit:
            return text
        return text[: max(0, limit - 1)] + "…"

    def _append_page_debug(worker_id, payload, seconds):
        if not debug_page_log:
            return

        trace = payload.get("observation_trace") or []
        trace_text = ">".join(f"{count}@{elapsed:g}s" for elapsed, count in trace)
        stable = "Y" if payload.get("stable_reached") else "N"
        requested_page = payload.get("requested_page", payload.get("page", ""))
        actual_page = payload.get("actual_page") or "?"
        keyword = _short_debug_text(payload.get("timing_keyword"), 28)

        line = (
            f"DBG W{worker_id:02d} p{payload.get('page', 0)} "
            f"obs={int(payload.get('observed_count') or 0)} "
            f"uniq={int(payload.get('page_unique') or 0)} "
            f"new={int(payload.get('new_on_page') or 0)} "
            f"total={int(payload.get('keyword_total') or 0)} | "
            f"nav={float(payload.get('navigation_seconds') or 0):.2f}s "
            f"wait={float(payload.get('stable_wait_seconds') or 0):.2f}s "
            f"ext={float(payload.get('extraction_seconds') or 0):.2f}s "
            f"all={float(seconds):.2f}s | "
            f"stable={stable} req/actual={requested_page}/{actual_page} | "
            f"{keyword} | trace={trace_text}"
        )

        with debug_lock:
            debug_events.append(line)

        if debug_page_output:
            output = Path(debug_page_output)
            output.parent.mkdir(parents=True, exist_ok=True)
            timestamp = payload.get("page_finished_at") or datetime.now().astimezone().isoformat(timespec="seconds")
            full_line = (
                f"{timestamp} device={device_id} run={'' if run_id is None else run_id} "
                f"{line} ready={payload.get('ready_state', '')} "
                f"anchors={payload.get('all_anchor_count', '')} "
                f"title={payload.get('page_title', '')!r} "
                f"current_url={payload.get('current_url', '')!r} "
                f"requested_url={payload.get('requested_url', '')!r} "
                f"body={payload.get('body_snippet', '')!r}\n"
            )
            with output.open("a", encoding="utf-8") as handle:
                handle.write(full_line)

        diag = {
            "type": "page",
            "worker_id": worker_id,
            "keyword": payload.get("timing_keyword") or "",
            "page": payload.get("page", 0),
            "requested_page": payload.get("requested_page"),
            "actual_page": payload.get("actual_page"),
            "observed_count": int(payload.get("observed_count") or 0),
            "page_unique": int(payload.get("page_unique") or 0),
            "new_on_page": int(payload.get("new_on_page") or 0),
            "keyword_total": int(payload.get("keyword_total") or 0),
            "navigation_seconds": round(float(payload.get("navigation_seconds") or 0), 3),
            "ready_wait_seconds": round(float(payload.get("ready_wait_seconds") or 0), 3),
            "stable_wait_seconds": round(float(payload.get("stable_wait_seconds") or 0), 3),
            "extraction_seconds": round(float(payload.get("extraction_seconds") or 0), 3),
            "page_read_seconds": round(float(seconds), 3),
            "stable_reached": bool(payload.get("stable_reached")),
            "stable_run": int(payload.get("stable_run") or 0),
            "observation_trace": payload.get("observation_trace") or [],
            "ready_state": payload.get("ready_state") or "",
            "page_title": payload.get("page_title") or "",
            "requested_url": payload.get("requested_url") or "",
            "current_url": payload.get("current_url") or "",
            "all_anchor_count": payload.get("all_anchor_count"),
            "body_text_length": payload.get("body_text_length"),
            "body_snippet": payload.get("body_snippet") or "",
            "resource_count": payload.get("resource_count"),
            "transfer_bytes": payload.get("transfer_bytes"),
            "dom_content_loaded_ms": payload.get("dom_content_loaded_ms"),
            "load_event_ms": payload.get("load_event_ms"),
            "navigator_online": payload.get("navigator_online"),
        }
        diag["classification"] = _classify_page(payload)
        _write_diagnostic(diag)

    def append_page_timing(worker_id, payload):
        nonlocal interval_page_unique, interval_new_on_page
        seconds = float(payload.get("page_read_seconds") or 0.0)
        if seconds < 0:
            return

        _append_page_debug(worker_id, payload, seconds)

        with timing_lock:
            page_read_times.append(seconds)

        if page_log_output:
            row = {
                "timestamp": payload.get("page_finished_at") or "",
                "device_id": device_id,
                "run_id": "" if run_id is None else run_id,
                "worker_id": worker_id,
                "keyword": payload.get("timing_keyword") or "",
                "page": payload.get("page") or 0,
                "read_seconds": f"{seconds:.3f}",
                "page_unique": payload.get("page_unique") if payload.get("page_unique") is not None else "",
                "new_on_page": payload.get("new_on_page") if payload.get("new_on_page") is not None else "",
                "keyword_total": payload.get("keyword_total") if payload.get("keyword_total") is not None else "",
                "started_at": payload.get("page_started_at") or "",
                "finished_at": payload.get("page_finished_at") or "",
            }
            fields = [
                "timestamp", "device_id", "run_id", "worker_id", "keyword", "page",
                "read_seconds", "page_unique", "new_on_page", "keyword_total",
                "started_at", "finished_at",
            ]
            _append_csv_row(page_log_output, fields, row)

        with interval_lock:
            interval_page_times.append(seconds)
            interval_page_unique += int(payload.get("page_unique") or 0)
            interval_new_on_page += int(payload.get("new_on_page") or 0)
            _flush_interval_locked(partial=False, force=False)

    def record_query_metric(new_count):
        nonlocal interval_queries_finished, interval_confirmed_new
        with interval_lock:
            interval_queries_finished += 1
            interval_confirmed_new += int(new_count or 0)
            _flush_interval_locked(partial=False, force=False)

    def flush_interval_metrics(partial=True):
        with interval_lock:
            return _flush_interval_locked(partial=partial, force=True)

    def summarize_page_timings():
        with timing_lock:
            values = list(page_read_times)
        summary = _summary(values)
        return {
            "pages_timed": summary["count"],
            "avg_page_read_seconds": summary["avg"],
            "median_page_read_seconds": summary["median"],
            "p95_page_read_seconds": summary["p95"],
            "min_page_read_seconds": summary["min"],
            "max_page_read_seconds": summary["max"],
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

                # v15: LIVE is display-only. Do not stop assigning keywords
                # until confirmed new documents committed to SQLite reach the
                # requested --collect target.

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
                    f"TOTAL {progress_bar(confirmed_new, target_new, 26)} "
                    f"{confirmed_new}/{target_new} confirmed | "
                    f"live {live_new} | "
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
        _write_diagnostic({"type": "browser", "event": "stopped", "worker_id": worker_id})

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
        started = time.perf_counter()
        driver = build_driver(
            headless=headless,
            block_images=block_images,
            page_load_strategy=page_load_strategy,
        )
        startup_seconds = time.perf_counter() - started
        register_driver(worker_id, driver)
        _write_diagnostic({
            "type": "browser",
            "event": "started",
            "worker_id": worker_id,
            "startup_seconds": round(startup_seconds, 3),
            "page_load_strategy": page_load_strategy,
            "images_blocked": bool(block_images),
        })
        return driver

    def process_worker(worker_id):
        driver = None

        def progress_callback(**kwargs):
            if kwargs.get("timing_event"):
                append_page_timing(worker_id, kwargs)

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

                    # v15 target rule: keep switching to new keywords until
                    # this run has actually COMMITTED at least target_new new,
                    # globally-unique documents. Active keywords still finish.
                    if run_new >= target_new:
                        target_reached_event.set()

                    record_query_metric(new_count)

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
                    _write_diagnostic({
                        "type": "worker_error",
                        "worker_id": worker_id,
                        "keyword": keyword,
                        "error_type": type(error).__name__,
                        "error": str(error),
                    })
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
        "\n========== SEARCH CONFIG (v16.2) ==========\n"
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

    diagnostic_thread = threading.Thread(
        target=diagnostic_monitor_loop,
        name="search-diagnostic-monitor",
        daemon=True,
    )
    diagnostic_thread.start()

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
        diagnostic_stop.set()
        dashboard_thread.join(timeout=1.5)
        diagnostic_thread.join(timeout=1.5)
        print()

    flush_interval_metrics(partial=True)

    queue_count = export_queue(db_path, queue_output)
    metadata_count = export_metadata_csv(db_path, metadata_output)
    timing_summary = summarize_page_timings()

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
    print(f"Device ID               : {device_id}")
    print(f"Metrics CSV             : {metrics_output}")
    print(f"Metrics interval        : {metrics_interval_hours:g} hour(s)")
    print(f"Search page strategy    : {page_load_strategy}")
    print(f"Search images           : {'blocked' if block_images else 'loaded'}")
    print(f"Diagnostic JSONL        : {diagnostic_output or 'disabled'}")
    print(f"Diagnostic interval     : {diagnostic_interval_seconds:g}s")
    print(f"System telemetry        : {'on' if diagnostic_system else 'off'} ({'psutil available' if psutil is not None else 'psutil not installed'})")
    if page_log_output:
        print(f"Raw page timing log     : {page_log_output}")
    print(f"Pages timed             : {timing_summary['pages_timed']}")
    print(f"Avg page read           : {timing_summary['avg_page_read_seconds']:.2f}s")
    print(f"Median page read        : {timing_summary['median_page_read_seconds']:.2f}s")
    print(f"P95 page read           : {timing_summary['p95_page_read_seconds']:.2f}s")
    print(f"Fastest / slowest       : {timing_summary['min_page_read_seconds']:.2f}s / {timing_summary['max_page_read_seconds']:.2f}s")
    if target_reached_event.is_set() and not interrupted:
        print("Target behavior         : confirmed target reached; active keywords finished")
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
        **timing_summary,
        "page_log_output": page_log_output,
        "metrics_output": metrics_output,
        "metrics_interval_hours": metrics_interval_hours,
        "device_id": device_id,
        "page_load_strategy": page_load_strategy,
        "images_blocked": bool(block_images),
        "diagnostic_output": diagnostic_output,
        "diagnostic_interval_seconds": diagnostic_interval_seconds,
        "diagnostic_system": bool(diagnostic_system),
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
        "--page-log",
        default="",
        help=(
            "Optional legacy raw per-page timing CSV. Disabled by default in "
            "v16; use --metrics-output for N-hour summaries."
        ),
    )
    parser.add_argument(
        "--device-id",
        default="LOCAL",
        help="Device code written to metrics, e.g. SYD-PC01 or AKAMAI-001.",
    )
    parser.add_argument(
        "--metrics-output",
        default="scribd_metrics.csv",
        help="Append N-hour aggregated search metrics to this CSV.",
    )
    parser.add_argument(
        "--metrics-interval-hours",
        type=float,
        default=1.0,
        help="Write one aggregated metrics row every N hours. Default: 1.",
    )
    parser.add_argument(
        "--search-images",
        choices=("off", "on"),
        default="off",
        help="Load search-result images. v16 default: off.",
    )
    parser.add_argument(
        "--search-page-load-strategy",
        choices=("eager", "normal"),
        default="eager",
        help="Selenium page-load strategy for search. v16 default: eager.",
    )
    parser.add_argument(
        "--debug-page-log",
        choices=("on", "off"),
        default="on",
        help="Write per-page diagnostics to files. v16.2 never adds debug lines to the main dashboard.",
    )
    parser.add_argument(
        "--debug-page-lines",
        type=int,
        default=8,
        help="Compatibility option retained from v16.1; main dashboard is unchanged in v16.2.",
    )
    parser.add_argument(
        "--debug-page-output",
        default="scribd_page_debug.log",
        help="Append full per-page diagnostics to this text log.",
    )
    parser.add_argument(
        "--diagnostic-output",
        default="scribd_diagnostic.jsonl",
        help="Structured live diagnostics consumed by watch_scribd_debug.py.",
    )
    parser.add_argument(
        "--diagnostic-interval",
        type=float,
        default=5.0,
        help="Seconds between worker/system diagnostic snapshots. Default: 5.",
    )
    parser.add_argument(
        "--diagnostic-system",
        choices=("on", "off"),
        default="on",
        help="Include CPU/RAM/network/Chrome telemetry when psutil is installed.",
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
    if args.metrics_interval_hours <= 0:
        raise SystemExit("--metrics-interval-hours must be greater than 0")
    if args.debug_page_lines <= 0:
        raise SystemExit("--debug-page-lines must be greater than 0")
    if args.diagnostic_interval <= 0:
        raise SystemExit("--diagnostic-interval must be greater than 0")

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
        page_log_output=args.page_log or None,
        device_id=args.device_id,
        metrics_output=args.metrics_output,
        metrics_interval_hours=args.metrics_interval_hours,
        block_images=(args.search_images == "off"),
        page_load_strategy=args.search_page_load_strategy,
        debug_page_log=(args.debug_page_log == "on"),
        debug_page_lines=args.debug_page_lines,
        debug_page_output=args.debug_page_output,
        diagnostic_output=args.diagnostic_output,
        diagnostic_interval_seconds=args.diagnostic_interval,
        diagnostic_system=(args.diagnostic_system == "on"),
    )
