import argparse
import csv
import os
import random
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlsplit

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
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
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
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


def search_scribd(driver, keyword):
    print(f"[SEARCH] {keyword}")
    driver.get(SCRIBD_HOME)
    time.sleep(3)
    dismiss_popups(driver)

    search_input = find_search_input(driver)
    if search_input is None:
        raise RuntimeError("Could not find Scribd search box.")

    search_input.click()
    search_input.clear()
    search_input.send_keys(keyword)
    search_input.send_keys(Keys.ENTER)
    time.sleep(5)
    dismiss_popups(driver)


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


def collect_search_results(
    driver,
    max_results,
    max_no_growth=8,
    scroll_pause=2.0,
):
    documents = {}
    no_growth_rounds = 0
    previous_count = 0
    round_number = 0

    while len(documents) < max_results:
        round_number += 1
        visible = collect_visible_documents(driver)

        for item in visible:
            doc_id = item["id"]
            if not doc_id:
                continue
            if doc_id not in documents:
                documents[doc_id] = item
            elif not documents[doc_id]["title"] and item["title"]:
                documents[doc_id]["title"] = item["title"]

        current_count = len(documents)
        print(
            f"    scroll={round_number} unique={current_count}/{max_results}"
        )

        if current_count >= max_results:
            break

        if current_count == previous_count:
            no_growth_rounds += 1
        else:
            no_growth_rounds = 0

        if no_growth_rounds >= max_no_growth:
            break

        previous_count = current_count

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause)
        driver.execute_script("window.scrollBy(0, -300);")
        time.sleep(0.3)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause)

    return list(documents.values())[:max_results]


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
):
    init_db(db_path)
    driver = build_driver(headless=headless)
    new_this_run = 0

    try:
        while new_this_run < target_new:
            if one_keyword:
                keyword = one_keyword.strip()
                if keyword_used(db_path, keyword):
                    print(f"[SKIP] Keyword already used: {keyword}")
                    break
            else:
                keyword = generate_unused_keyword(db_path)

            try:
                search_scribd(driver, keyword)
                results = collect_search_results(
                    driver,
                    max_results=max_per_query,
                )
                new_count = save_search_results(db_path, keyword, results)
                new_this_run += new_count

                total_docs, queued, searched = get_counts(db_path)
                print(
                    f"[DONE] results={len(results)} new={new_count} "
                    f"run_new={new_this_run}/{target_new} "
                    f"total_unique={total_docs} queued={queued} queries={searched}"
                )

                export_queue(db_path, queue_output)
                export_metadata_csv(db_path, metadata_output)

            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"[ERROR] {type(e).__name__}: {e}")
                save_query_error(db_path, keyword, e)

            if one_keyword:
                break

            time.sleep(pause_between_queries)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    queue_count = export_queue(db_path, queue_output)
    metadata_count = export_metadata_csv(db_path, metadata_output)

    print("\n========== SUMMARY ==========")
    print(f"New documents this run : {new_this_run}")
    print(f"Queued URLs             : {queue_count}")
    print(f"Metadata rows           : {metadata_count}")
    print(f"Database                : {db_path}")
    print(f"Queue file              : {queue_output}")
    print(f"Metadata CSV            : {metadata_output}")
    print("=============================")


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
        help="Stop after finding this many NEW unique documents in this run.",
    )
    parser.add_argument(
        "--max-per-query",
        type=int,
        default=100,
        help="Maximum documents to collect from one keyword search.",
    )
    parser.add_argument(
        "--db",
        default="scribd_state.db",
        help="SQLite state database.",
    )
    parser.add_argument(
        "--queue-output",
        default="scribd_download_queue.txt",
        help="Queued Scribd URLs for your batch downloader.",
    )
    parser.add_argument(
        "--metadata-output",
        default="scribd_documents.csv",
        help="CSV containing document_id, title, URL and discovery keyword.",
    )
    parser.add_argument(
        "--keyword",
        default=None,
        help="Optional one-keyword test mode instead of random generation.",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=2.0,
        help="Pause in seconds between keyword searches.",
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Show Chrome instead of headless mode.",
    )
    args = parser.parse_args()

    if args.target_new <= 0:
        raise SystemExit("--target-new must be greater than 0")
    if args.max_per_query <= 0:
        raise SystemExit("--max-per-query must be greater than 0")

    run_pipeline(
        db_path=args.db,
        target_new=args.target_new,
        max_per_query=args.max_per_query,
        queue_output=args.queue_output,
        metadata_output=args.metadata_output,
        headless=not args.show_browser,
        one_keyword=args.keyword,
        pause_between_queries=max(args.pause, 0),
    )
