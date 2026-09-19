import argparse
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


SCRIBD_HOME = "https://www.scribd.com/"

DOCUMENT_RE = re.compile(
    r"https?://(?:www\.)?scribd\.com/"
    r"(?:document|doc)/(\d+)(?:/[^?#]*)?",
    re.IGNORECASE,
)


# ============================================================
# CHROME
# ============================================================

def build_driver(headless=True):
    options = webdriver.ChromeOptions()

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1440,1200")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--lang=en-US")

    options.add_argument(
        "--user-agent="
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/153.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(60)

    return driver


# ============================================================
# COOKIE / POPUP
# ============================================================

def dismiss_popups(driver):
    """
    Try to close common cookie/login overlays.
    Failure is harmless.
    """

    texts = [
        "Accept",
        "Accept All",
        "Accept all",
        "I agree",
        "Got it",
        "Close",
        "No thanks",
        "Not now",
    ]

    for text in texts:
        try:
            elements = driver.find_elements(
                By.XPATH,
                f"//button[contains(normalize-space(.), '{text}')]"
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


# ============================================================
# SEARCH
# ============================================================

def find_search_input(driver):
    """
    Scribd frontend can change class names,
    so use semantic selectors instead of CSS classes.
    """

    selectors = [
        (By.CSS_SELECTOR, "input[type='search']"),
        (
            By.CSS_SELECTOR,
            "input[placeholder*='Search' i]"
        ),
        (
            By.CSS_SELECTOR,
            "input[aria-label*='Search' i]"
        ),
        (
            By.XPATH,
            "//input[contains("
            "translate(@placeholder,"
            "'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
            "'abcdefghijklmnopqrstuvwxyz'),"
            "'search')]"
        ),
    ]

    for by, selector in selectors:
        try:
            element = WebDriverWait(
                driver,
                5
            ).until(
                EC.presence_of_element_located(
                    (by, selector)
                )
            )

            if element:
                return element

        except TimeoutException:
            continue

    return None


def search_scribd(driver, keyword):
    print(f"[INFO] Opening Scribd")

    driver.get(SCRIBD_HOME)

    time.sleep(3)

    dismiss_popups(driver)

    search_input = find_search_input(driver)

    if search_input is None:
        raise RuntimeError(
            "Could not find Scribd search box."
        )

    print(
        f"[INFO] Searching: {keyword}"
    )

    search_input.click()

    search_input.clear()

    search_input.send_keys(keyword)

    search_input.send_keys(Keys.ENTER)

    time.sleep(5)

    dismiss_popups(driver)

    print(
        f"[INFO] Search page: {driver.current_url}"
    )


# ============================================================
# URL NORMALIZATION
# ============================================================

def normalize_document_url(url):
    if not url:
        return None

    match = DOCUMENT_RE.search(url)

    if not match:
        return None

    doc_id = match.group(1)

    parts = urlsplit(url)

    path_parts = [
        part
        for part in parts.path.split("/")
        if part
    ]

    if len(path_parts) < 2:
        return None

    # Preserve title slug when available.
    if len(path_parts) >= 3:
        slug = path_parts[2]

        normalized = (
            f"https://www.scribd.com/"
            f"document/{doc_id}/{slug}"
        )

    else:
        normalized = (
            f"https://www.scribd.com/"
            f"document/{doc_id}"
        )

    return normalized


def get_document_id(url):
    match = DOCUMENT_RE.search(url)

    if not match:
        return ""

    return match.group(1)


# ============================================================
# COLLECT RESULTS
# ============================================================

def collect_visible_documents(driver):
    """
    Collect Scribd document links currently loaded in the DOM.
    """

    anchors = driver.find_elements(
        By.CSS_SELECTOR,
        "a[href]"
    )

    results = []

    for anchor in anchors:
        try:
            href = anchor.get_attribute("href")

            normalized = normalize_document_url(
                href
            )

            if not normalized:
                continue

            title = ""

            try:
                title = (
                    anchor.get_attribute("aria-label")
                    or anchor.get_attribute("title")
                    or anchor.text
                    or ""
                ).strip()

            except Exception:
                title = ""

            results.append(
                {
                    "id": get_document_id(
                        normalized
                    ),
                    "url": normalized,
                    "title": title,
                }
            )

        except Exception:
            continue

    return results


# ============================================================
# SCROLL
# ============================================================

def collect_search_results(
    driver,
    max_results,
    max_no_growth=8,
    scroll_pause=2.0,
):
    """
    Scroll until:
      - enough results are found
      - or no new results appear for several rounds
    """

    documents = {}

    no_growth_rounds = 0
    previous_count = 0
    round_number = 0

    while len(documents) < max_results:

        round_number += 1

        visible = collect_visible_documents(
            driver
        )

        for item in visible:
            doc_id = item["id"]

            if not doc_id:
                continue

            if doc_id not in documents:
                documents[doc_id] = item

            else:
                # Prefer a non-empty title.
                if (
                    not documents[doc_id]["title"]
                    and item["title"]
                ):
                    documents[doc_id]["title"] = (
                        item["title"]
                    )

        current_count = len(documents)

        print(
            f"[INFO] Scroll {round_number}: "
            f"{current_count}/{max_results} "
            f"unique documents"
        )

        if current_count >= max_results:
            break

        if current_count == previous_count:
            no_growth_rounds += 1
        else:
            no_growth_rounds = 0

        if no_growth_rounds >= max_no_growth:
            print(
                "[INFO] No new documents found "
                "after several scrolls. Stopping."
            )
            break

        previous_count = current_count

        # Scroll near bottom.
        driver.execute_script(
            """
            window.scrollTo(
                0,
                document.body.scrollHeight
            );
            """
        )

        time.sleep(scroll_pause)

        # Some websites load when scroll position changes
        # slightly, so move upward and down again.
        driver.execute_script(
            """
            window.scrollBy(0, -300);
            """
        )

        time.sleep(0.3)

        driver.execute_script(
            """
            window.scrollTo(
                0,
                document.body.scrollHeight
            );
            """
        )

        time.sleep(scroll_pause)

    return list(documents.values())[
        :max_results
    ]


# ============================================================
# SAVE
# ============================================================

def save_urls(results, output_path):
    output = Path(output_path)

    output.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        output,
        "w",
        encoding="utf-8"
    ) as f:

        for item in results:
            f.write(item["url"] + "\n")


def save_jsonl(results, output_path, keyword):
    output = Path(output_path)

    output.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        output,
        "w",
        encoding="utf-8"
    ) as f:

        for item in results:

            row = {
                "keyword": keyword,
                "document_id": item["id"],
                "title": item["title"],
                "url": item["url"],
            }

            f.write(
                json.dumps(
                    row,
                    ensure_ascii=False
                )
                + "\n"
            )


# ============================================================
# MAIN
# ============================================================

def main(
    keyword,
    max_results,
    output,
    jsonl_output,
    headless,
):
    driver = None

    try:
        driver = build_driver(
            headless=headless
        )

        search_scribd(
            driver,
            keyword
        )

        results = collect_search_results(
            driver,
            max_results=max_results,
        )

        save_urls(
            results,
            output
        )

        save_jsonl(
            results,
            jsonl_output,
            keyword
        )

        print("")
        print(
            "========== DONE =========="
        )

        print(
            f"Keyword      : {keyword}"
        )

        print(
            f"Documents    : {len(results)}"
        )

        print(
            f"URL list     : {output}"
        )

        print(
            f"Metadata     : {jsonl_output}"
        )

        print(
            "=========================="
        )

    except KeyboardInterrupt:
        print(
            "\n[INFO] Interrupted by user."
        )

    except Exception as e:
        print(
            f"\n[ERROR] "
            f"{type(e).__name__}: {e}"
        )

        raise

    finally:
        if driver is not None:
            driver.quit()


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Collect Scribd document URLs "
            "from keyword search results."
        )
    )

    parser.add_argument(
        "--keyword",
        "-k",
        default=None,
        help="Search keyword",
    )

    parser.add_argument(
        "--max",
        "-m",
        type=int,
        default=100,
        help=(
            "Maximum number of document "
            "URLs to collect"
        ),
    )

    parser.add_argument(
        "--output",
        "-o",
        default="scribd_urls.txt",
        help="Output URL text file",
    )

    parser.add_argument(
        "--jsonl",
        default="scribd_search_results.jsonl",
        help="Output metadata JSONL",
    )

    parser.add_argument(
        "--show-browser",
        action="store_true",
        help=(
            "Show Chrome window "
            "instead of headless mode"
        ),
    )

    args = parser.parse_args()

    keyword = args.keyword

    if not keyword:
        keyword = input(
            "Search keyword: "
        ).strip()

    if not keyword:
        raise SystemExit(
            "Keyword cannot be empty."
        )

    max_results = args.max

    if max_results <= 0:
        raise SystemExit(
            "--max must be greater than 0."
        )

    main(
        keyword=keyword,
        max_results=max_results,
        output=args.output,
        jsonl_output=args.jsonl,
        headless=not args.show_browser,
    )