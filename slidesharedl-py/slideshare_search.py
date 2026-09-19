import argparse
import json
import re
import time
from pathlib import Path
from urllib.parse import quote_plus, urljoin

from playwright.sync_api import sync_playwright


SLIDESHARE_HOME = "https://www.slideshare.net"

SLIDESHOW_RE = re.compile(
    r"https?://(?:www\.)?slideshare\.net/slideshow/[^?#]+",
    re.IGNORECASE,
)


def normalize_url(url):
    if not url:
        return None

    if url.startswith("/"):
        url = urljoin(
            SLIDESHARE_HOME,
            url
        )

    match = SLIDESHOW_RE.search(url)

    if not match:
        return None

    return match.group(0).rstrip("/")


def collect_links(page):
    results = {}

    anchors = page.locator("a[href]")

    count = anchors.count()

    for i in range(count):

        try:
            anchor = anchors.nth(i)

            href = anchor.get_attribute("href")

            url = normalize_url(href)

            if not url:
                continue

            title = ""

            try:
                title = (
                    anchor.get_attribute("title")
                    or anchor.get_attribute("aria-label")
                    or anchor.inner_text()
                    or ""
                ).strip()

            except Exception:
                title = ""

            if url not in results:
                results[url] = {
                    "url": url,
                    "title": title,
                }

            elif (
                not results[url]["title"]
                and title
            ):
                results[url]["title"] = title

        except Exception:
            continue

    return list(results.values())


def open_search(page, keyword):
    print("[INFO] Opening SlideShare")

    page.goto(
        SLIDESHARE_HOME,
        wait_until="domcontentloaded",
        timeout=60000,
    )

    page.wait_for_timeout(3000)

    # 先嘗試直接找搜尋框
    selectors = [
        "input[type='search']",
        "input[placeholder*='Search' i]",
        "input[aria-label*='Search' i]",
    ]

    search_box = None

    for selector in selectors:

        try:
            locator = page.locator(selector)

            if locator.count() > 0:
                search_box = locator.first
                break

        except Exception:
            pass

    if search_box:

        try:
            print(
                f"[INFO] Searching: {keyword}"
            )

            search_box.fill(keyword)
            search_box.press("Enter")

            page.wait_for_timeout(5000)

            print(
                f"[INFO] Search page: {page.url}"
            )

            return

        except Exception:
            pass

    # 搜尋框找不到時使用搜尋網址 fallback
    search_url = (
        "https://www.slideshare.net/"
        "search/slideshow"
        f"?q={quote_plus(keyword)}"
    )

    print(
        f"[INFO] Opening fallback search: "
        f"{search_url}"
    )

    page.goto(
        search_url,
        wait_until="domcontentloaded",
        timeout=60000,
    )

    page.wait_for_timeout(5000)


def collect_results(
    page,
    max_results,
    max_no_growth=8,
):
    collected = {}

    previous_count = 0
    no_growth = 0
    round_number = 0

    while len(collected) < max_results:

        round_number += 1

        items = collect_links(page)

        for item in items:

            url = item["url"]

            if url not in collected:
                collected[url] = item

            elif (
                not collected[url]["title"]
                and item["title"]
            ):
                collected[url]["title"] = (
                    item["title"]
                )

        current_count = len(collected)

        print(
            f"[INFO] Scroll {round_number}: "
            f"{current_count}/{max_results} "
            f"unique presentations"
        )

        if current_count >= max_results:
            break

        if current_count == previous_count:
            no_growth += 1
        else:
            no_growth = 0

        if no_growth >= max_no_growth:
            print(
                "[INFO] No new presentations "
                "after several scrolls."
            )
            break

        previous_count = current_count

        page.evaluate(
            """
            window.scrollTo(
                0,
                document.body.scrollHeight
            )
            """
        )

        page.wait_for_timeout(2000)

        page.evaluate(
            "window.scrollBy(0, -300)"
        )

        page.wait_for_timeout(300)

        page.evaluate(
            """
            window.scrollTo(
                0,
                document.body.scrollHeight
            )
            """
        )

        page.wait_for_timeout(2000)

    return list(
        collected.values()
    )[:max_results]


def save_urls(results, path):
    output = Path(path)

    output.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        output,
        "w",
        encoding="utf-8",
    ) as f:

        for item in results:
            f.write(
                item["url"] + "\n"
            )


def save_jsonl(
    results,
    path,
    keyword,
):
    output = Path(path)

    output.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        output,
        "w",
        encoding="utf-8",
    ) as f:

        for item in results:

            row = {
                "keyword": keyword,
                "title": item["title"],
                "url": item["url"],
            }

            f.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                )
                + "\n"
            )


def main(
    keyword,
    max_results,
    output,
    jsonl_output,
    show_browser,
):

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=not show_browser
        )

        context = browser.new_context(
            viewport={
                "width": 1440,
                "height": 1100,
            }
        )

        page = context.new_page()

        try:

            open_search(
                page,
                keyword,
            )

            results = collect_results(
                page,
                max_results,
            )

            save_urls(
                results,
                output,
            )

            save_jsonl(
                results,
                jsonl_output,
                keyword,
            )

            print("")
            print(
                "========== DONE =========="
            )

            print(
                f"Keyword       : {keyword}"
            )

            print(
                f"Presentations : {len(results)}"
            )

            print(
                f"URL list      : {output}"
            )

            print(
                f"Metadata      : {jsonl_output}"
            )

            print(
                "=========================="
            )

        finally:
            context.close()
            browser.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--keyword",
        "-k",
        default=None,
    )

    parser.add_argument(
        "--max",
        "-m",
        type=int,
        default=100,
    )

    parser.add_argument(
        "--output",
        "-o",
        default="slideshare_urls.txt",
    )

    parser.add_argument(
        "--jsonl",
        default=(
            "slideshare_search_results.jsonl"
        ),
    )

    parser.add_argument(
        "--show-browser",
        action="store_true",
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

    if args.max <= 0:
        raise SystemExit(
            "--max must be greater than 0."
        )

    main(
        keyword=keyword,
        max_results=args.max,
        output=args.output,
        jsonl_output=args.jsonl,
        show_browser=args.show_browser,
    )
    