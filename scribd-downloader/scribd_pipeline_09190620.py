"""
Scribd Full Pipeline v11
========================

Workflow:
1. Generate unused keyword combinations.
2. Search Scribd.
3. Deduplicate by document_id in SQLite.
4. Record title + URL + discovery keyword.
5. Build a persistent download queue.
6. Batch-download queued documents.
7. Record downloaded / failed status and local file path.

Required sibling files:
    scribd_search_pipeline.py
    scribd_downloader.py
"""

import argparse
import inspect
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import scribd_search_pipeline as searcher
from scribd_downloader import download_document


DEFAULT_DB = "scribd_state.db"
DEFAULT_OUTPUT_DIR = r"E:\RMB\scribd"


def validate_searcher_api():
    """
    Fail early with a clear message if scribd_pipeline.py and
    scribd_search_pipeline.py are from different generations.
    """
    required = {
        "result_wait_seconds",
        "render_settle_seconds",
        "empty_retries",
        "page_delay_seconds",
    }

    params = set(inspect.signature(searcher.run_pipeline).parameters)
    missing = required - params

    if missing:
        raise SystemExit(
            "scribd_search_pipeline.py is an older/incompatible version. "
            "Replace it with the matching v9 file. Missing parameters: "
            + ", ".join(sorted(missing))
        )



def connect(db_path):
    con = sqlite3.connect(db_path, timeout=30)
    con.execute("PRAGMA foreign_keys = ON")
    return con


def get_queued_documents(db_path, limit=None):
    con = connect(db_path)

    sql = """
        SELECT
            d.document_id,
            d.title,
            d.url,
            q.attempts
        FROM documents d
        JOIN downloads q
          ON q.document_id = d.document_id
        WHERE q.status IN ('queued', 'failed')
        ORDER BY
            CASE q.status
                WHEN 'queued' THEN 0
                ELSE 1
            END,
            d.first_seen,
            d.document_id
    """

    params = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (limit,)

    rows = con.execute(sql, params).fetchall()
    con.close()
    return rows


def mark_downloading(db_path, document_id):
    con = connect(db_path)
    con.execute(
        """
        UPDATE downloads
        SET
            status = 'downloading',
            attempts = attempts + 1,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE document_id = ?
        """,
        (document_id,),
    )
    con.commit()
    con.close()


def mark_downloaded(db_path, document_id, file_path):
    con = connect(db_path)
    con.execute(
        """
        UPDATE downloads
        SET
            status = 'downloaded',
            file_path = ?,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE document_id = ?
        """,
        (str(file_path), document_id),
    )
    con.commit()
    con.close()


def mark_failed(db_path, document_id, error):
    con = connect(db_path)
    con.execute(
        """
        UPDATE downloads
        SET
            status = 'failed',
            last_error = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE document_id = ?
        """,
        (str(error)[:4000], document_id),
    )
    con.commit()
    con.close()


def download_counts(db_path):
    con = connect(db_path)
    rows = dict(
        con.execute(
            """
            SELECT status, COUNT(*)
            FROM downloads
            GROUP BY status
            """
        ).fetchall()
    )
    con.close()
    return rows


def _download_one(
    db_path,
    output_dir,
    row,
    max_attempts,
    index,
    total,
):
    document_id, title, url, attempts = row

    if attempts >= max_attempts:
        return {
            "status": "skipped",
            "document_id": document_id,
            "message": (
                f"[{index}/{total}] [SKIP] "
                f"{document_id} already attempted {attempts} times"
            ),
        }

    print()
    print("=" * 72)
    print(f"[{index}/{total}] {document_id}")
    print(f"Title: {title}")
    print(f"URL  : {url}")
    print("=" * 72)

    mark_downloading(db_path, document_id)

    try:
        saved_path = download_document(
            url,
            output_dir=output_dir,
            skip_existing=True,
        )

        mark_downloaded(
            db_path,
            document_id,
            saved_path,
        )

        return {
            "status": "downloaded",
            "document_id": document_id,
            "path": saved_path,
        }

    except Exception as error:
        print(
            f"[DOWNLOAD ERROR] {document_id} "
            f"{type(error).__name__}: {error}"
        )

        mark_failed(
            db_path,
            document_id,
            error,
        )

        return {
            "status": "failed",
            "document_id": document_id,
            "error": str(error),
        }


def batch_download(
    db_path,
    output_dir,
    limit=None,
    max_attempts=3,
    workers=1,
):
    """
    Download queued documents concurrently.

    Each worker launches its own independent Chrome instance through
    download_document(). The command-line limit is 1-64 workers.
    """
    searcher.init_db(db_path)

    rows = get_queued_documents(
        db_path,
        limit=limit,
    )

    if not rows:
        print("[INFO] No queued documents to download.")
        return

    print(
        f"\n========== DOWNLOAD BATCH ==========\n"
        f"Queued selected : {len(rows)}\n"
        f"Output          : {output_dir}\n"
        f"Workers         : {workers}\n"
        f"Max attempts    : {max_attempts}\n"
        f"====================================\n"
    )

    completed = 0
    failed = 0
    skipped_attempt_limit = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _download_one,
                db_path,
                output_dir,
                row,
                max_attempts,
                index,
                len(rows),
            ): row[0]
            for index, row in enumerate(rows, start=1)
        }

        try:
            for future in as_completed(futures):
                document_id = futures[future]

                try:
                    result = future.result()
                except Exception as error:
                    # This is a last-resort guard around unexpected worker errors.
                    print(
                        f"[WORKER ERROR] {document_id} "
                        f"{type(error).__name__}: {error}"
                    )
                    mark_failed(
                        db_path,
                        document_id,
                        error,
                    )
                    failed += 1
                    continue

                status = result["status"]

                if status == "downloaded":
                    completed += 1
                    print(
                        f"[OK] {document_id} "
                        f"({completed} downloaded this batch)"
                    )

                elif status == "failed":
                    failed += 1

                elif status == "skipped":
                    skipped_attempt_limit += 1
                    print(result["message"])

        except KeyboardInterrupt:
            print(
                "\n[INFO] Interrupted. "
                "Running Chrome workers may need a moment to close."
            )
            raise

    counts = download_counts(db_path)

    print("\n========== DOWNLOAD SUMMARY ==========")
    print(f"Downloaded this batch : {completed}")
    print(f"Failed this batch     : {failed}")
    print(f"Attempt-limit skips   : {skipped_attempt_limit}")
    print(f"Workers used          : {workers}")
    print(f"Database status       : {counts}")
    print("======================================\n")


def main():
    validate_searcher_api()
    parser = argparse.ArgumentParser(
        description=(
            "Generate/search/deduplicate Scribd documents, then batch-download "
            "the persistent queue."
        )
    )

    parser.add_argument(
        "--collect",
        type=int,
        default=100,
        help=(
            "Number of NEW unique documents to collect before downloading. "
            "Use 0 to skip searching."
        ),
    )
    parser.add_argument(
        "--search-workers",
        type=int,
        default=1,
        help=(
            "Concurrent search workers / Chrome instances. "
            "Allowed range: 1-64. Default: 1."
        ),
    )
    parser.add_argument(
        "--pages-per-keyword",
        type=int,
        default=3,
        help=(
            "Number of Scribd search result pages to visit per keyword. "
            "About 40 results are available per page, so 3 ~= 120 and 20 ~= 800."
        ),
    )
    parser.add_argument(
        "--max-per-query",
        type=int,
        default=None,
        help=(
            "Optional unique-document cap per keyword. "
            "Defaults to pages-per-keyword * 40."
        ),
    )
    parser.add_argument(
        "--download-batch",
        type=int,
        default=100,
        help=(
            "Maximum queued documents to download after search. "
            "Use 0 to skip downloading."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "Number of concurrent download workers / Chrome instances. "
            "Allowed range: 1-64. Default: 1."
        ),
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB,
        help="SQLite state database.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Folder where PDFs are saved.",
    )
    parser.add_argument(
        "--queue-output",
        default="scribd_download_queue.txt",
        help="Text export of currently queued URLs.",
    )
    parser.add_argument(
        "--metadata-output",
        default="scribd_documents.csv",
        help="CSV export of discovered document metadata.",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=2.0,
        help="Pause in seconds between searches.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Do not retry one document after this many download attempts.",
    )
    parser.add_argument(
        "--result-wait",
        type=float,
        default=15.0,
        help=(
            "Maximum seconds to wait for search result links on each Scribd "
            "result page. Default: 15."
        ),
    )
    parser.add_argument(
        "--render-settle",
        type=float,
        default=1.5,
        help=(
            "Extra seconds to wait after results first appear before scanning "
            "the page once. Default: 1.5."
        ),
    )
    parser.add_argument(
        "--empty-retries",
        type=int,
        default=1,
        help=(
            "Retry count when a result page returns zero documents. Default: 1."
        ),
    )
    parser.add_argument(
        "--page-delay",
        type=float,
        default=2.0,
        help=(
            "Seconds between result pages for the same keyword. Default: 2."
        ),
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Show the SEARCH browser window instead of headless mode.",
    )
    parser.add_argument(
        "--keyword",
        default=None,
        help="Optional one-keyword search test instead of random generation.",
    )

    args = parser.parse_args()

    if args.collect < 0:
        raise SystemExit("--collect cannot be negative")
    if args.download_batch < 0:
        raise SystemExit("--download-batch cannot be negative")
    if not 1 <= args.workers <= 64:
        raise SystemExit("--workers must be between 1 and 64")
    if not 1 <= args.search_workers <= 64:
        raise SystemExit("--search-workers must be between 1 and 64")
    if args.pages_per_keyword <= 0:
        raise SystemExit("--pages-per-keyword must be greater than 0")
    if args.result_wait <= 0:
        raise SystemExit("--result-wait must be greater than 0")
    if args.render_settle < 0:
        raise SystemExit("--render-settle cannot be negative")
    if args.empty_retries < 0:
        raise SystemExit("--empty-retries cannot be negative")
    if args.page_delay < 0:
        raise SystemExit("--page-delay cannot be negative")

    max_per_query = (
        args.max_per_query
        if args.max_per_query is not None
        else args.pages_per_keyword * 40
    )

    if max_per_query <= 0:
        raise SystemExit("--max-per-query must be greater than 0")
    if args.max_attempts <= 0:
        raise SystemExit("--max-attempts must be greater than 0")

    searcher.init_db(args.db)

    # Show the complete run configuration up front so both the newer
    # reliability/timing controls and the older search/download controls are
    # visible before any browser workers start.
    theoretical_min_pages = (
        (args.collect + 39) // 40
        if args.collect > 0
        else 0
    )

    print("\n========== FULL PIPELINE CONFIG (v11) ==========")
    print(f"Collect target         : {args.collect}")
    print(f"Search workers         : {args.search_workers}")
    print(f"Pages per keyword      : {args.pages_per_keyword}")
    print(f"Max docs per keyword   : {max_per_query}")
    if args.collect > 0:
        print(
            f"Theoretical min pages  : {theoretical_min_pages} "
            "(at 40 results/page, before duplicates/empty pages)"
        )
    else:
        print("Theoretical min pages  : 0 (search skipped)")
    print(f"Result wait            : {args.result_wait}s")
    print(f"Render settle          : {args.render_settle}s")
    print(f"Empty retries          : {args.empty_retries}")
    print(f"Delay between pages    : {args.page_delay}s")
    print(f"Delay between keywords : {max(args.pause, 0)}s")
    print(f"Download batch         : {args.download_batch}")
    print(f"Download workers       : {args.workers}")
    print(f"Max download attempts  : {args.max_attempts}")
    print(f"PDF output             : {args.output_dir}")
    print(f"Database               : {args.db}")
    print("================================================\n")

    if args.collect == 0:
        print("[INFO] Search phase skipped because --collect 0.")

    if args.collect > 0:
        searcher.run_pipeline(
            db_path=args.db,
            target_new=args.collect,
            max_per_query=max_per_query,
            queue_output=args.queue_output,
            metadata_output=args.metadata_output,
            headless=not args.show_browser,
            one_keyword=args.keyword,
            pause_between_queries=max(args.pause, 0),
            pages_per_keyword=args.pages_per_keyword,
            search_workers=args.search_workers,
            result_wait_seconds=args.result_wait,
            render_settle_seconds=args.render_settle,
            empty_retries=args.empty_retries,
            page_delay_seconds=args.page_delay,
        )

    if args.download_batch == 0:
        print("[INFO] Download phase skipped because --download-batch 0.")

    if args.download_batch > 0:
        batch_download(
            db_path=args.db,
            output_dir=args.output_dir,
            limit=args.download_batch,
            max_attempts=args.max_attempts,
            workers=args.workers,
        )

    searcher.export_queue(
        args.db,
        args.queue_output,
    )
    searcher.export_metadata_csv(
        args.db,
        args.metadata_output,
    )

    counts = download_counts(args.db)
    print("\n========== FINAL STATUS ==========")
    print(f"DB          : {Path(args.db).resolve()}")
    print(f"PDF output  : {args.output_dir}")
    print(f"Downloads   : {counts}")
    print("==================================\n")


if __name__ == "__main__":
    main()
