"""
Scribd Full Pipeline
====================

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
import sqlite3
from pathlib import Path

import scribd_search_pipeline as searcher
from scribd_downloader import download_document


DEFAULT_DB = "scribd_state.db"
DEFAULT_OUTPUT_DIR = r"E:\RMB\scribd"


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


def batch_download(
    db_path,
    output_dir,
    limit=None,
    max_attempts=3,
):
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
        f"Max attempts    : {max_attempts}\n"
        f"====================================\n"
    )

    completed = 0
    failed = 0
    skipped_attempt_limit = 0

    for index, (document_id, title, url, attempts) in enumerate(
        rows,
        start=1,
    ):
        if attempts >= max_attempts:
            print(
                f"[{index}/{len(rows)}] [SKIP] "
                f"{document_id} already attempted {attempts} times"
            )
            skipped_attempt_limit += 1
            continue

        print()
        print("=" * 72)
        print(f"[{index}/{len(rows)}] {document_id}")
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
            completed += 1

        except KeyboardInterrupt:
            con = connect(db_path)
            con.execute(
                """
                UPDATE downloads
                SET
                    status = 'queued',
                    updated_at = CURRENT_TIMESTAMP
                WHERE document_id = ?
                """,
                (document_id,),
            )
            con.commit()
            con.close()
            raise

        except Exception as error:
            print(
                f"[DOWNLOAD ERROR] "
                f"{type(error).__name__}: {error}"
            )
            mark_failed(
                db_path,
                document_id,
                error,
            )
            failed += 1

    counts = download_counts(db_path)

    print("\n========== DOWNLOAD SUMMARY ==========")
    print(f"Downloaded this batch : {completed}")
    print(f"Failed this batch     : {failed}")
    print(f"Attempt-limit skips   : {skipped_attempt_limit}")
    print(f"Database status       : {counts}")
    print("======================================\n")


def main():
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
        "--max-per-query",
        type=int,
        default=100,
        help="Maximum search results to collect from each generated keyword.",
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
    if args.max_per_query <= 0:
        raise SystemExit("--max-per-query must be greater than 0")
    if args.max_attempts <= 0:
        raise SystemExit("--max-attempts must be greater than 0")

    searcher.init_db(args.db)

    if args.collect > 0:
        searcher.run_pipeline(
            db_path=args.db,
            target_new=args.collect,
            max_per_query=args.max_per_query,
            queue_output=args.queue_output,
            metadata_output=args.metadata_output,
            headless=not args.show_browser,
            one_keyword=args.keyword,
            pause_between_queries=max(args.pause, 0),
        )

    if args.download_batch > 0:
        batch_download(
            db_path=args.db,
            output_dir=args.output_dir,
            limit=args.download_batch,
            max_attempts=args.max_attempts,
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
