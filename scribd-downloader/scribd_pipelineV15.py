"""
Scribd Full Pipeline v15
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
import queue
import sqlite3
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import scribd_search_pipeline as searcher
from scribd_downloader import download_document


DEFAULT_DB = "scribd_state.db"
DEFAULT_OUTPUT_DIR = r"E:\RMB\scribd"


def validate_searcher_api():
    """
    Fail early if scribd_pipeline.py and scribd_search_pipeline.py do not match.
    """
    required = {
        "result_wait_seconds",
        "stable_checks",
        "stable_interval_seconds",
        "page_delay_seconds",
    }

    params = set(inspect.signature(searcher.run_pipeline).parameters)
    missing = required - params

    if missing:
        raise SystemExit(
            "scribd_search_pipeline.py is an older/incompatible version. "
            "Replace it with the matching v15 file. Missing parameters: "
            + ", ".join(sorted(missing))
        )


def connect(db_path):
    con = sqlite3.connect(db_path, timeout=30)
    con.execute("PRAGMA foreign_keys = ON")
    return con


def ensure_monitoring_schema(db_path):
    """Migrate v14-and-earlier databases without deleting existing data."""
    con = connect(db_path)

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

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ended_at TEXT,
            last_heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL DEFAULT 'running',
            search_target INTEGER,
            search_workers INTEGER,
            download_batch INTEGER,
            download_workers INTEGER,
            search_elapsed_seconds REAL NOT NULL DEFAULT 0,
            download_elapsed_seconds REAL NOT NULL DEFAULT 0,
            total_elapsed_seconds REAL NOT NULL DEFAULT 0,
            error TEXT
        )
        """
    )

    con.commit()
    con.close()


def start_pipeline_run(
    db_path,
    search_target,
    search_workers,
    download_batch,
    download_workers,
):
    con = connect(db_path)
    cur = con.execute(
        """
        INSERT INTO pipeline_runs (
            search_target,
            search_workers,
            download_batch,
            download_workers,
            status,
            started_at,
            last_heartbeat_at
        )
        VALUES (?, ?, ?, ?, 'running', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (
            search_target,
            search_workers,
            download_batch,
            download_workers,
        ),
    )
    run_id = cur.lastrowid
    con.commit()
    con.close()
    return run_id


def heartbeat_pipeline_run(db_path, run_id):
    con = connect(db_path)
    con.execute(
        """
        UPDATE pipeline_runs
        SET last_heartbeat_at = CURRENT_TIMESTAMP
        WHERE run_id = ? AND status = 'running'
        """,
        (run_id,),
    )
    con.commit()
    con.close()


def finish_pipeline_run(
    db_path,
    run_id,
    status,
    search_elapsed,
    download_elapsed,
    total_elapsed,
    error=None,
):
    con = connect(db_path)
    con.execute(
        """
        UPDATE pipeline_runs
        SET
            status = ?,
            ended_at = CURRENT_TIMESTAMP,
            last_heartbeat_at = CURRENT_TIMESTAMP,
            search_elapsed_seconds = ?,
            download_elapsed_seconds = ?,
            total_elapsed_seconds = ?,
            error = ?
        WHERE run_id = ?
        """,
        (
            status,
            float(search_elapsed),
            float(download_elapsed),
            float(total_elapsed),
            None if error is None else str(error)[:4000],
            run_id,
        ),
    )
    con.commit()
    con.close()


def pipeline_run_heartbeat_loop(db_path, run_id, stop_event, interval_seconds):
    while not stop_event.wait(interval_seconds):
        try:
            heartbeat_pipeline_run(db_path, run_id)
        except Exception:
            # Monitoring must never terminate the crawler itself.
            pass


def heartbeat_active_downloads(db_path):
    con = connect(db_path)
    con.execute(
        """
        UPDATE downloads
        SET heartbeat_at = CURRENT_TIMESTAMP
        WHERE status = 'downloading'
        """
    )
    con.commit()
    con.close()


def download_heartbeat_loop(db_path, stop_event, interval_seconds):
    while not stop_event.wait(interval_seconds):
        try:
            heartbeat_active_downloads(db_path)
        except Exception:
            pass


def get_last_failure_time(db_path):
    con = connect(db_path)
    row = con.execute(
        "SELECT MAX(failed_at) FROM downloads WHERE failed_at IS NOT NULL"
    ).fetchone()
    con.close()
    return row[0] if row and row[0] else "-"


def local_timestamp():
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


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
        WHERE q.status = 'queued'
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
            started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
            last_attempt_at = CURRENT_TIMESTAMP,
            heartbeat_at = CURRENT_TIMESTAMP,
            finished_at = NULL,
            failed_at = NULL,
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
            heartbeat_at = CURRENT_TIMESTAMP,
            finished_at = CURRENT_TIMESTAMP,
            failed_at = NULL,
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
            heartbeat_at = CURRENT_TIMESTAMP,
            failed_at = CURRENT_TIMESTAMP,
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



def reset_stale_downloading(db_path):
    """
    Recover documents left in `downloading` by a prior interrupted run.

    Interrupted attempts are not charged against max-attempts.
    """
    con = connect(db_path)
    cur = con.execute(
        """
        UPDATE downloads
        SET
            status = 'queued',
            attempts = CASE
                WHEN attempts > 0 THEN attempts - 1
                ELSE 0
            END,
            last_error = NULL,
            heartbeat_at = CURRENT_TIMESTAMP,
            recovered_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE status = 'downloading'
        """
    )
    con.commit()
    count = cur.rowcount
    con.close()
    return count


def requeue_interrupted(db_path, document_id):
    con = connect(db_path)
    con.execute(
        """
        UPDATE downloads
        SET
            status = 'queued',
            attempts = CASE
                WHEN attempts > 0 THEN attempts - 1
                ELSE 0
            END,
            last_error = NULL,
            heartbeat_at = CURRENT_TIMESTAMP,
            recovered_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE document_id = ?
        """,
        (document_id,),
    )
    con.commit()
    con.close()


def format_elapsed(seconds):
    seconds = max(0, int(round(seconds)))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"



def _download_one(
    db_path,
    output_dir,
    row,
    max_attempts,
    worker_id,
    stop_event,
    update_worker,
    register_driver,
):
    document_id, title, url, attempts = row

    if stop_event.is_set():
        return {
            "status": "interrupted",
            "document_id": document_id,
        }

    if attempts >= max_attempts:
        update_worker(
            worker_id,
            document_id=document_id,
            title=title,
            status="attempt limit",
            current_page=0,
            total_pages=0,
        )
        return {
            "status": "skipped",
            "document_id": document_id,
        }

    mark_downloading(db_path, document_id)

    update_worker(
        worker_id,
        document_id=document_id,
        title=title,
        status="starting",
        current_page=0,
        total_pages=0,
    )

    def progress_callback(
        status=None,
        current_page=0,
        total_pages=0,
        **_,
    ):
        update_worker(
            worker_id,
            document_id=document_id,
            title=title,
            status=status or "working",
            current_page=current_page or 0,
            total_pages=total_pages or 0,
        )

    def driver_callback(driver):
        register_driver(worker_id, driver)

    try:
        saved_path = download_document(
            url,
            output_dir=output_dir,
            skip_existing=True,
            document_id=document_id,
            title=title,
            verbose=False,
            stop_event=stop_event,
            progress_callback=progress_callback,
            driver_callback=driver_callback,
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
        if stop_event.is_set() or isinstance(error, InterruptedError):
            requeue_interrupted(
                db_path,
                document_id,
            )
            return {
                "status": "interrupted",
                "document_id": document_id,
            }

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

    finally:
        register_driver(worker_id, None)


def batch_download(
    db_path,
    output_dir,
    limit=None,
    max_attempts=3,
    workers=1,
    heartbeat_interval=30.0,
):
    """
    Download queued documents with 1-64 persistent worker slots.

    Ctrl+C:
      - stops dispatching new documents;
      - terminates active ChromeDriver processes;
      - returns in-progress documents to `queued`;
      - leaves not-yet-started documents queued.
    """
    searcher.init_db(db_path)
    ensure_monitoring_schema(db_path)

    recovered = reset_stale_downloading(db_path)

    rows = get_queued_documents(
        db_path,
        limit=limit,
    )

    if not rows:
        print("[INFO] No queued documents to download.")
        return {
            "interrupted": False,
            "downloaded": 0,
            "failed": 0,
            "skipped": 0,
            "selected": 0,
        }

    print(
        f"\n========== DOWNLOAD BATCH ==========\n"
        f"Queued selected : {len(rows)}\n"
        f"Output          : {output_dir}\n"
        f"Workers         : {workers}\n"
        f"Max attempts    : {max_attempts}\n"
        f"Heartbeat       : {heartbeat_interval}s\n"
        f"Recovered queue : {recovered}\n"
        f"====================================\n"
    )

    task_queue = queue.Queue()
    for row in rows:
        task_queue.put(row)

    stop_event = threading.Event()
    state_lock = threading.Lock()
    drivers_lock = threading.Lock()
    dashboard_stop = threading.Event()

    active_drivers = {}

    counters = {
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "interrupted": 0,
    }

    worker_states = {
        wid: {
            "document_id": "-",
            "title": "waiting",
            "status": "waiting",
            "current_page": 0,
            "total_pages": 0,
        }
        for wid in range(1, workers + 1)
    }

    def short_text(value, width=38):
        value = str(value or "")
        return value if len(value) <= width else value[: width - 1] + "…"

    def progress_bar(current, total, width=18):
        if total <= 0:
            return "░" * width
        current = max(0, min(int(current), int(total)))
        filled = int(width * current / total)
        return "█" * filled + "░" * (width - filled)

    def update_worker(worker_id, **kwargs):
        with state_lock:
            worker_states[worker_id].update(kwargs)

    def register_driver(worker_id, driver):
        with drivers_lock:
            if driver is None:
                active_drivers.pop(worker_id, None)
            else:
                active_drivers[worker_id] = driver

    def terminate_active_drivers():
        with drivers_lock:
            snapshot = list(active_drivers.items())

        for _, driver in snapshot:
            try:
                service = getattr(driver, "service", None)
                process = getattr(service, "process", None)

                if process is not None and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=1.0)
                    except Exception:
                        try:
                            process.kill()
                        except Exception:
                            pass
            except Exception:
                pass

    def dashboard_loop():
        first_draw = True

        while not dashboard_stop.is_set():
            with state_lock:
                snapshot = {
                    wid: dict(values)
                    for wid, values in worker_states.items()
                }
                counts = dict(counters)

            finished = (
                counts["downloaded"]
                + counts["failed"]
                + counts["skipped"]
            )

            lines = [
                (
                    f"DOWNLOAD {progress_bar(finished, len(rows), 26)} "
                    f"{finished}/{len(rows)} | "
                    f"ok {counts['downloaded']} | "
                    f"fail {counts['failed']} | "
                    f"skip {counts['skipped']}"
                )
            ]

            for wid in sorted(snapshot):
                ws = snapshot[wid]
                page_text = (
                    f"{ws['current_page']}/{ws['total_pages']}"
                    if ws["total_pages"] > 0
                    else "-/-"
                )

                lines.append(
                    f"W{wid:02d} "
                    f"{progress_bar(ws['current_page'], ws['total_pages'])} "
                    f"{page_text:>7} | "
                    f"{str(ws['document_id']):<10} | "
                    f"{short_text(ws['title']):<38} | "
                    f"{ws['status']}"
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

    def worker_loop(worker_id):
        while not stop_event.is_set():
            try:
                row = task_queue.get_nowait()
            except queue.Empty:
                break

            try:
                if stop_event.is_set():
                    break

                result = _download_one(
                    db_path=db_path,
                    output_dir=output_dir,
                    row=row,
                    max_attempts=max_attempts,
                    worker_id=worker_id,
                    stop_event=stop_event,
                    update_worker=update_worker,
                    register_driver=register_driver,
                )

                with state_lock:
                    status = result["status"]
                    if status in counters:
                        counters[status] += 1

                if result["status"] == "downloaded":
                    update_worker(worker_id, status="done")
                elif result["status"] == "failed":
                    error_text = short_text(result.get("error", "failed"), 28)
                    update_worker(worker_id, status=f"failed: {error_text}")
                elif result["status"] == "skipped":
                    update_worker(worker_id, status="skipped")
                elif result["status"] == "interrupted":
                    update_worker(worker_id, status="interrupted")

            finally:
                task_queue.task_done()

        with state_lock:
            final_status = worker_states[worker_id].get("status", "")
            if final_status in {"waiting", "starting", "loading", "working"}:
                worker_states[worker_id]["status"] = "stopped"
        register_driver(worker_id, None)

    dashboard_thread = threading.Thread(
        target=dashboard_loop,
        name="download-dashboard",
        daemon=True,
    )
    dashboard_thread.start()

    download_heartbeat_stop = threading.Event()
    download_heartbeat_thread = threading.Thread(
        target=download_heartbeat_loop,
        args=(db_path, download_heartbeat_stop, heartbeat_interval),
        name="download-heartbeat",
        daemon=True,
    )
    download_heartbeat_thread.start()

    worker_threads = [
        threading.Thread(
            target=worker_loop,
            args=(wid,),
            name=f"scribd-download-{wid}",
            daemon=True,
        )
        for wid in range(1, workers + 1)
    ]

    for thread in worker_threads:
        thread.start()

    interrupted = False

    try:
        while any(thread.is_alive() for thread in worker_threads):
            for thread in worker_threads:
                thread.join(timeout=0.10)

    except KeyboardInterrupt:
        interrupted = True
        stop_event.set()
        terminate_active_drivers()

        # Give workers a short window to catch the terminated Selenium command
        # and return their in-progress rows to the queue.
        deadline = time.perf_counter() + 3.0
        for thread in worker_threads:
            remaining = max(0.0, deadline - time.perf_counter())
            if remaining <= 0:
                break
            thread.join(timeout=remaining)

    finally:
        if interrupted:
            stop_event.set()
            terminate_active_drivers()

        # Anything still marked downloading is returned to queued, including
        # a worker that was blocked when Ctrl+C arrived.
        reset_stale_downloading(db_path)

        download_heartbeat_stop.set()
        download_heartbeat_thread.join(timeout=1.5)

        dashboard_stop.set()
        dashboard_thread.join(timeout=1.5)
        print()

    counts = download_counts(db_path)

    with state_lock:
        local_counts = dict(counters)

    print("\n========== DOWNLOAD SUMMARY ==========")
    print(f"Downloaded this batch : {local_counts['downloaded']}")
    print(f"Failed this batch     : {local_counts['failed']}")
    print(f"Attempt-limit skips   : {local_counts['skipped']}")
    print(f"Workers used          : {workers}")
    print(f"Interrupted           : {'yes' if interrupted else 'no'}")
    print(f"Database status       : {counts}")
    print(f"Last failure recorded : {get_last_failure_time(db_path)} UTC")
    print("======================================\n")

    return {
        "interrupted": interrupted,
        "downloaded": local_counts["downloaded"],
        "failed": local_counts["failed"],
        "skipped": local_counts["skipped"],
        "selected": len(rows),
    }


def main():
    total_started = time.perf_counter()
    search_elapsed = 0.0
    download_elapsed = 0.0
    pipeline_interrupted = False

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
            "Confirmed NEW-document target for stopping NEW keyword assignment. "
            "Already-running keywords finish normally. Use 0 to skip search."
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
            "Maximum pages per keyword. Use 0 for automatic/unlimited paging "
            "until that keyword is exhausted."
        ),
    )
    parser.add_argument(
        "--max-per-query",
        type=int,
        default=None,
        help=(
            "Optional unique-document cap per keyword. If omitted: page_limit "
            "* 40 in limited mode; unlimited in --pages-per-keyword 0 mode."
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
            "Concurrent download workers / Chrome instances. "
            "Allowed range: 1-64. Default: 1."
        ),
    )
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--queue-output",
        default="scribd_download_queue.txt",
    )
    parser.add_argument(
        "--metadata-output",
        default="scribd_documents.csv",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=2.0,
        help="Pause in seconds between keyword searches.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Maximum failed download attempts per document.",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=30.0,
        help=(
            "Seconds between run/download heartbeats used to estimate when a "
            "long run crashed. Default: 30."
        ),
    )
    parser.add_argument(
        "--result-wait",
        type=float,
        default=60.0,
        help=(
            "Maximum seconds to wait on the SAME search result page. "
            "The page is not refreshed. Default: 60."
        ),
    )
    parser.add_argument(
        "--stable-checks",
        type=int,
        default=3,
        help=(
            "Positive result count must stay unchanged for this many checks "
            "before the page is scanned. Default: 3."
        ),
    )
    parser.add_argument(
        "--stable-interval",
        type=float,
        default=0.5,
        help="Seconds between result-count stability checks. Default: 0.5.",
    )
    # Backward compatibility for old commands. v14 does not use either option.
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
        help="Seconds between result pages for one keyword. Default: 2.",
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
    if args.max_attempts <= 0:
        raise SystemExit("--max-attempts must be greater than 0")
    if args.heartbeat_interval <= 0:
        raise SystemExit("--heartbeat-interval must be greater than 0")

    max_per_query = (
        args.max_per_query
        if args.max_per_query is not None
        else (
            None
            if args.pages_per_keyword == 0
            else args.pages_per_keyword * 40
        )
    )

    searcher.init_db(args.db)
    ensure_monitoring_schema(args.db)

    theoretical_min_pages = (
        (args.collect + 39) // 40
        if args.collect > 0
        else 0
    )

    pages_label = (
        "auto / until exhausted"
        if args.pages_per_keyword == 0
        else str(args.pages_per_keyword)
    )
    max_docs_label = (
        "unlimited"
        if max_per_query is None
        else str(max_per_query)
    )

    print("\n========== FULL PIPELINE CONFIG (v15) ==========")
    print(f"Collect target         : {args.collect}")
    print(f"Search workers         : {args.search_workers}")
    print(f"Pages per keyword      : {pages_label}")
    print(f"Max docs per keyword   : {max_docs_label}")
    if args.collect > 0:
        print(
            f"Theoretical min pages  : {theoretical_min_pages} "
            "(before duplicates/empty pages)"
        )
    else:
        print("Theoretical min pages  : 0 (search skipped)")
    print(f"Result max wait        : {args.result_wait}s")
    print(f"Stable checks          : {args.stable_checks}")
    print(f"Stable interval        : {args.stable_interval}s")
    print("Refresh/retry          : disabled")
    print(f"Delay between pages    : {args.page_delay}s")
    print(f"Delay between keywords : {max(args.pause, 0)}s")
    print(f"Download batch         : {args.download_batch}")
    print(f"Download workers       : {args.workers}")
    print(f"Max download attempts  : {args.max_attempts}")
    print(f"Heartbeat interval     : {args.heartbeat_interval}s")
    print(f"PDF output             : {args.output_dir}")
    print(f"Database               : {args.db}")
    print("================================================\n")

    run_started_wall = local_timestamp()
    run_id = start_pipeline_run(
        args.db,
        search_target=args.collect,
        search_workers=args.search_workers,
        download_batch=args.download_batch,
        download_workers=args.workers,
    )
    run_heartbeat_stop = threading.Event()
    run_heartbeat_thread = threading.Thread(
        target=pipeline_run_heartbeat_loop,
        args=(args.db, run_id, run_heartbeat_stop, args.heartbeat_interval),
        name="pipeline-heartbeat",
        daemon=True,
    )
    run_heartbeat_thread.start()

    if args.collect == 0:
        print("[INFO] Search phase skipped because --collect 0.")

    run_error = None

    try:
        if args.collect > 0:
            search_started = time.perf_counter()

            search_result = searcher.run_pipeline(
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
                stable_checks=args.stable_checks,
                stable_interval_seconds=args.stable_interval,
                page_delay_seconds=args.page_delay,
            )

            search_elapsed = time.perf_counter() - search_started

            if search_result and search_result.get("interrupted"):
                pipeline_interrupted = True

        if args.download_batch == 0:
            print("[INFO] Download phase skipped because --download-batch 0.")

        if pipeline_interrupted and args.download_batch > 0:
            print("[INFO] Download phase skipped because the pipeline was interrupted.")

        if args.download_batch > 0 and not pipeline_interrupted:
            download_started = time.perf_counter()

            download_result = batch_download(
                db_path=args.db,
                output_dir=args.output_dir,
                limit=args.download_batch,
                max_attempts=args.max_attempts,
                workers=args.workers,
                heartbeat_interval=args.heartbeat_interval,
            )

            download_elapsed = time.perf_counter() - download_started

            if download_result and download_result.get("interrupted"):
                pipeline_interrupted = True

        searcher.export_queue(args.db, args.queue_output)
        searcher.export_metadata_csv(args.db, args.metadata_output)

    except BaseException as error:
        run_error = error
        raise

    finally:
        total_elapsed = time.perf_counter() - total_started
        run_heartbeat_stop.set()
        run_heartbeat_thread.join(timeout=1.5)

        if run_error is not None:
            run_status = "error"
        elif pipeline_interrupted:
            run_status = "interrupted"
        else:
            run_status = "completed"

        finish_pipeline_run(
            args.db,
            run_id,
            status=run_status,
            search_elapsed=search_elapsed,
            download_elapsed=download_elapsed,
            total_elapsed=total_elapsed,
            error=run_error,
        )

    counts = download_counts(args.db)
    run_ended_wall = local_timestamp()

    print("\n========== FINAL SUMMARY ==========")
    print(f"Run ID           : {run_id}")
    print(f"Run started      : {run_started_wall}")
    print(f"Run ended        : {run_ended_wall}")
    print(f"DB               : {Path(args.db).resolve()}")
    print(f"PDF output       : {args.output_dir}")
    print(f"Downloads        : {counts}")
    print(f"Search elapsed   : {format_elapsed(search_elapsed)}")
    print(f"Download elapsed : {format_elapsed(download_elapsed)}")
    print(f"Total elapsed    : {format_elapsed(total_elapsed)}")
    print(
        f"Status           : "
        f"{'interrupted by user' if pipeline_interrupted else 'completed'}"
    )
    print("===================================\n")


if __name__ == "__main__":
    main()
