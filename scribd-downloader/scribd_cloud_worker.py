#!/usr/bin/env python3
"""Scribd cloud download worker for a small Linux VM.

Reads pre-exported JSONL tasks. It never searches Scribd and never opens the
master SQLite database. Each task is processed with the existing stable v16
Scribd PDF downloader core.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

try:
    import psutil
except ImportError:
    psutil = None

import scribd_downloader_cloud_v16 as downloader


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSON on {path}:{line_no}: {exc}") from exc
            for required in ("document_id", "url"):
                if not item.get(required):
                    raise RuntimeError(f"Missing {required} on {path}:{line_no}")
            rows.append(item)
    return rows


def load_existing_results(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    result = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            doc_id = str(row.get("document_id", ""))
            if doc_id:
                result[doc_id] = row
    return result


class SystemMonitor:
    def __init__(self, interval: float = 1.0):
        self.interval = max(0.25, interval)
        self.stop_event = threading.Event()
        self.thread = None
        self.samples = []

    def start(self):
        if psutil is None:
            return
        psutil.cpu_percent(interval=None)
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while not self.stop_event.wait(self.interval):
            vm = psutil.virtual_memory()
            self.samples.append({
                "cpu_percent": psutil.cpu_percent(interval=None),
                "memory_percent": vm.percent,
                "available_bytes": vm.available,
            })

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=3)

    def summary(self):
        if not self.samples:
            return None
        return {
            "avg_cpu_percent": round(statistics.mean(s["cpu_percent"] for s in self.samples), 1),
            "max_cpu_percent": round(max(s["cpu_percent"] for s in self.samples), 1),
            "max_memory_percent": round(max(s["memory_percent"] for s in self.samples), 1),
            "min_available_gib": round(min(s["available_bytes"] for s in self.samples) / (1024**3), 2),
        }


def process_task(task: dict, args: argparse.Namespace) -> dict:
    started_wall = utc_now()
    started = time.monotonic()
    doc_id = str(task["document_id"])
    title = task.get("title") or ""
    url = task["url"]

    result = {
        "batch_id": task.get("batch_id") or args.batch_id or "",
        "device_id": args.device_id,
        "document_id": doc_id,
        "title": title,
        "url": url,
        "started_at": started_wall,
        "status": "failed",
        "file_path": None,
        "file_size_bytes": 0,
        "elapsed_seconds": None,
        "error": None,
    }

    try:
        path = downloader.download_document(
            url,
            output_dir=str(args.output_dir),
            skip_existing=True,
            document_id=doc_id,
            title=title,
            verbose=args.verbose,
        )
        path_obj = Path(path)
        result["status"] = "downloaded"
        result["file_path"] = str(path_obj)
        if path_obj.exists():
            result["file_size_bytes"] = path_obj.stat().st_size
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"[:4000]
        if args.verbose:
            traceback.print_exc()
    finally:
        result["ended_at"] = utc_now()
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download pre-exported Scribd tasks on Linux; no search phase.")
    parser.add_argument("--tasks", required=True, help="Input JSONL task batch")
    parser.add_argument("--results", required=True, help="Append-only JSONL result file")
    parser.add_argument("--output-dir", default="/opt/rmb/output", help="PDF output directory")
    parser.add_argument("--tmp-dir", default="/opt/rmb/tmp", help="Temp directory for Chrome/PDF page spooling")
    parser.add_argument("--device-id", default="RMB-SCRIBD-W01")
    parser.add_argument("--batch-id", default="")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--limit", type=int, default=0, help="Optional max tasks for a benchmark run; 0 = all")
    parser.add_argument("--verbose", action="store_true", help="Show downloader internals")
    parser.add_argument("--monitor-interval", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    if args.workers > 2:
        print("[WARN] This worker was prepared for a 4 GB / 2 vCPU VM. Benchmark >2 workers carefully.")

    tasks_path = Path(args.tasks)
    results_path = Path(args.results)
    args.output_dir = Path(args.output_dir)
    args.tmp_dir = Path(args.tmp_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.tmp_dir.mkdir(parents=True, exist_ok=True)
    results_path.parent.mkdir(parents=True, exist_ok=True)

    # Force Python TemporaryDirectory (used by the stable downloader core) onto
    # the worker's disk-backed temp directory rather than RAM-backed /dev/shm.
    os.environ["TMPDIR"] = str(args.tmp_dir)
    tempfile.tempdir = str(args.tmp_dir)

    tasks = load_jsonl(tasks_path)
    if args.limit > 0:
        tasks = tasks[: args.limit]

    existing = load_existing_results(results_path)
    pending = [task for task in tasks if str(task["document_id"]) not in existing]

    print("========== CLOUD DOWNLOAD WORKER ==========")
    print(f"Device ID       : {args.device_id}")
    print(f"Workers         : {args.workers}")
    print(f"Tasks in file   : {len(tasks)}")
    print(f"Already recorded: {len(tasks) - len(pending)}")
    print(f"Pending         : {len(pending)}")
    print(f"Output          : {args.output_dir}")
    print(f"Results         : {results_path}")
    print("Search phase    : DISABLED (download-only)")
    print("===========================================")

    if not pending:
        print("Nothing to do.")
        return

    write_lock = threading.Lock()
    print_lock = threading.Lock()
    completed = 0
    success = 0
    failed = 0
    elapsed_values = []
    run_started = time.monotonic()

    monitor = SystemMonitor(args.monitor_interval)
    monitor.start()

    try:
        with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="dl") as pool:
            future_map = {pool.submit(process_task, task, args): task for task in pending}
            for future in as_completed(future_map):
                result = future.result()
                with write_lock:
                    with results_path.open("a", encoding="utf-8", newline="\n") as handle:
                        handle.write(json.dumps(result, ensure_ascii=False) + "\n")
                        handle.flush()
                        os.fsync(handle.fileno())

                completed += 1
                elapsed_values.append(float(result["elapsed_seconds"] or 0))
                if result["status"] == "downloaded":
                    success += 1
                else:
                    failed += 1

                run_elapsed = max(0.001, time.monotonic() - run_started)
                rate = success / run_elapsed * 3600
                with print_lock:
                    print(
                        f"[{completed:>4}/{len(pending)}] {result['status']:<10} "
                        f"id={result['document_id']} {result['elapsed_seconds']:>7.2f}s "
                        f"ok={success} fail={failed} rate={rate:.1f}/h"
                    )
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Ctrl+C received. Completed results are already safely written.")
    finally:
        monitor.stop()

    total_elapsed = max(0.001, time.monotonic() - run_started)
    success_rate = (success / completed * 100) if completed else 0.0
    success_per_hour = success / total_elapsed * 3600
    sys_summary = monitor.summary()

    print("\n========== CLOUD DOWNLOAD SUMMARY ==========")
    print(f"Device ID          : {args.device_id}")
    print(f"Workers used       : {args.workers}")
    print(f"Completed this run : {completed}")
    print(f"Downloaded         : {success}")
    print(f"Failed             : {failed}")
    print(f"Success rate       : {success_rate:.2f}%")
    print(f"Elapsed            : {total_elapsed:.1f}s")
    print(f"Download rate      : {success_per_hour:.1f} successful/hour")
    if elapsed_values:
        print(f"Median PDF time    : {statistics.median(elapsed_values):.2f}s")
        print(f"Avg PDF time       : {statistics.mean(elapsed_values):.2f}s")
    if sys_summary:
        print(f"Avg / max CPU      : {sys_summary['avg_cpu_percent']:.1f}% / {sys_summary['max_cpu_percent']:.1f}%")
        print(f"Max RAM usage      : {sys_summary['max_memory_percent']:.1f}%")
        print(f"Min RAM available  : {sys_summary['min_available_gib']:.2f} GiB")
    else:
        print("System metrics     : unavailable (pip install psutil)")
    print(f"Results JSONL      : {results_path}")
    print("============================================")


if __name__ == "__main__":
    main()
