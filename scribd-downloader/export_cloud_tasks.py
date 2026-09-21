#!/usr/bin/env python3
"""Export queued Scribd downloads from the master SQLite DB for a cloud worker.

This tool does not touch the search crawler. It only reserves already-queued
records by changing downloads.status from 'queued' -> 'assigned' and writing a
JSONL task batch.

Extra assignment columns are added to the downloads table in-place if needed;
the existing local pipeline safely ignores them.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def ensure_assignment_columns(con: sqlite3.Connection) -> None:
    existing = {row[1] for row in con.execute("PRAGMA table_info(downloads)")}
    additions = {
        "assigned_device": "TEXT",
        "assigned_at": "TEXT",
        "batch_id": "TEXT",
    }
    for name, col_type in additions.items():
        if name not in existing:
            con.execute(f"ALTER TABLE downloads ADD COLUMN {name} {col_type}")


def make_batch_id(device: str) -> str:
    stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    safe_device = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in device)
    return f"{safe_device}-{stamp}-{suffix}"


def export_batch(db_path: str, output: str, count: int, device: str, batch_id: str | None) -> None:
    if count <= 0:
        raise SystemExit("--count must be > 0")

    batch_id = batch_id or make_batch_id(device)
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")

    con = connect(db_path)
    try:
        ensure_assignment_columns(con)
        con.commit()

        con.execute("BEGIN IMMEDIATE")
        rows = con.execute(
            """
            SELECT d.document_id, d.title, d.url, q.attempts
            FROM documents d
            JOIN downloads q ON q.document_id = d.document_id
            WHERE q.status = 'queued'
            ORDER BY d.first_seen, d.document_id
            LIMIT ?
            """,
            (count,),
        ).fetchall()

        if not rows:
            con.rollback()
            raise SystemExit("No queued downloads are available to export.")

        exported_at = utc_now()
        document_ids = [str(row[0]) for row in rows]

        # Reserve first while the write lock is held so the local downloader
        # cannot claim the same records concurrently.
        con.executemany(
            """
            UPDATE downloads
            SET status = 'assigned',
                assigned_device = ?,
                assigned_at = ?,
                batch_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE document_id = ? AND status = 'queued'
            """,
            [(device, exported_at, batch_id, doc_id) for doc_id in document_ids],
        )

        reserved = con.execute(
            "SELECT COUNT(*) FROM downloads WHERE batch_id = ? AND status = 'assigned'",
            (batch_id,),
        ).fetchone()[0]
        if reserved != len(rows):
            raise RuntimeError(f"Reserved {reserved} of {len(rows)} records; refusing partial batch.")

        try:
            with tmp_path.open("w", encoding="utf-8", newline="\n") as handle:
                for document_id, title, url, attempts in rows:
                    record = {
                        "batch_id": batch_id,
                        "device_id": device,
                        "exported_at": exported_at,
                        "document_id": str(document_id),
                        "title": title or "",
                        "url": url,
                        "master_attempts": int(attempts or 0),
                    }
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, output_path)
            con.commit()
        except Exception:
            con.rollback()
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            try:
                output_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise

    finally:
        con.close()

    print("========== CLOUD TASK EXPORT ==========")
    print(f"Batch ID       : {batch_id}")
    print(f"Assigned device: {device}")
    print(f"Tasks exported : {len(rows)}")
    print(f"Output          : {output_path.resolve()}")
    print("Master status   : queued -> assigned")
    print("=======================================")


def release_batch(db_path: str, batch_id: str) -> None:
    con = connect(db_path)
    try:
        ensure_assignment_columns(con)
        cur = con.execute(
            """
            UPDATE downloads
            SET status = 'queued',
                assigned_device = NULL,
                assigned_at = NULL,
                batch_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE batch_id = ? AND status = 'assigned'
            """,
            (batch_id,),
        )
        con.commit()
        print(f"Released {cur.rowcount} assigned task(s) from batch {batch_id} back to queued.")
    finally:
        con.close()


def show_assigned(db_path: str) -> None:
    con = connect(db_path)
    try:
        ensure_assignment_columns(con)
        con.commit()
        rows = con.execute(
            """
            SELECT batch_id, assigned_device, COUNT(*), MIN(assigned_at), MAX(assigned_at)
            FROM downloads
            WHERE status = 'assigned'
            GROUP BY batch_id, assigned_device
            ORDER BY MIN(assigned_at)
            """
        ).fetchall()
    finally:
        con.close()

    if not rows:
        print("No assigned cloud batches.")
        return
    print("batch_id\tdevice\tcount\tassigned_at")
    for batch_id, device, count, first_at, _ in rows:
        print(f"{batch_id}\t{device}\t{count}\t{first_at or '-'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export/reserve queued Scribd tasks for cloud workers.")
    parser.add_argument("--db", default="./scribd_state.db", help="Master SQLite DB path")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--count", type=int, help="Export this many queued tasks")
    action.add_argument("--release-batch", help="Return an assigned batch to queued")
    action.add_argument("--list-assigned", action="store_true", help="List currently assigned cloud batches")
    parser.add_argument("--device", default="RMB-SCRIBD-W01", help="Cloud worker device ID")
    parser.add_argument("--batch-id", help="Optional explicit batch ID")
    parser.add_argument("--output", default="./cloud_tasks.jsonl", help="Task JSONL output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.release_batch:
        release_batch(args.db, args.release_batch)
    elif args.list_assigned:
        show_assigned(args.db)
    else:
        export_batch(args.db, args.output, args.count, args.device, args.batch_id)


if __name__ == "__main__":
    main()
