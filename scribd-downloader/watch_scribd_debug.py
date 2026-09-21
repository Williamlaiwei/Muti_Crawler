import argparse
import json
import os
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path


def parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except Exception:
        return None


def short(value, width=42):
    text = " ".join(str(value or "").split())
    if len(text) <= width:
        return text
    return text[: width - 1] + "…"


def fmt_num(value, digits=1, suffix=""):
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{digits}f}{suffix}"
    except Exception:
        return str(value)


def clear_screen():
    # ANSI works in current Windows Terminal / PowerShell and avoids spawning cls.
    print("\033[2J\033[H", end="")


def build_view(events, window_seconds, recent_count):
    now = time.time()
    monitors = [e for e in events if e.get("type") == "monitor"]
    pages = [e for e in events if e.get("type") == "page"]
    browser = [e for e in events if e.get("type") == "browser"]
    errors = [e for e in events if e.get("type") == "worker_error"]

    latest = monitors[-1] if monitors else {}
    recent_pages = []
    for e in pages:
        ts = parse_ts(e.get("timestamp"))
        if ts is None or now - ts <= window_seconds:
            recent_pages.append(e)

    page_count = len(recent_pages)
    zero_pages = [e for e in recent_pages if int(e.get("page_unique") or 0) == 0]
    ok_pages = [e for e in recent_pages if int(e.get("page_unique") or 0) > 0]
    stable_pages = [e for e in recent_pages if e.get("stable_reached")]
    classifications = Counter(e.get("classification") or "unknown" for e in recent_pages)

    def avg(key, subset=None):
        vals = []
        for e in subset if subset is not None else recent_pages:
            try:
                vals.append(float(e.get(key) or 0))
            except Exception:
                pass
        return sum(vals) / len(vals) if vals else 0.0

    lines = []
    lines.append("SCRIBD SEARCH DIAGNOSTIC — separate window (main dashboard untouched)")
    lines.append("=" * 86)

    if latest:
        lines.append(
            f"CONFIG workers={latest.get('configured_workers')}  "
            f"stable={latest.get('stable_checks')}x{latest.get('stable_interval_seconds')}s  "
            f"result_wait={latest.get('result_wait_seconds')}s  "
            f"page_delay={latest.get('page_delay_seconds')}s  "
            f"load={latest.get('page_load_strategy')}  images_blocked={latest.get('images_blocked')}"
        )
        severity = latest.get("severity", "ok")
        lines.append(
            f"LIVE   active={latest.get('active_workers', 0)}  "
            f"zero_waiting={latest.get('zero_waiting_workers', 0)}  "
            f"positive_waiting={latest.get('positive_waiting_workers', 0)}  "
            f"state={severity}"
        )
        lines.append(
            "SYSTEM "
            f"CPU={fmt_num(latest.get('cpu_percent'),1,'%')}  "
            f"RAM={fmt_num(latest.get('ram_percent'),1,'%')}  "
            f"avail={fmt_num(latest.get('ram_available_gb'),2,'GB')}  "
            f"browserRSS={fmt_num(latest.get('browser_rss_gb'),2,'GB')}  "
            f"browserProc={latest.get('browser_processes', 'n/a')}  "
            f"RX={fmt_num(latest.get('net_rx_mbps'),2,'Mbps')}  "
            f"TX={fmt_num(latest.get('net_tx_mbps'),2,'Mbps')}"
        )
        if latest.get("psutil_available") is False:
            lines.append("SYSTEM NOTE: psutil not installed -> CPU/RAM/network fields unavailable. Run: python -m pip install psutil")
    else:
        lines.append("Waiting for monitor events...")

    lines.append("-")
    if page_count:
        zero_pct = 100.0 * len(zero_pages) / page_count
        stable_pct = 100.0 * len(stable_pages) / page_count
        lines.append(
            f"LAST {window_seconds}s pages={page_count}  zero={len(zero_pages)} ({zero_pct:.1f}%)  "
            f"stable={len(stable_pages)} ({stable_pct:.1f}%)  "
            f"avg_nav={avg('navigation_seconds'):.2f}s  avg_wait={avg('stable_wait_seconds'):.2f}s  "
            f"avg_total={avg('page_read_seconds'):.2f}s"
        )
        cls = ", ".join(f"{k}:{v}" for k, v in classifications.most_common())
        lines.append(f"CLASS  {cls}")
    else:
        lines.append(f"LAST {window_seconds}s pages=0")

    # Fast interpretation to help distinguish local saturation from all-zero page behavior.
    lines.append("-")
    findings = []
    severity = latest.get("severity")
    cpu = latest.get("cpu_percent")
    ram = latest.get("ram_percent")
    avail = latest.get("ram_available_gb")
    if severity == "zero_storm":
        findings.append("ALERT: >=80% of active workers are simultaneously waiting with 0 document anchors.")
    elif severity == "many_zero_workers":
        findings.append("ALERT: >=50% of active workers are simultaneously waiting with 0 document anchors.")
    if isinstance(cpu, (int, float)) and cpu >= 90:
        findings.append("CPU is saturated; worker count may be above the local CPU sweet spot.")
    if isinstance(ram, (int, float)) and ram >= 90:
        findings.append("RAM is saturated; Chrome memory pressure can explain zero/slow pages.")
    if isinstance(avail, (int, float)) and avail < 2:
        findings.append("Available RAM is below 2GB; memory pressure is severe.")
    if classifications.get("page_mismatch"):
        findings.append("Some pages landed on the wrong page number -> pagination/redirect issue present.")
    if classifications.get("redirected"):
        findings.append("Some requests left /search -> redirect/login/error-page path present.")
    if classifications.get("challenge_or_block_text"):
        findings.append("Page text contains a challenge/block indicator; inspect recent zero events below.")
    if classifications.get("page_not_ready"):
        findings.append("Some zero pages never reached interactive/complete readyState.")
    if severity == "zero_storm" and not any(
        isinstance(v, (int, float)) and v >= 90 for v in (cpu, ram)
    ):
        findings.append("CPU/RAM are not obviously saturated; compare 6/8/10/12 workers and inspect URL/title/body fields before blaming hardware.")
    if not findings:
        findings.append("No strong failure signature in the current window.")
    for item in findings:
        lines.append("CHECK  " + item)

    lines.append("-")
    lines.append("WORKERS")
    workers = latest.get("workers") or {}
    if workers:
        for wid in sorted(workers, key=lambda x: int(x)):
            w = workers[wid]
            lines.append(
                f"W{int(wid):02d} p{int(w.get('page') or 0):<3} "
                f"{str(w.get('status') or ''):<10} obs={int(w.get('observed_count') or 0):<3} "
                f"page={str(w.get('page_unique')):<4} found={int(w.get('keyword_total') or 0):<4} "
                f"{short(w.get('keyword'), 34)}"
            )
    else:
        lines.append("(no worker snapshot yet)")

    problems = [e for e in pages if e.get("classification") != "ok"][-recent_count:]
    lines.append("-")
    lines.append(f"RECENT PROBLEMS (last {len(problems)})")
    if not problems:
        lines.append("(none)")
    else:
        for e in problems:
            lines.append(
                f"{str(e.get('timestamp',''))[-8:]} W{int(e.get('worker_id') or 0):02d} p{e.get('page')} "
                f"{e.get('classification')} obs={e.get('observed_count')} uniq={e.get('page_unique')} "
                f"nav={fmt_num(e.get('navigation_seconds'),2,'s')} wait={fmt_num(e.get('stable_wait_seconds'),2,'s')} "
                f"ready={e.get('ready_state')} req/actual={e.get('requested_page')}/{e.get('actual_page')}"
            )
            lines.append(
                f"    title={short(e.get('page_title'), 72)}"
            )
            lines.append(
                f"    url={short(e.get('current_url'), 110)}"
            )
            if e.get("body_snippet"):
                lines.append(f"    body={short(e.get('body_snippet'), 110)}")
            lines.append(
                f"    resources={e.get('resource_count')} transfer={e.get('transfer_bytes')}B "
                f"online={e.get('navigator_online')} body_len={e.get('body_text_length')}"
            )

    started = [e for e in browser if e.get("event") == "started"]
    if started:
        vals = [float(e.get("startup_seconds") or 0) for e in started if e.get("startup_seconds") is not None]
        if vals:
            lines.append("-")
            lines.append(
                f"BROWSER STARTUP count={len(vals)} avg={sum(vals)/len(vals):.2f}s max={max(vals):.2f}s"
            )
    if errors:
        lines.append(f"WORKER ERRORS total={len(errors)} recent={short(errors[-1].get('error'),90)}")

    lines.append("=" * 86)
    lines.append("Tip: send scribd_debug_snapshot.txt or copy this whole window when 12 workers turns all-zero.")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Live viewer for Scribd v16.2 diagnostic JSONL")
    parser.add_argument("--log", default="scribd_diagnostic.jsonl")
    parser.add_argument("--snapshot", default="scribd_debug_snapshot.txt")
    parser.add_argument("--window", type=int, default=120, help="Recent page analysis window in seconds")
    parser.add_argument("--recent", type=int, default=8, help="Recent problem pages to show")
    parser.add_argument("--refresh", type=float, default=2.0)
    args = parser.parse_args()

    path = Path(args.log)
    events = deque(maxlen=6000)
    position = 0

    while True:
        try:
            if path.exists():
                size = path.stat().st_size
                if size < position:
                    position = 0
                    events.clear()
                with path.open("r", encoding="utf-8", errors="replace") as handle:
                    handle.seek(position)
                    for line in handle:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
                    position = handle.tell()

            view = build_view(list(events), max(10, args.window), max(1, args.recent))
            clear_screen()
            print(view, flush=True)
            if args.snapshot:
                Path(args.snapshot).write_text(view + "\n", encoding="utf-8")
            time.sleep(max(0.5, args.refresh))
        except KeyboardInterrupt:
            break
        except Exception as exc:
            clear_screen()
            print(f"watcher error: {type(exc).__name__}: {exc}")
            time.sleep(2)


if __name__ == "__main__":
    main()
