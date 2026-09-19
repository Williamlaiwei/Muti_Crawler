# RMB Scribd Crawler — Local Windows README

> Current local version: **Pipeline v14 + Search v14 + Downloader v14.1**
>
> This README documents the current **single-machine Windows workflow** for the Scribd crawler/download pipeline.

---

## 1. What this project does

The local Scribd pipeline currently supports:

1. Generate/search Scribd keywords.
2. Search multiple keywords concurrently with Selenium/Chrome.
3. Deduplicate documents globally by `document_id`.
4. Store search/download state in SQLite.
5. Build a persistent download queue.
6. Download queued Scribd documents concurrently.
7. Export PDFs as `documentID_title.pdf`.
8. Retry failed downloads up to a configured attempt limit.
9. Recover downloads left in `downloading` state after interruption.
10. Show live search and download dashboards.
11. Show search/download/total elapsed time.
12. Stop cleanly with `Ctrl+C`.

Current architecture:

```text
Scribd Search
    ↓
scribd_state.db
    ↓
Download Queue
    ↓
Selenium / Chrome Workers
    ↓
PDF
    ↓
E:\RMB\scribd
```

The current local version uses **SQLite** and is intended for **one active machine at a time**.

---

# 2. Required files

Inside:

```text
C:\Users\wagor\Desktop\RMB\scribd-downloader\
```

the important files should be:

```text
scribd_pipeline.py
scribd_search_pipeline.py
scribd_downloader.py
scribd_state.db
```

Optional/generated files:

```text
scribd_download_queue.txt
scribd_documents.csv
requirements.txt
```

The version currently expected is:

```text
scribd_pipeline.py          → v14
scribd_search_pipeline.py   → v14
scribd_downloader.py        → v14.1 fixed downloader
```

The downloader fix in v14.1 corrects the previous `prepare_document_for_print` naming bug.

---

# 3. Local requirements

Recommended environment:

```text
Windows 10 / Windows 11
Python 3.10+
Google Chrome
64-bit Python
```

Current machine usage has been tested with:

```text
Search workers:   4–6
Download workers: 8
RAM:              ~64 GB
```

The program uses Selenium and launches one Chrome instance per active worker.

---

# 4. Open PowerShell in the project

```powershell
cd C:\Users\wagor\Desktop\RMB\scribd-downloader
```

If the wrong virtual environment is active, for example:

```text
(academia-preserver)
```

first run:

```powershell
deactivate
```

Then activate the Scribd environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

Confirm the Python executable:

```powershell
python -c "import sys; print(sys.executable)"
```

Expected path:

```text
C:\Users\wagor\Desktop\RMB\scribd-downloader\.venv\Scripts\python.exe
```

---

# 5. First-time Python environment setup

If `.venv` does not exist:

```powershell
cd C:\Users\wagor\Desktop\RMB\scribd-downloader

python -m venv .venv

.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip

pip install selenium pypdf
```

If the repository contains a `requirements.txt`:

```powershell
pip install -r requirements.txt
```

If using `uv` instead:

```powershell
uv pip install selenium pypdf
```

Important: make sure the correct Scribd `.venv` is active before using `uv pip`.

---

# 6. Check Chrome

Check that Chrome is installed:

```powershell
Get-Command chrome -ErrorAction SilentlyContinue
```

If Chrome is not on PATH, this does not necessarily mean it is missing. Normal Google Chrome installation is usually sufficient for Selenium Manager to locate it.

You can also open:

```text
chrome://version
```

inside Chrome to check the installed version.

---

# 7. Quick health check

Check that all three Python files import/compile:

```powershell
python -m py_compile `
  .\scribd_pipeline.py `
  .\scribd_search_pipeline.py `
  .\scribd_downloader.py
```

No output means syntax compilation succeeded.

Check the main CLI:

```powershell
python .\scribd_pipeline.py --help
```

---

# 8. Recommended normal local run

Current practical local configuration:

```powershell
python .\scribd_pipeline.py `
  --collect 1300 `
  --pages-per-keyword 20 `
  --search-workers 4 `
  --download-batch 1500 `
  --workers 8 `
  --result-wait 30 `
  --stable-checks 4 `
  --stable-interval 2 `
  --page-delay 3
```

Typical interpretation:

```text
Search:
    target threshold = 1300 new unique documents
    search workers   = 4
    max pages        = 20 per keyword

Download:
    batch size       = up to 1500
    download workers = 8
```

---

# 9. Important meaning of `--collect`

Example:

```text
--collect 300
```

does **NOT** mean:

```text
stop immediately at exactly 300 documents
```

In v14 it means:

```text
Once live unique documents >= 300:
    stop assigning NEW keywords
    but allow all currently-running keywords to finish
```

Example:

```text
Target = 300

W01 is already searching keyword A
W02 is already searching keyword B
W03 is already searching keyword C
W04 is already searching keyword D

TOTAL reaches 300

→ no keyword E/F/G/H will be assigned
→ A/B/C/D continue until their current keyword is complete
```

Therefore the final count may be:

```text
300 target
547 actual
800 actual
1200 actual
```

This is intentional.

---

# 10. Search page modes

## Fixed page limit

Example:

```text
--pages-per-keyword 20
```

means:

```text
Each keyword can scan at most 20 result pages.
```

If the keyword has 50 pages, pages 21–50 are not scanned.

---

## Automatic/unlimited mode

Use:

```powershell
--pages-per-keyword 0
```

This means:

```text
Keep paging until the keyword appears exhausted.
```

The current exhaustion rule is based on consecutive pages that add no new document IDs.

Example:

```powershell
python .\scribd_pipeline.py `
  --collect 1300 `
  --pages-per-keyword 0 `
  --search-workers 4 `
  --download-batch 1500 `
  --workers 8 `
  --result-wait 30 `
  --stable-checks 4 `
  --stable-interval 2 `
  --page-delay 3
```

Dashboard pages will appear as:

```text
6/∞
7/∞
8/∞
```

rather than:

```text
6/20
7/20
8/20
```

---

# 11. Search-page loading logic

v14 intentionally avoids repeatedly refreshing a slow Scribd result page.

Current logic:

```text
Open result page once
    ↓
wait
    ↓
check result count repeatedly
    ↓
result count becomes stable
    ↓
scan DOM once
    ↓
go to next page
```

Example:

```text
0
0
12
28
40
40
40
40
```

With:

```text
--stable-checks 4
--stable-interval 2
```

the page is considered ready after the positive result count remains unchanged for 4 checks.

---

# 12. `--result-wait`

Example:

```text
--result-wait 30
```

means:

```text
Maximum time allowed on the SAME result page = 30 seconds
```

It does **not** mean every page waits for 30 seconds.

If results stabilize earlier, the crawler proceeds earlier.

Recommended local starting values:

```text
--result-wait 30
--stable-checks 4
--stable-interval 2
```

If a page stays at zero results, it is allowed to wait until the timeout rather than immediately refreshing.

---

# 13. Search dashboard

Example:

```text
TOTAL █████████░░░░ 547/1300 live | confirmed 307 | queries 6
W01 ... 5/20 | history research report | waiting | found 160 | page 40
```

Meaning:

### `547/1300 live`

The crawler has observed:

```text
547 new globally unique document IDs
```

during this run.

Target:

```text
1300
```

---

### `confirmed 307`

Documents that have already been committed to SQLite after completed keyword runs.

`live` can be larger than `confirmed` because active keywords may have already found documents but have not finished yet.

---

### `queries 6`

Six keywords have completed and had their results saved.

---

### `found 160`

This worker's **current keyword** has accumulated 160 unique documents.

This does not mean 160 keywords.

---

### `page 40`

The most recently scanned result page contained 40 unique document results.

---

### `waiting`

The worker is waiting for the current page's Scribd results to stabilize.

This is not necessarily an error or a frozen worker.

---

# 14. Search only

To search and build the queue without downloading:

```powershell
python .\scribd_pipeline.py `
  --collect 1300 `
  --pages-per-keyword 20 `
  --search-workers 4 `
  --download-batch 0 `
  --result-wait 30 `
  --stable-checks 4 `
  --stable-interval 2 `
  --page-delay 3
```

---

# 15. Download only

To skip search and only process the existing queue:

```powershell
python .\scribd_pipeline.py `
  --collect 0 `
  --download-batch 1500 `
  --workers 8
```

Typical startup:

```text
[INFO] Search phase skipped because --collect 0.

========== DOWNLOAD BATCH ==========
Queued selected : 1500
Workers         : 8
...
```

---

# 16. Download dashboard

Example:

```text
DOWNLOAD ███████████████░░ 199/200 | ok 187 | fail 12 | skip 0

W01 ██████████████████ 2/2 | 408460860 | Example document | done
W02 ░░░░░░░░░░░░░░░░░░ -/- | 410358397 | Example document | loading
```

Meaning:

```text
199/200  = completed download jobs in current batch
ok 187   = successfully downloaded in this batch
fail 12  = failed in this batch
skip 0   = skipped because retry limit was reached
```

Worker page status:

```text
2/2
```

means two Scribd document pages were detected/exported.

```text
-/-
```

usually means the document is still loading or the printable page count has not yet been detected.

---

# 17. PDF filename format

Current expected format:

```text
documentID_title.pdf
```

Example:

```text
495333098_Civil-Liberties-and-Free-Speech-Issues.pdf
```

The pipeline passes `document_id` and title from SQLite directly to the downloader.

This avoids the old filenames:

```text
unknown_title.pdf
```

---

# 18. Default PDF output

Current Windows default:

```text
E:\RMB\scribd
```

Override it with:

```powershell
--output-dir "D:\OtherFolder\scribd"
```

Example:

```powershell
python .\scribd_pipeline.py `
  --collect 0 `
  --download-batch 500 `
  --workers 8 `
  --output-dir "D:\ScribdPDF"
```

---

# 19. SQLite database

Default database:

```text
scribd_state.db
```

Main tables:

```text
queries
documents
downloads
```

---

## `queries`

Stores searched keywords.

Important fields:

```text
keyword
searched_at
status
result_count
new_document_count
error
```

A keyword is reserved before searching so local concurrent search workers do not intentionally run the same keyword.

---

## `documents`

Stores globally deduplicated documents.

Important fields:

```text
document_id   PRIMARY KEY
title
url
found_by_keyword
first_seen
```

`document_id` is the global deduplication key.

---

## `downloads`

Stores persistent download state.

Important fields:

```text
document_id
status
file_path
attempts
last_error
updated_at
```

Common statuses:

```text
queued
downloading
downloaded
failed
```

---

# 20. Failed download behavior

The download selector includes both:

```text
queued
failed
```

Therefore a failed document is not automatically lost.

Conceptually:

```text
queued
↓
downloading
↓
success → downloaded

or

failure → failed
          ↓
          retry later if attempts < max attempts
```

Default:

```text
--max-attempts 3
```

After reaching the attempt limit, the document will no longer be actively retried in a normal batch.

---

# 21. Recovering from interruption

If the process crashes or is interrupted while a document has:

```text
status = downloading
```

the pipeline includes stale-download recovery.

At the start of the next download phase:

```text
downloading → queued
```

for stale/incomplete jobs.

This prevents a killed job from being permanently stuck.

---

# 22. Ctrl+C

## During search

Press:

```text
Ctrl+C
```

The pipeline attempts to:

```text
stop search workers
close active Chrome/ChromeDriver sessions
save/exit cleanly
show summary
```

---

## During download

Press:

```text
Ctrl+C
```

The download pipeline attempts to:

```text
stop taking new jobs
terminate active ChromeDriver processes
return interrupted downloads to queued
leave untouched queued jobs in queue
exit without waiting forever
```

The final status should report:

```text
Status : interrupted by user
```

---

# 23. Final timing summary

At the end of a run:

```text
========== FINAL SUMMARY ==========
DB               : ...
PDF output       : E:\RMB\scribd
Downloads        : {...}
Search elapsed   : 00:11:51
Download elapsed : 00:05:20
Total elapsed    : 00:17:11
Status           : completed
===================================
```

If search is skipped:

```text
Search elapsed : 00:00:00
```

If download is skipped:

```text
Download elapsed : 00:00:00
```

---

# 24. Current recommended local worker settings

Current practical starting point:

```text
Search workers   : 4
Download workers : 8
```

A more aggressive search test:

```text
Search workers : 6
```

Download worker scaling can be benchmarked:

```text
8
12
16
24
```

Do not assume throughput scales perfectly linearly.

For each test, compare:

```text
successful PDFs/hour
failed %
CPU %
RAM %
Chrome stability
disk utilization
network utilization
```

---

# 25. Current local benchmark example

One observed batch:

```text
Downloaded this batch : 187
Failed this batch     : 13
Workers used          : 8
Download elapsed      : 00:05:20
```

This corresponds roughly to:

```text
187 successful PDFs / 320 seconds
≈ 2100 successful PDFs/hour
```

This is a short-run benchmark only.

Long-duration production throughput may differ due to:

```text
slow documents
timeouts
search time
network variability
retry traffic
service-side throttling
Chrome instability
disk throughput
```

Use a 12–24 hour benchmark before making production capacity commitments.

---

# 26. Recommended benchmark command

Example:

```powershell
python .\scribd_pipeline.py `
  --collect 1300 `
  --pages-per-keyword 20 `
  --search-workers 4 `
  --download-batch 1500 `
  --workers 8 `
  --result-wait 30 `
  --stable-checks 4 `
  --stable-interval 2 `
  --page-delay 3
```

For download-only throughput testing:

```powershell
python .\scribd_pipeline.py `
  --collect 0 `
  --download-batch 1500 `
  --workers 8
```

---

# 27. Test a single keyword

Use:

```powershell
python .\scribd_pipeline.py `
  --collect 100 `
  --keyword "mechanical engineering report" `
  --pages-per-keyword 5 `
  --search-workers 1 `
  --download-batch 0 `
  --result-wait 30 `
  --stable-checks 4 `
  --stable-interval 2 `
  --page-delay 3
```

Single-keyword test mode should use one search worker.

---

# 28. Inspect database counts

Basic status:

```powershell
python -c "import sqlite3; c=sqlite3.connect('scribd_state.db'); print(c.execute('SELECT status, COUNT(*) FROM downloads GROUP BY status').fetchall()); c.close()"
```

Example output:

```text
[
  ('downloaded', 1047),
  ('failed', 117),
  ('queued', 1057)
]
```

---

# 29. Inspect recent failures

```powershell
python -c "import sqlite3; c=sqlite3.connect('scribd_state.db'); rows=c.execute(\"SELECT d.document_id,d.title,q.attempts,q.last_error FROM downloads q JOIN documents d ON d.document_id=q.document_id WHERE q.status='failed' ORDER BY q.updated_at DESC LIMIT 20\").fetchall(); [print('\nID:',r[0],'\nTitle:',r[1],'\nAttempts:',r[2],'\nError:',r[3]) for r in rows]; c.close()"
```

Use this before assuming all failures have the same cause.

---

# 30. Reset only failed/running search queries

If testing leaves search queries stuck in error/running state:

```powershell
python -c "import sqlite3; c=sqlite3.connect('scribd_state.db'); c.execute(\"DELETE FROM queries WHERE status IN ('error','running')\"); c.commit(); c.close(); print('cleared failed/running queries')"
```

This does not delete successful documents.

---

# 31. Back up the local database

Before major code changes:

```powershell
Copy-Item `
  .\scribd_state.db `
  ".\scribd_state_backup_$(Get-Date -Format yyyyMMdd_HHmmss).db"
```

The SQLite database is currently the most important persistent state file.

---

# 32. Moving the DB to another machine / Colab

You can copy:

```text
scribd_state.db
```

to another machine and continue from the same state **only if the original machine is stopped**.

Safe:

```text
Local stops
↓
copy latest DB to Colab
↓
Colab runs
↓
Colab stops
↓
copy latest DB back
↓
Local continues
```

Not safe:

```text
Local runs its SQLite copy
+
Colab runs another SQLite copy
at the same time
```

The copies will diverge and may claim/download the same documents.

---

# 33. IMPORTANT — SQLite is not the future multi-machine DB

Current local mode:

```text
Windows machine
↓
scribd_state.db
↓
local search/download workers
```

This is fine for a single active machine.

It is **not** the final architecture for:

```text
Windows PC
+ Colab
+ multiple ECS/cloud servers
```

running simultaneously.

For true multi-machine operation, the planned architecture should use a central transactional database/queue such as PostgreSQL:

```text
                  PostgreSQL
                      │
          ┌───────────┼───────────┐
          ↓           ↓           ↓
       Local        Colab        ECS
       Worker       Worker       Worker
          │           │           │
          └───────────┴───────────┘
                      ↓
                     OSS
```

Each machine would claim jobs atomically from the same central queue.

Until that exists:

> **Do not run separate SQLite copies concurrently if avoiding duplicate downloads matters.**

---

# 34. Future OSS integration

Current output:

```text
Chrome
↓
PDF
↓
E:\RMB\scribd
```

Planned production output:

```text
Chrome
↓
temporary PDF
↓
OSS upload
↓
verify upload
↓
mark uploaded in central DB
↓
delete temporary local PDF
```

Suggested future fields:

```text
download_status
upload_status
oss_key
oss_size
uploaded_at
last_upload_error
```

Do not hard-code OSS access credentials into GitHub.

---

# 35. Common problems

## Wrong virtual environment

Symptom:

```text
(academia-preserver)
```

while working inside `scribd-downloader`.

Fix:

```powershell
deactivate

cd C:\Users\wagor\Desktop\RMB\scribd-downloader

.\.venv\Scripts\Activate.ps1

python -c "import sys; print(sys.executable)"
```

---

## `ModuleNotFoundError: selenium`

```powershell
pip install selenium pypdf
```

Make sure the correct venv is active.

---

## Old/incompatible searcher

Example:

```text
scribd_search_pipeline.py is an older/incompatible version
```

Make sure:

```text
scribd_pipeline.py        = v14
scribd_search_pipeline.py = v14
```

are from the same release.

---

## Every download fails immediately

First inspect:

```text
last_error
```

using the failure query above.

A batch where all jobs fail very quickly usually suggests a shared code/environment problem rather than 100% bad documents.

---

## `unknown_*.pdf`

The fixed downloader should receive the document ID from SQLite.

Expected:

```text
495333098_Title.pdf
```

If new downloads still generate:

```text
unknown_Title.pdf
```

verify that the active file is the current `scribd_downloader.py`.

---

## Search result page seems stuck on `waiting`

This can be normal.

v14 deliberately waits for result count stability before scanning.

If using:

```text
--result-wait 30
--stable-checks 4
--stable-interval 2
```

a slow page may remain in `waiting` for several seconds.

---

# 36. Parameter reference

| Parameter | Meaning | Typical local value |
|---|---|---:|
| `--collect` | New-document threshold after which no new keyword is assigned | `1300` |
| `--pages-per-keyword` | Max pages per keyword; `0` = automatic/unlimited | `20` |
| `--max-per-query` | Optional max unique docs per keyword | omitted |
| `--search-workers` | Concurrent search Chrome workers | `4` |
| `--download-batch` | Max queued/failed records selected for download phase | `1500` |
| `--workers` | Concurrent download workers | `8` |
| `--result-wait` | Max seconds waiting on the same result page | `30` |
| `--stable-checks` | Number of unchanged positive-count observations required | `4` |
| `--stable-interval` | Seconds between stability observations | `2` |
| `--page-delay` | Delay between result pages | `3` |
| `--pause` | Delay between completed keywords | `2` |
| `--max-attempts` | Max failed attempts per document | `3` |
| `--db` | SQLite path | `scribd_state.db` |
| `--output-dir` | PDF output directory | `E:\RMB\scribd` |
| `--queue-output` | Exported queue text file | `scribd_download_queue.txt` |
| `--metadata-output` | Metadata CSV | `scribd_documents.csv` |
| `--show-browser` | Show search Chrome instead of headless | off |
| `--keyword` | Test a specific keyword | omitted |

Legacy options accepted for command compatibility but no longer used by v14 search logic:

```text
--render-settle
--empty-retries
```

v14 does not refresh/retry the same result page based on these values.

---

# 37. Recommended daily workflow

Start PowerShell:

```powershell
cd C:\Users\wagor\Desktop\RMB\scribd-downloader
.\.venv\Scripts\Activate.ps1
```

Check git/status if needed:

```powershell
git status
```

Run:

```powershell
python .\scribd_pipeline.py `
  --collect 1300 `
  --pages-per-keyword 20 `
  --search-workers 4 `
  --download-batch 1500 `
  --workers 8 `
  --result-wait 30 `
  --stable-checks 4 `
  --stable-interval 2 `
  --page-delay 3
```

At the end, record:

```text
Search elapsed
Download elapsed
successful downloads
failed downloads
queued documents
```

For capacity planning, use long-duration results rather than only a short 5-minute benchmark.

---

# 38. Current limitation summary

Current local v14/v14.1 is suitable for:

```text
single Windows machine
multiple local Chrome workers
persistent SQLite state
search + download
local PDF storage
```

Not yet implemented in this local version:

```text
central PostgreSQL queue
true simultaneous multi-machine coordination
direct OSS upload workflow
automatic cloud worker orchestration
Docker deployment
```

Those are the next architectural steps before large-scale multi-machine production use.

---

# 39. Most useful commands at a glance

## Normal run

```powershell
python .\scribd_pipeline.py `
  --collect 1300 `
  --pages-per-keyword 20 `
  --search-workers 4 `
  --download-batch 1500 `
  --workers 8 `
  --result-wait 30 `
  --stable-checks 4 `
  --stable-interval 2 `
  --page-delay 3
```

## Download only

```powershell
python .\scribd_pipeline.py `
  --collect 0 `
  --download-batch 1500 `
  --workers 8
```

## Search only

```powershell
python .\scribd_pipeline.py `
  --collect 1300 `
  --pages-per-keyword 20 `
  --search-workers 4 `
  --download-batch 0 `
  --result-wait 30 `
  --stable-checks 4 `
  --stable-interval 2 `
  --page-delay 3
```

## Automatic/unlimited pages

```powershell
python .\scribd_pipeline.py `
  --collect 1300 `
  --pages-per-keyword 0 `
  --search-workers 4 `
  --download-batch 1500 `
  --workers 8 `
  --result-wait 30 `
  --stable-checks 4 `
  --stable-interval 2 `
  --page-delay 3
```

---

**Local working directory**

```text
C:\Users\wagor\Desktop\RMB\scribd-downloader
```

**Default PDF output**

```text
E:\RMB\scribd
```

**Persistent state**

```text
scribd_state.db
```
