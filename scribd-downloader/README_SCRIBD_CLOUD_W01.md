
# Scribd Cloud Worker W01 — Shared 4 GB

This bundle keeps the current search/master pipeline untouched. The Windows master searches and owns SQLite; the Akamai Ubuntu VM only receives preselected tasks and downloads PDFs.

## Architecture

Windows master:

`scribd_state.db -> export_cloud_tasks.py -> tasks_W01.jsonl`

Akamai W01:

`tasks_W01.jsonl -> scribd_cloud_worker.py -> PDFs + results_W01.jsonl`

Windows master (after verifying/copying/uploading PDFs):

`results_W01.jsonl -> import_cloud_results.py -> scribd_state.db`

`failed` results stay failed and are not automatically retried.

## 1. Export a 100-document benchmark batch on Windows

From `C:\Users\wagor\Desktop\RMB\scribd-downloader`:

```powershell
python .\export_cloud_tasks.py `
  --db ".\scribd_state.db" `
  --count 100 `
  --device "RMB-SCRIBD-W01" `
  --output ".\tasks_W01_100.jsonl"
```

The exporter changes only those records from `queued` to `assigned`, so the local downloader will not claim them.

List reserved batches:

```powershell
python .\export_cloud_tasks.py --db ".\scribd_state.db" --list-assigned
```

If a test is abandoned, copy the Batch ID printed by the exporter and release it:

```powershell
python .\export_cloud_tasks.py `
  --db ".\scribd_state.db" `
  --release-batch "PASTE-BATCH-ID-HERE"
```

## 2. Copy files to Akamai W01

Run from Windows PowerShell (replace the IP if it changes):

```powershell
scp .\tasks_W01_100.jsonl root@172.105.162.61:/opt/rmb/tasks/
scp .\scribd_cloud_worker.py root@172.105.162.61:/opt/rmb/
scp .\scribd_downloader_cloud_v16.py root@172.105.162.61:/opt/rmb/
```

## 3. Run 1-worker benchmark on W01

SSH into W01:

```bash
cd /opt/rmb
source .venv/bin/activate

python scribd_cloud_worker.py \
  --tasks /opt/rmb/tasks/tasks_W01_100.jsonl \
  --results /opt/rmb/results/results_W01_w1.jsonl \
  --output-dir /opt/rmb/output/w1 \
  --tmp-dir /opt/rmb/tmp \
  --device-id RMB-SCRIBD-W01 \
  --workers 1
```

The summary prints successful PDFs/hour, success rate, CPU and RAM usage.

## 4. Test 2 workers fairly

For a clean 2-worker comparison, export a *different* 100-document batch from the master (recommended). Do not benchmark 2 workers on files already downloaded by the 1-worker run, because existing-file skips would invalidate the timing.

Export on Windows:

```powershell
python .\export_cloud_tasks.py `
  --db ".\scribd_state.db" `
  --count 100 `
  --device "RMB-SCRIBD-W01" `
  --output ".\tasks_W01_100_w2.jsonl"
```

Copy it:

```powershell
scp .\tasks_W01_100_w2.jsonl root@172.105.162.61:/opt/rmb/tasks/
```

Run on W01:

```bash
cd /opt/rmb
source .venv/bin/activate

python scribd_cloud_worker.py \
  --tasks /opt/rmb/tasks/tasks_W01_100_w2.jsonl \
  --results /opt/rmb/results/results_W01_w2.jsonl \
  --output-dir /opt/rmb/output/w2 \
  --tmp-dir /opt/rmb/tmp \
  --device-id RMB-SCRIBD-W01 \
  --workers 2
```

## 5. Copy results back to Windows

```powershell
scp root@172.105.162.61:/opt/rmb/results/results_W01_w1.jsonl .\
scp root@172.105.162.61:/opt/rmb/results/results_W01_w2.jsonl .\
```

For the benchmark, **do not import yet unless the PDFs have been copied back or uploaded to OSS**. Importing marks those records downloaded on the master.

When storage is in place, first dry-run:

```powershell
python .\import_cloud_results.py `
  --db ".\scribd_state.db" `
  --results ".\results_W01_w1.jsonl" `
  --dry-run
```

Then commit:

```powershell
python .\import_cloud_results.py `
  --db ".\scribd_state.db" `
  --results ".\results_W01_w1.jsonl"
```

## Important

- W01 performs **no Scribd search**.
- The existing search crawler remains unchanged.
- Master SQLite remains the source of truth.
- `assigned` tasks are excluded from the local downloader because it selects only `status='queued'`.
- Cloud failures import as `failed`, matching the current no-auto-retry policy.
- The 4 GB W01 starts at 1 worker; benchmark 2 workers before scaling further.
- PDFs currently stay on W01. The next production step is OSS upload + delete local PDF after successful upload.
