# Scribd Pipeline v16

v16 keeps the v15 dashboard and delay behavior, and only optimizes the **search side**.

## Main v16 changes

- Search dashboard (`TOTAL`, `W01...`) is unchanged.
- Existing delay/stability settings are unchanged unless you change the CLI values.
- Search Chrome defaults to `eager` page loading.
- Search result images are blocked by default to reduce bandwidth.
- DOM extraction scans only Scribd document links instead of every anchor.
- Adds `--device-id` for multi-machine tracking.
- Adds `--metrics-interval-hours` and `--metrics-output` for cumulative N-hour search metrics.
- Raw per-page CSV is disabled by default; set `--page-log` only if you want it.
- Downloader behavior is intentionally unchanged from v15.
- Failed downloads remain excluded from automatic download selection (`status = 'queued'` only).

## Recommended current full command

```powershell
python .\scribd_pipeline.py `
  --device-id "SYD-PC01" `
  --metrics-interval-hours 1 `
  --metrics-output ".\scribd_metrics.csv" `
  --search-images off `
  --search-page-load-strategy eager `
  --collect 30000 `
  --pages-per-keyword 20 `
  --search-workers 16 `
  --download-batch 30000 `
  --workers 8 `
  --result-wait 30 `
  --stable-checks 2 `
  --stable-interval 5 `
  --page-delay 5 `
  --pause 4 `
  --max-attempts 2 `
  --heartbeat-interval 120 `
  --db ".\scribd_state.db" `
  --queue-output ".\scribd_download_queue.txt" `
  --metadata-output ".\scribd_documents.csv" `
  --run-history ".\scribd_run_history.csv" `
  --output-dir "E:\RMB\scribd"
```

## Search-only benchmark command

Use this first if you want to compare v15 vs v16 search speed without download time affecting the result.

```powershell
python .\scribd_pipeline.py `
  --device-id "SYD-PC01" `
  --metrics-interval-hours 1 `
  --metrics-output ".\scribd_metrics.csv" `
  --search-images off `
  --search-page-load-strategy eager `
  --collect 5000 `
  --pages-per-keyword 20 `
  --search-workers 6 `
  --download-batch 0 `
  --result-wait 30 `
  --stable-checks 2 `
  --stable-interval 5 `
  --page-delay 5 `
  --pause 4 `
  --heartbeat-interval 120 `
  --db ".\scribd_state.db" `
  --queue-output ".\scribd_download_queue.txt" `
  --metadata-output ".\scribd_documents.csv" `
  --run-history ".\scribd_run_history.csv" `
  --output-dir "E:\RMB\scribd"
```

## A/B test: turn v16 Chrome optimizations off

To make v16 behave closer to the old browser loading behavior while keeping the same codebase:

```powershell
--search-images on `
--search-page-load-strategy normal
```

Compare this against:

```powershell
--search-images off `
--search-page-load-strategy eager
```

Keep `stable-checks`, `stable-interval`, and `page-delay` identical when comparing.

## Metrics

Default cumulative metrics file:

```text
scribd_metrics.csv
```

Each interval row includes:

- `device_id`
- `run_id`
- interval start/end time
- search worker count
- page timing average / median / P95 / min / max
- pages read
- queries finished
- confirmed new documents
- pages/hour
- confirmed documents/hour
- page-load strategy
- whether images were blocked

The final partial interval is written on normal completion or Ctrl+C.

If the process is forcibly killed or the machine loses power, the unfinished interval may not be written; the DB heartbeat remains the crash-time reference.

## Raw per-page log (optional)

v16 does not write a raw per-page CSV by default. To enable it:

```powershell
--page-log ".\scribd_page_read_log.csv"
```

The live terminal dashboard is always kept and is unrelated to this option.

## File placement

Rename the downloaded v16 files to:

```text
scribd_search_pipeline_v16.py -> scribd_search_pipeline.py
scribd_pipeline_v16.py        -> scribd_pipeline.py
scribd_downloader_v16.py      -> scribd_downloader.py
```

Keep your existing `scribd_state.db`; v16 migrates the monitoring schema without deleting existing document/download data.
