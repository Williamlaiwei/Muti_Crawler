# Academia Cookie 設定與測試

這份說明用來讓 `academia-preserver` 帶入已登入 Academia.edu 的瀏覽器 Cookie，避免 PDF 下載請求被重新導向到 `/signup`。

## 1. 在 Chrome 複製 cURL

先登入 Academia.edu。

接著：

1. 按 `F12`
2. 開啟 `Network`
3. 重新整理 Academia.edu 頁面
4. 點任一個 `academia.edu` request
5. 右鍵該 request
6. 選：

```text
Copy
→ Copy as cURL (cmd)
```

> 不要把 Cookie 或完整 cURL 貼到公開位置、GitHub 或聊天中。

---

## 2. 在 PowerShell 讀取剪貼簿

切回 PowerShell，確認目前位於：

```powershell
C:\Users\wagor\Desktop\RMB\academia-preserver
```

執行：

```powershell
$curl = Get-Clipboard -Raw
```

---

## 3. 從 cURL 自動抽出 Cookie

執行：

```powershell
$match = [regex]::Match(
    $curl,
    '(?ms)-b\s+\^"(?<cookie>.*?)\^"\s+\^'
)

$cookie = $match.Groups["cookie"].Value

$cookie = $cookie -replace '\^', ''

$env:ACADEMIA_COOKIE = $cookie

Write-Host "Academia cookie loaded:" $env:ACADEMIA_COOKIE.Length "characters"
```

正常應該會看到類似：

```text
Academia cookie loaded: 1500 characters
```

如果顯示：

```text
Academia cookie loaded: 0 characters
```

代表 Cookie 沒有成功從剪貼簿中的 cURL 抽出。

---

## 4. 確認 Cookie 已載入

執行：

```powershell
if ($env:ACADEMIA_COOKIE.Length -gt 100) {
    "Academia cookie loaded"
} else {
    "Cookie extraction failed"
}
```

成功時應看到：

```text
Academia cookie loaded
```

---

## 5. 執行 Academia crawler

Cookie 載入後，要在**同一個 PowerShell 視窗**執行 crawler：

```powershell
Remove-Item .\download_state.json -ErrorAction SilentlyContinue
uv run python .\main.py
```

建議第一次測試只抓 1～2 篇。

---

## 6. 正常下載時應看到

成功時：

```text
=== DOWNLOAD DEBUG ===
URL: https://www.academia.edu/attachments/.../download_file
STATUS: 200
FINAL URL: ...
CONTENT-TYPE: application/pdf
[OK] PDF downloaded: XXXXX bytes
```

如果看到：

```text
FINAL URL: https://www.academia.edu/signup?a_id=...
CONTENT-TYPE: text/html
```

代表 Academia 沒有接受目前的登入 session，下載請求仍然被導向註冊頁。

---

## 7. Python 端 Cookie 設定

`AcademiaDownloader.__init__()` 需要有：

```python
academia_cookie = os.getenv(
    "ACADEMIA_COOKIE",
    ""
).strip()

if academia_cookie:
    self.headers["Cookie"] = academia_cookie
```

這樣 `main.py` 執行時會自動使用 PowerShell 裡的 `ACADEMIA_COOKIE` 環境變數。

---

## 8. 目前已修正的 Academia 相容性問題

### 舊下載欄位

舊 repo 使用：

```python
attachment.get("bulkDownloadUrl")
```

目前 Academia 搜尋結果實際提供：

```python
download_url = (
    attachment.get("pdfUrl")
    or attachment.get("downloadUrl")
    or attachment.get("bulkDownloadUrl")
)
```

### 只下載 PDF

```python
file_type = (attachment.get("fileType") or "").lower()

if file_type != "pdf":
    continue
```

### 避免把登入頁 HTML 當成 PDF

下載時需要確認：

- HTTP status 是 `200`
- Final URL 不是 `/signup` 或 `/login`
- `Content-Type` 是 PDF，或檔案開頭是 `%PDF-`

如果不是 PDF，應該 `return False`，不能把 HTML 寫成 `.pdf`。

### 下載失敗不計入成功數量

```python
success = await downloader.download_paper_async(...)

if not success:
    continue

state.downloaded_papers.add(paper_id)
state.total_downloaded += 1
```

---

## 注意事項

- `ACADEMIA_COOKIE` 只在目前 PowerShell 視窗有效。
- 關閉 PowerShell 後，需要重新設定 Cookie。
- Cookie 等同登入 session，不要 commit 到 GitHub。
- 不要把 Cookie 寫死在 `main.py`。
- 如果 Cookie 已經失效，重新登入 Academia.edu 並重新執行 `Copy as cURL (cmd)`。
