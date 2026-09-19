# Scribd Search + Batch Download README

這份 README 說明目前 Scribd 的完整流程：

```text
Keyword
  ↓
scribd_search.py
  ↓
scribd_urls.txt
  ↓
batch_download.py
  ↓
scribd-downloader.py
  ↓
downloads\
```

目標是：

- 不需要手動一個一個貼 Scribd URL
- 用關鍵字自動搜尋 Scribd 文件
- 自動收集 `/document/` 或 `/doc/` URL
- URL 去重
- 批量下載
- 失敗網址另外記錄
- PDF 全部集中放到 `downloads\`

---

## 1. 專案位置

目前專案：

```text
C:\Users\wagor\Desktop\RMB\scribd-downloader
```

建議結構：

```text
scribd-downloader\
│
├─ scribd_search.py
├─ scribd-downloader.py
├─ batch_download.py
├─ scribd_urls.txt
├─ scribd_search_results.jsonl
├─ failed_urls.txt
│
├─ downloads\
│  ├─ document1.pdf
│  ├─ document2.pdf
│  └─ ...
│
├─ requirements.txt
└─ .venv\
```

---

# 2. 環境安裝

先進入專案：

```powershell
cd C:\Users\wagor\Desktop\RMB\scribd-downloader
```

建立虛擬環境：

```powershell
uv venv
```

安裝 requirements：

```powershell
uv pip install -r .\requirements.txt
```

如果 Selenium 尚未安裝：

```powershell
uv pip install selenium
```

確認：

```powershell
uv pip list
```

---

# 3. Scribd 搜尋器

檔案：

```text
scribd_search.py
```

用途：

```text
keyword
→ Scribd search
→ Selenium / Chrome
→ 自動讀取搜尋結果
→ 收集 document URLs
→ 去重
→ scribd_urls.txt
→ scribd_search_results.jsonl
```

---

## 4. 搜尋 20 筆 Scribd 文件

例如：

```powershell
uv run python .\scribd_search.py --keyword "machine learning" --max 20 --show-browser
```

參數：

```text
--keyword
```

搜尋關鍵字。

例如：

```powershell
--keyword "machine learning"
```

```text
--max
```

最多收集多少 unique document URLs。

例如：

```powershell
--max 100
```

```text
--show-browser
```

顯示 Chrome 視窗，方便第一次 debug。

不加這個參數時會使用 headless Chrome。

---

## 5. 搜尋成功範例

正常會看到：

```text
[INFO] Opening Scribd
[INFO] Searching: machine learning
[INFO] Search page: https://www.scribd.com/search?query=machine%20learning
[INFO] Scroll 1: 40/20 unique documents

========== DONE ==========
Keyword      : machine learning
Documents    : 20
URL list     : scribd_urls.txt
Metadata     : scribd_search_results.jsonl
==========================
```

例如：

```text
Scroll 1: 40/20 unique documents
```

代表目前頁面已經抓到 40 個 unique 文件，但因為指定：

```text
--max 20
```

所以最後只保留前 20 筆。

---

# 6. scribd_urls.txt

搜尋器會自動建立：

```text
scribd_urls.txt
```

格式：

```text
https://www.scribd.com/document/123456789/example-one
https://www.scribd.com/document/234567890/example-two
https://www.scribd.com/document/345678901/example-three
```

一行一個 URL。

不需要手動貼網址。

---

# 7. scribd_search_results.jsonl

搜尋器另外建立：

```text
scribd_search_results.jsonl
```

每行一筆 metadata，例如：

```json
{"keyword":"machine learning","document_id":"123456789","title":"Example title","url":"https://www.scribd.com/document/123456789/example-title"}
```

用途：

- 保留搜尋 keyword
- document_id
- title
- URL
- 後續可以做 converter 或索引

---

# 8. 單篇 Scribd Downloader

原本的：

```text
scribd-downloader.py
```

是單篇模式。

直接執行：

```powershell
uv run python .\scribd-downloader.py
```

它會問：

```text
Input link Scribd:
```

這裡只能輸入：

```text
https://www.scribd.com/document/...
```

不能輸入：

```text
scribd_urls.txt
```

否則會出現：

```text
Invalid Scribd URL
```

因為原版 downloader 一次只接受一個 Scribd URL。

---

# 9. 下載資料夾

目前設定 PDF 全部放：

```text
downloads\
```

例如：

```text
scribd-downloader\
└─ downloads\
   ├─ xxx.pdf
   ├─ yyy.pdf
   └─ zzz.pdf
```

在 `scribd-downloader.py` 中使用：

```python
output_dir = "downloads"
os.makedirs(output_dir, exist_ok=True)

pdf_filename = os.path.join(
    output_dir,
    get_filename_from_url(input_url)
)
```

---

# 10. Batch Downloader

為了不要一個一個貼 URL，另外使用：

```text
batch_download.py
```

這支程式負責：

```text
scribd_urls.txt
      ↓
讀取全部 URL
      ↓
去重
      ↓
逐個呼叫 scribd-downloader.py
      ↓
成功 → downloads\
失敗 → failed_urls.txt
```

---

# 11. batch_download.py

完整版本：

```python
import subprocess
import sys
from pathlib import Path


URL_FILE = "scribd_urls.txt"
DOWNLOADER = "scribd-downloader.py"
FAILED_FILE = "failed_urls.txt"


def load_urls(path):
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(
            f"URL file not found: {path}"
        )

    with open(
        path,
        "r",
        encoding="utf-8"
    ) as f:
        urls = [
            line.strip()
            for line in f
            if line.strip()
        ]

    return list(dict.fromkeys(urls))


def main():

    urls = load_urls(URL_FILE)

    print("")
    print("==============================")
    print("Scribd Batch Downloader")
    print("==============================")
    print(f"URLs loaded : {len(urls)}")
    print("Output      : downloads")
    print("==============================")
    print("")

    success_count = 0
    failed_urls = []

    for index, url in enumerate(urls, 1):

        print("")
        print(
            f"========== "
            f"{index}/{len(urls)} "
            f"=========="
        )

        print(url)

        try:

            result = subprocess.run(
                [
                    sys.executable,
                    DOWNLOADER,
                ],
                input=url + "\n",
                text=True,
                stdout=None,
                stderr=None,
            )

            if result.returncode == 0:
                success_count += 1
            else:
                failed_urls.append(url)

        except Exception as e:

            print(
                f"[ERROR] {type(e).__name__}: {e}"
            )

            failed_urls.append(url)

    if failed_urls:

        with open(
            FAILED_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            for url in failed_urls:
                f.write(url + "\n")

    print("")
    print("========== DONE ==========")
    print(f"Total   : {len(urls)}")
    print(f"Success : {success_count}")
    print(f"Failed  : {len(failed_urls)}")
    print("Output  : downloads")

    if failed_urls:
        print(
            f"Failed URLs: {FAILED_FILE}"
        )

    print("==========================")


if __name__ == "__main__":
    main()
```

---

# 12. 執行 Batch

搜尋完後：

```powershell
uv run python .\batch_download.py
```

程式會自動讀：

```text
scribd_urls.txt
```

不需要再次手動輸入 URL。

流程：

```text
1/20
→ scribd-downloader.py
→ downloads\xxx.pdf

2/20
→ scribd-downloader.py
→ downloads\yyy.pdf

...

20/20
```

---

# 13. 最常用的完整流程

例如要搜尋：

```text
machine learning
```

最多：

```text
100
```

第一步：

```powershell
uv run python .\scribd_search.py --keyword "machine learning" --max 100
```

第二步：

```powershell
uv run python .\batch_download.py
```

就完成：

```text
machine learning
        ↓
Scribd search
        ↓
100 URLs
        ↓
scribd_urls.txt
        ↓
Batch downloader
        ↓
downloads\
```

---

# 14. Debug 模式

如果搜尋器抓不到結果，顯示 Chrome：

```powershell
uv run python .\scribd_search.py --keyword "machine learning" --max 20 --show-browser
```

可以直接看到：

- 是否正常打開 Scribd
- 是否正常進搜尋頁
- 是否被 login / popup 擋住
- 是否開始載入搜尋結果
- 是否有 lazy loading

---

# 15. failed_urls.txt

如果 batch 裡某些文件失敗：

```text
failed_urls.txt
```

會自動保存失敗 URL：

```text
https://www.scribd.com/document/111111111/...
https://www.scribd.com/document/222222222/...
```

後續可以只針對失敗 URL 重跑。

---

# 16. 為什麼不用平行下載

目前建議：

```text
Sequential
```

也就是：

```text
URL 1
→ 完成

URL 2
→ 完成

URL 3
→ 完成
```

不要一開始就同時開大量 Chrome。

原因：

- Selenium Chrome 很吃 RAM
- Scribd 長文件可能有數百 / 數千頁
- Print to PDF 很吃 CPU / Memory
- 同時開太多 browser 比較容易 crash
- 也比較容易出現 rate limit / page load 問題

所以先使用 sequential batch 最穩。

---

# 17. Scribd 的資料取得邏輯

Scribd 跟 Academia 不同。

Academia：

```text
Search
→ attachment
→ original download URL
→ PDF
```

Scribd：

```text
Search
→ document URL
→ Selenium / Chrome
→ Scribd document rendering
→ browser print/export
→ merge
→ PDF
```

因此 Scribd 產生的 PDF 不一定是作者原始上傳檔。

它比較像：

```text
把目前網頁可以正常顯示的文件內容
→ 重新輸出成 PDF
```

---

# 18. 安全與權限

只處理目前頁面或帳號正常有權限查看的內容。

不要：

- 繞過付費牆
- 破解下載限制
- 繞過登入權限
- 嘗試取得帳號本來無權查看的文件

Batch downloader 只是把正常可處理的 URL 自動化。

---

# 19. 最終 Scribd Pipeline

```text
                    Scribd
                      │
                      ▼
               Keyword Search
                      │
                      ▼
              scribd_search.py
                      │
          ┌───────────┴────────────┐
          ▼                        ▼
 scribd_urls.txt      scribd_search_results.jsonl
          │
          ▼
   batch_download.py
          │
          ▼
 scribd-downloader.py
          │
          ▼
      downloads\
          │
      ├─ 1.pdf
      ├─ 2.pdf
      ├─ 3.pdf
      └─ ...
```

---

# 20. 最簡單使用方式

平常只需要記兩條：

```powershell
uv run python .\scribd_search.py --keyword "你的關鍵字" --max 100
```

然後：

```powershell
uv run python .\batch_download.py
```

完成。
