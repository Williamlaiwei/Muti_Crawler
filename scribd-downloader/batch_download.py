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

    # 去重並保持順序
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

                # 模擬手動把 URL 貼進：
                # Input link Scribd:
                input=url + "\n",

                text=True,

                # 直接顯示 downloader 原本的輸出
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

    # -----------------------------------------
    # FAILED URLS
    # -----------------------------------------

    if failed_urls:

        with open(
            FAILED_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            for url in failed_urls:
                f.write(url + "\n")

    # -----------------------------------------
    # SUMMARY
    # -----------------------------------------

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