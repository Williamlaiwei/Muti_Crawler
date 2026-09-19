import subprocess
import sys
from pathlib import Path


URL_FILE = "slideshare_urls.txt"

DOWNLOADER = "main.py"

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
        encoding="utf-8",
    ) as f:

        urls = [
            line.strip()
            for line in f
            if line.strip()
        ]

    # 去重但保持順序
    return list(
        dict.fromkeys(urls)
    )


def main():

    urls = load_urls(
        URL_FILE
    )

    print("")
    print(
        "=============================="
    )
    print(
        "SlideShare Batch Downloader"
    )
    print(
        "=============================="
    )

    print(
        f"URLs loaded : {len(urls)}"
    )

    print(
        "Output      : output"
    )

    print(
        "=============================="
    )

    success_count = 0

    failed_urls = []

    for index, url in enumerate(
        urls,
        1,
    ):

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
                    url,
                ],
                input="all\n",
                text=True,
                stdout=None,
                stderr=None,
            )

            if result.returncode == 0:

                success_count += 1

            else:

                failed_urls.append(
                    url
                )

        except Exception as e:

            print(
                f"[ERROR] "
                f"{type(e).__name__}: "
                f"{e}"
            )

            failed_urls.append(
                url
            )

    if failed_urls:

        with open(
            FAILED_FILE,
            "w",
            encoding="utf-8",
        ) as f:

            for url in failed_urls:
                f.write(
                    url + "\n"
                )

    print("")
    print(
        "========== DONE =========="
    )

    print(
        f"Total   : {len(urls)}"
    )

    print(
        f"Success : {success_count}"
    )

    print(
        f"Failed  : "
        f"{len(failed_urls)}"
    )

    print(
        "Output  : output"
    )

    if failed_urls:

        print(
            f"Failed URLs: "
            f"{FAILED_FILE}"
        )

    print(
        "=========================="
    )


if __name__ == "__main__":
    main()
    