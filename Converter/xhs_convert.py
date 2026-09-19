import argparse
import hashlib
import json
import uuid
from pathlib import Path


SITE_NAME = "兴趣内容社区"
PLATFORM_NAME = "小红书"


def load_jsonl(path):
    """讀取 JSONL，壞掉的行直接略過。"""
    rows = []

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()

            if not line:
                continue

            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"[WARN] Skip invalid JSON line {line_no}: {e}")

    return rows


def split_urls(value):
    """把 MediaCrawler 的逗號字串轉成 array。"""
    if not value:
        return []

    if isinstance(value, list):
        return [x for x in value if x]

    return [
        x.strip()
        for x in str(value).split(",")
        if x.strip()
    ]


def parse_count(value):
    """
    小紅書：
    10万+ -> 100000
    3.9万 -> 39000
    7894 -> 7894
    """
    if value is None or value == "":
        return 0

    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip().replace(",", "").replace("+", "")

    try:
        if text.endswith("万") or text.endswith("萬"):
            return int(float(text[:-1]) * 10000)

        if text.lower().endswith("w"):
            return int(float(text[:-1]) * 10000)

        if text.lower().endswith("k"):
            return int(float(text[:-1]) * 1000)

        return int(float(text))

    except (ValueError, TypeError):
        return 0


def to_seconds(value):
    """
    MediaCrawler 現在時間多半是毫秒。
    1742475087000 -> 1742475087
    """
    if not value:
        return 0

    try:
        value = int(value)
    except (ValueError, TypeError):
        return 0

    if value > 10_000_000_000:
        return value // 1000

    return value


def make_uuid(comment_id):
    """
    產生穩定 UUID。
    同一個 comment 每次轉換都會得到同一個 uuid。
    """
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"xiaohongshu.com|comment|{comment_id}"
        )
    )


def make_raw_id(comment_id):
    """產生 32 字元穩定 raw_id。"""
    return hashlib.md5(
        f"xiaohongshu.com|comment|{comment_id}".encode("utf-8")
    ).hexdigest()


def make_user(user_id, nickname, avatar="", red_id="", ip_location=""):
    user_id = user_id or ""
    nickname = nickname or ""
    avatar = avatar or ""
    red_id = red_id or ""
    ip_location = ip_location or ""

    return {
        "id": f"xiaohongshu.com|{user_id}" if user_id else "",
        "uid": user_id,
        "name": nickname,
        "url": (
            f"https://www.xiaohongshu.com/user/profile/{user_id}"
            if user_id
            else ""
        ),
        "profile_img_url": avatar,
        "red_id": red_id,
        "ip_region": [ip_location] if ip_location else [],
    }


def make_retweeted(note):
    note_id = note.get("note_id", "")

    pic_urls = split_urls(note.get("image_list"))
    video_url = note.get("video_url", "")

    return {
        "uuid": f"xiaohongshu.com|post|{note_id}",
        "mid": note_id,
        "url": note.get("note_url", ""),
        "title": note.get("title", ""),
        "content": note.get("desc", ""),

        "pic_urls": pic_urls,
        "surface_img": pic_urls[0] if pic_urls else "",

        "video_urls": [video_url] if video_url else [],

        # 現階段 raw data 沒有影片長度
        "duration": 0,

        "reply_count": parse_count(note.get("comment_count")),
        "like_count": parse_count(note.get("liked_count")),

        "ctime": to_seconds(note.get("time")),

        # 第二層分析再補
        "ocr": "",
        "istar_asr": "",
        "ocr_url_list": [],

        "user": make_user(
            user_id=note.get("creator_hash"),
            nickname=note.get("nickname"),
            avatar=note.get("avatar"),
            red_id=note.get("red_id"),
            ip_location=note.get("ip_location"),
        ),

        "media_matrix": {
            "mediaLevel": "",
            "platformName": PLATFORM_NAME,
        },
    }


def convert_comment(comment, note):
    comment_id = comment.get("comment_id", "")
    note_id = comment.get("note_id", "")

    parent_comment_id = comment.get("parent_comment_id") or ""

    # 一級留言 -> parent 是 post
    if not parent_comment_id:
        parent_mid = note_id
        parent_type = "post"

    # 二級 / 回覆留言 -> parent 是 comment
    else:
        parent_mid = parent_comment_id
        parent_type = "comment"

    root_url = note.get("note_url", "")

    if root_url:
        separator = "&" if "?" in root_url else "?"
        comment_url = (
            f"{root_url}"
            f"{separator}comment_id={comment_id}"
        )
    else:
        comment_url = ""

    token = note.get("xsec_token", "") or ""

    return {
        "uuid": make_uuid(comment_id),
        "raw_id": make_raw_id(comment_id),

        "mid": comment_id,
        "type": "comment",
        "site_name": SITE_NAME,

        "url": comment_url,

        "content": comment.get("content", ""),

        "pic_urls": split_urls(comment.get("pictures")),

        "reply_count": parse_count(
            comment.get("sub_comment_count")
        ),

        "like_count": parse_count(
            comment.get("like_count")
        ),

        "ctime": to_seconds(
            comment.get("create_time")
        ),

        "utime": to_seconds(
            comment.get("last_modify_ts")
        ),

        # root 永遠都是原貼文
        "root_mid": note_id,
        "root_type": "post",
        "root_url": root_url,

        "community_token": token,
        "xsec_token": token,

        # parent 可能是 post，也可能是 comment
        "parent_mid": parent_mid,
        "parent_type": parent_type,

        "user": make_user(
            user_id=comment.get("creator_hash"),
            nickname=comment.get("nickname"),
            avatar=comment.get("avatar"),
            red_id=comment.get("red_id"),
            ip_location=comment.get("ip_location"),
        ),

        # 目前先保留目標格式
        "analysis": {
            "sentiment": 0,
            "is_original": 0,
            "noise": 0,
            "hashcode": {
                "5": ""
            }
        },

        # 把原貼文塞進 comment 裡
        "retweeted": make_retweeted(note),
    }


def main(notes_path, comments_path, output_path):
    print(f"[INFO] Reading notes: {notes_path}")
    notes_raw = load_jsonl(notes_path)

    print(f"[INFO] Reading comments: {comments_path}")
    comments_raw = load_jsonl(comments_path)

    # -----------------------------
    # Note 去重
    # 同一天重跑 MediaCrawler 會 append，
    # 所以相同 note_id 保留最後一筆
    # -----------------------------
    notes = {}

    for note in notes_raw:
        note_id = note.get("note_id")

        if note_id:
            notes[note_id] = note

    # -----------------------------
    # Comment 去重
    # 相同 comment_id 保留最後一筆
    # -----------------------------
    comments = {}

    for comment in comments_raw:
        comment_id = comment.get("comment_id")

        if comment_id:
            comments[comment_id] = comment

    print(f"[INFO] Unique notes: {len(notes)}")
    print(f"[INFO] Unique comments: {len(comments)}")

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    converted = 0
    missing_note = 0

    with open(output, "w", encoding="utf-8") as f:

        for comment in comments.values():

            note_id = comment.get("note_id")

            note = notes.get(note_id)

            # comment 找不到對應貼文
            if note is None:
                missing_note += 1
                continue

            result = convert_comment(
                comment=comment,
                note=note
            )

            f.write(
                json.dumps(
                    result,
                    ensure_ascii=False,
                    separators=(",", ":")
                )
                + "\n"
            )

            converted += 1

    print("")
    print("========== DONE ==========")
    print(f"Converted comments : {converted}")
    print(f"Missing note       : {missing_note}")
    print(f"Output             : {output}")
    print("==========================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--notes",
        required=True,
        help="MediaCrawler search_contents JSONL"
    )

    parser.add_argument(
        "--comments",
        required=True,
        help="MediaCrawler search_comments JSONL"
    )

    parser.add_argument(
        "--output",
        default="output/xhs_converted.jsonl",
        help="Output JSONL"
    )

    args = parser.parse_args()

    main(
        notes_path=args.notes,
        comments_path=args.comments,
        output_path=args.output,
    )