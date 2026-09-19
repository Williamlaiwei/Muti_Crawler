import argparse
import hashlib
import json
import uuid
from datetime import datetime
from pathlib import Path


# ============================================================
# CONFIG
# ============================================================

SITE_NAME = "社交媒体"
PLATFORM_NAME = "Twitter/X"


# ============================================================
# JSONL
# ============================================================

def detect_encoding(path):
    """
    Detect common encodings.

    PowerShell redirection may occasionally create UTF-16 files,
    so we support both UTF-8 and UTF-16.
    """

    with open(path, "rb") as f:
        prefix = f.read(4)

    if prefix.startswith(b"\xff\xfe") or prefix.startswith(b"\xfe\xff"):
        return "utf-16"

    if prefix.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    return "utf-8"


def load_jsonl(path):
    """
    Load JSONL file.
    """

    encoding = detect_encoding(path)

    rows = []

    with open(path, "r", encoding=encoding) as f:

        for line_no, line in enumerate(f, 1):

            line = line.strip()

            if not line:
                continue

            try:
                rows.append(json.loads(line))

            except json.JSONDecodeError as e:
                print(
                    f"[WARN] Invalid JSON line "
                    f"{line_no}: {e}"
                )

    return rows


# ============================================================
# BASIC HELPERS
# ============================================================

def get_id(tweet):
    """
    Tweet ID as string.
    """

    return str(
        tweet.get("id_str")
        or tweet.get("id")
        or ""
    )


def get_conversation_id(tweet):
    """
    conversationId = root tweet ID on X.
    """

    return str(
        tweet.get("conversationIdStr")
        or tweet.get("conversationId")
        or get_id(tweet)
    )


def get_parent_id(tweet):
    """
    Immediate parent tweet ID.
    """

    value = (
        tweet.get("inReplyToTweetIdStr")
        or tweet.get("inReplyToTweetId")
    )

    if value is None:
        return ""

    return str(value)


def parse_datetime(value):
    """
    Convert ISO datetime into Unix seconds.

    Example:
        2026-09-17 09:12:16+00:00
        ->
        1789636336
    """

    if not value:
        return 0

    try:

        dt = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )

        return int(dt.timestamp())

    except (ValueError, TypeError):
        return 0


def safe_int(value):
    """
    Convert counts safely to integer.
    """

    if value is None:
        return 0

    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def make_uuid(tweet_id):
    """
    Stable UUID for the same tweet.
    """

    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"x.com|tweet|{tweet_id}"
        )
    )


def make_raw_id(tweet_id):
    """
    Stable MD5 raw_id.
    """

    return hashlib.md5(
        f"x.com|tweet|{tweet_id}".encode("utf-8")
    ).hexdigest()


# ============================================================
# USER
# ============================================================

def make_user(user):
    """
    Convert twscrape user structure into target user structure.
    """

    if not user:
        user = {}

    uid = str(
        user.get("id_str")
        or user.get("id")
        or ""
    )

    username = (
        user.get("username")
        or ""
    )

    display_name = (
        user.get("displayname")
        or username
        or ""
    )

    user_url = (
        user.get("url")
        or (
            f"https://x.com/{username}"
            if username
            else ""
        )
    )

    avatar = (
        user.get("profileImageUrl")
        or ""
    )

    return {
        "id": (
            f"x.com|{uid}"
            if uid
            else ""
        ),

        "uid": uid,

        "name": display_name,

        "url": user_url,

        "profile_img_url": avatar,

        # X 的 username 是公開 account handle。
        # 對應原 schema 的公開帳號識別欄位。
        "red_id": username,

        # user.location 是使用者自己填寫的位置，
        # 並不是 IP region，因此不直接塞入 ip_region。
        "ip_region": [],
    }


# ============================================================
# MEDIA
# ============================================================

def extract_photos(tweet):
    """
    Extract photo URLs.
    """

    media = tweet.get("media") or {}

    photos = media.get("photos") or []

    result = []

    for photo in photos:

        if not isinstance(photo, dict):
            continue

        url = photo.get("url")

        if url:
            result.append(url)

    return result


def extract_video_urls(tweet):
    """
    Extract best available video URL.
    """

    media = tweet.get("media") or {}

    videos = media.get("videos") or []

    result = []

    for video in videos:

        if not isinstance(video, dict):
            continue

        # Some versions expose a direct URL.
        direct_url = (
            video.get("url")
            or video.get("playbackUrl")
        )

        if direct_url:
            result.append(direct_url)
            continue

        # Some versions expose variants.
        variants = (
            video.get("variants")
            or []
        )

        best_url = ""
        best_bitrate = -1

        for variant in variants:

            if not isinstance(variant, dict):
                continue

            url = variant.get("url")

            if not url:
                continue

            bitrate = safe_int(
                variant.get("bitrate")
            )

            if bitrate > best_bitrate:
                best_bitrate = bitrate
                best_url = url

        if best_url:
            result.append(best_url)

    return result


def extract_video_duration(tweet):
    """
    Return first video's duration in seconds if available.
    """

    media = tweet.get("media") or {}

    videos = media.get("videos") or []

    if not videos:
        return 0

    video = videos[0]

    if not isinstance(video, dict):
        return 0

    value = (
        video.get("duration")
        or video.get("durationMs")
        or video.get("duration_millis")
        or 0
    )

    try:
        value = int(value)
    except (ValueError, TypeError):
        return 0

    # Usually milliseconds when very large.
    if value > 10000:
        return value // 1000

    return value


def get_surface_image(tweet):
    """
    First photo, otherwise video preview image.
    """

    photos = extract_photos(tweet)

    if photos:
        return photos[0]

    media = tweet.get("media") or {}

    videos = media.get("videos") or []

    if not videos:
        return ""

    video = videos[0]

    if not isinstance(video, dict):
        return ""

    return (
        video.get("thumbnailUrl")
        or video.get("previewImageUrl")
        or ""
    )


# ============================================================
# ROOT / RETWEETED
# ============================================================

def make_empty_root(root_id):
    """
    Root tweet was not included in the search result.

    We still preserve root_mid and a usable URL,
    but we do NOT perform another crawl.
    """

    return {
        "uuid": (
            f"x.com|post|{root_id}"
            if root_id
            else ""
        ),

        "mid": root_id,

        "url": (
            f"https://x.com/i/status/{root_id}"
            if root_id
            else ""
        ),

        "title": "",

        "content": "",

        "pic_urls": [],

        "surface_img": "",

        "video_urls": [],

        "duration": 0,

        "reply_count": 0,

        "like_count": 0,

        "ctime": 0,

        "ocr": "",

        "istar_asr": "",

        "ocr_url_list": [],

        "user": make_user({}),

        "media_matrix": {
            "mediaLevel": "",
            "platformName": PLATFORM_NAME,
        },
    }


def make_retweeted(root_tweet, root_id):
    """
    Build embedded root/original tweet.

    If root is available in current search data:
        include complete data.

    If root is not available:
        use minimal placeholder without additional crawling.
    """

    if root_tweet is None:
        return make_empty_root(root_id)

    actual_root_id = get_id(root_tweet)

    return {
        "uuid": (
            f"x.com|post|{actual_root_id}"
            if actual_root_id
            else ""
        ),

        "mid": actual_root_id,

        "url": (
            root_tweet.get("url")
            or (
                f"https://x.com/i/status/{actual_root_id}"
                if actual_root_id
                else ""
            )
        ),

        "title": "",

        "content": (
            root_tweet.get("rawContent")
            or ""
        ),

        "pic_urls": extract_photos(
            root_tweet
        ),

        "surface_img": get_surface_image(
            root_tweet
        ),

        "video_urls": extract_video_urls(
            root_tweet
        ),

        "duration": extract_video_duration(
            root_tweet
        ),

        "reply_count": safe_int(
            root_tweet.get("replyCount")
        ),

        "like_count": safe_int(
            root_tweet.get("likeCount")
        ),

        "ctime": parse_datetime(
            root_tweet.get("date")
        ),

        "ocr": "",

        "istar_asr": "",

        "ocr_url_list": [],

        "user": make_user(
            root_tweet.get("user") or {}
        ),

        "media_matrix": {
            "mediaLevel": "",
            "platformName": PLATFORM_NAME,
        },
    }


# ============================================================
# CONVERT ONE TWEET
# ============================================================

def convert_tweet(tweet, tweet_index):
    """
    Convert one twscrape Tweet into target schema.
    """

    tweet_id = get_id(tweet)

    conversation_id = get_conversation_id(
        tweet
    )

    parent_id = get_parent_id(
        tweet
    )

    is_reply = bool(parent_id)

    # --------------------------------------------------------
    # TYPE / PARENT
    # --------------------------------------------------------

    if is_reply:

        item_type = "comment"

        parent_mid = parent_id

        # Reply directly to root post.
        if parent_id == conversation_id:
            parent_type = "post"

        # Reply to another reply.
        else:
            parent_type = "comment"

    else:

        item_type = "post"

        parent_mid = ""

        parent_type = ""

    # --------------------------------------------------------
    # ROOT
    # --------------------------------------------------------

    root_tweet = tweet_index.get(
        conversation_id
    )

    # Current tweet itself is the root.
    if conversation_id == tweet_id:
        root_tweet = tweet

    # --------------------------------------------------------
    # TIME
    # --------------------------------------------------------

    ctime = parse_datetime(
        tweet.get("date")
    )

    # --------------------------------------------------------
    # ROOT URL
    # --------------------------------------------------------

    if root_tweet:

        root_url = (
            root_tweet.get("url")
            or (
                f"https://x.com/i/status/"
                f"{conversation_id}"
            )
        )

    else:

        root_url = (
            f"https://x.com/i/status/"
            f"{conversation_id}"
            if conversation_id
            else ""
        )

    # --------------------------------------------------------
    # OUTPUT
    # --------------------------------------------------------

    result = {
        "uuid": make_uuid(
            tweet_id
        ),

        "raw_id": make_raw_id(
            tweet_id
        ),

        "mid": tweet_id,

        "type": item_type,

        "site_name": SITE_NAME,

        "url": (
            tweet.get("url")
            or ""
        ),

        "content": (
            tweet.get("rawContent")
            or ""
        ),

        "pic_urls": extract_photos(
            tweet
        ),

        "reply_count": safe_int(
            tweet.get("replyCount")
        ),

        "like_count": safe_int(
            tweet.get("likeCount")
        ),

        "ctime": ctime,

        # twscrape result does not provide a dependable
        # separate update timestamp.
        "utime": ctime,

        "root_mid": conversation_id,

        "root_type": "post",

        "root_url": root_url,

        # X has no direct equivalent of XHS xsec_token.
        "community_token": "",

        "xsec_token": "",

        "parent_mid": parent_mid,

        "parent_type": parent_type,

        "user": make_user(
            tweet.get("user") or {}
        ),

        "analysis": {
            "sentiment": 0,
            "is_original": 0,
            "noise": 0,
            "hashcode": {
                "5": ""
            },
        },

        # Reply:
        # embed root tweet when available.
        #
        # Root tweet itself:
        # no separate parent/root object required.
        "retweeted": (
            make_retweeted(
                root_tweet,
                conversation_id
            )
            if is_reply
            else None
        ),
    }

    return result


# ============================================================
# MAIN
# ============================================================

def main(
    tweets_path,
    output_path,
    only_comments=False,
):

    print(
        f"[INFO] Reading tweets: "
        f"{tweets_path}"
    )

    tweets_raw = load_jsonl(
        tweets_path
    )

    # --------------------------------------------------------
    # REMOVE DUPLICATES
    # --------------------------------------------------------

    tweets = {}

    for tweet in tweets_raw:

        tweet_id = get_id(tweet)

        if tweet_id:
            # Last occurrence wins.
            tweets[tweet_id] = tweet

    print(
        f"[INFO] Unique tweets: "
        f"{len(tweets)}"
    )

    # --------------------------------------------------------
    # INDEX
    # --------------------------------------------------------

    tweet_index = dict(tweets)

    # --------------------------------------------------------
    # ROOT POSTS NOT INCLUDED IN SEARCH
    # --------------------------------------------------------

    roots_not_in_search = set()

    for tweet in tweets.values():

        tweet_id = get_id(tweet)

        root_id = get_conversation_id(
            tweet
        )

        if (
            root_id
            and root_id != tweet_id
            and root_id not in tweet_index
        ):
            roots_not_in_search.add(
                root_id
            )

    # --------------------------------------------------------
    # OUTPUT
    # --------------------------------------------------------

    output = Path(
        output_path
    )

    output.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    converted = 0

    skipped_posts = 0

    with open(
        output,
        "w",
        encoding="utf-8"
    ) as f:

        for tweet in tweets.values():

            parent_id = get_parent_id(
                tweet
            )

            # Optional mode:
            # only output replies/comments.
            if only_comments and not parent_id:

                skipped_posts += 1

                continue

            result = convert_tweet(
                tweet,
                tweet_index
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

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    print("")
    print("========== DONE ==========")

    print(
        f"Converted tweets : "
        f"{converted}"
    )

    if only_comments:

        print(
            f"Skipped posts    : "
            f"{skipped_posts}"
        )

    print(
        "Root posts not included in "
        f"search results : "
        f"{len(roots_not_in_search)}"
    )

    print(
        f"Output           : "
        f"{output}"
    )

    print("==========================")


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Convert twscrape JSONL "
            "into normalized JSONL."
        )
    )

    parser.add_argument(
        "--tweets",
        required=True,
        help=(
            "Input twscrape JSONL file"
        ),
    )

    parser.add_argument(
        "--output",
        default=(
            "output/"
            "twitter_converted.jsonl"
        ),
        help=(
            "Output normalized JSONL file"
        ),
    )

    parser.add_argument(
        "--only-comments",
        action="store_true",
        help=(
            "Only output replies/comments. "
            "Default outputs both posts "
            "and replies."
        ),
    )

    args = parser.parse_args()

    main(
        tweets_path=args.tweets,
        output_path=args.output,
        only_comments=args.only_comments,
    )