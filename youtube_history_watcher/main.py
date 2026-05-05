import json
import sys
from pathlib import Path

import httpx
import yt_dlp

from henchman_sdk import get_config, log_error, log_info, log_warning, set_result

HISTORY_URL = "https://www.youtube.com/feed/history"
EVENT_TYPE = "web.youtube.watch"


def load_seen_ids(state_file: str) -> set[str]:
    path = Path(state_file)
    if not path.exists():
        return set()
    try:
        return set(json.loads(path.read_text()))
    except Exception as e:
        log_warning(f"Could not load state file, starting fresh: {e}")
        return set()


def save_seen_ids(state_file: str, seen_ids: set[str]) -> None:
    Path(state_file).write_text(json.dumps(sorted(seen_ids)))


def fetch_youtube_history(cookies_file: str, limit: int) -> list[dict] | None:
    ydl_opts = {
        "cookiefile": cookies_file,
        "extract_flat": True,
        "playlistend": limit,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "extractor_args": {"youtube": ["player_client=web"]},
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(HISTORY_URL, download=False)

    if not info or "entries" not in info:
        return None

    videos = []
    for entry in info.get("entries") or []:
        if not entry:
            continue
        video_id = entry.get("id", "")
        url = entry.get("url") or entry.get("original_url", "")
        if url and not url.startswith("http"):
            url = f"https://www.youtube.com/watch?v={url}"
        videos.append({
            "id": video_id,
            "url": url,
            "title": entry.get("title", "Без названия"),
        })
    return videos


def post_event(eventus_url: str, token: str, channel: str, initiator_id: str, ttl: str, url: str, title: str) -> str | None:
    try:
        resp = httpx.post(
            f"{eventus_url.rstrip('/')}/api/v1/events",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "channel": channel,
                "initiator_id": initiator_id,
                "type": EVENT_TYPE,
                "state": "new",
                "ttl": ttl,
                "url": url,
                "description": title,
            },
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("uid")
    except Exception as e:
        log_error(f"Eventus POST failed for {url}: {e}")
        return None


def main() -> None:
    config = get_config()
    cookies_file = config["cookies_file"]
    state_file = config["state_file"]
    fetch_limit = int(config["fetch_limit"])
    stop_after = int(config["stop_after_consecutive_dupes"])
    eventus_url = config["eventus_url"]
    eventus_token = config["eventus_token"]
    eventus_channel = config["eventus_channel"]
    eventus_initiator_id = config["eventus_initiator_id"]
    eventus_ttl = config["eventus_ttl"]

    if not Path(cookies_file).exists():
        msg = f"Cookies file not found: {cookies_file}"
        log_error(msg)
        set_result({"_outcome": {"code": "missing_cookies", "status": "error", "message": msg}})
        sys.exit(1)

    log_info(f"Fetching last {fetch_limit} videos from YouTube history...")
    try:
        videos = fetch_youtube_history(cookies_file, fetch_limit)
    except Exception as e:
        msg = f"yt-dlp error: {e}"
        log_error(msg)
        set_result({"_outcome": {"code": "yt_dlp_error", "status": "error", "message": msg}})
        sys.exit(1)

    if videos is None:
        msg = "YouTube returned empty result — cookies may be expired"
        log_warning(msg)
        set_result({"_outcome": {"code": "yt_dlp_empty", "status": "warning", "message": msg}})
        return

    seen_ids = load_seen_ids(state_file)
    log_info(f"Loaded {len(seen_ids)} known video IDs from state")

    new_videos = []
    consecutive_dupes = 0
    for video in videos:
        if video["id"] in seen_ids:
            consecutive_dupes += 1
            if consecutive_dupes >= stop_after:
                log_info(f"Reached {stop_after} consecutive duplicates, stopping scan")
                break
        else:
            consecutive_dupes = 0
            new_videos.append(video)

    if not new_videos:
        log_info("No new videos found")
        set_result({"_outcome": {"code": "no_new_videos", "status": "success", "message": "Новых видео нет"}})
        return

    log_info(f"Sending {len(new_videos)} new videos to Eventus...")
    confirmed = 0
    for video in new_videos:
        uid = post_event(eventus_url, eventus_token, eventus_channel, eventus_initiator_id, eventus_ttl, video["url"], video["title"])
        if uid:
            seen_ids.add(video["id"])
            confirmed += 1
            log_info(f"NEW id={video['id']} | {video['title']} | eventus_uid={uid}")
        else:
            log_warning(f"FAILED id={video['id']} | {video['title']}")

    save_seen_ids(state_file, seen_ids)
    log_info(f"State saved: {len(seen_ids)} total known IDs")

    message = f"Обнаружено {confirmed} новых видео"
    set_result({
        "_outcome": {"code": "new_videos_found", "status": "success", "message": message},
        "new_videos": confirmed,
    })


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log_error(f"Unhandled error: {exc}")
        set_result({"_outcome": {"code": "failed", "status": "error", "message": str(exc)}})
        raise
