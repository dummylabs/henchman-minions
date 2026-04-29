import logging
import re
import time
from typing import Optional

import yt_dlp
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled
from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR

from models import Comment, Subtitles

logger = logging.getLogger(__name__)

PREFERRED_LANGUAGES = ["ru", "en"]


def extract_video_id(url: str) -> str:
    """Extract YouTube video ID from URL."""
    patterns = [
        r"(?:v=|youtu\.be/|/embed/)([A-Za-z0-9_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    raise ValueError(f"Cannot extract video ID from URL: {url}")


def fetch_metadata(video_url: str) -> dict:
    """Fetch video title and description using yt-dlp."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }
    t0 = time.time()
    logger.info("metadata.yt_dlp.extract_info start")
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    logger.info("metadata.yt_dlp.extract_info done elapsed=%.2fs", time.time() - t0)
    return {
        "video_id": info.get("id", ""),
        "title": info.get("title"),
        "description": info.get("description"),
    }


def fetch_subtitles(video_id: str) -> Optional[Subtitles]:
    """
    Fetch subtitles with priority: manual ru/en > auto ru/en > auto en.
    Returns Subtitles with plain text, or None if unavailable.
    """
    api = YouTubeTranscriptApi()
    try:
        t0 = time.time()
        logger.info("subtitles.api.list start video_id=%s", video_id)
        transcript_list = api.list(video_id)
        logger.info("subtitles.api.list done video_id=%s elapsed=%.2fs", video_id, time.time() - t0)
    except TranscriptsDisabled:
        logger.info("Subtitles are disabled for video %s", video_id)
        return None
    except Exception as e:
        logger.warning("Failed to list transcripts for %s: %s", video_id, e)
        return None

    # Build lookup of available transcripts
    manual = {}
    auto = {}
    for t in transcript_list:
        if t.is_generated:
            auto[t.language_code] = t
        else:
            manual[t.language_code] = t

    # Try manual first, then auto, in preferred language order
    candidates = [
        (manual, "manual"),
        (auto, "auto"),
    ]
    for lang_map, sub_type in candidates:
        for lang in PREFERRED_LANGUAGES:
            if lang in lang_map:
                try:
                    t0 = time.time()
                    logger.info(
                        "subtitles.fetch start video_id=%s type=%s language=%s",
                        video_id,
                        sub_type,
                        lang,
                    )
                    snippets = lang_map[lang].fetch()
                    text = " ".join(s.text for s in snippets)
                    logger.info(
                        "subtitles.fetch done video_id=%s type=%s language=%s elapsed=%.2fs snippets=%d",
                        video_id,
                        sub_type,
                        lang,
                        time.time() - t0,
                        len(snippets),
                    )
                    return Subtitles(language=lang, type=sub_type, text=text)
                except Exception as e:
                    logger.warning("Failed to fetch %s transcript %s: %s", sub_type, lang, e)

    logger.info("No suitable subtitles found for %s", video_id)
    return None


def fetch_comments(video_url: str, top_n: int, max_scan: int) -> list[Comment]:
    """
    Fetch top comment threads using youtube-comment-downloader.

    Strategy: single-pass with SORT_BY_POPULAR.
    - Collect first top_n top-level comments.
    - Collect replies that belong to those threads as they appear in stream.
    - Stop after scanning max_scan total items.
    """
    t0 = time.time()
    logger.info("comments.downloader.create start")
    downloader = YoutubeCommentDownloader()
    logger.info("comments.downloader.create done elapsed=%.2fs", time.time() - t0)

    t0 = time.time()
    logger.info("comments.get_comments_from_url start sort_by=popular")
    generator = downloader.get_comments_from_url(video_url, sort_by=SORT_BY_POPULAR)
    logger.info("comments.get_comments_from_url generator_created elapsed=%.2fs", time.time() - t0)

    # top_comments: ordered list of top-level comment dicts
    top_comments: list[dict] = []
    top_cids: set[str] = set()
    # replies_map: parent_cid -> list of reply dicts
    replies_map: dict[str, list[dict]] = {}

    scanned = 0
    iter_t0 = time.time()
    first_item_logged = False
    for item in generator:
        if not first_item_logged:
            first_item_logged = True
            logger.info("comments.first_item received elapsed=%.2fs", time.time() - iter_t0)
        if scanned >= max_scan:
            logger.info("Reached max_scan=%d, stopping comment collection", max_scan)
            break
        scanned += 1
        if scanned == 1 or scanned % 25 == 0:
            logger.info(
                "comments.scan progress scanned=%d top_level=%d replies_for_top=%d elapsed=%.2fs",
                scanned,
                len(top_comments),
                sum(len(items) for items in replies_map.values()),
                time.time() - iter_t0,
            )

        is_reply = item.get("reply", False)

        if not is_reply:
            if len(top_comments) < top_n:
                cid = item["cid"]
                top_comments.append(item)
                top_cids.add(cid)
                replies_map[cid] = []
        else:
            # cid format for replies: "parentCid.replyCid"
            cid = item.get("cid", "")
            parent_cid = cid.split(".")[0] if "." in cid else None
            if parent_cid and parent_cid in top_cids:
                replies_map[parent_cid].append(item)

    def parse_votes(votes) -> int:
        if isinstance(votes, int):
            return votes
        if isinstance(votes, str):
            votes = votes.replace(",", "").strip()
            if votes.endswith("K"):
                return int(float(votes[:-1]) * 1000)
            if votes.endswith("M"):
                return int(float(votes[:-1]) * 1_000_000)
            try:
                return int(votes)
            except ValueError:
                return 0
        return 0

    def make_comment(item: dict, include_replies: bool = False) -> Comment:
        votes = parse_votes(item.get("votes", 0))
        replies = []
        if include_replies:
            cid = item["cid"]
            raw_replies = replies_map.get(cid, [])
            # Sort replies by votes descending
            raw_replies.sort(key=lambda r: parse_votes(r.get("votes", 0)), reverse=True)
            replies = [make_comment(r) for r in raw_replies]
        return Comment(
            author=item.get("author", ""),
            text=item.get("text", ""),
            votes=votes,
            reply_count=len(replies),
            replies=replies,
        )

    result = [make_comment(c, include_replies=True) for c in top_comments]
    logger.info(
        "Collected %d top-level threads (scanned %d items, elapsed %.2fs)",
        len(result),
        scanned,
        time.time() - iter_t0,
    )
    return result
