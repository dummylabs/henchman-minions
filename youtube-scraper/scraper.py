import logging
import math
import re
import time
from typing import Any, Optional

import yt_dlp
from youtube_comment_downloader import SORT_BY_POPULAR, YoutubeCommentDownloader
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled

from models import Comment, CommentsMeta, Subtitles

logger = logging.getLogger(__name__)

PREFERRED_LANGUAGES = ["ru", "en"]
USEFUL_KEYWORDS = {
    "actually": 1.0,
    "alternative": 1.2,
    "because": 0.6,
    "correction": 1.8,
    "doesn't work": 1.5,
    "doesnt work": 1.5,
    "does not work": 1.5,
    "error": 0.8,
    "for anyone": 1.0,
    "for those": 1.0,
    "github": 1.0,
    "i tried": 1.4,
    "in my case": 1.3,
    "instead": 0.8,
    "issue": 0.8,
    "link": 0.8,
    "note that": 1.0,
    "problem": 0.8,
    "source": 1.2,
    "tested": 1.4,
    "update": 1.5,
    "version": 0.9,
    "warning": 1.2,
    "workaround": 1.8,
}
GENERIC_PRAISE_PATTERNS = [
    r"^\s*(great|good|nice|awesome|amazing|excellent|cool)\s+(video|content|work)\s*[!.🔥❤\s]*$",
    r"^\s*(thanks|thank you|thx|first|lol|wow|nice|cool|great)\s*[!.🔥❤🙏\s]*$",
]
URL_RE = re.compile(r"https?://|www\.", re.IGNORECASE)
TIMESTAMP_RE = re.compile(r"\b(?:\d{1,2}:)?\d{1,2}:\d{2}\b")
NUMBER_OR_VERSION_RE = re.compile(r"\b(?:v?\d+(?:\.\d+){1,3}|\d{4}|\d+%)\b", re.IGNORECASE)
TOOLISH_RE = re.compile(r"\b[A-Za-z][A-Za-z0-9_-]{2,}(?:\.[A-Za-z0-9_-]+)?\b")
PROMO_RE = re.compile(r"\b(subscribe|check out my|my channel|telegram|whatsapp|giveaway)\b", re.IGNORECASE)
EMOJI_OR_PUNCT_RE = re.compile(r"^[\W_]+$", re.UNICODE)


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
    raw_date = info.get("upload_date")
    upload_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}" if raw_date else None
    return {
        "video_id": info.get("id", ""),
        "title": info.get("title"),
        "description": info.get("description"),
        "channel": info.get("channel") or info.get("uploader"),
        "duration": info.get("duration"),
        "upload_date": upload_date,
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "channel_id": info.get("channel_id"),
        "categories": info.get("categories") or [],
        "tags": info.get("tags") or [],
    }


def fetch_subtitles(video_id: str) -> Optional[Subtitles]:
    """
    Fetch subtitles with priority: manual ru/en > auto ru/en.
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

    manual = {}
    auto = {}
    for t in transcript_list:
        if t.is_generated:
            auto[t.language_code] = t
        else:
            manual[t.language_code] = t

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


def parse_votes(votes: Any) -> int:
    if isinstance(votes, int):
        return votes
    if isinstance(votes, str):
        normalized = votes.replace(",", "").strip().upper()
        if normalized.endswith("K"):
            return int(float(normalized[:-1]) * 1000)
        if normalized.endswith("M"):
            return int(float(normalized[:-1]) * 1_000_000)
        try:
            return int(normalized)
        except ValueError:
            return 0
    return 0


def _score_comment(item: dict, replies: list[Comment]) -> tuple[float, list[str]]:
    text = item.get("text", "") or ""
    lower = text.lower()
    stripped = text.strip()
    votes = parse_votes(item.get("votes", 0))
    reasons: list[str] = []
    score = 0.0

    if votes > 0:
        part = min(math.log10(votes + 1) * 2.0, 6.0)
        score += part
        reasons.append("community_votes")

    text_len = len(stripped)
    if 80 <= text_len <= 1200:
        score += 1.5
        reasons.append("informative_length")
    elif 30 <= text_len < 80:
        score += 0.5
        reasons.append("nontrivial_length")
    elif text_len < 30:
        score -= 1.5
        reasons.append("too_short")

    if URL_RE.search(text):
        score += 1.5
        reasons.append("has_link")
    if TIMESTAMP_RE.search(text):
        score += 1.0
        reasons.append("has_timestamp")
    if NUMBER_OR_VERSION_RE.search(text):
        score += 0.8
        reasons.append("has_numbers_or_versions")

    reply_count = len(replies)
    if reply_count:
        score += min(reply_count * 0.4, 2.0)
        reasons.append("has_replies")
        reply_votes = sum(max(reply.votes, 0) for reply in replies)
        if reply_votes:
            score += min(math.log10(reply_votes + 1), 2.0)
            reasons.append("reply_votes")

    for keyword, weight in USEFUL_KEYWORDS.items():
        if keyword in lower:
            score += weight
            reasons.append(f"keyword:{keyword.replace(' ', '_')}")

    # Lightweight specificity signal: comments naming tools/projects often add useful context.
    toolish_words = [w for w in TOOLISH_RE.findall(text) if len(w) >= 4]
    if len(set(toolish_words)) >= 3:
        score += 0.8
        reasons.append("specific_terms")

    if item.get("heart"):
        score += 1.0
        reasons.append("creator_hearted")

    if any(re.search(pattern, lower) for pattern in GENERIC_PRAISE_PATTERNS):
        score -= 2.0
        reasons.append("generic_praise")
    if PROMO_RE.search(text):
        score -= 2.0
        reasons.append("promo_or_spam")
    if stripped and EMOJI_OR_PUNCT_RE.match(stripped):
        score -= 2.0
        reasons.append("emoji_only")
    if URL_RE.search(text) and votes == 0 and text_len < 120:
        score -= 1.0
        reasons.append("low_signal_link")

    return round(score, 2), reasons


def _make_comment(item: dict, replies_map: dict[str, list[dict]], include_replies: bool = False) -> Comment:
    votes = parse_votes(item.get("votes", 0))
    replies: list[Comment] = []
    if include_replies:
        cid = item["cid"]
        raw_replies = replies_map.get(cid, [])
        raw_replies.sort(key=lambda r: parse_votes(r.get("votes", 0)), reverse=True)
        replies = [_make_comment(r, replies_map) for r in raw_replies]
    score, reasons = _score_comment(item, replies) if include_replies else (None, [])
    return Comment(
        author=item.get("author", ""),
        text=item.get("text", ""),
        votes=votes,
        reply_count=len(replies),
        replies=replies,
        usefulness_score=score,
        usefulness_reasons=reasons,
    )


def fetch_comments(
    video_url: str,
    *,
    output_top_n: int,
    candidate_top_level_limit: int,
    max_scan: int,
    reply_patience: int,
) -> tuple[list[Comment], CommentsMeta]:
    """
    Fetch and rank relevant top-level comments from SORT_BY_POPULAR.

    Strategy:
    - Build a candidate pool of popular top-level comments.
    - Collect replies to those candidates while scanning.
    - Stop at max_scan, or once candidate pool is full and no candidate replies
      appeared for reply_patience scanned items.
    - Rank candidates by deterministic usefulness_score and return output_top_n.
    """
    t0 = time.time()
    logger.info("comments.downloader.create start")
    downloader = YoutubeCommentDownloader()
    logger.info("comments.downloader.create done elapsed=%.2fs", time.time() - t0)

    t0 = time.time()
    logger.info("comments.get_comments_from_url start sort_by=popular")
    generator = downloader.get_comments_from_url(video_url, sort_by=SORT_BY_POPULAR)
    logger.info("comments.get_comments_from_url generator_created elapsed=%.2fs", time.time() - t0)

    candidate_comments: list[dict] = []
    candidate_cids: set[str] = set()
    replies_map: dict[str, list[dict]] = {}

    scanned = 0
    last_candidate_reply_at = 0
    stopped_reason = "stream_exhausted"
    iter_t0 = time.time()
    first_item_logged = False

    for item in generator:
        if not first_item_logged:
            first_item_logged = True
            logger.info("comments.first_item received elapsed=%.2fs", time.time() - iter_t0)

        if scanned >= max_scan:
            stopped_reason = "max_scan_reached"
            logger.info("Reached max_scan=%d, stopping comment collection", max_scan)
            break

        scanned += 1
        is_reply = item.get("reply", False)

        if not is_reply:
            if len(candidate_comments) < candidate_top_level_limit:
                cid = item["cid"]
                candidate_comments.append(item)
                candidate_cids.add(cid)
                replies_map[cid] = []
        else:
            cid = item.get("cid", "")
            parent_cid = cid.split(".")[0] if "." in cid else None
            if parent_cid and parent_cid in candidate_cids:
                replies_map[parent_cid].append(item)
                last_candidate_reply_at = scanned

        replies_for_candidates = sum(len(items) for items in replies_map.values())
        if scanned == 1 or scanned % 25 == 0:
            logger.info(
                "comments.scan progress scanned=%d candidates=%d replies_for_candidates=%d "
                "since_last_reply=%d elapsed=%.2fs",
                scanned,
                len(candidate_comments),
                replies_for_candidates,
                scanned - last_candidate_reply_at,
                time.time() - iter_t0,
            )

        if (
            len(candidate_comments) >= candidate_top_level_limit
            and scanned - last_candidate_reply_at >= reply_patience
        ):
            stopped_reason = "reply_patience_exhausted"
            logger.info(
                "comments.early_stop reason=%s scanned=%d candidates=%d reply_patience=%d",
                stopped_reason,
                scanned,
                len(candidate_comments),
                reply_patience,
            )
            break

    ranked = [_make_comment(c, replies_map, include_replies=True) for c in candidate_comments]
    ranked.sort(key=lambda c: (c.usefulness_score or 0.0, c.votes), reverse=True)
    selected = ranked[:output_top_n]
    replies_for_candidates = sum(len(items) for items in replies_map.values())

    logger.info(
        "comments.ranked scanned=%d candidates=%d selected=%d replies_for_candidates=%d "
        "stopped_reason=%s elapsed=%.2fs",
        scanned,
        len(candidate_comments),
        len(selected),
        replies_for_candidates,
        stopped_reason,
        time.time() - iter_t0,
    )
    logger.info(
        "comments.top_scores %s",
        [
            {
                "score": c.usefulness_score,
                "votes": c.votes,
                "reply_count": c.reply_count,
                "reasons": c.usefulness_reasons[:5],
            }
            for c in selected[:5]
        ],
    )

    meta = CommentsMeta(
        strategy="popular_ranked",
        scanned=scanned,
        candidates=len(candidate_comments),
        selected=len(selected),
        replies_for_candidates=replies_for_candidates,
        stopped_reason=stopped_reason,
    )
    return selected, meta
