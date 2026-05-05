import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from henchman_sdk import get_config, get_params, log_error, log_info, log_warning, set_result

from models import CommentsMeta, ScrapeResponse
from scraper import extract_video_id, fetch_comments, fetch_metadata, fetch_subtitles


#EVENTUS_API_URL = os.environ.get("EVENTUS_API_URL", "http://192.168.1.75:8765")
#EVENTUS_API_TOKEN = os.environ.get("EVENTUS_API_TOKEN", "")
EVENTUS_API_URL = get_config()["eventus_api_url"]
EVENTUS_API_TOKEN = get_config()["eventus_api_token"]

EVENTUS_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class EventusEnvelope:
    event_uid: str
    claim_owner: str
    event: dict[str, Any]


class HenchmanLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        if record.levelno >= logging.ERROR:
            log_error(message)
        elif record.levelno >= logging.WARNING:
            log_warning(message)
        else:
            log_info(message)


def _configure_library_logging() -> None:
    handler = HenchmanLogHandler()
    handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))

    for logger_name in ("scraper", "youtube_comment_downloader"):
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False


def _as_positive_int(value: Any, *, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be >= 1")
    return parsed


def _param_int(params: dict, config: dict, name: str, default: int, *aliases: str) -> int:
    for key in (name, *aliases):
        if key in params:
            return _as_positive_int(params[key], name=key)
    for key in (name, *aliases):
        if key in config:
            return _as_positive_int(config[key], name=key)
    return default


def _get_nested_url(event: dict[str, Any]) -> str | None:
    candidates = [event.get("url")]
    payload = event.get("payload")
    if isinstance(payload, dict):
        candidates.extend([payload.get("url"), payload.get("href")])
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _eventus_envelope(params: dict[str, Any]) -> EventusEnvelope | None:
    event = params.get("event")
    event_uid = params.get("event_uid")
    claim_owner = params.get("claim_owner")
    if (
        params.get("source") == "eventus"
        and isinstance(event_uid, str)
        and event_uid.strip()
        and isinstance(claim_owner, str)
        and claim_owner.strip()
        and isinstance(event, dict)
    ):
        return EventusEnvelope(
            event_uid=event_uid.strip(),
            claim_owner=claim_owner.strip(),
            event=event,
        )
    return None


def _eventus_request(path: str, body: dict[str, Any]) -> dict[str, Any]:
    if not EVENTUS_API_TOKEN:
        raise RuntimeError("EVENTUS_API_TOKEN is required for Eventus envelope mode")

    payload = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"{EVENTUS_API_URL.rstrip('/')}{path}",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {EVENTUS_API_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=EVENTUS_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Eventus API returned HTTP {exc.code}: {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Eventus API request failed: {exc.reason}") from exc


def _complete_eventus_step(
    envelope: EventusEnvelope,
    *,
    new_state: str,
    comment: str,
    details: dict[str, Any] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
    error: str | None = None,
) -> None:
    body: dict[str, Any] = {
        "agent": envelope.claim_owner,
        "new_state": new_state,
        "comment": comment,
        "details": details or {},
        "artifacts": artifacts or [],
    }
    if error:
        body["error"] = error
    _eventus_request(f"/api/v1/events/{envelope.event_uid}/complete-step", body)


def _fail_eventus_step(envelope: EventusEnvelope, *, error: str) -> None:
    _eventus_request(
        f"/api/v1/events/{envelope.event_uid}/fail-step",
        {
            "agent": envelope.claim_owner,
            "error": error,
            "comment": "YouTube scrape failed",
            "details": {"phase": "youtube_scrape"},
            "release": True,
        },
    )


def _result_details(result: ScrapeResponse) -> dict[str, Any]:
    return {
        "video_id": result.video_id,
        "comments_count": len(result.comments),
        "has_subtitles": result.subtitles is not None,
        "errors_count": len(result.errors),
        "errors": result.errors,
    }


def _result_artifact(result: ScrapeResponse) -> dict[str, Any]:
    title = result.title or result.video_id or "YouTube scrape"
    return {
        "kind": "youtube.scrape",
        "title": title,
        "content_type": "application/json",
        "data": result.model_dump(),
    }


def _match_excluded_category(
    categories: list[str], excluded: list[str]
) -> str | None:
    excluded_lower = {c.lower() for c in excluded}
    for cat in categories:
        if cat.lower() in excluded_lower:
            return cat
    return None


def _empty_error_result(message: str, video_id: str = "") -> dict:
    return ScrapeResponse(
        video_id=video_id,
        title=None,
        description=None,
        subtitles=None,
        comments=[],
        comments_meta=None,
        errors=[message],
    ).model_dump()


def scrape_video(
    url: str,
    *,
    output_top_n: int,
    candidate_top_level_limit: int,
    max_scan: int,
    reply_patience: int,
    prefetched_meta: dict[str, Any] | None = None,
) -> ScrapeResponse:
    errors: list[str] = []
    t0 = time.time()

    video_id = extract_video_id(url)
    log_info(
        f"Scraping video_id={video_id} output_top_n={output_top_n} "
        f"candidate_top_level_limit={candidate_top_level_limit} "
        f"max_scan={max_scan} reply_patience={reply_patience}"
    )

    title = None
    description = None
    channel = None
    duration = None
    upload_date = None
    view_count = None
    like_count = None
    channel_id = None
    categories: list = []
    tags: list = []
    step_t0 = time.time()
    if prefetched_meta is not None:
        meta = prefetched_meta
        log_info(f"Using prefetched metadata video_id={video_id}")
    else:
        log_info(f"Request start metadata video_id={video_id}")
        try:
            meta = fetch_metadata(url)
            log_info(
                f"Request done metadata video_id={video_id} "
                f"elapsed={time.time() - step_t0:.2f}s title_present={meta.get('title') is not None}"
            )
        except Exception as exc:  # noqa: BLE001 - component errors should not fail whole scrape
            log_warning(
                f"Request failed metadata video_id={video_id} "
                f"elapsed={time.time() - step_t0:.2f}s error={exc}"
            )
            errors.append(f"metadata: {exc}")
            meta = {}
    title = meta.get("title")
    description = meta.get("description")
    channel = meta.get("channel")
    duration = meta.get("duration")
    upload_date = meta.get("upload_date")
    view_count = meta.get("view_count")
    like_count = meta.get("like_count")
    channel_id = meta.get("channel_id")
    categories = meta.get("categories", [])
    tags = meta.get("tags", [])

    subtitles = None
    step_t0 = time.time()
    log_info(f"Request start subtitles.list video_id={video_id}")
    try:
        subtitles = fetch_subtitles(video_id)
        if subtitles is None:
            errors.append("subtitles: no suitable subtitles found")
        log_info(
            f"Request done subtitles video_id={video_id} "
            f"elapsed={time.time() - step_t0:.2f}s found={subtitles is not None}"
        )
    except Exception as exc:  # noqa: BLE001 - component errors should not fail whole scrape
        log_warning(
            f"Request failed subtitles video_id={video_id} "
            f"elapsed={time.time() - step_t0:.2f}s error={exc}"
        )
        errors.append(f"subtitles: {exc}")

    comments = []
    comments_meta: CommentsMeta | None = None
    step_t0 = time.time()
    log_info(
        f"Request start comments video_id={video_id} output_top_n={output_top_n} "
        f"candidate_top_level_limit={candidate_top_level_limit} max_scan={max_scan} "
        f"reply_patience={reply_patience}"
    )
    try:
        comments, comments_meta = fetch_comments(
            url,
            output_top_n=output_top_n,
            candidate_top_level_limit=candidate_top_level_limit,
            max_scan=max_scan,
            reply_patience=reply_patience,
        )
        log_info(
            f"Request done comments video_id={video_id} "
            f"elapsed={time.time() - step_t0:.2f}s comments={len(comments)} "
            f"scanned={comments_meta.scanned} stopped_reason={comments_meta.stopped_reason}"
        )
    except Exception as exc:  # noqa: BLE001 - component errors should not fail whole scrape
        log_warning(
            f"Request failed comments video_id={video_id} "
            f"elapsed={time.time() - step_t0:.2f}s error={exc}"
        )
        errors.append(f"comments: {exc}")

    elapsed = round(time.time() - t0, 2)
    log_info(
        f"Done video_id={video_id} elapsed={elapsed:.2f}s "
        f"comments={len(comments)} errors={len(errors)}"
    )

    return ScrapeResponse(
        video_id=video_id,
        title=title,
        description=description,
        channel=channel,
        duration=duration,
        upload_date=upload_date,
        view_count=view_count,
        like_count=like_count,
        channel_id=channel_id,
        categories=categories,
        tags=tags,
        subtitles=subtitles,
        comments=comments,
        comments_meta=comments_meta,
        errors=errors,
    )


def main() -> None:
    _configure_library_logging()
    config = get_config()
    params = get_params()
    envelope = _eventus_envelope(params)

    url = _get_nested_url(envelope.event) if envelope else params.get("url")
    if not isinstance(url, str) or not url.strip():
        message = "input: url parameter is required"
        log_error(message)
        set_result(_empty_error_result(message))
        if envelope:
            _complete_eventus_step(
                envelope,
                new_state="skipped",
                comment="YouTube scrape skipped: missing URL",
                error=message,
                details={"phase": "input"},
            )
            return
        raise ValueError(message)
    url = url.strip()

    excluded_categories: list[str] = config.get("excluded_categories") or []

    # Fetch metadata early to check categories before committing to a full scrape
    prefetched_meta: dict[str, Any] | None = None
    try:
        prefetched_meta = fetch_metadata(url)
        video_categories: list[str] = prefetched_meta.get("categories") or []
        matched = _match_excluded_category(video_categories, excluded_categories)
        if matched:
            comment = f"not scrapped due to category: {matched}"
            log_info(comment)
            set_result({"_outcome": {"code": "category_excluded", "status": "info", "message": comment}})
            if envelope:
                _complete_eventus_step(envelope, new_state="actionable", comment=comment)
            return
    except Exception as exc:  # noqa: BLE001 - metadata failure must not block scraping
        log_warning(f"Pre-scrape metadata fetch failed, proceeding without category check: {exc}")
        prefetched_meta = None

    try:
        output_top_n = _param_int(params, config, "output_top_n", 10, "top_n")
        candidate_top_level_limit = _param_int(
            params,
            config,
            "candidate_top_level_limit",
            30,
        )
        max_scan = _param_int(params, config, "max_scan", 150)
        reply_patience = _param_int(params, config, "reply_patience", 50)
        if candidate_top_level_limit < output_top_n:
            raise ValueError("candidate_top_level_limit must be >= output_top_n")
        result = scrape_video(
            url,
            output_top_n=output_top_n,
            candidate_top_level_limit=candidate_top_level_limit,
            max_scan=max_scan,
            reply_patience=reply_patience,
            prefetched_meta=prefetched_meta,
        )
    except ValueError as exc:
        message = f"input: {exc}"
        log_error(message)
        set_result(_empty_error_result(message))
        if envelope:
            _complete_eventus_step(
                envelope,
                new_state="skipped",
                comment="YouTube scrape skipped: invalid input",
                error=message,
                details={"phase": "input"},
            )
            return
        raise
    except Exception as exc:
        message = f"runtime: {exc}"
        log_error(message)
        if envelope:
            try:
                _fail_eventus_step(envelope, error=message)
            except Exception as fail_exc:  # noqa: BLE001 - preserve original failure context in logs
                log_error(f"Failed to notify Eventus fail-step: {fail_exc}")
        raise

    set_result(result.model_dump())

    if envelope:
        _complete_eventus_step(
            envelope,
            new_state="actionable",
            comment="YouTube scrape completed",
            details=_result_details(result),
            artifacts=[_result_artifact(result)],
        )


if __name__ == "__main__":
    main()
