import logging
import time
from typing import Any

from henchman_sdk import get_config, get_params, log_error, log_info, log_warning, set_result

from models import ScrapeResponse
from scraper import extract_video_id, fetch_comments, fetch_metadata, fetch_subtitles


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


def _empty_error_result(message: str, video_id: str = "") -> dict:
    return ScrapeResponse(
        video_id=video_id,
        title=None,
        description=None,
        subtitles=None,
        comments=[],
        errors=[message],
    ).model_dump()


def scrape_video(url: str, *, top_n: int, max_scan: int) -> ScrapeResponse:
    errors: list[str] = []
    t0 = time.time()

    video_id = extract_video_id(url)
    log_info(f"Scraping video_id={video_id} top_n={top_n} max_scan={max_scan}")

    title = None
    description = None
    step_t0 = time.time()
    log_info(f"Request start metadata video_id={video_id}")
    try:
        meta = fetch_metadata(url)
        title = meta.get("title")
        description = meta.get("description")
        log_info(
            f"Request done metadata video_id={video_id} "
            f"elapsed={time.time() - step_t0:.2f}s title_present={title is not None}"
        )
    except Exception as exc:  # noqa: BLE001 - component errors should not fail whole scrape
        log_warning(
            f"Request failed metadata video_id={video_id} "
            f"elapsed={time.time() - step_t0:.2f}s error={exc}"
        )
        errors.append(f"metadata: {exc}")

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
    step_t0 = time.time()
    log_info(f"Request start comments video_id={video_id} top_n={top_n} max_scan={max_scan}")
    try:
        comments = fetch_comments(url, top_n=top_n, max_scan=max_scan)
        log_info(
            f"Request done comments video_id={video_id} "
            f"elapsed={time.time() - step_t0:.2f}s comments={len(comments)}"
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
        subtitles=subtitles,
        comments=comments,
        errors=errors,
    )


def main() -> None:
    _configure_library_logging()
    config = get_config()
    params = get_params()

    url = params.get("url")
    if not isinstance(url, str) or not url.strip():
        message = "input: url parameter is required"
        log_error(message)
        set_result(_empty_error_result(message))
        raise ValueError(message)
    url = url.strip()

    try:
        top_n = _as_positive_int(params.get("top_n", config.get("top_n", 10)), name="top_n")
        max_scan = _as_positive_int(
            params.get("max_scan", config.get("max_scan", 500)),
            name="max_scan",
        )
        result = scrape_video(url, top_n=top_n, max_scan=max_scan)
    except ValueError as exc:
        message = f"input: {exc}"
        log_error(message)
        set_result(_empty_error_result(message))
        raise

    set_result(result.model_dump())


if __name__ == "__main__":
    main()
