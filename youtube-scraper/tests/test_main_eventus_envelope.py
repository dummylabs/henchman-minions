from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "main.py"


class HenchmanFake(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("henchman_sdk")
        self.params: dict[str, Any] = {}
        self.config: dict[str, Any] = {}
        self.results: list[dict[str, Any]] = []
        self.logs: list[tuple[str, str]] = []

    def get_config(self) -> dict[str, Any]:
        return self.config

    def get_params(self) -> dict[str, Any]:
        return self.params

    def set_result(self, value: dict[str, Any]) -> None:
        self.results.append(value)

    def log_info(self, message: str) -> None:
        self.logs.append(("info", message))

    def log_warning(self, message: str) -> None:
        self.logs.append(("warning", message))

    def log_error(self, message: str) -> None:
        self.logs.append(("error", message))


def load_main(monkeypatch: pytest.MonkeyPatch, fake_henchman: HenchmanFake):
    monkeypatch.setenv("EVENTUS_API_TOKEN", "test-token")
    monkeypatch.setitem(sys.modules, "henchman_sdk", fake_henchman)

    fake_scraper = types.ModuleType("scraper")
    fake_scraper.extract_video_id = lambda url: "dQw4w9WgXcQ"
    fake_scraper.fetch_comments = lambda *args, **kwargs: []
    fake_scraper.fetch_metadata = lambda *args, **kwargs: {}
    fake_scraper.fetch_subtitles = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "scraper", fake_scraper)
    monkeypatch.syspath_prepend(str(ROOT))

    spec = importlib.util.spec_from_file_location("youtube_scraper_main_under_test", MAIN_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sample_result(module, *, errors: list[str] | None = None):
    return module.ScrapeResponse(
        video_id="dQw4w9WgXcQ",
        title="Video title",
        description="Description",
        subtitles=None,
        comments=[],
        comments_meta=None,
        errors=errors or [],
    )


def test_url_mode_sets_result_without_eventus_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_henchman = HenchmanFake()
    fake_henchman.params = {"url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"}
    module = load_main(monkeypatch, fake_henchman)
    eventus_calls: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(module, "_eventus_request", lambda path, body: eventus_calls.append((path, body)))
    monkeypatch.setattr(module, "scrape_video", lambda *args, **kwargs: sample_result(module))

    module.main()

    assert fake_henchman.results[-1]["video_id"] == "dQw4w9WgXcQ"
    assert eventus_calls == []


def test_envelope_success_completes_step_with_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_henchman = HenchmanFake()
    fake_henchman.params = {
        "source": "eventus",
        "event_uid": "evt-1",
        "claim_owner": "henchman:youtube-scraper",
        "event": {"url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "payload": {}},
    }
    module = load_main(monkeypatch, fake_henchman)
    eventus_calls: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(module, "_eventus_request", lambda path, body: eventus_calls.append((path, body)))
    monkeypatch.setattr(module, "scrape_video", lambda *args, **kwargs: sample_result(module, errors=["subtitles: none"]))

    module.main()

    assert fake_henchman.results[-1]["errors"] == ["subtitles: none"]
    assert eventus_calls[0][0] == "/api/v1/events/evt-1/complete-step"
    body = eventus_calls[0][1]
    assert body["agent"] == "henchman:youtube-scraper"
    assert body["new_state"] == "actionable"
    assert body["artifacts"][0]["kind"] == "youtube.scrape"
    assert body["artifacts"][0]["data"]["video_id"] == "dQw4w9WgXcQ"
    assert body["details"]["errors_count"] == 1


def test_envelope_missing_url_completes_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_henchman = HenchmanFake()
    fake_henchman.params = {
        "source": "eventus",
        "event_uid": "evt-1",
        "claim_owner": "henchman:youtube-scraper",
        "event": {"payload": {}},
    }
    module = load_main(monkeypatch, fake_henchman)
    eventus_calls: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(module, "_eventus_request", lambda path, body: eventus_calls.append((path, body)))

    module.main()

    body = eventus_calls[0][1]
    assert body["new_state"] == "skipped"
    assert "url parameter is required" in body["error"]


def test_envelope_runtime_error_fails_step_and_reraises(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_henchman = HenchmanFake()
    fake_henchman.params = {
        "source": "eventus",
        "event_uid": "evt-1",
        "claim_owner": "henchman:youtube-scraper",
        "event": {"payload": {"href": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"}},
    }
    module = load_main(monkeypatch, fake_henchman)
    eventus_calls: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(module, "_eventus_request", lambda path, body: eventus_calls.append((path, body)))

    def boom(*args, **kwargs):
        raise RuntimeError("network exploded")

    monkeypatch.setattr(module, "scrape_video", boom)

    with pytest.raises(RuntimeError, match="network exploded"):
        module.main()

    assert eventus_calls[0][0] == "/api/v1/events/evt-1/fail-step"
    assert eventus_calls[0][1]["release"] is True
    assert "network exploded" in eventus_calls[0][1]["error"]
