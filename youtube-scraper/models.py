from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class Comment(BaseModel):
    author: str
    text: str
    votes: int
    reply_count: int = 0
    replies: list[Comment] = Field(default_factory=list)
    usefulness_score: float | None = None
    usefulness_reasons: list[str] = Field(default_factory=list)


Comment.model_rebuild()


class CommentsMeta(BaseModel):
    strategy: str
    scanned: int
    candidates: int
    selected: int
    replies_for_candidates: int
    stopped_reason: str


class Subtitles(BaseModel):
    language: str
    type: str  # "manual" or "auto"
    text: str


class ScrapeResponse(BaseModel):
    video_id: str
    title: Optional[str] = None
    description: Optional[str] = None
    channel: Optional[str] = None
    duration: Optional[int] = None
    upload_date: Optional[str] = None
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    channel_id: Optional[str] = None
    categories: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    subtitles: Optional[Subtitles] = None
    comments: list[Comment] = Field(default_factory=list)
    comments_meta: CommentsMeta | None = None
    errors: list[str] = Field(default_factory=list)
