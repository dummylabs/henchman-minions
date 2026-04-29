from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class Comment(BaseModel):
    author: str
    text: str
    votes: int
    reply_count: int = 0
    replies: list[Comment] = Field(default_factory=list)


Comment.model_rebuild()


class Subtitles(BaseModel):
    language: str
    type: str  # "manual" or "auto"
    text: str


class ScrapeResponse(BaseModel):
    video_id: str
    title: Optional[str] = None
    description: Optional[str] = None
    subtitles: Optional[Subtitles] = None
    comments: list[Comment] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
