"""Preparing structure and tables for RethinkDB.

Single canonical format (id-based joins only).

Item:
- id: str UUID-like
- universe: str
- title: str
- card: str
- short_card_description: str
- target_ids: list[str]
- personal_names_ids: list[str]

Target (referenced by Item.target_ids):
- id: str UUID-like
- item_id: str UUID-like
- content: str
- explanation: str | None
- level: str | None
- score: float | None
- reasons: list[str]

PersonalName (referenced by Item.personal_names_ids):
- id: str UUID-like
- item_id: str UUID-like
- name: str
- url: str | None

Per-user fields:
User:
- id: str/int
- seen_item_ids: list[str]
- liked_item_ids: list[str]
- disliked_item_ids: list[str]
- saved_item_ids: list[str]
- skipped_item_ids: list[str]
- seen_target_ids: list[str]
- saved_target_ids: list[str]
- clicked_target_ids: list[str]

Analytics tables:
ItemEvent / TargetEvent (append-only):
- id: str UUID
- user_id: str|int
- item_id/target_id: str UUID-like
- event_type: str
- created_at: datetime

ItemStats / TargetStats (aggregate counters):
- id: str (item_id/target_id)
- views/likes/dislikes/saves/skips/clicks: int
- created_at/updated_at: datetime
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ExplanationSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    text: str
    explanation: str | None = None


class TargetDifficultySchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    text: str
    level: str | None = None
    score: float | None = None
    reasons: list[str] = Field(default_factory=list)


class DifficultySchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    overall: str | None = None
    targets: list[TargetDifficultySchema] = Field(default_factory=list)


class TargetSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    item_id: str
    content: str
    explanation: str | None = None
    level: str | None = None
    score: float | None = Field(default=None, ge=0.0, le=1.0)
    reasons: list[str] = Field(default_factory=list)
    created_at: Any | None = None
    updated_at: Any | None = None


class TargetInputSchema(BaseModel):
    """Inbound schema for creating/updating a target.

    `item_id` может быть передан, но всегда нормализуется и хранится явно.
    """

    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    item_id: str | None = None
    content: str
    explanation: str | None = None
    level: str | None = None
    score: float | None = Field(default=None, ge=0.0, le=1.0)
    reasons: list[str] = Field(default_factory=list)


class PersonalNameSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    item_id: str
    name: str
    url: str | None = None
    created_at: Any | None = None
    updated_at: Any | None = None


class PersonalNameInputSchema(BaseModel):
    """Inbound schema for creating/updating a personal name.

    `item_id` может быть передан, но хранится явно.
    """

    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    item_id: str | None = None
    name: str
    url: str | None = None


class ItemInputSchema(BaseModel):
    """Inbound schema for creating/updating an item.

    Canonical (id-based) only.
    """

    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    universe: str | None = None
    title: str | None = None
    card: str
    short_card_description: str | None = None

    # Canonical aggregate score (e.g. CEFR like "B1")
    score: str | None = None

    # Canonical join ids
    target_ids: list[str] = Field(default_factory=list)
    personal_names_ids: list[str] = Field(default_factory=list)


class ItemSchema(BaseModel):
    """Stored item schema (canonical)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    universe: str | None = None
    title: str | None = None
    card: str
    short_card_description: str | None = None
    score: str | None = None
    target_ids: list[str] = Field(default_factory=list)
    personal_names_ids: list[str] = Field(default_factory=list)
    created_at: Any | None = None
    updated_at: Any | None = None


class ItemFeedSchema(BaseModel):
    """Outbound schema for feed response (expanded)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    universe: str | None = None
    title: str | None = None
    card: str
    short_card_description: str | None = None

    score: str | None = None

    target: list[str] = Field(default_factory=list)
    personal_names: list[str] = Field(default_factory=list)
    target_ids: list[str] = Field(default_factory=list)
    personal_names_ids: list[str] = Field(default_factory=list)

    targets: list[TargetSchema] = Field(default_factory=list)
    personal_names_docs: list[PersonalNameSchema] = Field(default_factory=list)

    created_at: Any | None = None
    updated_at: Any | None = None


class UserSchema(BaseModel):
    """Stored user schema (flexible)."""

    model_config = ConfigDict(extra="allow")

    id: int | str
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None
    is_premium: bool | None = None
    language_code: str | None = None
    active_thread_id: int | None = None
    current_thread_offset: int | None = None

    seen_item_ids: list[str] = Field(default_factory=list)
    liked_item_ids: list[str] = Field(default_factory=list)
    disliked_item_ids: list[str] = Field(default_factory=list)
    saved_item_ids: list[str] = Field(default_factory=list)
    skipped_item_ids: list[str] = Field(default_factory=list)

    seen_target_ids: list[str] = Field(default_factory=list)
    saved_target_ids: list[str] = Field(default_factory=list)
    clicked_target_ids: list[str] = Field(default_factory=list)

    created_at: Any | None = None
    updated_at: Any | None = None


class UserInputSchema(UserSchema):
    """Inbound schema for upserting a user."""

    model_config = ConfigDict(extra="allow")


class ItemEventTypeEnum(str, Enum):
    view = "view"
    like = "like"
    dislike = "dislike"
    save = "save"
    skip = "skip"
    ask = "ask"


class TargetEventTypeEnum(str, Enum):
    view = "view"
    save = "save"
    click = "click"


class ItemEventInputSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    user_id: int | str
    item_id: str
    event_type: ItemEventTypeEnum | str
    meta: dict[str, Any] = Field(default_factory=dict)


class ItemEventSchema(ItemEventInputSchema):
    model_config = ConfigDict(extra="allow")

    id: str
    created_at: Any | None = None


class TargetEventInputSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    user_id: int | str
    target_id: str
    event_type: TargetEventTypeEnum | str
    meta: dict[str, Any] = Field(default_factory=dict)


class TargetEventSchema(TargetEventInputSchema):
    model_config = ConfigDict(extra="allow")

    id: str
    created_at: Any | None = None


class ItemStatsSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    views: int = 0
    likes: int = 0
    dislikes: int = 0
    saves: int = 0
    skips: int = 0
    asks: int = 0
    created_at: Any | None = None
    updated_at: Any | None = None


class TargetStatsSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    views: int = 0
    saves: int = 0
    clicks: int = 0
    created_at: Any | None = None
    updated_at: Any | None = None


class MessageTopicEnum(Enum):
    greetings = "GREETINGS"
    search = "SEARCH"
    lechat = "LECHAT"


class MessageRelevanceEnum(Enum):
    is_relevant_towards_context = "is_relevant_towards_context"


class MessageTypeEnum(Enum):
    system = "system"
    human = "human"
    ai = "ai"


class MessageRatingEnum(Enum):
    like = "like"
    dislike = "dislike"


class ItemPerUserRatingEnum(Enum):
    like = "like"
    dislike = "dislike"


class ItemRatingEnum(Enum):
    score = 0.0


class User:
    """Модель пользователя."""

    # - id: str UUID
    # - first_name: str
    # - last_name: str (optional)
    # - username: str (optional)
    # - is_premium: bool
    # - language_code: str (optional)
    # - active_thread_id: str (optional)
    # - current_thread_offset: int
    # - seen_item_ids: list[str(uuid of items)]
    # - liked_item_ids: list[str(uuid of items)]
    # - disliked_item_ids: list[str(uuid of items)]
    # - skipped_item_ids: list[str(uuid of items)]
    # - seen_target_ids: list[str(uuid of targets)]
    # - saved_target_ids: list[str(uuid of targets)]
    # - saved_item_ids: list[str(uuid of items)]
    # - clicked_target_ids: list[str(uuid of targets)] (to track what targets user clicked on to see explanation)
    # - other fields as needed

    def __init__(
        self,
        id: int,
        first_name: str,
        last_name: str | None = None,
        username: str | None = None,
        is_premium: bool = False,
        language_code: str | None = None,
        active_thread_id: int | None = None,
        current_thread_offset: int = 0,
        seen_item_ids: list[str] | None = None,
        liked_item_ids: list[str] | None = None,
        disliked_item_ids: list[str] | None = None,
        skipped_item_ids: list[str] | None = None,
        seen_target_ids: list[str] | None = None,
        saved_target_ids: list[str] | None = None,
        saved_item_ids: list[str] | None = None,
        clicked_target_ids: list[str] | None = None,
    ):
        self.id = id
        self.first_name = first_name
        self.last_name = last_name
        self.username = username
        self.is_premium = is_premium
        self.language_code = language_code
        self.active_thread_id = active_thread_id
        self.current_thread_offset = current_thread_offset
        self.seen_item_ids = seen_item_ids or []
        self.liked_item_ids = liked_item_ids or []
        self.disliked_item_ids = disliked_item_ids or []
        self.skipped_item_ids = skipped_item_ids or []
        self.seen_target_ids = seen_target_ids or []
        self.saved_target_ids = saved_target_ids or []
        self.saved_item_ids = saved_item_ids or []
        self.clicked_target_ids = clicked_target_ids or []
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


class Item:
    """Модель карточки (общая в хранилище)."""

    # - id: str UUID like (reuuid(universe+title+card))
    # - target_ids: list[str(uuid of targets)]
    # - universe: str (e.g. book, game, movie, song, etc...)
    # - title: str (e.g. "The Witcher 3: Wild Hunt")
    # - card: str
    # - short_card_description: str
    # - score: str | None (e.g. "B1")
    # - personal_names_ids: list[str](uuid of personal names)
    def __init__(
        self,
        id: str,
        target_ids: list[str] | None = None,
        universe: str | None = None,
        title: str | None = None,
        card: str | None = None,
        short_card_description: str | None = None,
        score: str | None = None,
        personal_names_ids: list[str] | None = None,
    ):
        self.id = id
        self.target_ids = target_ids or []
        self.universe = universe
        self.title = title
        self.card = card
        self.short_card_description = short_card_description
        self.score = score
        self.personal_names_ids = personal_names_ids or []
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


class Target:
    """Модель фразы (таргета)."""

    # - id: str UUID like (deterministic from content)
    # - item_id: str UUID like
    # - content: str
    # - explanation: str
    # - level: str
    # - score: float
    def __init__(
        self,
        id: str,
        item_id: str,
        content: str,
        explanation: str,
        level: str,
        score: float,
    ):
        self.id = id
        self.item_id = item_id
        self.content = content
        self.explanation = explanation
        self.level = level
        self.score = score
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


class PersonalName(PersonalNameSchema):
    """Pydantic model for PersonalName rows."""


class Thread:
    """Модель потока (чата)."""

    def __init__(self, id: int, user_id: int, title: str):
        self.id = id
        self.user_id = user_id
        self.title = title
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


class Message:
    """Модель сообщения."""

    def __init__(
        self,
        id: int,
        thread_id: int,
        text: str,
        message_type: MessageTypeEnum,
        rating: MessageRatingEnum | None = None,
    ):
        self.id = id
        self.thread_id = thread_id
        self.text = text
        self.message_type = message_type
        self.rating = rating
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


class KV:
    """Модель для KV-хранилища."""

    def __init__(self, key: str, value: str | None = None):
        self.key = key
        self.value = value


class PipelineLog:
    """Модель лога для операций, связанных непосредственно с пайплайном LLM."""

    def __init__(
        self,
        message_id: str | None = None,
        log_id: str | None = None,
        log_data: str | None = None,
        log_owner: str | None = None,
        log_datatime: int | None = None,
        pipeline_version: str | None = None,
    ):
        self.message_id = message_id
        self.log_id = log_id
        self.log_data = log_data
        self.log_owner = log_owner
        self.log_datatime = log_datatime
        self.pipeline_version = pipeline_version


class BackLog:
    """Модель лога для всех вспомогательных действий (BD-операции, нажатия кнопок, ошибки и т.д.)."""

    def __init__(
        self,
        log_id: str | None = None,
        log_data: str | None = None,
        log_owner: str | None = None,
        log_datatime: int | None = None,
    ):
        self.log_id = log_id
        self.log_data = log_data
        self.log_owner = log_owner
        self.log_datatime = log_datatime
