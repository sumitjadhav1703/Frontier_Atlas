"""Five Pydantic v2 record types with employer casing and nesting."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal, Union

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)

SCHEMA_VERSION: Literal["1.0"] = "1.0"


class RecordType(str, Enum):
    STARTUP = "STARTUP"
    PRODUCT = "PRODUCT"
    RESEARCH_PAPER = "RESEARCH_PAPER"
    JOB = "JOB"
    NEWS = "NEWS"


# Convenience aliases matching gate language.
STARTUP = RecordType.STARTUP
PRODUCT = RecordType.PRODUCT
RESEARCH_PAPER = RecordType.RESEARCH_PAPER
JOB = RecordType.JOB
NEWS = RecordType.NEWS


class PricingModel(str, Enum):
    FREE = "FREE"
    FREEMIUM = "FREEMIUM"
    PAID = "PAID"
    ENTERPRISE = "ENTERPRISE"


class _ExactModel(BaseModel):
    """Reject unknown keys; do not invent defaults for required fields."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=False,
        validate_assignment=True,
        # Employer JSON uses exact keys; do not accept alternate cases via aliases.
        populate_by_name=False,
    )


class SourceInfo(_ExactModel):
    name: str
    url: str


def _reject_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware (UTC)")
    return value


class StartupData(_ExactModel):
    employeeCount: Annotated[int | None, Field(default=None, ge=0)]


class StartupContent(_ExactModel):
    entityName: str
    data: StartupData


class StartupRecord(_ExactModel):
    schemaVersion: Literal["1.0"]
    source: SourceInfo
    content: StartupContent
    collectedAt: AwareDatetime

    @field_validator("collectedAt")
    @classmethod
    def _aware_collected(cls, value: datetime) -> datetime:
        return _reject_naive(value)


class ProductContent(_ExactModel):
    startupName: str
    pricingModel: PricingModel


class ProductRecord(_ExactModel):
    schemaVersion: Literal["1.0"]
    source: SourceInfo
    content: ProductContent
    collectedAt: AwareDatetime

    @field_validator("collectedAt")
    @classmethod
    def _aware_collected(cls, value: datetime) -> datetime:
        return _reject_naive(value)


class ResearchPaperContent(_ExactModel):
    title: str
    authors: list[str]
    paper_url: str
    github_url: str
    github_stars: Annotated[int, Field(ge=0)]
    published_date: AwareDatetime

    @field_validator("published_date")
    @classmethod
    def _aware_published(cls, value: datetime) -> datetime:
        return _reject_naive(value)

    @field_validator("authors")
    @classmethod
    def _authors_nonempty(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("authors must be a non-empty list")
        return value


class ResearchPaperRecord(_ExactModel):
    schemaVersion: Literal["1.0"]
    source: SourceInfo
    content: ResearchPaperContent
    collectedAt: AwareDatetime

    @field_validator("collectedAt")
    @classmethod
    def _aware_collected(cls, value: datetime) -> datetime:
        return _reject_naive(value)


class JobContent(_ExactModel):
    company: str
    date: AwareDatetime
    is_remote: StrictBool
    role_family: str

    @field_validator("date")
    @classmethod
    def _aware_date(cls, value: datetime) -> datetime:
        return _reject_naive(value)

    @field_validator("is_remote", mode="before")
    @classmethod
    def _reject_int_bool(cls, value: Any) -> Any:
        if isinstance(value, int) and not isinstance(value, bool):
            raise ValueError("is_remote must be a boolean, not an integer")
        return value


class JobRecord(_ExactModel):
    schemaVersion: Literal["1.0"]
    source: SourceInfo
    content: JobContent
    collectedAt: AwareDatetime

    @field_validator("collectedAt")
    @classmethod
    def _aware_collected(cls, value: datetime) -> datetime:
        return _reject_naive(value)


class NewsContent(_ExactModel):
    title: str
    date: AwareDatetime
    text: str
    article_url: str

    @field_validator("date")
    @classmethod
    def _aware_date(cls, value: datetime) -> datetime:
        return _reject_naive(value)


class NewsRecord(_ExactModel):
    schemaVersion: Literal["1.0"]
    source: SourceInfo
    content: NewsContent
    collectedAt: AwareDatetime

    @field_validator("collectedAt")
    @classmethod
    def _aware_collected(cls, value: datetime) -> datetime:
        return _reject_naive(value)


AnyRecord = Union[
    StartupRecord,
    ProductRecord,
    ResearchPaperRecord,
    JobRecord,
    NewsRecord,
]

_RECORD_BY_TYPE: dict[RecordType, type[AnyRecord]] = {
    RecordType.STARTUP: StartupRecord,
    RecordType.PRODUCT: ProductRecord,
    RecordType.RESEARCH_PAPER: ResearchPaperRecord,
    RecordType.JOB: JobRecord,
    RecordType.NEWS: NewsRecord,
}


class TypedEnvelope(_ExactModel):
    """Optional wrapper when fixtures include an explicit recordType discriminator."""

    recordType: RecordType
    payload: dict[str, Any]

    @model_validator(mode="after")
    def _validate_payload(self) -> TypedEnvelope:
        parse_record(self.recordType, self.payload)
        return self


def parse_record(record_type: RecordType | str, data: dict[str, Any]) -> AnyRecord:
    """Validate a payload for the given record type; raise ValidationError on failure."""
    if isinstance(record_type, str):
        record_type = RecordType(record_type)
    model = _RECORD_BY_TYPE[record_type]
    return model.model_validate(data)
