"""Core module - shared foundation for Snowflake object management."""

from schemadrift.core.base_object import (
    ObjectDiff,
    ObjectScope,
    ObjectExtractionError,
    ObjectNotFoundError,
    ObjectParseError,
    SnowflakeObject,
    SnowflakeObjectError,
)
from schemadrift.core.comparison import (
    ComparisonEntry,
    ComparisonStatus,
)
from schemadrift.core.file_manager import FileManager
from schemadrift.core.service import DriftService

__all__ = [
    "SnowflakeObject",
    "ObjectScope",
    "ObjectDiff",
    "SnowflakeObjectError",
    "ObjectNotFoundError",
    "ObjectExtractionError",
    "ObjectParseError",
    "FileManager",
    "DriftService",
    "ComparisonEntry",
    "ComparisonStatus",
]
