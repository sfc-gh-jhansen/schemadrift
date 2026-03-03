"""Database model - Snowflake DATABASE object aligned with the resource schema."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from schemadrift.core.base_object import ObjectScope, SnowflakeObject


@dataclass
class Database(SnowflakeObject):
    """Snowflake DATABASE object.

    Field names match the Snowflake resource schema (Python API / REST API /
    ``DESCRIBE AS RESOURCE``).  Generic ``extract()``, ``from_dict()``, and
    ``to_dict()`` from the base class handle serialization automatically.
    """

    OBJECT_TYPE: ClassVar[str] = "DATABASE"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.ACCOUNT

    name: str
    kind: str | None = "PERMANENT"
    data_retention_time_in_days: int | None = None
    max_data_extension_time_in_days: int | None = None
    default_ddl_collation: str | None = None
    comment: str | None = None
