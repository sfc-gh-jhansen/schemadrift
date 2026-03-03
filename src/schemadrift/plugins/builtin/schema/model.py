"""Schema model - Snowflake SCHEMA object aligned with the resource schema."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from schemadrift.core.base_object import ObjectScope, SnowflakeObject


@dataclass
class Schema(SnowflakeObject):
    """Snowflake SCHEMA object.

    Field names match the Snowflake resource schema (Python API / REST API /
    ``DESCRIBE AS RESOURCE``).  Generic ``extract()``, ``from_dict()``, and
    ``to_dict()`` from the base class handle serialization automatically.
    """

    OBJECT_TYPE: ClassVar[str] = "SCHEMA"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.DATABASE

    name: str
    database_name: str = ""
    kind: str | None = "PERMANENT"
    managed_access: bool = False
    data_retention_time_in_days: int | None = None
    default_ddl_collation: str | None = None
    comment: str | None = None
