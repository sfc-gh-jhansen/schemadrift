"""Table model - Snowflake TABLE object aligned with the resource schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from schemadrift.core.base_object import ObjectScope, SnowflakeObject


@dataclass
class Table(SnowflakeObject):
    """Snowflake TABLE object.

    Field names match the Snowflake resource schema (Python API / REST API /
    ``DESCRIBE AS RESOURCE``).  Generic ``extract()``, ``from_dict()``, and
    ``to_dict()`` from the base class handle serialization automatically.
    """

    OBJECT_TYPE: ClassVar[str] = "TABLE"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.SCHEMA

    name: str
    database_name: str = ""
    schema_name: str = ""
    columns: list[dict] = field(default_factory=list)
    constraints: list[dict] = field(default_factory=list)
    change_tracking: bool = False
    cluster_by: list[str] = field(default_factory=list)
    comment: str | None = None
    data_retention_time_in_days: int | None = None
    default_ddl_collation: str | None = None
    enable_schema_evolution: bool | None = None
    kind: str | None = "PERMANENT"
    max_data_extension_time_in_days: int | None = None
