"""Role model - Snowflake ROLE object aligned with the resource schema."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from schemadrift.core.base_object import ObjectScope, SnowflakeObject


@dataclass
class Role(SnowflakeObject):
    """Snowflake ROLE object.

    Field names match the Snowflake resource schema (Python API / REST API /
    ``DESCRIBE AS RESOURCE``).  Generic ``extract()``, ``from_dict()``, and
    ``to_dict()`` from the base class handle serialization automatically.
    """

    OBJECT_TYPE: ClassVar[str] = "ROLE"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.ACCOUNT

    name: str
    comment: str | None = None
