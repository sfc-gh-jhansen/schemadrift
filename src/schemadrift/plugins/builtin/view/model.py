"""View model - Snowflake VIEW object aligned with the resource schema."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from schemadrift.core.base_object import ObjectScope, SnowflakeObject

if TYPE_CHECKING:
    from schemadrift.connection.interface import SnowflakeConnectionInterface

_DDL_PREAMBLE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(?:SECURE\s+)?(?:RECURSIVE\s+)?"
    r"VIEW\s+\S+(?:\s*\([^)]*\))?\s+AS\s+",
    re.IGNORECASE | re.DOTALL,
)

_COLUMN_READ_ONLY_KEYS = frozenset({"datatype"})


@dataclass
class View(SnowflakeObject):
    """Snowflake VIEW object.

    Field names match the Snowflake resource schema (Python API / REST API /
    ``DESCRIBE AS RESOURCE``).  Generic ``from_dict()`` from the base class
    handles deserialization; ``extract()`` and ``to_dict()`` are overridden
    to handle the DDL-wrapped query and nested read-only column fields.
    """

    OBJECT_TYPE: ClassVar[str] = "VIEW"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.SCHEMA

    name: str
    database_name: str = ""
    schema_name: str = ""
    query: str = ""
    columns: list[dict] = field(default_factory=list)
    secure: bool = False
    kind: str | None = "PERMANENT"
    recursive: bool | None = None
    comment: str | None = None

    @staticmethod
    def _strip_ddl_wrapper(query: str) -> str:
        """Strip the ``CREATE ... VIEW ... AS`` preamble if present.

        Snowflake's ``DESCRIBE AS RESOURCE`` returns the full DDL, but the
        REST API create endpoint expects only the query body.
        """
        m = _DDL_PREAMBLE_RE.match(query)
        if m:
            body = query[m.end():]
            return body.rstrip().rstrip(";").rstrip()
        return query

    @staticmethod
    def _strip_column_read_only(columns: list[dict]) -> list[dict]:
        """Return a copy of *columns* with read-only keys removed."""
        return [
            {k: v for k, v in col.items() if k not in _COLUMN_READ_ONLY_KEYS}
            for col in columns
        ]

    @classmethod
    def extract(
        cls,
        connection: SnowflakeConnectionInterface,
        identifier: str,
    ) -> View:
        """Extract a view, stripping the DDL wrapper from the query."""
        view: View = super().extract(connection, identifier)  # type: ignore[assignment]
        view.query = cls._strip_ddl_wrapper(view.query)
        view.columns = cls._strip_column_read_only(view.columns)
        return view
