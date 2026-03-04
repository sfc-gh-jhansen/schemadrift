"""Abstract base class for all Snowflake objects."""

from __future__ import annotations

import dataclasses
from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from schemadrift.connection.interface import SnowflakeConnectionInterface
    from schemadrift.core.diff_strategy import DiffStrategy


class ObjectScope(Enum):
    """Enumeration of Snowflake object scope levels.

    Represents the hierarchical level at which an object exists:
    Organization -> Account -> Database -> Schema
    """

    ORGANIZATION = "organization"
    ACCOUNT = "account"
    DATABASE = "database"
    SCHEMA = "schema"


# =============================================================================
# Custom Exceptions
# =============================================================================


class SnowflakeObjectError(Exception):
    """Base exception for Snowflake object operations."""

    pass


class ObjectNotFoundError(SnowflakeObjectError):
    """Raised when an object cannot be found in Snowflake."""

    def __init__(self, object_type: str, identifier: str):
        self.object_type = object_type
        self.identifier = identifier
        super().__init__(f"{object_type} not found: {identifier}")


class ObjectExtractionError(SnowflakeObjectError):
    """Raised when extraction of an object fails."""

    def __init__(self, object_type: str, identifier: str, cause: Exception | None = None):
        self.object_type = object_type
        self.identifier = identifier
        self.cause = cause
        msg = f"Failed to extract {object_type} '{identifier}'"
        if cause:
            msg += f": {cause}"
        super().__init__(msg)


class ObjectParseError(SnowflakeObjectError):
    """Raised when parsing DDL or dict data fails."""

    def __init__(self, object_type: str, source: str, cause: Exception | None = None):
        self.object_type = object_type
        self.source = source
        self.cause = cause
        msg = f"Failed to parse {object_type} from {source}"
        if cause:
            msg += f": {cause}"
        super().__init__(msg)


_NOT_FOUND_PATTERNS = (
    "does not exist",
    "object does not exist",
    "not found",
    "unknown",
)

_NOT_FOUND_SQL_CODES = {
    2003,   # Object does not exist
    2043,   # Object does not exist or not authorized
    2082,   # Does not exist or not authorized
}


def _is_object_not_found_error(exc: BaseException) -> bool:
    """Return True if *exc* is a Snowflake connector error for a missing object.

    Checks ``errno`` (if present) against known SQL error codes, and falls
    back to substring matching on the error message.
    """
    errno = getattr(exc, "errno", None)
    if errno is not None and errno in _NOT_FOUND_SQL_CODES:
        return True
    msg = str(exc).lower()
    return any(pat in msg for pat in _NOT_FOUND_PATTERNS)


@dataclass
class ObjectDiff:
    """Represents the differences between two Snowflake objects.

    Attributes:
        added: Dictionary of attributes that exist in target but not source.
        removed: Dictionary of attributes that exist in source but not target.
        modified: Dictionary of attributes that differ between source and target.
                  Keys are attribute names, values are tuples of (source_value, target_value).
    """

    added: dict[str, Any] = field(default_factory=dict)
    removed: dict[str, Any] = field(default_factory=dict)
    modified: dict[str, tuple[Any, Any]] = field(default_factory=dict)

    @property
    def has_changes(self) -> bool:
        """Check if there are any differences."""
        return bool(self.added or self.removed or self.modified)


@dataclass
class SnowflakeObject(ABC):
    """Abstract base class for all Snowflake objects.

    Provides a consistent interface for extraction, serialization, comparison,
    and DDL generation across all object types.  Subclasses declare dataclass
    fields whose names match Snowflake's resource schema; the generic
    ``extract()``, ``from_dict()``, and ``to_dict()`` implementations use
    field introspection so that most plugins need zero custom serialization
    code.

    Class Attributes:
        OBJECT_TYPE: The Snowflake object type identifier (e.g., 'DATABASE').
        SCOPE: The scope level where this object type exists.
        DIFF_STRATEGY: The diff strategy to use.  Defaults to DefaultDiffStrategy.
        EXCLUDE_FIELDS: Per-plugin fields to exclude from serialized output,
            merged with ``_DEFAULT_EXCLUDE_FIELDS`` at serialization time.
        CONTEXTUAL_FIELDS: Scope/identity fields inferred from the file path.
            Present on the in-memory object but excluded from YAML output.

    Scope/field contract (enforced at class-definition time):
        - DATABASE or SCHEMA scope requires a ``database_name`` field.
        - SCHEMA scope additionally requires a ``schema_name`` field.
    """

    # Class-level constants to be defined by subclasses
    OBJECT_TYPE: ClassVar[str] = ""
    SCOPE: ClassVar[ObjectScope] = ObjectScope.SCHEMA
    DIFF_STRATEGY: ClassVar[DiffStrategy | None] = None

    _DEFAULT_EXCLUDE_FIELDS: ClassVar[frozenset[str]] = frozenset({
        "created_on", "dropped_on", "owner", "owner_role_type",
        "is_default", "is_current", "origin", "options",
    })
    EXCLUDE_FIELDS: ClassVar[frozenset[str]] = frozenset()
    CONTEXTUAL_FIELDS: ClassVar[frozenset[str]] = frozenset({
        "database_name", "schema_name",
    })

    # Common attributes all objects have
    name: str

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "SCOPE" not in cls.__dict__:
            return
        scope: ObjectScope = cls.SCOPE
        all_annotations: dict[str, Any] = {}
        for klass in reversed(cls.__mro__):
            all_annotations.update(getattr(klass, "__annotations__", {}))
        if scope in (ObjectScope.DATABASE, ObjectScope.SCHEMA):
            if "database_name" not in all_annotations:
                raise TypeError(
                    f"{cls.__name__} declares SCOPE={scope.value} "
                    "but does not define a 'database_name' field"
                )
        if scope == ObjectScope.SCHEMA:
            if "schema_name" not in all_annotations:
                raise TypeError(
                    f"{cls.__name__} declares SCOPE={scope.value} "
                    "but does not define a 'schema_name' field"
                )

    @property
    def object_type(self) -> str:
        """Return the object type identifier."""
        return self.OBJECT_TYPE

    def get_database(self) -> str | None:
        """Return the database this object belongs to."""
        if self.SCOPE in (ObjectScope.DATABASE, ObjectScope.SCHEMA):
            return getattr(self, "database_name")
        return None

    def get_schema(self) -> str | None:
        """Return the schema this object belongs to."""
        if self.SCOPE == ObjectScope.SCHEMA:
            return getattr(self, "schema_name")
        return None

    @property
    def fully_qualified_name(self) -> str:
        """Return the fully qualified name of the object.

        Built automatically from SCOPE + name/database_name/schema_name.
        Subclasses may override for non-standard naming.
        """
        if self.SCOPE == ObjectScope.ACCOUNT:
            return self.normalize_identifier(self.name)
        elif self.SCOPE == ObjectScope.DATABASE:
            db = self.normalize_identifier(getattr(self, "database_name"))
            return f"{db}.{self.normalize_identifier(self.name)}"
        elif self.SCOPE == ObjectScope.SCHEMA:
            db = self.normalize_identifier(getattr(self, "database_name"))
            schema = self.normalize_identifier(getattr(self, "schema_name"))
            return f"{db}.{schema}.{self.normalize_identifier(self.name)}"
        return self.normalize_identifier(self.name)

    def __str__(self) -> str:
        """Return string representation."""
        return f"{self.OBJECT_TYPE} {self.fully_qualified_name}"

    def __repr__(self) -> str:
        """Return detailed representation."""
        return f"{type(self).__name__}(name={self.name!r})"

    # ---- identifier helpers ------------------------------------------------

    @staticmethod
    def normalize_identifier(identifier: str) -> str:
        """Normalize a Snowflake identifier.

        Handles quoted and unquoted identifiers according to Snowflake rules:
        - Unquoted identifiers are uppercased
        - Quoted identifiers preserve case
        """
        if not identifier:
            return identifier
        if identifier.startswith('"') and identifier.endswith('"'):
            return identifier
        elif any(c in identifier for c in [" ", "-", "."]) or identifier[0].isdigit():
            return f'"{identifier}"'
        else:
            return identifier.upper()

    @staticmethod
    def parse_fully_qualified_name(fqn: str) -> tuple[str, ...]:
        """Parse a fully qualified name into components.

        Handles quoted identifiers correctly.

        Examples:
            >>> SnowflakeObject.parse_fully_qualified_name('MYDB.MYSCHEMA.MYVIEW')
            ('MYDB', 'MYSCHEMA', 'MYVIEW')
        """
        parts: list[str] = []
        current: list[str] = []
        in_quotes = False

        for char in fqn:
            if char == '"':
                in_quotes = not in_quotes
                current.append(char)
            elif char == "." and not in_quotes:
                parts.append("".join(current))
                current = []
            else:
                current.append(char)

        if current:
            parts.append("".join(current))

        return tuple(SnowflakeObject.normalize_identifier(p) for p in parts)

    # ---- extraction helpers ------------------------------------------------

    @classmethod
    def _describe_as_resource(
        cls,
        connection: SnowflakeConnectionInterface,
        identifier: str,
    ) -> dict[str, Any]:
        """Execute ``DESCRIBE AS RESOURCE`` and return the parsed JSON dict.

        Handles the SQL call, single-row/single-column JSON extraction, and
        error wrapping so that each plugin only needs to map the resulting
        dict to its own fields.

        Raises:
            ObjectNotFoundError: If the object does not exist (either empty
                result set or a Snowflake SQL error indicating the object
                was not found).
            ObjectExtractionError: If the result cannot be parsed as JSON.
        """
        import json

        sql = f"DESCRIBE AS RESOURCE {cls.OBJECT_TYPE} {identifier}"
        try:
            results = connection.execute(sql)
        except Exception as exc:
            if _is_object_not_found_error(exc):
                raise ObjectNotFoundError(cls.OBJECT_TYPE, identifier) from exc
            raise ObjectExtractionError(cls.OBJECT_TYPE, identifier, exc) from exc

        if not results:
            raise ObjectNotFoundError(cls.OBJECT_TYPE, identifier)

        row = results[0]
        json_str = list(row.values())[0] if isinstance(row, dict) else row[0]

        try:
            data = json.loads(json_str) if isinstance(json_str, str) else json_str
        except (json.JSONDecodeError, TypeError) as exc:
            raise ObjectExtractionError(cls.OBJECT_TYPE, identifier, exc) from exc

        return data

    # ---- listing helpers ---------------------------------------------------

    @classmethod
    def _show_objects(
        cls,
        connection: SnowflakeConnectionInterface,
        scope: str,
    ) -> list[str]:
        """Execute a ``SHOW`` command and return fully qualified names."""
        plural = cls.OBJECT_TYPE + "S"

        if cls.SCOPE in (ObjectScope.ORGANIZATION, ObjectScope.ACCOUNT):
            sql = f"SHOW {plural}"
        elif cls.SCOPE == ObjectScope.DATABASE:
            sql = f"SHOW {plural} IN DATABASE {scope}"
        else:
            sql = f"SHOW {plural} IN {scope}"

        results = connection.execute(sql)

        objects: list[str] = []
        for row in results:
            name = row.get("name", "")
            if not name:
                continue
            if cls.SCOPE in (ObjectScope.ORGANIZATION, ObjectScope.ACCOUNT):
                objects.append(name)
            elif cls.SCOPE == ObjectScope.DATABASE:
                db = row.get("database_name", scope)
                objects.append(f"{db}.{name}")
            else:
                db = row.get("database_name", "")
                schema = row.get("schema_name", "")
                objects.append(f"{db}.{schema}.{name}")

        return objects

    @classmethod
    def _show_as_resource_objects(
        cls,
        connection: SnowflakeConnectionInterface,
        scope: str,
    ) -> list[str]:
        """Execute ``SHOW AS RESOURCE TERSE`` and return fully qualified names.

        Works like :meth:`_show_objects` but uses the ``SHOW AS RESOURCE``
        variant which returns JSON rows.  The ``TERSE`` keyword reduces
        the payload to only essential fields.
        """
        import json

        plural = cls.OBJECT_TYPE + "S"

        if cls.SCOPE in (ObjectScope.ORGANIZATION, ObjectScope.ACCOUNT):
            sql = f"SHOW AS RESOURCE TERSE {plural}"
        elif cls.SCOPE == ObjectScope.DATABASE:
            sql = f"SHOW AS RESOURCE TERSE {plural} IN DATABASE {scope}"
        else:
            sql = f"SHOW AS RESOURCE TERSE {plural} IN {scope}"

        results = connection.execute(sql)

        objects: list[str] = []
        for row in results:
            json_str = list(row.values())[0] if isinstance(row, dict) else row[0]
            try:
                data = json.loads(json_str) if isinstance(json_str, str) else json_str
            except (json.JSONDecodeError, TypeError) as exc:
                raise ObjectExtractionError(cls.OBJECT_TYPE, scope, exc) from exc

            name = data.get("name", "")
            if not name:
                continue
            if cls.SCOPE in (ObjectScope.ORGANIZATION, ObjectScope.ACCOUNT):
                objects.append(name)
            elif cls.SCOPE == ObjectScope.DATABASE:
                db = data.get("database_name", scope)
                objects.append(f"{db}.{name}")
            else:
                db = data.get("database_name", "")
                schema = data.get("schema_name", "")
                objects.append(f"{db}.{schema}.{name}")

        return objects

    # ---- generic serialization ---------------------------------------------

    @classmethod
    def _all_field_names(cls) -> set[str]:
        """Return the names of all dataclass fields."""
        return {f.name for f in dataclasses.fields(cls)}

    @staticmethod
    def _strip_none(obj: Any) -> Any:
        """Recursively remove keys whose value is ``None``.

        - Dicts: keys with ``None`` values are dropped.
        - Lists: each element is processed recursively.
        - All other types pass through unchanged.
        """
        if isinstance(obj, dict):
            return {
                k: SnowflakeObject._strip_none(v)
                for k, v in obj.items()
                if v is not None
            }
        if isinstance(obj, list):
            return [SnowflakeObject._strip_none(item) for item in obj]
        return obj

    def to_dict(self) -> dict[str, Any]:
        """Export definition fields as a serializable dictionary.

        Excludes ``CONTEXTUAL_FIELDS``, ``_DEFAULT_EXCLUDE_FIELDS``, and
        per-plugin ``EXCLUDE_FIELDS``.  Keys whose value is ``None`` are
        stripped so the YAML output contains only meaningful attributes.
        """
        exclude = self.CONTEXTUAL_FIELDS | self._DEFAULT_EXCLUDE_FIELDS | self.EXCLUDE_FIELDS
        raw = {
            f.name: getattr(self, f.name)
            for f in dataclasses.fields(self)
            if f.name not in exclude
        }
        return self._strip_none(raw)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        context: dict[str, str] | None = None,
    ) -> SnowflakeObject:
        """Load an object from a dictionary, optionally merging path context.

        Args:
            data: Writable fields from the YAML file.
            context: Contextual fields (database_name, schema_name) derived
                from the file path.  Merged into *data* before construction.
        """
        merged = dict(data)
        if context:
            merged.update(context)
        known = cls._all_field_names()
        filtered = {k: v for k, v in merged.items() if k in known}
        return cls(**filtered)

    @classmethod
    def extract(
        cls,
        connection: SnowflakeConnectionInterface,
        identifier: str,
    ) -> SnowflakeObject:
        """Extract an object definition from Snowflake.

        Uses ``DESCRIBE AS RESOURCE`` and keeps every field that matches
        a declared dataclass field.  Filtering of metadata fields happens
        at serialization time (``to_dict()``), not here.
        """
        raw = cls._describe_as_resource(connection, identifier)
        known = cls._all_field_names()
        filtered = {k: v for k, v in raw.items() if k in known}
        return cls(**filtered)

    @classmethod
    def list_objects(
        cls, connection: SnowflakeConnectionInterface, scope: str
    ) -> list[str]:
        """List all objects of this type within a scope.

        The default implementation uses ``SHOW {OBJECT_TYPE}S``.
        Subclasses may override for non-standard SHOW formats.
        """
        return cls._show_as_resource_objects(connection, scope)

    @classmethod
    def from_ddl(cls, sql: str) -> SnowflakeObject:
        """Parse an object from a CREATE statement.

        Not implemented -- use from_dict() with YAML instead.
        """
        raise NotImplementedError(
            f"{cls.__name__}.from_ddl() is not implemented. "
            "Use from_dict() with YAML definitions instead."
        )

    def to_ddl(self) -> str:
        """Generate a normalized CREATE statement.

        Not implemented -- use to_dict() with YAML instead.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.to_ddl() is not implemented. "
            "Use to_dict() with YAML definitions instead."
        )

    def compare(self, other: SnowflakeObject) -> ObjectDiff:
        """Compare this object with another of the same type.

        Delegates to the class's DIFF_STRATEGY. If none is defined,
        uses the DefaultDiffStrategy.
        """
        from schemadrift.core.diff_strategy import DEFAULT_DIFF_STRATEGY

        strategy = self.DIFF_STRATEGY or DEFAULT_DIFF_STRATEGY
        return strategy.diff(self, other)
