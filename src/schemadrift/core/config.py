"""Configuration for schemadrift.

This module provides the data model and loader for the project configuration
file (schemadrift.toml). The config defines which Snowflake objects
are in scope for comparison and management, and per-object-type settings
such as exclusion rules and name mappings.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

from schemadrift.core.base_object import SnowflakeObject

if TYPE_CHECKING:
    from schemadrift.core.base_object import ObjectScope


# =============================================================================
# Object renaming (multi-environment name mapping)
# =============================================================================


@dataclass
class ObjectRenamer:
    """Translates logical object names to physical Snowflake names and back.

    Supports two levels of mapping:

    - **Account-level**: databases, warehouses, roles, and other
      account-scoped objects.  Configured via ``[name_mapping.account]``.
    - **Schema-level**: schemas within a specific logical database.
      Configured via ``[name_mapping.schemas.<LOGICAL_DB>]``.

    The renamer is scope-aware: :meth:`to_physical_identifier` and
    :meth:`to_logical_identifier` accept an :class:`ObjectScope` so they
    know which components of a fully qualified identifier to rename.
    Leaf object names (e.g. view or table names) are never renamed.
    """

    _acct_to_physical: dict[str, str] = field(default_factory=dict)
    _acct_to_logical: dict[str, str] = field(default_factory=dict)
    _schema_to_physical: dict[str, dict[str, str]] = field(default_factory=dict)
    _schema_to_logical: dict[str, dict[str, str]] = field(default_factory=dict)

    @classmethod
    def identity(cls) -> ObjectRenamer:
        """Return a no-op renamer (all names pass through unchanged)."""
        return cls()

    @classmethod
    def from_config(
        cls,
        account: dict[str, str],
        schemas: dict[str, dict[str, str]],
    ) -> ObjectRenamer:
        """Build a renamer from parsed config data.

        Args:
            account: Mapping of logical -> physical for account-level names.
            schemas: Mapping of logical_db -> {logical_schema -> physical_schema}.
        """
        acct_to_physical = {k.upper(): v.upper() for k, v in account.items()}
        acct_vals = list(acct_to_physical.values())
        dupes = {v for v in acct_vals if acct_vals.count(v) > 1}
        if dupes:
            raise ValueError(
                f"Non-bijective account name mapping: multiple logical names "
                f"map to the same physical name(s): {', '.join(sorted(dupes))}"
            )
        acct_to_logical = {v: k for k, v in acct_to_physical.items()}

        schema_to_physical: dict[str, dict[str, str]] = {}
        schema_to_logical: dict[str, dict[str, str]] = {}
        for db, mapping in schemas.items():
            fwd = {s.upper(): v.upper() for s, v in mapping.items()}
            vals = list(fwd.values())
            schema_dupes = {v for v in vals if vals.count(v) > 1}
            if schema_dupes:
                raise ValueError(
                    f"Non-bijective schema name mapping for database "
                    f"{db.upper()}: multiple logical schemas map to the same "
                    f"physical name(s): {', '.join(sorted(schema_dupes))}"
                )
            schema_to_physical[db.upper()] = fwd
            schema_to_logical[db.upper()] = {v: k for k, v in fwd.items()}
        return cls(acct_to_physical, acct_to_logical, schema_to_physical, schema_to_logical)

    @property
    def is_identity(self) -> bool:
        """True when no mappings are configured."""
        return not self._acct_to_physical and not self._schema_to_physical

    # -- single-component helpers -------------------------------------------

    def to_physical_account(self, logical_name: str) -> str:
        """Map a logical account-level name to its physical Snowflake name."""
        return self._acct_to_physical.get(logical_name.upper(), logical_name)

    def to_logical_account(self, physical_name: str) -> str:
        """Map a physical Snowflake account-level name back to its logical name."""
        return self._acct_to_logical.get(physical_name.upper(), physical_name)

    def to_physical_schema(self, logical_db: str, logical_schema: str) -> str:
        """Map a logical schema name to its physical name within *logical_db*."""
        db_schemas = self._schema_to_physical.get(logical_db.upper(), {})
        return db_schemas.get(logical_schema.upper(), logical_schema)

    def to_logical_schema(self, logical_db: str, physical_schema: str) -> str:
        """Map a physical schema name back to its logical name within *logical_db*."""
        db_schemas = self._schema_to_logical.get(logical_db.upper(), {})
        return db_schemas.get(physical_schema.upper(), physical_schema)

    # -- full-identifier helpers --------------------------------------------

    def to_physical_identifier(self, scope: ObjectScope, identifier: str) -> str:
        """Forward-map the appropriate components of a fully qualified identifier.

        - ACCOUNT scope: rename the single name via account mapping.
        - DATABASE scope: rename both parts (database + schema name).
        - SCHEMA scope: rename database + schema, keep the leaf object name.
        """
        parts = SnowflakeObject.parse_fully_qualified_name(identifier)
        return self._apply_to_physical(scope, parts)

    def to_logical_identifier(self, scope: ObjectScope, identifier: str) -> str:
        """Reverse-map the appropriate components of a fully qualified identifier."""
        parts = SnowflakeObject.parse_fully_qualified_name(identifier)
        return self._apply_to_logical(scope, parts)

    # -- private apply helpers ----------------------------------------------

    def _apply_to_physical(self, scope: ObjectScope, parts: tuple[str, ...]) -> str:
        from schemadrift.core.base_object import ObjectScope as OS

        if scope == OS.ACCOUNT:
            return self.to_physical_account(parts[0])
        elif scope == OS.DATABASE and len(parts) >= 2:
            db_logical = parts[0]
            return f"{self.to_physical_account(db_logical)}.{self.to_physical_schema(db_logical, parts[1])}"
        elif scope == OS.SCHEMA and len(parts) >= 3:
            db_logical = parts[0]
            return (
                f"{self.to_physical_account(db_logical)}"
                f".{self.to_physical_schema(db_logical, parts[1])}"
                f".{parts[2]}"
            )
        return ".".join(parts)

    def _apply_to_logical(self, scope: ObjectScope, parts: tuple[str, ...]) -> str:
        from schemadrift.core.base_object import ObjectScope as OS

        if scope == OS.ACCOUNT:
            return self.to_logical_account(parts[0])
        elif scope == OS.DATABASE and len(parts) >= 2:
            logical_db = self.to_logical_account(parts[0])
            return f"{logical_db}.{self.to_logical_schema(logical_db, parts[1])}"
        elif scope == OS.SCHEMA and len(parts) >= 3:
            logical_db = self.to_logical_account(parts[0])
            return (
                f"{logical_db}"
                f".{self.to_logical_schema(logical_db, parts[1])}"
                f".{parts[2]}"
            )
        return ".".join(parts)


# =============================================================================
# Per-object-type configuration
# =============================================================================


@dataclass
class ObjectTypeConfig:
    """Per-object-type settings.

    Attributes:
        exclude_names: Exact object names to exclude from management.
        exclude_prefixes: Name prefixes to exclude from management.
    """

    exclude_names: set[str] = field(default_factory=set)
    exclude_prefixes: tuple[str, ...] = ()


DEFAULT_OBJECT_TYPE_CONFIGS: dict[str, ObjectTypeConfig] = {
    "DATABASE": ObjectTypeConfig(
        exclude_names={"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"},
        exclude_prefixes=("USER$",),
    ),
    "SCHEMA": ObjectTypeConfig(
        exclude_names={"INFORMATION_SCHEMA", "PUBLIC"},
    ),
}


def is_managed(
    object_type: str,
    identifier: str,
    config: ProjectConfig | None = None,
) -> bool:
    """Check if an identifier should be managed (not excluded).

    Extracts the last component of the fully qualified name and checks it
    against the exclusion rules for the object type. When a config is
    provided its merged object_types are used; otherwise falls back to
    built-in defaults.

    Args:
        object_type: The Snowflake object type (e.g. 'DATABASE', 'SCHEMA').
        identifier: Fully qualified object name.
        config: Project configuration. When None, built-in defaults are used.

    Returns:
        True if the object should be managed, False if excluded.
    """
    key = object_type.upper()
    if config and key in config.object_types:
        ot_config = config.object_types[key]
    else:
        ot_config = DEFAULT_OBJECT_TYPE_CONFIGS.get(key, ObjectTypeConfig())

    name = identifier.rsplit(".", 1)[-1].upper()
    if name in ot_config.exclude_names:
        return False
    if ot_config.exclude_prefixes and name.startswith(ot_config.exclude_prefixes):
        return False
    return True


# =============================================================================
# Target and project configuration
# =============================================================================


@dataclass
class TargetConfig:
    """A single target scope for comparison and management.

    Defines which Snowflake objects to manage. The combination of fields
    determines the scope:

    - No database: account/org-level objects only (e.g., DATABASE, WAREHOUSE, ROLE).
    - database set, no schemas: the database + all schemas + all schema-level objects.
    - database + schemas: only schema-level objects within those schemas (e.g., VIEWs).
      The schemas themselves are not extracted; use object_types = ["SCHEMA"] for that.
    - object_types always acts as an additional filter at any level.

    Attributes:
        database: Database name, or None for account/org-level targets.
        schemas: List of schema names to include, or None for all schemas.
                 Ignored when database is None.
        object_types: List of object types to include (e.g., ["VIEW", "SCHEMA"]),
                      or None for all registered types.
    """

    database: str | None = None
    schemas: list[str] | None = None
    object_types: list[str] | None = None


@dataclass
class ProjectConfig:
    """Project-level configuration.

    Attributes:
        targets: List of target scopes to manage.
        object_types: Per-object-type settings (exclusion rules, etc.).
                      Built-in defaults are merged with user overrides.
        object_renamer: Renamer for translating logical names to physical
                        Snowflake names. Identity (no-op) when no
                        ``[name_mapping]`` section is configured.
    """

    targets: list[TargetConfig] = field(default_factory=list)
    object_types: dict[str, ObjectTypeConfig] = field(default_factory=dict)
    object_renamer: ObjectRenamer = field(default_factory=ObjectRenamer.identity)


# =============================================================================
# TOML loading
# =============================================================================


def load_config(path: Path) -> ProjectConfig:
    """Load project configuration from a TOML file.

    Args:
        path: Path to the TOML config file.

    Returns:
        A ProjectConfig instance.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        ValueError: If the config file is invalid.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "rb") as f:
        data = tomllib.load(f)

    return ProjectConfig(
        targets=_parse_targets(data.get("targets", [])),
        object_types=_parse_object_types(data.get("object_types", {})),
        object_renamer=_parse_name_mapping(data.get("name_mapping", {})),
    )


def _parse_targets(raw: list) -> list[TargetConfig]:
    """Parse ``[[targets]]`` TOML entries into a list of :class:`TargetConfig`.

    All identifiers (database, schemas, object_types) are normalized to
    uppercase.
    """
    targets = []
    for entry in raw:
        if not isinstance(entry, dict):
            raise ValueError(
                f"Each [[targets]] entry must be a table, got: {type(entry).__name__}"
            )

        object_types = entry.get("object_types")
        if object_types is not None:
            object_types = [t.upper() for t in object_types]

        schemas = entry.get("schemas")
        if schemas is not None:
            schemas = [s.upper() for s in schemas]

        database = entry.get("database")
        if database is not None:
            database = database.upper()

        targets.append(
            TargetConfig(
                database=database,
                schemas=schemas,
                object_types=object_types,
            )
        )

    if not targets:
        raise ValueError("Config file must define at least one [[targets]] entry")

    return targets


def _parse_object_types(
    raw: dict[str, dict],
) -> dict[str, ObjectTypeConfig]:
    """Parse [object_types.*] TOML sections and merge with built-in defaults.

    User values are merged additively: user exclude_names are unioned with
    defaults, and user exclude_prefixes are appended to defaults.
    """
    merged = deepcopy(DEFAULT_OBJECT_TYPE_CONFIGS)

    for type_name, settings in raw.items():
        key = type_name.upper()
        user_names = {n.upper() for n in settings.get("exclude_names", [])}
        user_prefixes = tuple(p.upper() for p in settings.get("exclude_prefixes", []))

        if key in merged:
            existing = merged[key]
            merged[key] = ObjectTypeConfig(
                exclude_names=existing.exclude_names | user_names,
                exclude_prefixes=existing.exclude_prefixes + user_prefixes,
            )
        else:
            merged[key] = ObjectTypeConfig(
                exclude_names=user_names,
                exclude_prefixes=user_prefixes,
            )

    return merged


def _parse_name_mapping(raw: dict) -> ObjectRenamer:
    """Parse ``[name_mapping]`` TOML section into an :class:`ObjectRenamer`.

    Expected structure::

        [name_mapping.account]
        SOURCE = "SOURCE_DEV"

        [name_mapping.schemas.SOURCE]
        RAW = "RAW_DEV"

    Returns ``ObjectRenamer.identity()`` when the section is empty.
    """
    if not raw:
        return ObjectRenamer.identity()

    account: dict[str, str] = {}
    for key, value in raw.get("account", {}).items():
        if not isinstance(value, str):
            raise ValueError(
                f"[name_mapping.account] values must be strings, "
                f"got {type(value).__name__} for key '{key}'"
            )
        account[key] = value

    schemas: dict[str, dict[str, str]] = {}
    for db_name, mapping in raw.get("schemas", {}).items():
        if not isinstance(mapping, dict):
            raise ValueError(
                f"[name_mapping.schemas.{db_name}] must be a table, "
                f"got {type(mapping).__name__}"
            )
        for key, value in mapping.items():
            if not isinstance(value, str):
                raise ValueError(
                    f"[name_mapping.schemas.{db_name}] values must be strings, "
                    f"got {type(value).__name__} for key '{key}'"
                )
        schemas[db_name] = mapping

    if not account and not schemas:
        return ObjectRenamer.identity()

    return ObjectRenamer.from_config(account, schemas)
