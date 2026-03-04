"""High-level service for Snowflake schema drift detection and management.

This module provides the DriftService class which orchestrates
all high-level operations for extracting, comparing, and managing
Snowflake objects. This is the primary API for programmatic use.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from schemadrift.core.base_object import (
    ObjectNotFoundError,
    ObjectScope,
    _is_object_not_found_error,
)
from schemadrift.core.comparison import ComparisonEntry, ComparisonStatus
from schemadrift.core.config import ObjectRenamer, is_managed

if TYPE_CHECKING:
    from schemadrift.connection.interface import SnowflakeConnectionInterface
    from schemadrift.core.base_object import SnowflakeObject
    from schemadrift.core.config import ProjectConfig, TargetConfig
    from schemadrift.core.file_manager import FileManager
    from schemadrift.plugins.manager import PluginDispatcher

logger = logging.getLogger(__name__)


def _exc_summary() -> str:
    """Return a concise summary of the current exception for log messages."""
    exc = sys.exc_info()[1]
    if exc is None:
        return "unknown error"
    return f"{type(exc).__name__}: {exc}"


class DriftService:
    """High-level service for Snowflake schema drift detection and management.

    This class orchestrates all operations, coordinating between:
    - PluginDispatcher (plugin delegation)
    - FileManager (file system operations)
    - SnowflakeConnection (Snowflake communication)

    This is the primary API for programmatic integration with other tools.
    The CLI is a thin wrapper around this service.

    Example:
        ```python
        from schemadrift.core.service import DriftService
        from schemadrift.core.file_manager import FileManager
        from schemadrift.connection import SnowflakeConnection

        conn = SnowflakeConnection(connection_name="myconn")
        fm = FileManager("./snowflake")
        service = DriftService(conn, fm)

        # Extract a view and get its DDL
        ddl = service.extract_as_ddl("VIEW", "MYDB.MYSCHEMA.MYVIEW")

        # Compare a single object
        entry = service.compare_object("VIEW", "MYDB.MYSCHEMA.MYVIEW")

        # Batch comparison via config
        from schemadrift.core.config import load_config
        config = load_config(Path("schemadrift.toml"))
        entries = service.compare_targets(config)

        # Serialize as YAML changeset
        from schemadrift.core.comparison import to_changeset_yaml
        yaml_output = to_changeset_yaml(entries)
        ```
    """

    DEPENDENCY_ORDER: list[str] = ["ROLE", "DATABASE", "SCHEMA", "TABLE", "VIEW"]

    def __init__(
        self,
        connection: SnowflakeConnectionInterface,
        file_manager: FileManager | None = None,
        dispatcher: PluginDispatcher | None = None,
        config: ProjectConfig | None = None,
    ):
        """Initialize the service.

        Args:
            connection: Snowflake connection for communicating with Snowflake.
            file_manager: FileManager for source control operations.
                          Required for compare/sync operations.
            dispatcher: PluginDispatcher for plugin delegation.
                        Uses global dispatcher if not provided.
            config: Project configuration for exclusion rules and other
                    per-object-type settings. When None, built-in defaults
                    are used for exclusion filtering.
        """
        self._conn = connection
        self._fm = file_manager
        self._dispatcher: PluginDispatcher | None = dispatcher
        self._config = config

    @property
    def dispatcher(self) -> PluginDispatcher:
        """Get the plugin dispatcher (lazy initialization)."""
        if self._dispatcher is None:
            from schemadrift.plugins.manager import PluginDispatcher

            self._dispatcher = PluginDispatcher()
        return self._dispatcher

    @property
    def object_renamer(self) -> ObjectRenamer:
        """Get the object renamer from project config (identity if unset)."""
        if self._config is not None:
            return self._config.object_renamer
        return ObjectRenamer.identity()

    @property
    def file_manager(self) -> FileManager:
        """Get the file manager.

        Raises:
            ValueError: If file_manager was not provided.
        """
        if self._fm is None:
            raise ValueError(
                "FileManager not configured. "
                "Provide file_manager to DriftService for file operations."
            )
        return self._fm

    # ========================================================================
    # Object Type Information
    # ========================================================================

    def get_object_types(self) -> list[str]:
        """Get all registered object types.

        Returns:
            List of object type names (e.g., ['DATABASE', 'SCHEMA', 'VIEW']).
        """
        return self.dispatcher.get_object_types()

    def has_object_type(self, object_type: str) -> bool:
        """Check if an object type is registered.

        Args:
            object_type: The object type to check.

        Returns:
            True if the object type is registered.
        """
        return self.dispatcher.has_object_type(object_type)

    # ========================================================================
    # Extraction + Serialization Convenience Methods
    # ========================================================================

    def extract_as_ddl(self, object_type: str, identifier: str) -> str:
        """Extract an object from Snowflake and return its DDL.

        Args:
            object_type: The object type (e.g., 'VIEW').
            identifier: Fully qualified object name.

        Returns:
            The CREATE statement SQL.
        """
        obj = self.extract_object(object_type, identifier)
        return self.generate_ddl(obj)

    def extract_as_dict(self, object_type: str, identifier: str) -> dict:
        """Extract an object from Snowflake and return its dictionary representation.

        Args:
            object_type: The object type (e.g., 'VIEW').
            identifier: Fully qualified object name.

        Returns:
            Serializable dictionary.
        """
        obj = self.extract_object(object_type, identifier)
        return self.generate_dict(obj)

    def extract_to_file(
        self,
        object_type: str,
        identifier: str,
        format: str = "yaml",
    ) -> Path:
        """Extract an object from Snowflake and save it to a file.

        Skips objects that have a corresponding directory in source control
        (externally managed).

        Args:
            object_type: The object type (e.g., 'VIEW').
            identifier: Fully qualified object name.
            format: Output format, either 'sql' or 'yaml'.

        Returns:
            The path to the written file.

        Raises:
            ValueError: If the object is externally managed.
        """
        if self._fm is not None and self._is_externally_managed(object_type, identifier):
            raise ValueError(
                f"{object_type} {identifier} is externally managed (directory exists); "
                "skipping extract"
            )
        obj = self.extract_object(object_type, identifier)
        return self.save_object_to_file(obj, format)

    # ========================================================================
    # Low-level Object Serialization (via plugin dispatcher)
    # ========================================================================

    def generate_ddl(self, obj: SnowflakeObject) -> str:
        """Generate DDL (CREATE statement) for an object.

        Args:
            obj: The SnowflakeObject to generate DDL for.

        Returns:
            The CREATE statement SQL.
        """
        return self.dispatcher.generate_ddl(obj)

    def generate_dict(self, obj: SnowflakeObject) -> dict:
        """Generate dictionary representation of an object.

        Args:
            obj: The SnowflakeObject to serialize.

        Returns:
            Serializable dictionary.
        """
        return self.dispatcher.generate_dict(obj)

    def save_object_to_file(
        self,
        obj: SnowflakeObject,
        format: str = "yaml",
    ) -> Path:
        """Save an object to a file.

        Args:
            obj: The SnowflakeObject to save.
            format: Output format, either 'sql' or 'yaml'.

        Returns:
            The path to the written file.
        """
        if format == "yaml":
            from schemadrift.core.yaml_utils import dump_yaml

            content = dump_yaml(self.generate_dict(obj))
            extension = "yaml"
        else:
            content = self.generate_ddl(obj)
            extension = "sql"

        return self.file_manager.write_object(
            content=content,
            object_type=obj.object_type,
            database=obj.get_database(),
            schema=obj.get_schema(),
            name=obj.name,
            extension=extension,
        )

    # ========================================================================
    # Single Object Operations
    # ========================================================================

    def extract_object(
        self,
        object_type: str,
        identifier: str,
    ) -> SnowflakeObject:
        """Extract a single object from Snowflake.

        The identifier is expected in **logical** form.  If a name mapping
        is configured, the identifier is translated to its physical Snowflake
        name before querying, and the returned object's container fields
        (database, schema, or name depending on scope) are translated back
        to logical names.

        Args:
            object_type: The object type (e.g., 'VIEW').
            identifier: Fully qualified object name (e.g., 'DB.SCHEMA.MYVIEW').

        Returns:
            The extracted SnowflakeObject instance.

        Raises:
            ValueError: If the object type is not registered or extraction fails.
        """
        resolver = self.object_renamer
        scope = self.dispatcher.get_scope(object_type)
        physical_id = resolver.to_physical_identifier(scope, identifier)

        obj = self.dispatcher.extract_object(object_type, self._conn, physical_id)
        self._to_logical_object_fields(obj, resolver)
        return obj

    def list_objects(
        self,
        object_type: str,
        scope: str = "",
    ) -> list[str]:
        """List all managed objects of a type in Snowflake.

        The *scope* is expected in **physical** form (already translated).
        Returned identifiers are translated back to logical names.

        Results are filtered through the exclusion rules in the project
        configuration (or built-in defaults when no config is set).

        Args:
            object_type: The object type to list.
            scope: The scope to list within (e.g., 'DB.SCHEMA' for views).

        Returns:
            List of fully qualified logical object names, excluding system objects.
        """
        results = self.dispatcher.list_objects(object_type, self._conn, scope)

        resolver = self.object_renamer
        obj_scope = self.dispatcher.get_scope(object_type)
        unresolved = [
            resolver.to_logical_identifier(obj_scope, r) for r in results
        ]

        return [r for r in unresolved if is_managed(object_type, r, self._config)]

    def load_object_from_file(
        self,
        object_type: str,
        identifier: str,
        format: str = "yaml",
    ) -> SnowflakeObject | None:
        """Load an object from source control.

        Contextual fields (database_name, schema_name) are derived from the
        file path and injected into the object via the ``context`` parameter,
        so YAML files need only contain writable fields.

        Args:
            object_type: The object type.
            identifier: Fully qualified object name.
            format: File format to read, either 'sql' or 'yaml'.

        Returns:
            The loaded SnowflakeObject, or None if not found.
        """
        from schemadrift.core.file_manager import FileStructure

        scope = self.dispatcher.get_scope(object_type)
        parts = FileStructure.parse_identifier(scope, identifier)

        extension = "yaml" if format == "yaml" else "sql"
        content = self.file_manager.read_object(object_type, **parts, extension=extension)
        if content is None:
            return None

        if format == "yaml":
            import yaml as yaml_module

            data = yaml_module.safe_load(content)
            context = self._build_context_from_parts(parts)
            return self.dispatcher.object_from_dict(object_type, data, context=context)
        else:
            return self.dispatcher.object_from_ddl(object_type, content)

    @staticmethod
    def _build_context_from_parts(parts: dict[str, str]) -> dict[str, str]:
        """Build a context dict (database_name, schema_name) from parsed identifier parts."""
        context: dict[str, str] = {}
        if "database" in parts:
            context["database_name"] = parts["database"]
        if "schema" in parts:
            context["schema_name"] = parts["schema"]
        return context

    # ========================================================================
    # Comparison
    # ========================================================================

    def compare_object(
        self,
        object_type: str,
        identifier: str,
        format: str = "yaml",
    ) -> ComparisonEntry | None:
        """Compare a single object between source control and Snowflake.

        Returns a ComparisonEntry describing the relationship:
        - MISSING_IN_TARGET: exists in source but not in Snowflake.
        - DIFFERS: exists in both but the definitions don't match.
        - MISSING_IN_SOURCE: exists in Snowflake but not in source.
        - EQUIVALENT: exists in both and the definitions match.
        - None: exists in neither.

        Args:
            object_type: The object type.
            identifier: Fully qualified object name.
            format: File format to read, either 'sql' or 'yaml'.

        Returns:
            A ComparisonEntry, or None if the object exists in neither location.
        """
        source_obj = self.load_object_from_file(object_type, identifier, format)

        try:
            target_obj = self.extract_object(object_type, identifier)
        except ObjectNotFoundError:
            target_obj = None

        display_id = identifier
        if target_obj is None and source_obj is not None:
            scope = self.dispatcher.get_scope(object_type)
            physical_id = self.object_renamer.to_physical_identifier(scope, identifier)
            if physical_id.upper() != identifier.upper():
                display_id = physical_id

        return self._build_comparison_entry(object_type, display_id, source_obj, target_obj)

    def compare_targets(
        self,
        config: ProjectConfig,
        format: str = "yaml",
    ) -> list[ComparisonEntry]:
        """Compare all objects matching the configured targets bidirectionally.

        For each target scope and object type a single directory scan
        discovers both resource-model files and externally-managed
        directories:

        1. **Files** are compared against Snowflake (detects
           MISSING_IN_TARGET, DIFFERS, EQUIVALENT).
        2. **Directories** are emitted as EXTERNALLY_MANAGED immediately.
        3. A Snowflake-side pass detects objects not represented in source
           (MISSING_IN_SOURCE).

        Args:
            config: Project configuration with target scopes.
            format: File format to read, either 'sql' or 'yaml'.

        Returns:
            List of ComparisonEntry objects.
        """
        from schemadrift.core.file_manager import SourceEntry

        entries: list[ComparisonEntry] = []

        for target in config.targets:
            for obj_type in self.get_target_object_types(target):
                source_ids: set[str] = set()

                for identifier, is_external in self._iter_source_entries(
                    obj_type, target, format
                ):
                    source_ids.add(identifier.upper())

                    if is_external:
                        entries.append(
                            ComparisonEntry(
                                status=ComparisonStatus.EXTERNALLY_MANAGED,
                                object_type=obj_type,
                                identifier=identifier,
                                definition={},
                            )
                        )
                        continue

                    try:
                        entry = self.compare_object(obj_type, identifier, format)
                        if entry is not None:
                            entries.append(entry)
                    except Exception:
                        logger.warning(
                            "Failed to compare %s %s: %s",
                            obj_type, identifier, _exc_summary(),
                        )

                self._detect_missing_in_source(
                    obj_type, target, source_ids, entries,
                )

        return entries

    # ========================================================================
    # Batch Extraction (config-driven)
    # ========================================================================

    def extract_targets(
        self,
        config: ProjectConfig,
        format: str = "yaml",
    ) -> list[Path]:
        """Extract and save all objects matching the configured targets.

        Queries Snowflake for each applicable object type within each target
        scope, extracts each object, and saves it to the file system.

        Args:
            config: Project configuration with target scopes.
            format: Output format, either 'sql' or 'yaml'.

        Returns:
            List of file paths written.
        """
        paths: list[Path] = []

        for target in config.targets:
            for obj_type in self.get_target_object_types(target):
                identifiers = self.get_identifiers_from_snowflake(obj_type, target)
                for identifier in identifiers:
                    if self._fm is not None and self._is_externally_managed(
                        obj_type, identifier,
                    ):
                        logger.info(
                            "Skipping externally managed %s %s",
                            obj_type, identifier,
                        )
                        continue
                    try:
                        path = self.extract_to_file(obj_type, identifier, format)
                        paths.append(path)
                    except Exception:
                        logger.warning(
                            "Failed to extract %s %s: %s",
                            obj_type, identifier, _exc_summary(),
                        )

        return paths

    # ========================================================================
    # Target Resolution Helpers
    # ========================================================================

    def get_target_object_types(self, target: TargetConfig) -> list[str]:
        """Determine which registered object types apply to a target.

        Filters the dependency-ordered list of object types based on:
        - target.object_types (explicit whitelist, if set)
        - target.database presence (controls org/account vs database/schema scope)

        Args:
            target: The target configuration.

        Returns:
            Ordered list of applicable object type names.
        """
        all_types = self.DEPENDENCY_ORDER
        if target.object_types is not None:
            all_types = [t for t in all_types if t in target.object_types]

        result: list[str] = []
        for obj_type in all_types:
            if not self.has_object_type(obj_type):
                continue

            obj_scope = self.dispatcher.get_scope(obj_type)

            if target.database is None:
                if obj_scope not in (ObjectScope.ORGANIZATION, ObjectScope.ACCOUNT):
                    continue
            else:
                if obj_scope == ObjectScope.ORGANIZATION:
                    continue
                if target.schemas and obj_scope in (
                    ObjectScope.ACCOUNT,
                    ObjectScope.DATABASE,
                ):
                    continue

            result.append(obj_type)

        return result

    def get_scopes_for_type(
        self,
        object_type: str,
        target: TargetConfig,
        *,
        include_unmapped: bool = False,
    ) -> list[str]:
        """Build the list of **physical** Snowflake scope strings for querying.

        Target database and schema names (logical) are translated to their
        physical Snowflake names before constructing scope strings.

        When *include_unmapped* is True and a schema's translated physical name
        differs from its logical name, the logical name is appended as an
        additional scope.  This enables bidirectional comparison to discover
        orphaned objects that still live under the old (unmapped) schema name
        in Snowflake.

        Args:
            object_type: The object type.
            target: The target configuration (uses logical names).
            include_unmapped: If True, also include unmapped logical schema
                names as extra scopes for orphan discovery.

        Returns:
            List of physical scope strings to pass to list_objects().
        """
        obj_scope = self.dispatcher.get_scope(object_type)
        resolver = self.object_renamer

        if obj_scope == ObjectScope.SCHEMA and target.database:
            physical_db = resolver.to_physical_account(target.database)
            if target.schemas:
                scopes: list[str] = []
                for s in target.schemas:
                    physical_schema = resolver.to_physical_schema(target.database, s)
                    scopes.append(f"{physical_db}.{physical_schema}")
                    if include_unmapped and physical_schema.upper() != s.upper():
                        scopes.append(f"{physical_db}.{s.upper()}")
                return scopes
            else:
                try:
                    logical_ids = self.list_objects("SCHEMA", physical_db)
                    scopes = []
                    for fqn in logical_ids:
                        physical_scope = resolver.to_physical_identifier(
                            ObjectScope.DATABASE, fqn,
                        )
                        scopes.append(physical_scope)
                        if include_unmapped:
                            if physical_scope.upper() != fqn.upper():
                                scopes.append(fqn)
                    return scopes
                except Exception as exc:
                    _log = logger.debug if _is_object_not_found_error(exc) else logger.warning
                    _log(
                        "Failed to list schemas in %s: %s",
                        physical_db, _exc_summary(),
                    )
                    return []
        elif obj_scope == ObjectScope.DATABASE and target.database:
            return [resolver.to_physical_account(target.database)]
        elif obj_scope == ObjectScope.ACCOUNT:
            return [""]
        elif obj_scope == ObjectScope.ORGANIZATION:
            return [""]
        else:
            return []

    def _iter_source_entries(
        self,
        object_type: str,
        target: TargetConfig,
        format: str = "yaml",
    ) -> list[tuple[str, bool]]:
        """Iterate source entries (files and directories) for an object type.

        Returns (identifier, is_external) pairs from a single directory scan.
        Files produce identifiers for normal comparison; directories are flagged
        as externally managed.

        Args:
            object_type: The object type.
            target: The target configuration.
            format: File format to filter by.

        Returns:
            List of (fully_qualified_identifier, is_external) tuples.
        """
        from schemadrift.core.file_manager import SourceEntry

        obj_scope = self.dispatcher.get_scope(object_type)
        extension = "yaml" if format == "yaml" else "sql"
        results: list[tuple[str, bool]] = []

        if obj_scope == ObjectScope.ACCOUNT and target.database:
            results.append((target.database.upper(), False))
            return results

        schemas_to_process = target.schemas if target.schemas else [None]

        if obj_scope == ObjectScope.SCHEMA and target.database:
            for schema in schemas_to_process:
                for se in self.file_manager.list_source_entries(
                    object_type, database=target.database, schema=schema,
                    extension=extension,
                ):
                    identifier = self.identifier_from_file_path(
                        object_type, se.path, target.database,
                    )
                    if is_managed(object_type, identifier, self._config):
                        results.append((identifier, se.is_external))
                    elif not se.is_external:
                        logger.warning(
                            "Skipping excluded object from source: %s %s",
                            object_type, identifier,
                        )
        elif obj_scope == ObjectScope.DATABASE and target.database:
            for se in self.file_manager.list_source_entries(
                object_type, database=target.database, extension=extension,
            ):
                identifier = self.identifier_from_file_path(
                    object_type, se.path, target.database,
                )
                if is_managed(object_type, identifier, self._config):
                    results.append((identifier, se.is_external))
                elif not se.is_external:
                    logger.warning(
                        "Skipping excluded object from source: %s %s",
                        object_type, identifier,
                    )
        else:
            for se in self.file_manager.list_source_entries(
                object_type, extension=extension,
            ):
                identifier = self.identifier_from_file_path(
                    object_type, se.path, "",
                )
                if is_managed(object_type, identifier, self._config):
                    results.append((identifier, se.is_external))
                elif not se.is_external:
                    logger.warning(
                        "Skipping excluded object from source: %s %s",
                        object_type, identifier,
                    )

        return results

    def get_identifiers_from_snowflake(
        self,
        object_type: str,
        target: TargetConfig,
        *,
        include_unmapped: bool = False,
    ) -> list[str]:
        """Get fully qualified **logical** identifiers from Snowflake.

        Short-circuits when the target already specifies the exact identifier,
        avoiding an overly broad SHOW command (e.g., ACCOUNT-scoped DATABASE
        type when target.database is set returns only that database).

        Returned identifiers use logical names (the target already does).

        When *include_unmapped* is True, also scans unmapped physical scopes
        to discover orphaned objects for bidirectional comparison.

        Args:
            object_type: The object type.
            target: The target configuration (logical names).
            include_unmapped: If True, include unmapped scopes (see
                :meth:`get_scopes_for_type`).

        Returns:
            List of fully qualified logical identifiers from Snowflake.
        """
        obj_scope = self.dispatcher.get_scope(object_type)

        # Short-circuit: target.database IS the logical identifier
        if obj_scope == ObjectScope.ACCOUNT and target.database:
            return [target.database]

        identifiers: list[str] = []
        for scope in self.get_scopes_for_type(
            object_type, target, include_unmapped=include_unmapped,
        ):
            try:
                identifiers.extend(self.list_objects(object_type, scope))
            except Exception as exc:
                _log = logger.debug if _is_object_not_found_error(exc) else logger.warning
                _log(
                    "Failed to list %s objects in scope '%s': %s",
                    object_type, scope, _exc_summary(),
                )
        return identifiers

    # ========================================================================
    # Private Helpers
    # ========================================================================

    def _build_comparison_entry(
        self,
        object_type: str,
        identifier: str,
        source_obj: SnowflakeObject | None,
        target_obj: SnowflakeObject | None,
    ) -> ComparisonEntry | None:
        """Create a ComparisonEntry from a source/target object pair.

        Args:
            object_type: The object type.
            identifier: Fully qualified object name.
            source_obj: Object from source control, or None.
            target_obj: Object from Snowflake, or None.

        Returns:
            A ComparisonEntry, or None if both objects are absent.
        """
        if source_obj is None and target_obj is None:
            return None

        if source_obj is None and target_obj is not None:
            return ComparisonEntry(
                status=ComparisonStatus.MISSING_IN_SOURCE,
                object_type=object_type,
                identifier=identifier,
                definition=self.generate_dict(target_obj),
            )

        if target_obj is None and source_obj is not None:
            return ComparisonEntry(
                status=ComparisonStatus.MISSING_IN_TARGET,
                object_type=object_type,
                identifier=identifier,
                definition=self.generate_dict(source_obj),
            )

        assert source_obj is not None and target_obj is not None
        diff = self.dispatcher.compare_objects(target_obj, source_obj)

        if not diff.has_changes:
            return ComparisonEntry(
                status=ComparisonStatus.EQUIVALENT,
                object_type=object_type,
                identifier=identifier,
                definition=self.generate_dict(source_obj),
            )

        return ComparisonEntry(
            status=ComparisonStatus.DIFFERS,
            object_type=object_type,
            identifier=identifier,
            definition=self.generate_dict(source_obj),
            diff=diff,
        )

    # ========================================================================
    # Name Resolution Helpers
    # ========================================================================

    def _detect_missing_in_source(
        self,
        object_type: str,
        target: TargetConfig,
        source_ids: set[str],
        entries: list[ComparisonEntry],
    ) -> None:
        """Detect Snowflake objects not present in source control.

        Iterates raw physical identifiers from Snowflake, translates each
        to a logical name, and checks against *source_ids*.  To handle
        phantom collisions (where a physical name accidentally matches a
        forward-mapped logical name), the round-trip is verified: if
        re-resolving the logical name produces a different physical name
        than was originally listed, the object is treated as orphaned and
        flagged as MISSING_IN_SOURCE regardless of the string match.

        Phantom collision objects are extracted using their physical
        identifier directly (bypassing the renamer).
        """
        resolver = self.object_renamer
        obj_scope = self.dispatcher.get_scope(object_type)

        for scope in self.get_scopes_for_type(
            object_type, target, include_unmapped=True,
        ):
            try:
                raw_physical_ids = self.dispatcher.list_objects(
                    object_type, self._conn, scope,
                )
            except Exception as exc:
                _log = logger.debug if _is_object_not_found_error(exc) else logger.warning
                _log(
                    "Failed to list %s objects in scope '%s': %s",
                    object_type, scope, _exc_summary(),
                )
                continue

            for physical_id in raw_physical_ids:
                logical_id = resolver.to_logical_identifier(obj_scope, physical_id)

                if not is_managed(object_type, logical_id, self._config):
                    continue

                re_resolved = resolver.to_physical_identifier(obj_scope, logical_id)
                phantom = re_resolved.upper() != physical_id.upper()

                if logical_id.upper() not in source_ids or phantom:
                    try:
                        if phantom:
                            target_obj = self._extract_object_physical(
                                object_type, physical_id,
                            )
                        else:
                            target_obj = self.extract_object(
                                object_type, logical_id,
                            )
                    except ObjectNotFoundError:
                        continue
                    entries.append(
                        ComparisonEntry(
                            status=ComparisonStatus.MISSING_IN_SOURCE,
                            object_type=object_type,
                            identifier=logical_id,
                            definition=self.generate_dict(target_obj),
                        )
                    )

    def _is_externally_managed(
        self,
        object_type: str,
        identifier: str,
    ) -> bool:
        """Check if an object is represented by a directory (externally managed)."""
        if self._fm is None:
            return False

        from schemadrift.core.file_manager import FileStructure

        scope = self.dispatcher.get_scope(object_type)
        parts = FileStructure.parse_identifier(scope, identifier)
        return self._fm.is_external_object(
            object_type,
            parts["name"],
            database=parts.get("database"),
            schema=parts.get("schema"),
        )

    def _extract_object_physical(
        self,
        object_type: str,
        identifier: str,
    ) -> SnowflakeObject:
        """Extract an object using its physical identifier, bypassing name resolution.

        Used for phantom collision objects whose physical name doesn't map
        to a logical name through the renamer.  The returned object retains
        its physical field values (no to_logical step).
        """
        return self.dispatcher.extract_object(object_type, self._conn, identifier)

    @staticmethod
    def _to_logical_object_fields(
        obj: SnowflakeObject,
        resolver: ObjectRenamer,
    ) -> None:
        """Translate an extracted object's container fields to logical names.

        Must be called after extracting from Snowflake so that the object
        uses logical names for comparison, file serialization, and display.
        """
        if resolver.is_identity:
            return

        if obj.SCOPE == ObjectScope.ACCOUNT:
            obj.name = resolver.to_logical_account(obj.name)
        elif obj.SCOPE == ObjectScope.DATABASE:
            obj.database_name = resolver.to_logical_account(obj.database_name)  # type: ignore[attr-defined]
            obj.name = resolver.to_logical_schema(obj.database_name, obj.name)  # type: ignore[attr-defined]
        elif obj.SCOPE == ObjectScope.SCHEMA:
            obj.database_name = resolver.to_logical_account(obj.database_name)  # type: ignore[attr-defined]
            obj.schema_name = resolver.to_logical_schema(obj.database_name, obj.schema_name)  # type: ignore[attr-defined]

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def identifier_from_file_path(
        self,
        object_type: str,
        file_path: Path,
        database: str,
    ) -> str:
        """Build a fully qualified identifier from a file path.

        Args:
            object_type: The object type.
            file_path: Path to the object file.
            database: The database context.

        Returns:
            Fully qualified identifier.
        """
        scope = self.dispatcher.get_scope(object_type)
        return self.file_manager.identifier_from_file_path(scope, file_path, database)
