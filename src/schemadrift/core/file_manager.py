"""Serialization utilities for Snowflake objects.

This module provides file management for storing Snowflake object
definitions in the file system.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from schemadrift.core.base_object import ObjectScope, SnowflakeObject


@dataclass
class SourceEntry:
    """A single entry discovered under a type directory.

    Files represent resource-model objects managed by schemadrift.
    Directories represent objects managed by external tools (e.g. Snowflake CLI).
    """

    path: Path
    is_external: bool


class FileStructure:
    """Centralized file structure conventions.

    This class encapsulates all conventions for how object definitions
    are organized in the file system, making it easy to change the
    structure in one place.

    Account level objects: <root>/<object_type>s/<object_name>.<ext>
    Database level objects: <root>/<database>/<object_type>s/<object_name>.<ext>
    Schema level objects: <root>/<database>/<schema>/<object_type>s/<object_name>.<ext>

    Examples:
        - Database: <root>/databases/MYDB.sql
        - Schema: <root>/MYDB/schemas/MYSCHEMA.sql
        - View: <root>/MYDB/MYSCHEMA/views/MYVIEW.sql
    """

    # Directory naming convention for object types
    # Maps OBJECT_TYPE -> directory name (pluralized by default)
    OBJECT_TYPE_DIRS: dict[str, str] = {
        # Override specific types if needed (e.g., for irregular plurals)
        # "SCHEMA": "schemas",  # default would be "schemas" anyway
    }

    @classmethod
    def get_type_directory(cls, object_type: str) -> str:
        """Get the directory name for an object type.

        Args:
            object_type: The object type (e.g., 'DATABASE', 'VIEW').

        Returns:
            The directory name (e.g., 'databases', 'views').
        """
        upper_type = object_type.upper()
        if upper_type in cls.OBJECT_TYPE_DIRS:
            return cls.OBJECT_TYPE_DIRS[upper_type]
        # Default: lowercase + 's' for pluralization
        return f"{object_type.lower()}s"

    @classmethod
    def normalize_name(cls, name: str) -> str:
        """Normalize an object name for file system use.

        Args:
            name: The object name.

        Returns:
            Normalized name (uppercase).
        """
        return name.upper()

    @classmethod
    def get_file_name(cls, name: str, extension: str) -> str:
        """Get the file name for an object.

        Args:
            name: The object name.
            extension: File extension (without dot).

        Returns:
            The file name (e.g., 'MYVIEW.sql').
        """
        return f"{cls.normalize_name(name)}.{extension}"

    @classmethod
    def build_path(
        cls,
        root_path: Path,
        object_type: str,
        database: str | None = None,
        schema: str | None = None,
        name: str | None = None,
        extension: str = "yaml",
    ) -> Path:
        """Build the full file path for an object.

        This method encapsulates the directory structure convention:
        Account level objects: <root>/<object_type>s/<object_name>.<ext>
        Database level objects: <root>/<database>/<object_type>s/<object_name>.<ext>
        Schema level objects: <root>/<database>/<schema>/<object_type>s/<object_name>.<ext>

        Args:
            root_path: Root directory of the file structure.
            object_type: The object type (e.g., 'DATABASE', 'VIEW').
            database: Optional database name.
            schema: Optional schema name.
            name: Optional object name.
            extension: File extension (default: 'sql').

        Returns:
            The full path to the object file or directory.
        """
        path = root_path

        if database:
            path = path / cls.normalize_name(database)

        if schema:
            path = path / cls.normalize_name(schema)

        path = path / cls.get_type_directory(object_type)

        if name:
            path = path / cls.get_file_name(name, extension)

        return path

    @classmethod
    def parse_hierarchy_from_path(
        cls, file_path: Path, root_path: Path
    ) -> dict[str, str]:
        """Parse database/schema from a file path relative to root.

        Args:
            file_path: Path to an object file.
            root_path: Root directory of the file structure.

        Returns:
            Dict with 'database', 'schema', 'name' keys as available.
        """
        # Get path relative to root
        try:
            rel_path = file_path.relative_to(root_path)
        except ValueError:
            # Not relative to root, use absolute path parts
            rel_path = file_path

        parts = list(rel_path.parts)
        result = {"name": file_path.stem}

        # Remove file name from parts
        if parts:
            parts = parts[:-1]

        # Remove type directory (e.g., 'views', 'schemas')
        if parts:
            parts = parts[:-1]

        # Remaining parts are database/schema hierarchy
        if len(parts) >= 1:
            result["database"] = parts[0]
        if len(parts) >= 2:
            result["schema"] = parts[1]

        return result

    @classmethod
    def build_identifier(
        cls,
        scope: ObjectScope,
        hierarchy: dict[str, str],
        database: str,
    ) -> str:
        """Build a fully qualified identifier from parsed hierarchy.

        This method encapsulates the convention for constructing Snowflake
        identifiers from file path components.

        Args:
            scope: The object's scope level (ACCOUNT, DATABASE, or SCHEMA).
            hierarchy: Dict from parse_hierarchy_from_path() with 'name',
                      'database', 'schema' keys as available.
            database: The database context.

        Returns:
            Fully qualified identifier (e.g., 'DB.SCHEMA.VIEW').

        Raises:
            ValueError: If scope is SCHEMA but schema is missing from hierarchy.
            ValueError: If scope is unsupported.
        """
        name = hierarchy.get("name", "")

        if scope == ObjectScope.ACCOUNT:
            # Account-level object (e.g., DATABASE)
            return name
        elif scope == ObjectScope.DATABASE:
            # Database-level object (e.g., SCHEMA)
            return f"{database}.{name}"
        elif scope == ObjectScope.SCHEMA:
            # Schema-level object (e.g., VIEW, TABLE, etc.)
            schema = hierarchy.get("schema")
            if not schema:
                raise ValueError(
                    f"Schema-scoped object missing schema in hierarchy: {hierarchy}"
                )
            return f"{database}.{schema}.{name}"
        else:
            raise ValueError(f"Unsupported scope: {scope}")


    @classmethod
    def parse_identifier(cls, scope: ObjectScope, identifier: str) -> dict[str, str]:
        """Parse an identifier into its component parts based on scope.

        This is the inverse of :meth:`build_identifier`.

        Args:
            scope: The object's scope level (ACCOUNT, DATABASE, or SCHEMA).
            identifier: Fully qualified identifier (e.g., 'DB.SCHEMA.VIEW').

        Returns:
            Dict with 'database', 'schema', 'name' keys as applicable.

        Raises:
            ValueError: If the identifier doesn't have enough parts for the scope.

        Examples:
            >>> FileStructure.parse_identifier(ObjectScope.ACCOUNT, 'MYDB')
            {'name': 'MYDB'}
            >>> FileStructure.parse_identifier(ObjectScope.DATABASE, 'MYDB.MYSCHEMA')
            {'database': 'MYDB', 'name': 'MYSCHEMA'}
            >>> FileStructure.parse_identifier(ObjectScope.SCHEMA, 'MYDB.MYSCHEMA.MYVIEW')
            {'database': 'MYDB', 'schema': 'MYSCHEMA', 'name': 'MYVIEW'}
        """
        parts = SnowflakeObject.parse_fully_qualified_name(identifier)

        if scope == ObjectScope.ACCOUNT:
            return {"name": parts[0]}
        elif scope == ObjectScope.DATABASE:
            if len(parts) < 2:
                raise ValueError(
                    f"Invalid identifier '{identifier}' for DATABASE-scoped object"
                )
            return {"database": parts[0], "name": parts[1]}
        elif scope == ObjectScope.SCHEMA:
            if len(parts) < 3:
                raise ValueError(
                    f"Invalid identifier '{identifier}' for SCHEMA-scoped object"
                )
            return {"database": parts[0], "schema": parts[1], "name": parts[2]}
        else:
            raise ValueError(f"Unsupported scope: {scope}")


class FileManager:
    """Manages file storage for Snowflake object definitions.

    Handles the directory structure and file naming conventions for
    storing object definitions in the file system.

    Uses FileStructure for centralized convention management.
    """

    def __init__(self, root_path: str | Path):
        """Initialize the file manager.

        Args:
            root_path: Root directory for storing object definitions.
        """
        self.root_path = Path(root_path)
        self.structure = FileStructure

    def get_object_path(
        self,
        object_type: str,
        database: str | None = None,
        schema: str | None = None,
        name: str | None = None,
        extension: str = "yaml",
    ) -> Path:
        """Get the file path for an object.

        Delegates to FileStructure.build_path() for path construction.

        Args:
            object_type: The object type (e.g., 'database', 'schema', 'view').
            database: Optional database name.
            schema: Optional schema name.
            name: Optional object name.
            extension: File extension (default: 'sql').

        Returns:
            The full path to the object file.
        """
        return self.structure.build_path(
            self.root_path, object_type, database, schema, name, extension
        )

    def write_object(
        self,
        content: str,
        object_type: str,
        database: str | None = None,
        schema: str | None = None,
        name: str | None = None,
        extension: str = "yaml",
    ) -> Path:
        """Write an object definition to a file.

        Creates parent directories if they don't exist.

        Args:
            content: The file content to write.
            object_type: The object type.
            database: Optional database name.
            schema: Optional schema name.
            name: Object name.
            extension: File extension.

        Returns:
            The path to the written file.
        """
        path = self.get_object_path(object_type, database, schema, name, extension)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path

    def read_object(
        self,
        object_type: str,
        database: str | None = None,
        schema: str | None = None,
        name: str | None = None,
        extension: str = "yaml",
    ) -> str | None:
        """Read an object definition from a file.

        Args:
            object_type: The object type.
            database: Optional database name.
            schema: Optional schema name.
            name: Object name.
            extension: File extension.

        Returns:
            The file content, or None if the file doesn't exist.
        """
        path = self.get_object_path(object_type, database, schema, name, extension)
        if path.exists():
            return path.read_text()
        return None

    def read_file(self, file_path: Path) -> str | None:
        """Read content from a known file path.

        Unlike read_object(), this accepts a path directly (e.g., one
        returned by list_objects()) rather than building it from components.

        Args:
            file_path: Path to the file.

        Returns:
            The file content, or None if the file doesn't exist.
        """
        if file_path.exists():
            return file_path.read_text()
        return None

    def list_objects(
        self,
        object_type: str,
        database: str | None = None,
        schema: str | None = None,
        extension: str = "yaml",
    ) -> list[Path]:
        """List all object files of a given type.

        Args:
            object_type: The object type.
            database: Optional database filter.
            schema: Optional schema filter.
            extension: File extension filter.

        Returns:
            List of paths to matching object files.
        """
        path = self.get_object_path(object_type, database, schema)

        if not path.exists():
            return []

        return list(path.glob(f"*.{extension}"))

    def list_source_entries(
        self,
        object_type: str,
        database: str | None = None,
        schema: str | None = None,
        extension: str = "yaml",
    ) -> list[SourceEntry]:
        """List all source entries (files and directories) under a type directory.

        A single ``iterdir()`` call discovers both resource-model files and
        externally-managed directories in one pass.

        Args:
            object_type: The object type.
            database: Optional database filter.
            schema: Optional schema filter.
            extension: File extension filter for managed files.

        Returns:
            List of SourceEntry objects, sorted by name.
        """
        path = self.get_object_path(object_type, database, schema)
        if not path.exists():
            return []
        entries: list[SourceEntry] = []
        for child in sorted(path.iterdir()):
            if child.is_dir() and not child.name.startswith("."):
                entries.append(SourceEntry(path=child, is_external=True))
            elif child.is_file() and child.suffix == f".{extension}":
                entries.append(SourceEntry(path=child, is_external=False))
        return entries

    def is_external_object(
        self,
        object_type: str,
        name: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> bool:
        """Check if an object exists as a directory (externally managed).

        Args:
            object_type: The object type.
            name: Object name.
            database: Optional database name.
            schema: Optional schema name.

        Returns:
            True if a directory with the normalized object name exists.
        """
        path = self.get_object_path(object_type, database, schema)
        candidate = path / self.structure.normalize_name(name)
        return candidate.is_dir()

    def identifier_from_file_path(
        self,
        scope: ObjectScope,
        file_path: Path,
        database: str,
    ) -> str:
        """Build a fully qualified identifier from a file path.

        Delegates to FileStructure for path parsing and identifier construction.

        Args:
            scope: The object's scope level (ACCOUNT, DATABASE, or SCHEMA).
            file_path: Path to the object file.
            database: The database context.

        Returns:
            Fully qualified identifier (e.g., 'DB.SCHEMA.VIEW').
        """
        hierarchy = self.structure.parse_hierarchy_from_path(file_path, self.root_path)
        return self.structure.build_identifier(scope, hierarchy, database)
