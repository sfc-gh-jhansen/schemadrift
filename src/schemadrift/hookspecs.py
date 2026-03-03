"""Pluggy hook specifications for Snowflake object plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pluggy

from schemadrift.core.base_object import ObjectScope

if TYPE_CHECKING:
    from schemadrift.core.base_object import SnowflakeObject

hookspec = pluggy.HookspecMarker("schemadrift")


class SnowflakeObjectSpec:
    """Hook specifications that all object plugins must implement."""

    @hookspec
    def get_object_type(self) -> str:
        """Return the Snowflake object type name (e.g., 'SCHEMA', 'VIEW')."""

    @hookspec
    def get_scope(self) -> ObjectScope:
        """Return the scope level for this object type."""

    @hookspec
    def extract_object(self, connection, identifier: str) -> SnowflakeObject:
        """Extract an object definition from Snowflake.

        Args:
            connection: A SnowflakeConnectionInterface instance.
            identifier: Fully qualified object name (e.g., 'DB.SCHEMA.VIEW').
        """

    @hookspec
    def list_objects(self, connection, scope: str) -> list[str]:
        """List all objects of this type within a scope."""

    @hookspec
    def object_from_ddl(self, sql: str) -> SnowflakeObject:
        """Parse an object from a CREATE statement."""

    @hookspec
    def object_from_dict(
        self, data: dict, context: dict | None = None,
    ) -> SnowflakeObject:
        """Load an object from a dictionary definition.

        Args:
            data: Writable fields from the YAML file.
            context: Optional contextual fields (database_name, schema_name)
                derived from the file path.
        """

    @hookspec
    def generate_ddl(self, obj: SnowflakeObject) -> str:
        """Generate a CREATE statement from an object."""

    @hookspec
    def generate_dict(self, obj: SnowflakeObject) -> dict:
        """Generate a serializable dictionary from an object."""

    @hookspec
    def compare_objects(self, source: SnowflakeObject, target: SnowflakeObject):
        """Compare two object definitions."""
