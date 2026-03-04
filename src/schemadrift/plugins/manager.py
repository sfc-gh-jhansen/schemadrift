"""Plugin manager for Snowflake object types.

This module provides the Pluggy-based plugin management system that
discovers, loads, and manages object type plugins.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pluggy

from schemadrift import hookspecs
from schemadrift.core.base_object import ObjectScope

if TYPE_CHECKING:
    from schemadrift.connection.interface import SnowflakeConnectionInterface
    from schemadrift.core.base_object import ObjectDiff, SnowflakeObject


# Global plugin manager instance
_plugin_manager: pluggy.PluginManager | None = None


def get_plugin_manager() -> pluggy.PluginManager:
    """Get or create the global plugin manager.

    The plugin manager is created lazily and cached. It:
    1. Registers the hook specifications
    2. Loads external plugins via setuptools entry points
    3. Registers built-in plugins

    Returns:
        The configured PluginManager instance.
    """
    global _plugin_manager

    if _plugin_manager is None:
        _plugin_manager = create_plugin_manager()

    return _plugin_manager


def create_plugin_manager() -> pluggy.PluginManager:
    """Create a new plugin manager instance.

    Useful for testing or when you need a fresh manager.

    Returns:
        A new PluginManager instance.
    """
    pm = pluggy.PluginManager("schemadrift")

    # Register hook specifications
    pm.add_hookspecs(hookspecs.SnowflakeObjectSpec)

    # Load external plugins via entry points
    # This allows third-party plugins to be discovered automatically
    pm.load_setuptools_entrypoints("schemadrift")

    # Register built-in plugins
    _register_builtin_plugins(pm)

    return pm


def _register_builtin_plugins(pm: pluggy.PluginManager) -> None:
    """Register all built-in plugins with the manager.

    Args:
        pm: The plugin manager to register with.
    """
    # Import here to avoid circular imports
    from schemadrift.plugins.builtin import database, role, schema, table, view

    # Register each plugin's HookAdapter instance
    pm.register(database.plugin, name="builtin_database")
    pm.register(role.plugin, name="builtin_role")
    pm.register(schema.plugin, name="builtin_schema")
    pm.register(table.plugin, name="builtin_table")
    pm.register(view.plugin, name="builtin_view")


class PluginDispatcher:
    """Dispatcher for routing operations to object type plugins.

    Provides a unified interface for working with Snowflake objects,
    routing calls to the appropriate plugin based on object type.
    """

    def __init__(self, pm: pluggy.PluginManager | None = None):
        """Initialize the plugin dispatcher.

        Args:
            pm: Optional plugin manager. Uses global if not provided.
        """
        self._pm = pm or get_plugin_manager()
        self._type_map: dict[str, object] | None = None

    def _build_type_map(self) -> dict[str, object]:
        """Build a mapping of object types to their plugin modules."""
        type_map = {}
        for plugin in self._pm.get_plugins():
            if hasattr(plugin, "get_object_type"):
                try:
                    obj_type = plugin.get_object_type()
                    if obj_type:
                        type_map[obj_type.upper()] = plugin
                except Exception:
                    pass
        return type_map

    @property
    def type_map(self) -> dict[str, object]:
        """Get the type-to-plugin mapping (cached)."""
        if self._type_map is None:
            self._type_map = self._build_type_map()
        return self._type_map

    def get_object_types(self) -> list[str]:
        """Get all registered object types."""
        return list(self.type_map.keys())

    def has_object_type(self, object_type: str) -> bool:
        """Check if an object type is registered."""
        return object_type.upper() in self.type_map

    def get_scope(self, object_type: str) -> ObjectScope:
        """Get the scope level for an object type.

        Args:
            object_type: The object type.

        Returns:
            The ObjectScope enum value (ACCOUNT, DATABASE, or SCHEMA).
        """
        if not self.has_object_type(object_type):
            raise ValueError(f"Unknown object type: {object_type}")

        plugin = self.type_map[object_type.upper()]
        return plugin.get_scope()

    def extract_object(
        self,
        object_type: str,
        connection: SnowflakeConnectionInterface,
        identifier: str,
    ) -> SnowflakeObject:
        """Extract an object using the appropriate plugin.

        Args:
            object_type: The object type to extract.
            connection: Snowflake connection.
            identifier: Fully qualified object name.

        Returns:
            The extracted SnowflakeObject instance.

        Raises:
            ValueError: If the object type is not registered.
            ObjectNotFoundError: If the object doesn't exist in Snowflake.
            ObjectExtractionError: If extraction fails for other reasons.
        """
        if not self.has_object_type(object_type):
            raise ValueError(f"Unknown object type: {object_type}")

        plugin = self.type_map[object_type.upper()]
        return plugin.extract_object(connection=connection, identifier=identifier)

    def list_objects(
        self,
        object_type: str,
        connection: SnowflakeConnectionInterface,
        scope: str,
    ) -> list[str]:
        """List objects of a type using the appropriate plugin.

        Args:
            object_type: The object type to list.
            connection: Snowflake connection.
            scope: The scope to list within.

        Returns:
            List of fully qualified object names.

        Raises:
            ValueError: If the object type is not registered.
        """
        if not self.has_object_type(object_type):
            raise ValueError(f"Unknown object type: {object_type}")

        plugin = self.type_map[object_type.upper()]
        return plugin.list_objects(connection=connection, scope=scope)

    def object_from_ddl(self, object_type: str, sql: str) -> SnowflakeObject:
        """Parse an object from DDL.

        Args:
            object_type: The object type.
            sql: The CREATE statement.

        Returns:
            The parsed SnowflakeObject instance.

        Raises:
            ValueError: If the object type is not registered.
            ObjectParseError: If the DDL cannot be parsed.
        """
        if not self.has_object_type(object_type):
            raise ValueError(f"Unknown object type: {object_type}")

        plugin = self.type_map[object_type.upper()]
        return plugin.object_from_ddl(sql=sql)

    def generate_ddl(self, obj: SnowflakeObject) -> str:
        """Generate DDL for an object.

        Args:
            obj: The SnowflakeObject instance.

        Returns:
            CREATE statement.

        Raises:
            ValueError: If the object type is not registered.
        """
        object_type = obj.object_type
        if not self.has_object_type(object_type):
            raise ValueError(f"Unknown object type: {object_type}")

        plugin = self.type_map[object_type.upper()]
        return plugin.generate_ddl(obj=obj)

    def object_from_dict(
        self,
        object_type: str,
        data: dict,
        context: dict | None = None,
    ) -> SnowflakeObject:
        """Load an object from a dictionary.

        Args:
            object_type: The object type.
            data: Writable fields from the YAML file.
            context: Optional contextual fields (database_name, schema_name)
                derived from the file path.

        Returns:
            The loaded SnowflakeObject instance.

        Raises:
            ValueError: If the object type is not registered.
            ObjectParseError: If the data is invalid.
        """
        if not self.has_object_type(object_type):
            raise ValueError(f"Unknown object type: {object_type}")

        plugin = self.type_map[object_type.upper()]
        return plugin.object_from_dict(data=data, context=context)

    def generate_dict(self, obj: SnowflakeObject) -> dict:
        """Generate a serializable dictionary for an object.

        Args:
            obj: The SnowflakeObject instance.

        Returns:
            Serializable dictionary.

        Raises:
            ValueError: If the object type is not registered.
        """
        object_type = obj.object_type
        if not self.has_object_type(object_type):
            raise ValueError(f"Unknown object type: {object_type}")

        plugin = self.type_map[object_type.upper()]
        return plugin.generate_dict(obj=obj)

    def compare_objects(
        self,
        source: SnowflakeObject,
        target: SnowflakeObject,
    ) -> ObjectDiff:
        """Compare two objects.

        Args:
            source: Source SnowflakeObject instance.
            target: Target SnowflakeObject instance.

        Returns:
            ObjectDiff describing the differences.

        Raises:
            ValueError: If the object type is not registered.
        """
        object_type = source.object_type
        if not self.has_object_type(object_type):
            raise ValueError(f"Unknown object type: {object_type}")

        plugin = self.type_map[object_type.upper()]
        return plugin.compare_objects(source=source, target=target)

