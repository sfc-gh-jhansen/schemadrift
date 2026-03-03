"""Database plugin for Snowflake DATABASE objects."""

from schemadrift.plugins.builtin.database.model import Database
from schemadrift.plugins.hook_adapter import HookAdapter

__all__ = ["Database"]

plugin = HookAdapter(Database)
