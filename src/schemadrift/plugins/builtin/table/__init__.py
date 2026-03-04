"""Table plugin for Snowflake TABLE objects."""

from schemadrift.plugins.builtin.table.model import Table
from schemadrift.plugins.hook_adapter import HookAdapter

__all__ = ["Table"]

plugin = HookAdapter(Table)
