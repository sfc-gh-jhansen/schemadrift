"""Role plugin for Snowflake ROLE objects."""

from schemadrift.plugins.builtin.role.model import Role
from schemadrift.plugins.hook_adapter import HookAdapter

__all__ = ["Role"]

plugin = HookAdapter(Role)
