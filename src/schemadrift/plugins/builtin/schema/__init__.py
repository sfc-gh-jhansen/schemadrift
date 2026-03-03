"""Schema plugin for Snowflake SCHEMA objects."""

from schemadrift.plugins.builtin.schema.model import Schema
from schemadrift.plugins.hook_adapter import HookAdapter

__all__ = ["Schema"]

plugin = HookAdapter(Schema)
