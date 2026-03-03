"""View plugin for Snowflake VIEW objects."""

from schemadrift.plugins.builtin.view.model import View
from schemadrift.plugins.hook_adapter import HookAdapter

__all__ = ["View"]

plugin = HookAdapter(View)
