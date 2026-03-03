"""schemadrift - Detect and manage Snowflake schema drift."""

import pluggy

__version__ = "0.1.0"

# Export the hookimpl marker for plugins to use
hookimpl = pluggy.HookimplMarker("schemadrift")
"""Marker to be imported and used in plugins (and for own implementations)."""

__all__ = ["hookimpl", "__version__"]
