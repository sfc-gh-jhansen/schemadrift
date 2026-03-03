"""Diff strategies for Snowflake objects.

This module provides the Strategy Pattern implementation for diffing
Snowflake objects. The default strategy handles most cases, while
custom strategies can be created for objects with special diffing needs
(e.g., SQL normalization for views).

The strategies produce :class:`~schemadrift.core.base_object.ObjectDiff`
instances describing what changed between two objects.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

from schemadrift.core.base_object import ObjectDiff

if TYPE_CHECKING:
    from schemadrift.core.base_object import SnowflakeObject


class DiffStrategy(ABC):
    """Abstract base class for object diff strategies.

    Implementations define how two SnowflakeObjects are diffed to
    produce an ObjectDiff.
    """

    @abstractmethod
    def diff(
        self,
        source: SnowflakeObject,
        target: SnowflakeObject,
    ) -> ObjectDiff:
        """Diff two objects and return their differences.

        Args:
            source: The source (current) object state.
            target: The target (desired) object state.

        Returns:
            An ObjectDiff describing the differences.
        """


class DefaultDiffStrategy(DiffStrategy):
    """Default diff strategy for Snowflake objects.

    Diffs objects by their dictionary representation, with support for:
    - Ignoring specific attributes (like 'name', 'object_type')
    - Custom normalizers for specific attributes

    Example:
        strategy = DefaultDiffStrategy(
            ignore_attrs={"name", "object_type"},
            normalizers={"query": normalize_sql},
        )
        result = strategy.diff(source_view, target_view)
    """

    def __init__(
        self,
        ignore_attrs: set[str] | None = None,
        normalizers: dict[str, Callable[[Any], Any]] | None = None,
    ):
        """Initialize the diff strategy.

        Args:
            ignore_attrs: Set of attribute names to skip during diffing.
                         Defaults to {"name", "object_type"}.
            normalizers: Dict mapping attribute names to normalizer functions.
                        Normalizers transform values before comparison.
        """
        self.ignore_attrs = ignore_attrs or {"name"}
        self.normalizers = normalizers or {}

    def _apply_normalizers(self, data: dict[str, Any]) -> dict[str, Any]:
        """Return a copy of *data* with normalizer functions applied."""
        if not self.normalizers:
            return data
        normalized = dict(data)
        for key, normalizer in self.normalizers.items():
            if key in normalized and normalized[key] is not None:
                normalized[key] = normalizer(normalized[key])
        return normalized

    def diff(
        self,
        source: SnowflakeObject,
        target: SnowflakeObject,
    ) -> ObjectDiff:
        """Diff two objects by their dictionary representations.

        Args:
            source: The source object.
            target: The target object.

        Returns:
            An ObjectDiff with added, removed, and modified attributes.
        """
        if type(source) != type(target):
            raise TypeError(
                f"Cannot diff {type(source).__name__} with {type(target).__name__}"
            )

        source_original = source.to_dict()
        target_original = target.to_dict()

        source_data = self._apply_normalizers(source_original)
        target_data = self._apply_normalizers(target_original)

        added = {}
        removed = {}
        modified = {}

        all_keys = (set(source_data.keys()) | set(target_data.keys())) - self.ignore_attrs

        for key in all_keys:
            source_value = source_data.get(key)
            target_value = target_data.get(key)

            if source_value == target_value:
                continue
            elif source_value is None or key not in source_data:
                added[key] = target_original.get(key)
            elif target_value is None or key not in target_data:
                removed[key] = source_original.get(key)
            else:
                modified[key] = (source_original.get(key), target_original.get(key))

        return ObjectDiff(
            added=added,
            removed=removed,
            modified=modified,
        )


DEFAULT_DIFF_STRATEGY = DefaultDiffStrategy()
"""Pre-configured diff strategy with default settings."""
