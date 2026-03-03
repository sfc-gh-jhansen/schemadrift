"""Comparison results for Snowflake schema drift detection.

This module provides the data model for comparison results between source
control and Snowflake. Each compared object produces a ComparisonEntry with
a status describing the relationship:

- MISSING_IN_TARGET: exists in source control but not in Snowflake.
- DIFFERS: exists in both but the definitions don't match.
- MISSING_IN_SOURCE: exists in Snowflake but not in source control.
- EQUIVALENT: exists in both and the definitions match.
- EXTERNALLY_MANAGED: exists in Snowflake and is managed by an external tool.

The same list of entries can be serialized differently depending on the
consumer (human-readable summary, YAML changeset for an LLM, etc.).

For the low-level object diffing strategy, see :mod:`schemadrift.core.diff_strategy`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from schemadrift.core.base_object import ObjectDiff


class ComparisonStatus(Enum):
    """The relationship between an object in source control and Snowflake."""

    MISSING_IN_TARGET = "MISSING_IN_TARGET"
    DIFFERS = "DIFFERS"
    MISSING_IN_SOURCE = "MISSING_IN_SOURCE"
    EQUIVALENT = "EQUIVALENT"
    EXTERNALLY_MANAGED = "EXTERNALLY_MANAGED"


@dataclass
class ComparisonEntry:
    """Result of comparing a single object between source control and Snowflake.

    Every compared object produces a ComparisonEntry -- including objects
    that are equivalent. This allows a single comparison pass to be
    serialized in multiple ways (console summary, YAML changeset, etc.).

    Attributes:
        status: The comparison status.
        object_type: The Snowflake object type (e.g., 'DATABASE', 'VIEW').
        identifier: Fully qualified object name (e.g., 'MYDB.MYSCHEMA.MYVIEW').
        definition: Object definition dictionary.
                    For MISSING_IN_TARGET/DIFFERS/EQUIVALENT: the desired
                    state from source.
                    For MISSING_IN_SOURCE: the current Snowflake state.
        diff: The ObjectDiff details (only present for DIFFERS).
    """

    status: ComparisonStatus
    object_type: str
    identifier: str
    definition: dict[str, Any]
    diff: ObjectDiff | None = None

    @property
    def has_changes(self) -> bool:
        """True if this entry represents an actionable difference."""
        return self.status not in (
            ComparisonStatus.EQUIVALENT,
            ComparisonStatus.EXTERNALLY_MANAGED,
        )

    def format_summary(self) -> str:
        """Format this entry as a human-readable summary block.

        Returns:
            A multi-line string describing the comparison result.
        """
        lines = [f"  {self.object_type} {self.identifier}:"]

        if self.status is ComparisonStatus.MISSING_IN_TARGET:
            lines.append("    Exists in source control but not in Snowflake")
        elif self.status is ComparisonStatus.MISSING_IN_SOURCE:
            lines.append("    Exists in Snowflake but not in source control")
        elif self.status is ComparisonStatus.DIFFERS:
            lines.append("    DRIFT DETECTED:")
            if self.diff and self.diff.added:
                lines.append(f"      Added: {list(self.diff.added.keys())}")
            if self.diff and self.diff.removed:
                lines.append(f"      Removed: {list(self.diff.removed.keys())}")
            if self.diff and self.diff.modified:
                lines.append(f"      Modified: {list(self.diff.modified.keys())}")
        elif self.status is ComparisonStatus.EXTERNALLY_MANAGED:
            lines.append("    Managed by external tool (directory detected)")
        else:
            lines.append("    Equivalent")

        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this entry to a plain dictionary.

        Returns:
            A dictionary suitable for YAML serialization.
        """
        result: dict[str, Any] = {
            "status": self.status.value,
            "object_type": self.object_type,
            "identifier": self.identifier,
            "definition": self.definition,
        }

        if self.diff is not None:
            diff_dict: dict[str, Any] = {}
            if self.diff.added:
                diff_dict["added"] = self.diff.added
            if self.diff.removed:
                diff_dict["removed"] = self.diff.removed
            if self.diff.modified:
                diff_dict["modified"] = {
                    key: {"current": current, "desired": desired}
                    for key, (current, desired) in self.diff.modified.items()
                }
            result["diff"] = diff_dict

        return result


def format_summary_report(entries: list[ComparisonEntry]) -> str:
    """Format a list of comparison entries as a human-readable report.

    Args:
        entries: List of ComparisonEntry objects.

    Returns:
        A multi-line string with per-entry summaries and a totals footer.
    """
    lines: list[str] = []
    diff_count = 0
    for entry in entries:
        lines.append(entry.format_summary())
        if entry.has_changes:
            diff_count += 1

    lines.append(
        f"\n{len(entries)} object(s) compared, {diff_count} with differences"
    )
    return "\n".join(lines)


def to_changeset_yaml(entries: list[ComparisonEntry]) -> str:
    """Serialize actionable comparison entries as a YAML changeset.

    EQUIVALENT entries are excluded -- the changeset only contains entries
    where source and target differ.

    Args:
        entries: List of ComparisonEntry objects (may include EQUIVALENT).

    Returns:
        YAML string representation of the changeset.
    """
    from schemadrift.core.yaml_utils import dump_yaml

    actionable = [entry.to_dict() for entry in entries if entry.has_changes]
    return dump_yaml({"changes": actionable})
