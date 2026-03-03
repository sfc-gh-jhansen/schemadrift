"""Tests for schemadrift.core.comparison."""

from __future__ import annotations

import pytest

from schemadrift.core.base_object import ObjectDiff
from schemadrift.core.comparison import (
    ComparisonEntry,
    ComparisonStatus,
    format_summary_report,
    to_changeset_yaml,
)


# =============================================================================
# ComparisonStatus enum
# =============================================================================


class TestComparisonStatus:
    def test_values(self):
        assert ComparisonStatus.MISSING_IN_TARGET.value == "MISSING_IN_TARGET"
        assert ComparisonStatus.DIFFERS.value == "DIFFERS"
        assert ComparisonStatus.MISSING_IN_SOURCE.value == "MISSING_IN_SOURCE"
        assert ComparisonStatus.EQUIVALENT.value == "EQUIVALENT"
        assert ComparisonStatus.EXTERNALLY_MANAGED.value == "EXTERNALLY_MANAGED"


# =============================================================================
# ComparisonEntry.has_changes
# =============================================================================


class TestComparisonEntryHasChanges:
    def test_equivalent_is_false(self):
        entry = ComparisonEntry(
            status=ComparisonStatus.EQUIVALENT,
            object_type="VIEW",
            identifier="DB.SCH.V",
            definition={"name": "V"},
        )
        assert entry.has_changes is False

    @pytest.mark.parametrize("status", [
        ComparisonStatus.MISSING_IN_TARGET,
        ComparisonStatus.MISSING_IN_SOURCE,
        ComparisonStatus.DIFFERS,
    ])
    def test_non_equivalent_is_true(self, status):
        entry = ComparisonEntry(
            status=status,
            object_type="VIEW",
            identifier="DB.SCH.V",
            definition={"name": "V"},
        )
        assert entry.has_changes is True

    def test_externally_managed_is_false(self):
        entry = ComparisonEntry(
            status=ComparisonStatus.EXTERNALLY_MANAGED,
            object_type="VIEW",
            identifier="DB.SCH.V",
            definition={},
        )
        assert entry.has_changes is False


# =============================================================================
# ComparisonEntry.format_summary
# =============================================================================


class TestFormatSummary:
    def test_missing_in_target(self):
        entry = ComparisonEntry(
            status=ComparisonStatus.MISSING_IN_TARGET,
            object_type="VIEW",
            identifier="DB.SCH.V",
            definition={},
        )
        text = entry.format_summary()
        assert "source control" in text.lower()
        assert "not in Snowflake" in text

    def test_missing_in_source(self):
        entry = ComparisonEntry(
            status=ComparisonStatus.MISSING_IN_SOURCE,
            object_type="VIEW",
            identifier="DB.SCH.V",
            definition={},
        )
        text = entry.format_summary()
        assert "Snowflake" in text
        assert "not in source" in text.lower()

    def test_differs(self):
        diff = ObjectDiff(
            added={"new_col": "TEXT"},
            removed={"old_col": "INT"},
            modified={"query": ("SELECT 1", "SELECT 2")},
        )
        entry = ComparisonEntry(
            status=ComparisonStatus.DIFFERS,
            object_type="VIEW",
            identifier="DB.SCH.V",
            definition={},
            diff=diff,
        )
        text = entry.format_summary()
        assert "DRIFT" in text
        assert "Added" in text
        assert "Removed" in text
        assert "Modified" in text

    def test_equivalent(self):
        entry = ComparisonEntry(
            status=ComparisonStatus.EQUIVALENT,
            object_type="VIEW",
            identifier="DB.SCH.V",
            definition={},
        )
        text = entry.format_summary()
        assert "Equivalent" in text

    def test_externally_managed(self):
        entry = ComparisonEntry(
            status=ComparisonStatus.EXTERNALLY_MANAGED,
            object_type="PROCEDURE",
            identifier="DB.SCH.ML_INFERENCE",
            definition={},
        )
        text = entry.format_summary()
        assert "external tool" in text.lower()


# =============================================================================
# ComparisonEntry.to_dict
# =============================================================================


class TestComparisonEntryToDict:
    def test_without_diff(self):
        entry = ComparisonEntry(
            status=ComparisonStatus.EQUIVALENT,
            object_type="VIEW",
            identifier="DB.SCH.V",
            definition={"name": "V"},
        )
        d = entry.to_dict()
        assert d["status"] == "EQUIVALENT"
        assert d["object_type"] == "VIEW"
        assert d["identifier"] == "DB.SCH.V"
        assert d["definition"] == {"name": "V"}
        assert "diff" not in d

    def test_with_diff(self):
        diff = ObjectDiff(
            added={"a": 1},
            removed={"b": 2},
            modified={"c": ("old", "new")},
        )
        entry = ComparisonEntry(
            status=ComparisonStatus.DIFFERS,
            object_type="VIEW",
            identifier="DB.SCH.V",
            definition={"name": "V"},
            diff=diff,
        )
        d = entry.to_dict()
        assert "diff" in d
        assert d["diff"]["added"] == {"a": 1}
        assert d["diff"]["removed"] == {"b": 2}
        assert d["diff"]["modified"]["c"] == {"current": "old", "desired": "new"}


# =============================================================================
# format_summary_report
# =============================================================================


class TestFormatSummaryReport:
    def test_multi_entry_report(self):
        entries = [
            ComparisonEntry(
                status=ComparisonStatus.EQUIVALENT,
                object_type="VIEW",
                identifier="DB.SCH.V1",
                definition={},
            ),
            ComparisonEntry(
                status=ComparisonStatus.DIFFERS,
                object_type="VIEW",
                identifier="DB.SCH.V2",
                definition={},
                diff=ObjectDiff(modified={"query": ("a", "b")}),
            ),
        ]
        report = format_summary_report(entries)
        assert "2 object(s) compared" in report
        assert "1 with differences" in report

    def test_all_equivalent(self):
        entries = [
            ComparisonEntry(
                status=ComparisonStatus.EQUIVALENT,
                object_type="DATABASE",
                identifier="DB",
                definition={},
            ),
        ]
        report = format_summary_report(entries)
        assert "0 with differences" in report

    def test_empty_list(self):
        report = format_summary_report([])
        assert "0 object(s) compared" in report


# =============================================================================
# to_changeset_yaml
# =============================================================================


class TestToChangesetYaml:
    def test_excludes_equivalent(self):
        entries = [
            ComparisonEntry(
                status=ComparisonStatus.EQUIVALENT,
                object_type="VIEW",
                identifier="DB.SCH.V",
                definition={"name": "V"},
            ),
            ComparisonEntry(
                status=ComparisonStatus.MISSING_IN_TARGET,
                object_type="VIEW",
                identifier="DB.SCH.V2",
                definition={"name": "V2"},
            ),
        ]
        yaml_str = to_changeset_yaml(entries)
        assert "MISSING_IN_TARGET" in yaml_str
        assert "EQUIVALENT" not in yaml_str

    def test_all_equivalent_produces_empty_changes(self):
        entries = [
            ComparisonEntry(
                status=ComparisonStatus.EQUIVALENT,
                object_type="VIEW",
                identifier="DB.SCH.V",
                definition={},
            ),
        ]
        yaml_str = to_changeset_yaml(entries)
        assert "changes:" in yaml_str

    def test_excludes_externally_managed(self):
        entries = [
            ComparisonEntry(
                status=ComparisonStatus.EXTERNALLY_MANAGED,
                object_type="PROCEDURE",
                identifier="DB.SCH.ML",
                definition={},
            ),
            ComparisonEntry(
                status=ComparisonStatus.MISSING_IN_TARGET,
                object_type="VIEW",
                identifier="DB.SCH.V2",
                definition={"name": "V2"},
            ),
        ]
        yaml_str = to_changeset_yaml(entries)
        assert "MISSING_IN_TARGET" in yaml_str
        assert "EXTERNALLY_MANAGED" not in yaml_str
