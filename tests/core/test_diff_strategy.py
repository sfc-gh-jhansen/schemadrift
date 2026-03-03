"""Tests for schemadrift.core.diff_strategy."""

from __future__ import annotations

import pytest

from schemadrift.core.diff_strategy import (
    DEFAULT_DIFF_STRATEGY,
    DefaultDiffStrategy,
)

from tests.conftest import AccountObject, SchemaObject


# =============================================================================
# DefaultDiffStrategy
# =============================================================================


class TestDefaultDiffStrategy:
    def test_identical_no_changes(self):
        a = AccountObject(name="X", comment="same")
        b = AccountObject(name="X", comment="same")
        diff = DEFAULT_DIFF_STRATEGY.diff(a, b)
        assert not diff.has_changes

    def test_modified_field(self):
        a = SchemaObject(name="V", database_name="DB", schema_name="S", query="SELECT 1")
        b = SchemaObject(name="V", database_name="DB", schema_name="S", query="SELECT 2")
        diff = DEFAULT_DIFF_STRATEGY.diff(a, b)
        assert diff.has_changes
        assert "query" in diff.modified
        assert diff.modified["query"] == ("SELECT 1", "SELECT 2")

    def test_added_field(self):
        a = AccountObject(name="X", comment=None)
        b = AccountObject(name="X", comment="new")
        diff = DEFAULT_DIFF_STRATEGY.diff(a, b)
        assert "comment" in diff.added
        assert diff.added["comment"] == "new"

    def test_removed_field(self):
        a = AccountObject(name="X", comment="old")
        b = AccountObject(name="X", comment=None)
        diff = DEFAULT_DIFF_STRATEGY.diff(a, b)
        assert "comment" in diff.removed
        assert diff.removed["comment"] == "old"

    def test_name_ignored_by_default(self):
        a = AccountObject(name="A", comment="same")
        b = AccountObject(name="B", comment="same")
        diff = DEFAULT_DIFF_STRATEGY.diff(a, b)
        assert "name" not in diff.modified

    def test_type_mismatch_raises(self):
        a = AccountObject(name="X")
        b = SchemaObject(name="X", database_name="DB", schema_name="S")
        with pytest.raises(TypeError, match="Cannot diff"):
            DEFAULT_DIFF_STRATEGY.diff(a, b)


class TestCustomDiffStrategy:
    def test_custom_ignore_attrs(self):
        strategy = DefaultDiffStrategy(ignore_attrs={"name", "comment"})
        a = AccountObject(name="X", comment="old")
        b = AccountObject(name="X", comment="new")
        diff = strategy.diff(a, b)
        assert not diff.has_changes

    def test_custom_normalizer(self):
        strategy = DefaultDiffStrategy(
            normalizers={"query": lambda q: q.strip().upper()},
        )
        a = SchemaObject(name="V", database_name="DB", schema_name="S", query="  select 1  ")
        b = SchemaObject(name="V", database_name="DB", schema_name="S", query="SELECT 1")
        diff = strategy.diff(a, b)
        assert not diff.has_changes

    def test_normalizer_only_applied_to_non_none(self):
        strategy = DefaultDiffStrategy(
            normalizers={"query": lambda q: q.upper()},
        )
        a = SchemaObject(name="V", database_name="DB", schema_name="S", query="")
        b = SchemaObject(name="V", database_name="DB", schema_name="S", query="")
        diff = strategy.diff(a, b)
        assert not diff.has_changes


class TestDefaultDiffStrategySingleton:
    def test_default_instance_exists(self):
        assert DEFAULT_DIFF_STRATEGY is not None
        assert isinstance(DEFAULT_DIFF_STRATEGY, DefaultDiffStrategy)

    def test_default_ignore_attrs(self):
        assert "name" in DEFAULT_DIFF_STRATEGY.ignore_attrs
