"""Tests for schemadrift.core.resource_metadata."""

from __future__ import annotations

from schemadrift.core.resource_metadata import (
    RESOURCE_METADATA,
    get_read_only_fields,
    get_required_fields,
    get_writable_fields,
)


# =============================================================================
# get_read_only_fields
# =============================================================================


class TestGetReadOnlyFields:
    def test_known_type_database(self):
        fields = get_read_only_fields("DATABASE")
        assert "owner" in fields
        assert "created_on" in fields
        assert "dropped_on" in fields
        assert "name" not in fields

    def test_known_type_view(self):
        fields = get_read_only_fields("VIEW")
        assert "owner" in fields
        assert "created_on" in fields
        assert "query" not in fields

    def test_unknown_type(self):
        fields = get_read_only_fields("DOES_NOT_EXIST")
        assert fields == frozenset()

    def test_case_insensitive(self):
        assert get_read_only_fields("database") == get_read_only_fields("DATABASE")


# =============================================================================
# get_writable_fields
# =============================================================================


class TestGetWritableFields:
    def test_known_type_database(self):
        fields = get_writable_fields("DATABASE")
        assert "name" in fields
        assert "comment" in fields
        assert "data_retention_time_in_days" in fields
        assert "owner" not in fields

    def test_known_type_view(self):
        fields = get_writable_fields("VIEW")
        assert "name" in fields
        assert "query" in fields
        assert "secure" in fields
        assert "owner" not in fields

    def test_unknown_type(self):
        assert get_writable_fields("NONEXISTENT") == frozenset()


# =============================================================================
# get_required_fields
# =============================================================================


class TestGetRequiredFields:
    def test_known_type_database(self):
        fields = get_required_fields("DATABASE")
        assert "name" in fields

    def test_known_type_view(self):
        fields = get_required_fields("VIEW")
        assert "name" in fields
        assert "query" in fields
        assert "columns" in fields

    def test_unknown_type(self):
        assert get_required_fields("NONEXISTENT") == frozenset()


# =============================================================================
# Spot-check field classifications
# =============================================================================


class TestFieldClassifications:
    def test_database_owner_is_read_only_not_writable(self):
        assert "owner" in get_read_only_fields("DATABASE")
        assert "owner" not in get_writable_fields("DATABASE")

    def test_database_name_is_writable_and_required(self):
        assert "name" in get_writable_fields("DATABASE")
        assert "name" in get_required_fields("DATABASE")

    def test_schema_database_name_is_read_only(self):
        assert "database_name" in get_read_only_fields("SCHEMA")
        assert "database_name" not in get_writable_fields("SCHEMA")

    def test_metadata_has_expected_types(self):
        assert "DATABASE" in RESOURCE_METADATA
        assert "SCHEMA" in RESOURCE_METADATA
        assert "VIEW" in RESOURCE_METADATA
        assert "TABLE" in RESOURCE_METADATA
        assert "WAREHOUSE" in RESOURCE_METADATA
