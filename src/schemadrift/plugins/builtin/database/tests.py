"""Tests for the Database plugin.

Run with: pytest src/schemadrift/plugins/builtin/database/tests.py -v
"""

import json

import pytest

from schemadrift.core.base_object import ObjectExtractionError, ObjectNotFoundError
from schemadrift.plugins.builtin.database.model import Database
from schemadrift.connection.snowflake_impl import MockConnection


class TestDatabaseModel:
    """Test the Database model class directly."""

    def test_create_database(self):
        """Test creating a Database instance."""
        db = Database(
            name="ANALYTICS",
            data_retention_time_in_days=7,
            comment="Analytics data warehouse",
        )

        assert db.name == "ANALYTICS"
        assert db.object_type == "DATABASE"
        assert db.fully_qualified_name == "ANALYTICS"
        assert db.data_retention_time_in_days == 7

    def test_to_ddl_raises(self):
        """to_ddl() is a stub and should raise NotImplementedError."""
        db = Database(name="MY_DATABASE")
        with pytest.raises(NotImplementedError):
            db.to_ddl()

    def test_from_ddl_raises(self):
        """from_ddl() is a stub and should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            Database.from_ddl("CREATE DATABASE MY_DATABASE")

    def test_to_dict(self):
        """Test dictionary export excludes contextual/read-only fields."""
        db = Database(
            name="TEST",
            data_retention_time_in_days=1,
        )

        data = db.to_dict()
        assert data["name"] == "TEST"
        assert data["data_retention_time_in_days"] == 1
        assert "object_type" not in data

    def test_from_dict(self):
        """Test loading from dictionary."""
        data = {
            "name": "MY_DB",
            "kind": "PERMANENT",
            "data_retention_time_in_days": 7,
            "comment": "Production database",
        }

        db = Database.from_dict(data)
        assert db.name == "MY_DB"
        assert db.kind == "PERMANENT"
        assert db.data_retention_time_in_days == 7
        assert db.comment == "Production database"

    def test_dict_round_trip(self):
        """Test that to_dict -> from_dict produces an equivalent object."""
        original = Database(
            name="ROUND_TRIP",
            kind="TRANSIENT",
            data_retention_time_in_days=14,
            max_data_extension_time_in_days=30,
            comment="round trip test",
        )
        restored = Database.from_dict(original.to_dict())
        assert restored == original

    def test_compare_identical(self):
        """Test comparing identical databases."""
        db1 = Database(name="DB")
        db2 = Database(name="DB")

        diff = db1.compare(db2)
        assert not diff.has_changes

    def test_compare_retention_change(self):
        """Test comparing databases with retention change."""
        db1 = Database(name="DB", data_retention_time_in_days=1)
        db2 = Database(name="DB", data_retention_time_in_days=7)

        diff = db1.compare(db2)
        assert diff.has_changes
        assert "data_retention_time_in_days" in diff.modified

    def test_compare_kind_change(self):
        """Test comparing databases with kind change."""
        db1 = Database(name="DB", kind="PERMANENT")
        db2 = Database(name="DB", kind="TRANSIENT")

        diff = db1.compare(db2)
        assert diff.has_changes
        assert "kind" in diff.modified


class TestDatabaseWithMockConnection:
    """Test Database extraction with mock connection."""

    def _mock_describe_response(self, mock: MockConnection, identifier: str, data: dict) -> None:
        """Register a DESCRIBE AS RESOURCE DATABASE mock response."""
        mock.add_response(
            f"DESCRIBE AS RESOURCE DATABASE {identifier}",
            [{"resource": json.dumps(data)}],
        )

    def test_extract(self):
        """Test extracting a database via DESCRIBE AS RESOURCE."""
        mock = MockConnection()
        self._mock_describe_response(mock, "ANALYTICS", {
            "name": "ANALYTICS",
            "kind": "PERMANENT",
            "data_retention_time_in_days": 7,
            "max_data_extension_time_in_days": 14,
            "comment": "Analytics data warehouse",
        })

        db = Database.extract(mock, "ANALYTICS")
        assert db.name == "ANALYTICS"
        assert db.kind == "PERMANENT"
        assert db.data_retention_time_in_days == 7
        assert db.max_data_extension_time_in_days == 14
        assert db.comment == "Analytics data warehouse"

    def test_extract_transient(self):
        """Test that kind=TRANSIENT is preserved."""
        mock = MockConnection()
        self._mock_describe_response(mock, "TEMP_DB", {
            "name": "TEMP_DB",
            "kind": "TRANSIENT",
        })

        db = Database.extract(mock, "TEMP_DB")
        assert db.kind == "TRANSIENT"

    def test_extract_not_found(self):
        """Test that extracting a missing database raises ObjectNotFoundError."""
        mock = MockConnection()
        with pytest.raises(ObjectNotFoundError) as exc_info:
            Database.extract(mock, "NONEXISTENT")
        assert exc_info.value.object_type == "DATABASE"

    def test_extract_malformed_json(self):
        """Test that malformed JSON raises ObjectExtractionError."""
        mock = MockConnection()
        mock.add_response(
            "DESCRIBE AS RESOURCE DATABASE BAD",
            [{"resource": "NOT VALID JSON {{{"}],
        )
        with pytest.raises(ObjectExtractionError) as exc_info:
            Database.extract(mock, "BAD")
        assert exc_info.value.object_type == "DATABASE"

    def test_list_databases(self):
        """Test listing databases returns all results unfiltered.

        Exclusion filtering (e.g. SNOWFLAKE, USER$*) is applied by the
        service layer via config, not by the model.
        """
        mock = MockConnection()
        mock.add_response(
            "SHOW AS RESOURCE TERSE DATABASES",
            [
                {"resource": '{"name": "ANALYTICS"}'},
                {"resource": '{"name": "RAW_DATA"}'},
                {"resource": '{"name": "SNOWFLAKE"}'},
            ],
        )

        databases = Database.list_objects(mock, "")
        assert len(databases) == 3
        assert "ANALYTICS" in databases
        assert "RAW_DATA" in databases
        assert "SNOWFLAKE" in databases


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
