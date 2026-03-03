"""Tests for the Schema plugin.

Run with: pytest src/schemadrift/plugins/builtin/schema/tests.py -v
"""

import json

import pytest

from schemadrift.core.base_object import ObjectExtractionError, ObjectNotFoundError
from schemadrift.plugins.builtin.schema.model import Schema
from schemadrift.connection.snowflake_impl import MockConnection


class TestSchemaModel:
    """Test the Schema model class directly."""

    def test_create_schema(self):
        """Test creating a Schema instance."""
        schema = Schema(
            name="RAW",
            database_name="ANALYTICS",
            managed_access=True,
            comment="Raw data landing zone",
        )

        assert schema.name == "RAW"
        assert schema.object_type == "SCHEMA"
        assert schema.fully_qualified_name == "ANALYTICS.RAW"
        assert schema.managed_access is True

    def test_to_ddl_raises(self):
        """to_ddl() is a stub and should raise NotImplementedError."""
        schema = Schema(name="STAGING", database_name="DW")
        with pytest.raises(NotImplementedError):
            schema.to_ddl()

    def test_from_ddl_raises(self):
        """from_ddl() is a stub and should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            Schema.from_ddl("CREATE SCHEMA MYDB.MYSCHEMA")

    def test_to_dict(self):
        """Test dictionary export excludes contextual fields."""
        schema = Schema(
            name="TEST",
            database_name="DB",
            managed_access=True,
        )

        data = schema.to_dict()
        assert data["name"] == "TEST"
        assert data["managed_access"] is True
        assert "object_type" not in data
        assert "database_name" not in data

    def test_from_dict(self):
        """Test loading from dictionary with context."""
        data = {
            "name": "MY_SCHEMA",
            "managed_access": True,
            "comment": "Test schema",
        }
        context = {"database_name": "MYDB"}

        schema = Schema.from_dict(data, context=context)
        assert schema.name == "MY_SCHEMA"
        assert schema.database_name == "MYDB"
        assert schema.managed_access is True
        assert schema.comment == "Test schema"

    def test_dict_round_trip(self):
        """Test that to_dict -> from_dict produces an equivalent object."""
        original = Schema(
            name="ROUND_TRIP",
            database_name="DB",
            kind="TRANSIENT",
            managed_access=True,
            data_retention_time_in_days=7,
            comment="round trip test",
        )
        context = {"database_name": "DB"}
        restored = Schema.from_dict(original.to_dict(), context=context)
        assert restored == original

    def test_compare_identical(self):
        """Test comparing identical schemas."""
        schema1 = Schema(name="S", database_name="DB")
        schema2 = Schema(name="S", database_name="DB")

        diff = schema1.compare(schema2)
        assert not diff.has_changes

    def test_compare_managed_access_change(self):
        """Test comparing schemas with managed access change."""
        schema1 = Schema(name="S", database_name="DB", managed_access=False)
        schema2 = Schema(name="S", database_name="DB", managed_access=True)

        diff = schema1.compare(schema2)
        assert diff.has_changes
        assert "managed_access" in diff.modified

    def test_compare_kind_change(self):
        """Test comparing schemas with kind change."""
        schema1 = Schema(name="S", database_name="DB", kind="PERMANENT")
        schema2 = Schema(name="S", database_name="DB", kind="TRANSIENT")

        diff = schema1.compare(schema2)
        assert diff.has_changes
        assert "kind" in diff.modified


class TestSchemaWithMockConnection:
    """Test Schema extraction with mock connection."""

    def _mock_describe_response(self, mock: MockConnection, identifier: str, data: dict) -> None:
        """Register a DESCRIBE AS RESOURCE SCHEMA mock response."""
        mock.add_response(
            f"DESCRIBE AS RESOURCE SCHEMA {identifier}",
            [{"resource": json.dumps(data)}],
        )

    def test_extract(self):
        """Test extracting a schema via DESCRIBE AS RESOURCE."""
        mock = MockConnection(database="TESTDB")
        self._mock_describe_response(mock, "TESTDB.RAW", {
            "name": "RAW",
            "database_name": "TESTDB",
            "kind": "PERMANENT",
            "managed_access": False,
            "data_retention_time_in_days": 1,
            "comment": "Raw landing zone",
        })

        schema = Schema.extract(mock, "TESTDB.RAW")
        assert schema.name == "RAW"
        assert schema.database_name == "TESTDB"
        assert schema.kind == "PERMANENT"
        assert schema.managed_access is False
        assert schema.data_retention_time_in_days == 1
        assert schema.comment == "Raw landing zone"

    def test_extract_transient(self):
        """Test that kind=TRANSIENT is preserved."""
        mock = MockConnection(database="TESTDB")
        self._mock_describe_response(mock, "TESTDB.TEMP", {
            "name": "TEMP",
            "database_name": "TESTDB",
            "kind": "TRANSIENT",
            "managed_access": False,
        })

        schema = Schema.extract(mock, "TESTDB.TEMP")
        assert schema.kind == "TRANSIENT"

    def test_extract_not_found(self):
        """Test that extracting a missing schema raises ObjectNotFoundError."""
        mock = MockConnection(database="TESTDB")
        with pytest.raises(ObjectNotFoundError) as exc_info:
            Schema.extract(mock, "TESTDB.NONEXISTENT")
        assert exc_info.value.object_type == "SCHEMA"
        assert "NONEXISTENT" in exc_info.value.identifier

    def test_extract_malformed_json(self):
        """Test that malformed JSON raises ObjectExtractionError."""
        mock = MockConnection(database="TESTDB")
        mock.add_response(
            "DESCRIBE AS RESOURCE SCHEMA TESTDB.BAD",
            [{"resource": "NOT VALID JSON {{{"}],
        )
        with pytest.raises(ObjectExtractionError) as exc_info:
            Schema.extract(mock, "TESTDB.BAD")
        assert exc_info.value.object_type == "SCHEMA"

    def test_list_schemas(self):
        """Test listing schemas returns all results unfiltered.

        Exclusion filtering (e.g. INFORMATION_SCHEMA) is applied by the
        service layer via config, not by the model.
        """
        mock = MockConnection(database="TESTDB")
        mock.add_response(
            "SHOW AS RESOURCE TERSE SCHEMAS IN DATABASE TESTDB",
            [
                {"resource": '{"name": "PUBLIC", "database_name": "TESTDB"}'},
                {"resource": '{"name": "RAW", "database_name": "TESTDB"}'},
                {"resource": '{"name": "INFORMATION_SCHEMA", "database_name": "TESTDB"}'},
            ],
        )

        schemas = Schema.list_objects(mock, "TESTDB")
        assert len(schemas) == 3
        assert "TESTDB.PUBLIC" in schemas
        assert "TESTDB.RAW" in schemas
        assert "TESTDB.INFORMATION_SCHEMA" in schemas


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
