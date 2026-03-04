"""Tests for the Table plugin.

Run with: pytest src/schemadrift/plugins/builtin/table/tests.py -v
"""

import json

import pytest

from schemadrift.core.base_object import ObjectExtractionError, ObjectNotFoundError
from schemadrift.plugins.builtin.table.model import Table
from schemadrift.connection.snowflake_impl import MockConnection


class TestTableModel:
    """Test the Table model class directly."""

    def test_create_table(self):
        """Test creating a Table instance."""
        table = Table(
            name="CUSTOMERS",
            database_name="ANALYTICS",
            schema_name="PUBLIC",
            columns=[{"name": "ID"}, {"name": "NAME"}],
            comment="Customer data",
        )

        assert table.name == "CUSTOMERS"
        assert table.object_type == "TABLE"
        assert table.fully_qualified_name == "ANALYTICS.PUBLIC.CUSTOMERS"
        assert len(table.columns) == 2

    def test_to_ddl_raises(self):
        """to_ddl() is a stub and should raise NotImplementedError."""
        table = Table(name="T", database_name="DB", schema_name="SCH")
        with pytest.raises(NotImplementedError):
            table.to_ddl()

    def test_from_ddl_raises(self):
        """from_ddl() is a stub and should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            Table.from_ddl("CREATE TABLE DB.SCH.T (ID INT)")

    def test_to_dict(self):
        """Test dictionary export excludes contextual fields and strips None."""
        table = Table(
            name="TEST_TABLE",
            database_name="DB",
            schema_name="SCH",
            data_retention_time_in_days=7,
        )

        data = table.to_dict()
        assert data["name"] == "TEST_TABLE"
        assert data["data_retention_time_in_days"] == 7
        assert "object_type" not in data
        assert "database_name" not in data
        assert "schema_name" not in data
        assert "comment" not in data  # None values stripped

    def test_from_dict(self):
        """Test loading from dictionary with context."""
        data = {
            "name": "MY_TABLE",
            "columns": [{"name": "ID"}, {"name": "NAME"}],
            "comment": "Production table",
            "change_tracking": True,
        }
        context = {"database_name": "MYDB", "schema_name": "PUBLIC"}

        table = Table.from_dict(data, context=context)
        assert table.name == "MY_TABLE"
        assert table.database_name == "MYDB"
        assert table.schema_name == "PUBLIC"
        assert table.change_tracking is True
        assert len(table.columns) == 2

    def test_from_dict_with_columns_and_constraints(self):
        """Test loading with columns and constraints."""
        data = {
            "name": "ORDERS",
            "columns": [
                {"name": "ID", "nullable": False},
                {"name": "CUSTOMER_ID", "nullable": False},
                {"name": "AMOUNT"},
            ],
            "constraints": [
                {"name": "PK_ORDERS", "column_names": ["ID"], "constraint_type": "PRIMARY KEY"},
            ],
        }
        context = {"database_name": "DB", "schema_name": "SCH"}

        table = Table.from_dict(data, context=context)
        assert len(table.columns) == 3
        assert len(table.constraints) == 1
        assert table.constraints[0]["constraint_type"] == "PRIMARY KEY"

    def test_dict_round_trip(self):
        """Test that to_dict -> from_dict produces an equivalent object."""
        original = Table(
            name="ROUND_TRIP",
            database_name="DB",
            schema_name="SCH",
            columns=[{"name": "A"}, {"name": "B"}],
            cluster_by=["A"],
            data_retention_time_in_days=14,
            kind="TRANSIENT",
            comment="round trip test",
        )
        context = {"database_name": "DB", "schema_name": "SCH"}
        restored = Table.from_dict(original.to_dict(), context=context)
        assert restored == original

    def test_compare_identical(self):
        """Test comparing identical tables."""
        table1 = Table(
            name="T",
            database_name="DB",
            schema_name="SCH",
            columns=[{"name": "ID"}],
        )
        table2 = Table(
            name="T",
            database_name="DB",
            schema_name="SCH",
            columns=[{"name": "ID"}],
        )

        diff = table1.compare(table2)
        assert not diff.has_changes

    def test_compare_column_change(self):
        """Test comparing tables with column changes."""
        table1 = Table(
            name="T",
            database_name="DB",
            schema_name="SCH",
            columns=[{"name": "ID"}, {"name": "NAME"}],
        )
        table2 = Table(
            name="T",
            database_name="DB",
            schema_name="SCH",
            columns=[{"name": "ID"}, {"name": "EMAIL"}],
        )

        diff = table1.compare(table2)
        assert diff.has_changes
        assert "columns" in diff.modified

    def test_compare_retention_change(self):
        """Test comparing tables with retention change."""
        table1 = Table(
            name="T",
            database_name="DB",
            schema_name="SCH",
            data_retention_time_in_days=1,
        )
        table2 = Table(
            name="T",
            database_name="DB",
            schema_name="SCH",
            data_retention_time_in_days=7,
        )

        diff = table1.compare(table2)
        assert diff.has_changes
        assert "data_retention_time_in_days" in diff.modified


class TestTableWithMockConnection:
    """Test Table extraction with mock connection."""

    def _mock_describe_response(self, mock: MockConnection, identifier: str, data: dict) -> None:
        """Register a DESCRIBE AS RESOURCE TABLE mock response."""
        mock.add_response(
            f"DESCRIBE AS RESOURCE TABLE {identifier}",
            [{"resource": json.dumps(data)}],
        )

    def test_extract(self):
        """Test extracting a table via DESCRIBE AS RESOURCE.

        Snowflake returns ``datatype`` on each column; ``extract()`` keeps
        it since it's part of the object definition.
        """
        mock = MockConnection(database="TESTDB", schema="PUBLIC")
        self._mock_describe_response(mock, "TESTDB.PUBLIC.CUSTOMERS", {
            "name": "CUSTOMERS",
            "database_name": "TESTDB",
            "schema_name": "PUBLIC",
            "columns": [
                {"name": "ID", "datatype": "NUMBER(38,0)", "nullable": False},
                {"name": "NAME", "datatype": "VARCHAR(16777216)", "nullable": True},
            ],
            "change_tracking": False,
            "comment": "Customer data",
            "created_on": "2024-01-01T00:00:00Z",
            "owner": "SYSADMIN",
            "owner_role_type": "ROLE",
            "rows": 1000,
            "bytes": 65536,
        })

        table = Table.extract(mock, "TESTDB.PUBLIC.CUSTOMERS")
        assert table.name == "CUSTOMERS"
        assert table.database_name == "TESTDB"
        assert table.schema_name == "PUBLIC"
        assert table.comment == "Customer data"
        assert len(table.columns) == 2
        assert all("datatype" in col for col in table.columns)
        assert table.columns[0]["name"] == "ID"
        assert table.columns[0]["datatype"] == "NUMBER(38,0)"

    def test_extract_not_found(self):
        """Test that extracting a missing table raises ObjectNotFoundError."""
        mock = MockConnection(database="TESTDB", schema="PUBLIC")
        with pytest.raises(ObjectNotFoundError) as exc_info:
            Table.extract(mock, "TESTDB.PUBLIC.NONEXISTENT")
        assert exc_info.value.object_type == "TABLE"

    def test_extract_malformed_json(self):
        """Test that malformed JSON raises ObjectExtractionError."""
        mock = MockConnection(database="TESTDB", schema="PUBLIC")
        mock.add_response(
            "DESCRIBE AS RESOURCE TABLE TESTDB.PUBLIC.BAD",
            [{"resource": "NOT VALID JSON {{{"}],
        )
        with pytest.raises(ObjectExtractionError) as exc_info:
            Table.extract(mock, "TESTDB.PUBLIC.BAD")
        assert exc_info.value.object_type == "TABLE"

    def test_list_tables(self):
        """Test listing tables."""
        mock = MockConnection(database="TESTDB", schema="PUBLIC")
        mock.add_response(
            "SHOW AS RESOURCE TERSE TABLES IN TESTDB.PUBLIC",
            [
                {"resource": '{"name": "CUSTOMERS", "database_name": "TESTDB", "schema_name": "PUBLIC"}'},
                {"resource": '{"name": "ORDERS", "database_name": "TESTDB", "schema_name": "PUBLIC"}'},
            ],
        )

        tables = Table.list_objects(mock, "TESTDB.PUBLIC")
        assert len(tables) == 2
        assert "TESTDB.PUBLIC.CUSTOMERS" in tables
        assert "TESTDB.PUBLIC.ORDERS" in tables


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
