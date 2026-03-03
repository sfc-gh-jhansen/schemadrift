"""Tests for the View plugin.

Run with: pytest src/schemadrift/plugins/builtin/view/tests.py -v
"""

import json

import pytest

from schemadrift.core.base_object import ObjectExtractionError, ObjectNotFoundError
from schemadrift.plugins.builtin.view.model import View
from schemadrift.connection.snowflake_impl import MockConnection


class TestViewModel:
    """Test the View model class directly."""

    def test_create_view(self):
        """Test creating a View instance."""
        view = View(
            name="CUSTOMER_SUMMARY",
            database_name="ANALYTICS",
            schema_name="REPORTING",
            query="SELECT ID, NAME FROM CUSTOMERS",
            secure=True,
            comment="Customer summary view",
        )

        assert view.name == "CUSTOMER_SUMMARY"
        assert view.object_type == "VIEW"
        assert view.fully_qualified_name == "ANALYTICS.REPORTING.CUSTOMER_SUMMARY"
        assert view.secure is True

    def test_to_ddl_raises(self):
        """to_ddl() is a stub and should raise NotImplementedError."""
        view = View(name="V", database_name="DB", schema_name="SCH", query="SELECT 1")
        with pytest.raises(NotImplementedError):
            view.to_ddl()

    def test_from_ddl_raises(self):
        """from_ddl() is a stub and should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            View.from_ddl("CREATE VIEW DB.SCH.V AS SELECT 1")

    def test_to_dict(self):
        """Test dictionary export excludes contextual fields."""
        view = View(
            name="TEST_VIEW",
            database_name="DB",
            schema_name="SCH",
            query="SELECT 1",
        )

        data = view.to_dict()
        assert data["name"] == "TEST_VIEW"
        assert data["query"] == "SELECT 1"
        assert "object_type" not in data
        assert "database_name" not in data
        assert "schema_name" not in data

    def test_from_dict(self):
        """Test loading from dictionary with context."""
        data = {
            "name": "MY_VIEW",
            "query": "SELECT ID, NAME FROM CUSTOMERS",
            "secure": True,
        }
        context = {"database_name": "MYDB", "schema_name": "PUBLIC"}

        view = View.from_dict(data, context=context)
        assert view.name == "MY_VIEW"
        assert view.database_name == "MYDB"
        assert view.schema_name == "PUBLIC"
        assert view.secure is True
        assert "SELECT ID" in view.query

    def test_dict_round_trip(self):
        """Test that to_dict -> from_dict produces an equivalent object."""
        original = View(
            name="ROUND_TRIP",
            database_name="DB",
            schema_name="SCH",
            query="SELECT * FROM T WHERE X > 1",
            secure=True,
            comment="round trip test",
            columns=[{"name": "A"}, {"name": "B"}],
        )
        context = {"database_name": "DB", "schema_name": "SCH"}
        restored = View.from_dict(original.to_dict(), context=context)
        assert restored == original

    def test_compare_identical(self):
        """Test comparing identical views."""
        view1 = View(
            name="V",
            database_name="DB",
            schema_name="SCH",
            query="SELECT 1",
        )
        view2 = View(
            name="V",
            database_name="DB",
            schema_name="SCH",
            query="SELECT 1",
        )

        diff = view1.compare(view2)
        assert not diff.has_changes

    def test_compare_query_change(self):
        """Test comparing views with query changes."""
        view1 = View(
            name="V",
            database_name="DB",
            schema_name="SCH",
            query="SELECT 1",
        )
        view2 = View(
            name="V",
            database_name="DB",
            schema_name="SCH",
            query="SELECT 2",
        )

        diff = view1.compare(view2)
        assert diff.has_changes
        assert "query" in diff.modified

    def test_compare_secure_change(self):
        """Test comparing views with secure flag change."""
        view1 = View(
            name="V",
            database_name="DB",
            schema_name="SCH",
            query="SELECT 1",
            secure=False,
        )
        view2 = View(
            name="V",
            database_name="DB",
            schema_name="SCH",
            query="SELECT 1",
            secure=True,
        )

        diff = view1.compare(view2)
        assert diff.has_changes
        assert "secure" in diff.modified


class TestViewWithMockConnection:
    """Test View extraction with mock connection."""

    def _mock_describe_response(self, mock: MockConnection, identifier: str, data: dict) -> None:
        """Register a DESCRIBE AS RESOURCE VIEW mock response."""
        mock.add_response(
            f"DESCRIBE AS RESOURCE VIEW {identifier}",
            [{"resource": json.dumps(data)}],
        )

    def test_extract(self):
        """Test extracting a view via DESCRIBE AS RESOURCE.

        Snowflake returns the full DDL in the query field and read-only
        ``datatype`` on each column.  ``extract()`` should strip both.
        """
        mock = MockConnection(database="TESTDB", schema="PUBLIC")
        self._mock_describe_response(mock, "TESTDB.PUBLIC.SUMMARY", {
            "name": "SUMMARY",
            "database_name": "TESTDB",
            "schema_name": "PUBLIC",
            "query": (
                "CREATE OR REPLACE VIEW SUMMARY\nAS\n"
                "SELECT ID, NAME FROM CUSTOMERS"
            ),
            "columns": [
                {"name": "ID", "datatype": "NUMBER(38,0)"},
                {"name": "NAME", "datatype": "VARCHAR(16777216)"},
            ],
            "secure": False,
            "comment": "Customer summary",
        })

        view = View.extract(mock, "TESTDB.PUBLIC.SUMMARY")
        assert view.name == "SUMMARY"
        assert view.database_name == "TESTDB"
        assert view.schema_name == "PUBLIC"
        assert view.query == "SELECT ID, NAME FROM CUSTOMERS"
        assert view.secure is False
        assert view.comment == "Customer summary"
        assert all("datatype" not in col for col in view.columns)

    def test_extract_secure(self):
        """Test extracting a secure view."""
        mock = MockConnection(database="TESTDB", schema="PUBLIC")
        self._mock_describe_response(mock, "TESTDB.PUBLIC.SECURE_V", {
            "name": "SECURE_V",
            "database_name": "TESTDB",
            "schema_name": "PUBLIC",
            "query": "SELECT 1",
            "secure": True,
        })

        view = View.extract(mock, "TESTDB.PUBLIC.SECURE_V")
        assert view.secure is True

    def test_extract_not_found(self):
        """Test that extracting a missing view raises ObjectNotFoundError."""
        mock = MockConnection(database="TESTDB", schema="PUBLIC")
        with pytest.raises(ObjectNotFoundError) as exc_info:
            View.extract(mock, "TESTDB.PUBLIC.NONEXISTENT")
        assert exc_info.value.object_type == "VIEW"

    def test_extract_malformed_json(self):
        """Test that malformed JSON raises ObjectExtractionError."""
        mock = MockConnection(database="TESTDB", schema="PUBLIC")
        mock.add_response(
            "DESCRIBE AS RESOURCE VIEW TESTDB.PUBLIC.BAD",
            [{"resource": "NOT VALID JSON {{{"}],
        )
        with pytest.raises(ObjectExtractionError) as exc_info:
            View.extract(mock, "TESTDB.PUBLIC.BAD")
        assert exc_info.value.object_type == "VIEW"

    def test_list_views(self):
        """Test listing views."""
        mock = MockConnection(database="TESTDB", schema="PUBLIC")
        mock.add_response(
            "SHOW AS RESOURCE TERSE VIEWS IN TESTDB.PUBLIC",
            [
                {"resource": '{"name": "SUMMARY", "database_name": "TESTDB", "schema_name": "PUBLIC"}'},
                {"resource": '{"name": "REPORT", "database_name": "TESTDB", "schema_name": "PUBLIC"}'},
            ],
        )

        views = View.list_objects(mock, "TESTDB.PUBLIC")
        assert len(views) == 2
        assert "TESTDB.PUBLIC.SUMMARY" in views


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
