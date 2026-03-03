"""Tests for MockConnection and BaseConnection."""

from __future__ import annotations

import pytest

from schemadrift.connection.snowflake_impl import MockConnection


# =============================================================================
# MockConnection
# =============================================================================


class TestMockConnectionDefaults:
    def test_default_account(self):
        mc = MockConnection()
        assert mc.get_current_account() == "test_account"

    def test_default_database(self):
        mc = MockConnection()
        assert mc.get_current_database() == "test_db"

    def test_default_schema(self):
        mc = MockConnection()
        assert mc.get_current_schema() == "test_schema"

    def test_custom_defaults(self):
        mc = MockConnection(account="acct", database="db", schema="sch")
        assert mc.get_current_account() == "acct"
        assert mc.get_current_database() == "db"
        assert mc.get_current_schema() == "sch"


class TestMockConnectionExecute:
    def test_exact_match(self):
        mc = MockConnection()
        mc.add_response("SELECT 1", [{"col": 1}])
        result = mc.execute("SELECT 1")
        assert result == [{"col": 1}]

    def test_case_insensitive(self):
        mc = MockConnection()
        mc.add_response("SELECT 1", [{"col": 1}])
        result = mc.execute("select 1")
        assert result == [{"col": 1}]

    def test_whitespace_normalized(self):
        mc = MockConnection()
        mc.add_response("SELECT  1  FROM  T", [{"col": 1}])
        result = mc.execute("SELECT 1 FROM T")
        assert result == [{"col": 1}]

    def test_partial_match(self):
        mc = MockConnection()
        mc.add_response("SHOW DATABASES", [{"name": "DB1"}])
        result = mc.execute("SHOW DATABASES IN ACCOUNT")
        assert result == [{"name": "DB1"}]

    def test_no_match_returns_empty(self):
        mc = MockConnection()
        result = mc.execute("SELECT 1")
        assert result == []


class TestMockConnectionExecuteScalar:
    def test_returns_first_value(self):
        mc = MockConnection()
        mc.add_response("SELECT CURRENT_DATABASE()", [{"col": "MYDB"}])
        result = mc.execute_scalar("SELECT CURRENT_DATABASE()")
        assert result == "MYDB"

    def test_no_results_returns_none(self):
        mc = MockConnection()
        result = mc.execute_scalar("SELECT 1")
        assert result is None


class TestMockConnectionClose:
    def test_close_is_noop(self):
        mc = MockConnection()
        mc.close()


class TestMockConnectionNativeConnection:
    def test_returns_none(self):
        mc = MockConnection()
        assert mc.get_native_connection() is None


# =============================================================================
# BaseConnection.execute_scalar default implementation
# =============================================================================


class TestBaseConnectionExecuteScalar:
    def test_delegates_to_execute(self):
        mc = MockConnection()
        mc.add_response("SELECT 42", [{"val": 42}])
        result = mc.execute_scalar("SELECT 42")
        assert result == 42

    def test_empty_result(self):
        mc = MockConnection()
        result = mc.execute_scalar("SELECT NOTHING")
        assert result is None
