"""Tests for schemadrift.core.base_object.

Consolidates base-class behaviour tests that were previously duplicated
across every plugin's tests.py, and adds coverage for helpers that had
no tests at all.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import ClassVar

import pytest

from schemadrift.connection.snowflake_impl import MockConnection
from schemadrift.core.base_object import (
    ObjectDiff,
    ObjectExtractionError,
    ObjectNotFoundError,
    ObjectParseError,
    ObjectScope,
    SnowflakeObject,
    SnowflakeObjectError,
    _is_object_not_found_error,
)

from tests.conftest import AccountObject, DatabaseObject, SchemaObject


# =============================================================================
# ObjectScope enum
# =============================================================================


class TestObjectScope:
    def test_values(self):
        assert ObjectScope.ORGANIZATION.value == "organization"
        assert ObjectScope.ACCOUNT.value == "account"
        assert ObjectScope.DATABASE.value == "database"
        assert ObjectScope.SCHEMA.value == "schema"


# =============================================================================
# Exception classes
# =============================================================================


class TestExceptions:
    def test_base_exception(self):
        exc = SnowflakeObjectError("boom")
        assert str(exc) == "boom"
        assert isinstance(exc, Exception)

    def test_object_not_found_error(self):
        exc = ObjectNotFoundError("VIEW", "DB.SCH.V")
        assert exc.object_type == "VIEW"
        assert exc.identifier == "DB.SCH.V"
        assert "VIEW" in str(exc)
        assert "DB.SCH.V" in str(exc)

    def test_object_extraction_error_without_cause(self):
        exc = ObjectExtractionError("DATABASE", "MYDB")
        assert exc.object_type == "DATABASE"
        assert exc.identifier == "MYDB"
        assert exc.cause is None
        assert "MYDB" in str(exc)

    def test_object_extraction_error_with_cause(self):
        cause = RuntimeError("connection lost")
        exc = ObjectExtractionError("DATABASE", "MYDB", cause)
        assert exc.cause is cause
        assert "connection lost" in str(exc)

    def test_object_parse_error_without_cause(self):
        exc = ObjectParseError("SCHEMA", "DDL")
        assert exc.object_type == "SCHEMA"
        assert exc.source == "DDL"
        assert exc.cause is None

    def test_object_parse_error_with_cause(self):
        cause = ValueError("bad format")
        exc = ObjectParseError("SCHEMA", "dict", cause)
        assert exc.cause is cause
        assert "bad format" in str(exc)


# =============================================================================
# _is_object_not_found_error
# =============================================================================


class TestIsObjectNotFoundError:
    def test_known_sql_error_codes(self):
        for code in (2003, 2043, 2082):
            exc = Exception("some message")
            exc.errno = code
            assert _is_object_not_found_error(exc) is True

    def test_message_substring_match(self):
        for msg in ("Object does not exist", "not found", "unknown"):
            assert _is_object_not_found_error(Exception(msg)) is True

    def test_non_matching_error(self):
        exc = Exception("permission denied")
        assert _is_object_not_found_error(exc) is False

    def test_errno_takes_precedence(self):
        exc = Exception("permission denied")
        exc.errno = 2003
        assert _is_object_not_found_error(exc) is True


# =============================================================================
# ObjectDiff
# =============================================================================


class TestObjectDiff:
    def test_empty_diff_has_no_changes(self):
        diff = ObjectDiff()
        assert diff.has_changes is False

    def test_added_only(self):
        diff = ObjectDiff(added={"new_col": "TEXT"})
        assert diff.has_changes is True

    def test_removed_only(self):
        diff = ObjectDiff(removed={"old_col": "TEXT"})
        assert diff.has_changes is True

    def test_modified_only(self):
        diff = ObjectDiff(modified={"col": ("INT", "TEXT")})
        assert diff.has_changes is True

    def test_all_populated(self):
        diff = ObjectDiff(
            added={"a": 1},
            removed={"b": 2},
            modified={"c": (3, 4)},
        )
        assert diff.has_changes is True


# =============================================================================
# __init_subclass__ scope enforcement
# =============================================================================


class TestInitSubclassEnforcement:
    def test_database_scope_requires_database_name(self):
        with pytest.raises(TypeError, match="database_name"):

            @dataclass
            class BadDB(SnowflakeObject):
                OBJECT_TYPE: ClassVar[str] = "BAD_DB"
                SCOPE: ClassVar[ObjectScope] = ObjectScope.DATABASE
                name: str

    def test_schema_scope_requires_schema_name(self):
        with pytest.raises(TypeError, match="schema_name"):

            @dataclass
            class BadSchema(SnowflakeObject):
                OBJECT_TYPE: ClassVar[str] = "BAD_SCH"
                SCOPE: ClassVar[ObjectScope] = ObjectScope.SCHEMA
                name: str
                database_name: str = ""

    def test_account_scope_no_extra_fields_required(self):
        @dataclass
        class GoodAccount(SnowflakeObject):
            OBJECT_TYPE: ClassVar[str] = "GOOD_ACCT"
            SCOPE: ClassVar[ObjectScope] = ObjectScope.ACCOUNT
            name: str

        obj = GoodAccount(name="X")
        assert obj.name == "X"


# =============================================================================
# normalize_identifier
# =============================================================================


class TestNormalizeIdentifier:
    def test_empty_string(self):
        assert SnowflakeObject.normalize_identifier("") == ""

    def test_unquoted_uppercased(self):
        assert SnowflakeObject.normalize_identifier("my_view") == "MY_VIEW"

    def test_quoted_preserved(self):
        assert SnowflakeObject.normalize_identifier('"MixedCase"') == '"MixedCase"'

    def test_space_gets_quoted(self):
        assert SnowflakeObject.normalize_identifier("my view") == '"my view"'

    def test_dash_gets_quoted(self):
        assert SnowflakeObject.normalize_identifier("my-view") == '"my-view"'

    def test_dot_gets_quoted(self):
        assert SnowflakeObject.normalize_identifier("my.view") == '"my.view"'

    def test_leading_digit_gets_quoted(self):
        assert SnowflakeObject.normalize_identifier("1view") == '"1view"'

    def test_already_upper(self):
        assert SnowflakeObject.normalize_identifier("MYVIEW") == "MYVIEW"


# =============================================================================
# parse_fully_qualified_name
# =============================================================================


class TestParseFullyQualifiedName:
    def test_single_part(self):
        assert SnowflakeObject.parse_fully_qualified_name("MYDB") == ("MYDB",)

    def test_two_parts(self):
        assert SnowflakeObject.parse_fully_qualified_name("MYDB.SCH") == ("MYDB", "SCH")

    def test_three_parts(self):
        result = SnowflakeObject.parse_fully_qualified_name("MYDB.SCH.V")
        assert result == ("MYDB", "SCH", "V")

    def test_quoted_identifier(self):
        result = SnowflakeObject.parse_fully_qualified_name('"my.db".SCH.V')
        assert result == ('"my.db"', "SCH", "V")

    def test_all_lowercase_uppercased(self):
        result = SnowflakeObject.parse_fully_qualified_name("mydb.mysch.myview")
        assert result == ("MYDB", "MYSCH", "MYVIEW")


# =============================================================================
# fully_qualified_name property
# =============================================================================


class TestFullyQualifiedName:
    def test_account_scope(self):
        obj = AccountObject(name="mydb")
        assert obj.fully_qualified_name == "MYDB"

    def test_database_scope(self):
        obj = DatabaseObject(name="mysch", database_name="mydb")
        assert obj.fully_qualified_name == "MYDB.MYSCH"

    def test_schema_scope(self):
        obj = SchemaObject(name="v", database_name="db", schema_name="sch")
        assert obj.fully_qualified_name == "DB.SCH.V"


# =============================================================================
# get_database / get_schema
# =============================================================================


class TestGetDatabaseSchema:
    def test_account_scope_returns_none(self):
        obj = AccountObject(name="X")
        assert obj.get_database() is None
        assert obj.get_schema() is None

    def test_database_scope(self):
        obj = DatabaseObject(name="S", database_name="DB")
        assert obj.get_database() == "DB"
        assert obj.get_schema() is None

    def test_schema_scope(self):
        obj = SchemaObject(name="V", database_name="DB", schema_name="SCH")
        assert obj.get_database() == "DB"
        assert obj.get_schema() == "SCH"


# =============================================================================
# __str__ / __repr__
# =============================================================================


class TestStringRepresentations:
    def test_str(self):
        obj = AccountObject(name="MYDB")
        assert str(obj) == "TEST_ACCOUNT_OBJ MYDB"

    def test_repr(self):
        obj = AccountObject(name="MYDB")
        assert "AccountObject" in repr(obj)
        assert "name='MYDB'" in repr(obj)


# =============================================================================
# to_dict / from_dict (consolidated from all plugin tests)
# =============================================================================


class TestToDict:
    def test_excludes_contextual_fields(self):
        obj = SchemaObject(
            name="V", database_name="DB", schema_name="SCH", query="SELECT 1"
        )
        d = obj.to_dict()
        assert "database_name" not in d
        assert "schema_name" not in d
        assert d["name"] == "V"
        assert d["query"] == "SELECT 1"

    def test_account_object_includes_all_non_none(self):
        obj = AccountObject(name="DB", comment="test")
        d = obj.to_dict()
        assert d == {"name": "DB", "comment": "test"}

    def test_none_values_stripped(self):
        obj = AccountObject(name="DB", comment=None)
        d = obj.to_dict()
        assert "comment" not in d
        assert d == {"name": "DB"}

    def test_none_values_stripped_in_nested_dicts(self):
        obj = SchemaObject(
            name="V", database_name="DB", schema_name="SCH",
            query="SELECT 1", comment=None,
        )
        d = obj.to_dict()
        assert "comment" not in d
        assert "database_name" not in d
        assert "schema_name" not in d

    def test_database_object_excludes_database_name(self):
        obj = DatabaseObject(name="SCH", database_name="DB", comment="c")
        d = obj.to_dict()
        assert "database_name" not in d
        assert d["name"] == "SCH"
        assert d["comment"] == "c"


class TestFromDict:
    def test_basic_load(self):
        data = {"name": "V", "query": "SELECT 1"}
        obj = SchemaObject.from_dict(data)
        assert obj.name == "V"
        assert obj.query == "SELECT 1"

    def test_context_merging(self):
        data = {"name": "V", "query": "SELECT 1"}
        context = {"database_name": "DB", "schema_name": "SCH"}
        obj = SchemaObject.from_dict(data, context=context)
        assert obj.database_name == "DB"
        assert obj.schema_name == "SCH"

    def test_unknown_fields_filtered(self):
        data = {"name": "V", "query": "SELECT 1", "nonsense_field": 42}
        obj = SchemaObject.from_dict(data)
        assert obj.name == "V"
        assert not hasattr(obj, "nonsense_field")

    def test_round_trip_account(self):
        original = AccountObject(name="MYDB", comment="test")
        restored = AccountObject.from_dict(original.to_dict())
        assert restored == original

    def test_round_trip_database(self):
        original = DatabaseObject(name="SCH", database_name="DB", comment="c")
        context = {"database_name": "DB"}
        restored = DatabaseObject.from_dict(original.to_dict(), context=context)
        assert restored == original

    def test_round_trip_schema(self):
        original = SchemaObject(
            name="V", database_name="DB", schema_name="SCH",
            query="SELECT 1", comment="x",
        )
        context = {"database_name": "DB", "schema_name": "SCH"}
        restored = SchemaObject.from_dict(original.to_dict(), context=context)
        assert restored == original


# =============================================================================
# _writable_field_names / _all_field_names
# =============================================================================


class TestFieldNameHelpers:
    def test_all_field_names_includes_everything(self):
        names = SchemaObject._all_field_names()
        assert "database_name" in names
        assert "schema_name" in names
        assert "name" in names
        assert "query" in names
        assert "comment" in names


# =============================================================================
# extract() with MockConnection
# =============================================================================


class TestExtract:
    def _mock_describe(self, mock, obj_type, identifier, data):
        mock.add_response(
            f"DESCRIBE AS RESOURCE {obj_type} {identifier}",
            [{"resource": json.dumps(data)}],
        )

    def test_extract_account_object(self):
        mock = MockConnection()
        self._mock_describe(mock, "TEST_ACCOUNT_OBJ", "MYDB", {
            "name": "MYDB", "comment": "hello",
        })
        obj = AccountObject.extract(mock, "MYDB")
        assert obj.name == "MYDB"
        assert obj.comment == "hello"

    def test_extract_database_object(self):
        mock = MockConnection()
        self._mock_describe(mock, "TEST_DB_OBJ", "DB.SCH", {
            "name": "SCH", "database_name": "DB", "comment": "test",
        })
        obj = DatabaseObject.extract(mock, "DB.SCH")
        assert obj.name == "SCH"
        assert obj.database_name == "DB"

    def test_extract_schema_object(self):
        mock = MockConnection()
        self._mock_describe(mock, "TEST_SCHEMA_OBJ", "DB.SCH.V", {
            "name": "V", "database_name": "DB", "schema_name": "SCH",
            "query": "SELECT 1",
        })
        obj = SchemaObject.extract(mock, "DB.SCH.V")
        assert obj.name == "V"
        assert obj.query == "SELECT 1"

    def test_extract_not_found_empty_result(self):
        mock = MockConnection()
        with pytest.raises(ObjectNotFoundError):
            AccountObject.extract(mock, "MISSING")

    def test_extract_malformed_json(self):
        mock = MockConnection()
        mock.add_response(
            "DESCRIBE AS RESOURCE TEST_ACCOUNT_OBJ BAD",
            [{"resource": "NOT VALID JSON {{{"}],
        )
        with pytest.raises(ObjectExtractionError):
            AccountObject.extract(mock, "BAD")

    def test_extract_keeps_all_known_fields(self):
        mock = MockConnection()
        self._mock_describe(mock, "TEST_ACCOUNT_OBJ", "X", {
            "name": "X",
            "comment": "keep",
            "owner": "SYSADMIN",
            "created_on": "2024-01-01",
        })
        obj = AccountObject.extract(mock, "X")
        assert obj.name == "X"
        assert obj.comment == "keep"
        assert not hasattr(obj, "owner")


# =============================================================================
# _show_objects / _show_as_resource_objects / list_objects
# =============================================================================


class TestListObjects:
    def test_show_as_resource_account_scope(self):
        mock = MockConnection()
        mock.add_response(
            "SHOW AS RESOURCE TERSE TEST_ACCOUNT_OBJS",
            [
                {"resource": '{"name": "A"}'},
                {"resource": '{"name": "B"}'},
            ],
        )
        result = AccountObject.list_objects(mock, "")
        assert result == ["A", "B"]

    def test_show_as_resource_database_scope(self):
        mock = MockConnection()
        mock.add_response(
            "SHOW AS RESOURCE TERSE TEST_DB_OBJS IN DATABASE MYDB",
            [
                {"resource": '{"name": "S1", "database_name": "MYDB"}'},
                {"resource": '{"name": "S2", "database_name": "MYDB"}'},
            ],
        )
        result = DatabaseObject.list_objects(mock, "MYDB")
        assert "MYDB.S1" in result
        assert "MYDB.S2" in result

    def test_show_as_resource_schema_scope(self):
        mock = MockConnection()
        mock.add_response(
            "SHOW AS RESOURCE TERSE TEST_SCHEMA_OBJS IN DB.SCH",
            [
                {"resource": '{"name": "V1", "database_name": "DB", "schema_name": "SCH"}'},
            ],
        )
        result = SchemaObject.list_objects(mock, "DB.SCH")
        assert result == ["DB.SCH.V1"]

    def test_show_objects_account_scope(self):
        mock = MockConnection()
        mock.add_response(
            "SHOW TEST_ACCOUNT_OBJS",
            [{"name": "A"}, {"name": "B"}],
        )
        result = AccountObject._show_objects(mock, "")
        assert result == ["A", "B"]

    def test_show_objects_database_scope(self):
        mock = MockConnection()
        mock.add_response(
            "SHOW TEST_DB_OBJS IN DATABASE MYDB",
            [
                {"name": "S1", "database_name": "MYDB"},
                {"name": "S2", "database_name": "MYDB"},
            ],
        )
        result = DatabaseObject._show_objects(mock, "MYDB")
        assert "MYDB.S1" in result
        assert "MYDB.S2" in result


# =============================================================================
# compare (consolidated from plugin tests)
# =============================================================================


class TestCompare:
    def test_identical_objects_no_changes(self):
        a = AccountObject(name="X", comment="same")
        b = AccountObject(name="X", comment="same")
        diff = a.compare(b)
        assert not diff.has_changes

    def test_modified_field(self):
        a = SchemaObject(name="V", database_name="DB", schema_name="S", query="SELECT 1")
        b = SchemaObject(name="V", database_name="DB", schema_name="S", query="SELECT 2")
        diff = a.compare(b)
        assert diff.has_changes
        assert "query" in diff.modified

    def test_added_field(self):
        a = AccountObject(name="X", comment=None)
        b = AccountObject(name="X", comment="new comment")
        diff = a.compare(b)
        assert diff.has_changes
        assert "comment" in diff.added

    def test_uses_default_strategy_when_none(self):
        assert AccountObject.DIFF_STRATEGY is None
        a = AccountObject(name="X")
        b = AccountObject(name="X")
        diff = a.compare(b)
        assert not diff.has_changes


# =============================================================================
# from_ddl / to_ddl stubs (consolidated from all 3 plugins)
# =============================================================================


class TestDDLStubs:
    def test_from_ddl_raises(self):
        with pytest.raises(NotImplementedError):
            AccountObject.from_ddl("CREATE ...")

    def test_to_ddl_raises(self):
        obj = AccountObject(name="X")
        with pytest.raises(NotImplementedError):
            obj.to_ddl()
