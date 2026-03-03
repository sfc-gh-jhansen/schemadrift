"""Tests for schemadrift.plugins.hook_adapter.HookAdapter."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from schemadrift.connection.snowflake_impl import MockConnection
from schemadrift.core.base_object import (
    ObjectExtractionError,
    ObjectNotFoundError,
    ObjectParseError,
    ObjectScope,
)
from schemadrift.plugins.hook_adapter import HookAdapter

from tests.conftest import AccountObject, SchemaObject


# =============================================================================
# Basic delegation
# =============================================================================


class TestHookAdapterBasics:
    @pytest.fixture()
    def adapter(self):
        return HookAdapter(AccountObject)

    def test_get_object_type(self, adapter):
        assert adapter.get_object_type() == "TEST_ACCOUNT_OBJ"

    def test_get_scope(self, adapter):
        assert adapter.get_scope() == ObjectScope.ACCOUNT


# =============================================================================
# extract_object
# =============================================================================


class TestHookAdapterExtract:
    @pytest.fixture()
    def adapter(self):
        return HookAdapter(AccountObject)

    def test_successful_extract(self, adapter):
        mock = MockConnection()
        mock.add_response(
            "DESCRIBE AS RESOURCE TEST_ACCOUNT_OBJ MYDB",
            [{"resource": json.dumps({"name": "MYDB", "comment": "test"})}],
        )
        obj = adapter.extract_object(connection=mock, identifier="MYDB")
        assert obj.name == "MYDB"
        assert obj.comment == "test"

    def test_not_found_passthrough(self, adapter):
        mock = MockConnection()
        with pytest.raises(ObjectNotFoundError):
            adapter.extract_object(connection=mock, identifier="MISSING")

    def test_value_error_not_found_converts(self, adapter):
        """ValueError with 'not found' in message -> ObjectNotFoundError."""
        mock = MagicMock()
        mock.execute.side_effect = ValueError("Object not found in catalog")
        with pytest.raises(ObjectNotFoundError):
            adapter.extract_object(connection=mock, identifier="X")

    def test_value_error_other_wraps(self, adapter):
        """ValueError without 'not found' -> ObjectExtractionError."""
        mock = MagicMock()
        mock.execute.side_effect = ValueError("bad input")
        with pytest.raises(ObjectExtractionError):
            adapter.extract_object(connection=mock, identifier="X")

    def test_generic_exception_wraps(self, adapter):
        mock = MagicMock()
        mock.execute.side_effect = RuntimeError("network issue")
        with pytest.raises(ObjectExtractionError) as exc_info:
            adapter.extract_object(connection=mock, identifier="X")
        assert exc_info.value.object_type == "TEST_ACCOUNT_OBJ"


# =============================================================================
# list_objects
# =============================================================================


class TestHookAdapterListObjects:
    def test_list_objects(self):
        adapter = HookAdapter(AccountObject)
        mock = MockConnection()
        mock.add_response(
            "SHOW AS RESOURCE TERSE TEST_ACCOUNT_OBJS",
            [{"resource": '{"name": "A"}'}, {"resource": '{"name": "B"}'}],
        )
        result = adapter.list_objects(connection=mock, scope="")
        assert "A" in result
        assert "B" in result


# =============================================================================
# object_from_ddl
# =============================================================================


class TestHookAdapterFromDDL:
    def test_wraps_error_as_parse_error(self):
        adapter = HookAdapter(AccountObject)
        with pytest.raises(ObjectParseError) as exc_info:
            adapter.object_from_ddl(sql="CREATE ...")
        assert exc_info.value.object_type == "TEST_ACCOUNT_OBJ"
        assert exc_info.value.source == "DDL"


# =============================================================================
# object_from_dict
# =============================================================================


class TestHookAdapterFromDict:
    def test_successful_load(self):
        adapter = HookAdapter(AccountObject)
        obj = adapter.object_from_dict(data={"name": "X", "comment": "hi"})
        assert obj.name == "X"
        assert obj.comment == "hi"

    def test_with_context(self):
        adapter = HookAdapter(SchemaObject)
        obj = adapter.object_from_dict(
            data={"name": "V", "query": "SELECT 1"},
            context={"database_name": "DB", "schema_name": "SCH"},
        )
        assert obj.database_name == "DB"
        assert obj.schema_name == "SCH"

    def test_invalid_data_wraps(self):
        adapter = HookAdapter(AccountObject)
        with pytest.raises(ObjectParseError) as exc_info:
            adapter.object_from_dict(data={"wrong_field": "x"})
        assert exc_info.value.source == "dict"


# =============================================================================
# generate_ddl / generate_dict / compare_objects
# =============================================================================


class TestHookAdapterGenerate:
    def test_generate_ddl_delegates(self):
        adapter = HookAdapter(AccountObject)
        obj = AccountObject(name="X")
        with pytest.raises(NotImplementedError):
            adapter.generate_ddl(obj=obj)

    def test_generate_dict_delegates(self):
        adapter = HookAdapter(AccountObject)
        obj = AccountObject(name="X", comment="hi")
        d = adapter.generate_dict(obj=obj)
        assert d["name"] == "X"
        assert d["comment"] == "hi"

    def test_compare_objects_delegates(self):
        adapter = HookAdapter(AccountObject)
        a = AccountObject(name="X", comment="a")
        b = AccountObject(name="X", comment="b")
        diff = adapter.compare_objects(source=a, target=b)
        assert diff.has_changes
