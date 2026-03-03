"""Tests for schemadrift.plugins.manager."""

from __future__ import annotations

import json

import pytest

from schemadrift.connection.snowflake_impl import MockConnection
from schemadrift.core.base_object import ObjectScope
from schemadrift.plugins.manager import (
    PluginDispatcher,
    create_plugin_manager,
)


# =============================================================================
# create_plugin_manager
# =============================================================================


class TestCreatePluginManager:
    def test_returns_plugin_manager(self):
        pm = create_plugin_manager()
        assert pm is not None

    def test_builtin_plugins_registered(self):
        pm = create_plugin_manager()
        types = []
        for p in pm.get_plugins():
            if hasattr(p, "get_object_type"):
                types.append(p.get_object_type())
        assert "DATABASE" in types
        assert "SCHEMA" in types
        assert "VIEW" in types


# =============================================================================
# PluginDispatcher
# =============================================================================


class TestPluginDispatcher:
    @pytest.fixture()
    def dispatcher(self):
        pm = create_plugin_manager()
        return PluginDispatcher(pm)

    def test_get_object_types(self, dispatcher):
        types = dispatcher.get_object_types()
        assert "DATABASE" in types
        assert "SCHEMA" in types
        assert "VIEW" in types

    def test_has_object_type_true(self, dispatcher):
        assert dispatcher.has_object_type("VIEW") is True

    def test_has_object_type_case_insensitive(self, dispatcher):
        assert dispatcher.has_object_type("view") is True

    def test_has_object_type_false(self, dispatcher):
        assert dispatcher.has_object_type("NONEXISTENT") is False

    def test_get_scope_database(self, dispatcher):
        assert dispatcher.get_scope("DATABASE") == ObjectScope.ACCOUNT

    def test_get_scope_schema(self, dispatcher):
        assert dispatcher.get_scope("SCHEMA") == ObjectScope.DATABASE

    def test_get_scope_view(self, dispatcher):
        assert dispatcher.get_scope("VIEW") == ObjectScope.SCHEMA

    def test_get_scope_unknown_raises(self, dispatcher):
        with pytest.raises(ValueError, match="Unknown object type"):
            dispatcher.get_scope("BOGUS")

    def test_extract_object_unknown_raises(self, dispatcher):
        with pytest.raises(ValueError, match="Unknown object type"):
            dispatcher.extract_object("BOGUS", MockConnection(), "X")

    def test_list_objects_unknown_raises(self, dispatcher):
        with pytest.raises(ValueError, match="Unknown object type"):
            dispatcher.list_objects("BOGUS", MockConnection(), "")

    def test_object_from_dict_unknown_raises(self, dispatcher):
        with pytest.raises(ValueError, match="Unknown object type"):
            dispatcher.object_from_dict("BOGUS", {"name": "X"})

    def test_generate_dict_via_dispatcher(self, dispatcher):
        mock = MockConnection()
        mock.add_response(
            "DESCRIBE AS RESOURCE DATABASE MYDB",
            [{"resource": json.dumps({"name": "MYDB", "kind": "PERMANENT"})}],
        )
        obj = dispatcher.extract_object("DATABASE", mock, "MYDB")
        d = dispatcher.generate_dict(obj)
        assert d["name"] == "MYDB"

    def test_object_from_dict_via_dispatcher(self, dispatcher):
        obj = dispatcher.object_from_dict("DATABASE", {"name": "X", "kind": "PERMANENT"})
        assert obj.name == "X"

    def test_compare_objects_via_dispatcher(self, dispatcher):
        obj1 = dispatcher.object_from_dict("DATABASE", {"name": "X", "kind": "PERMANENT"})
        obj2 = dispatcher.object_from_dict("DATABASE", {"name": "X", "kind": "TRANSIENT"})
        diff = dispatcher.compare_objects(obj1, obj2)
        assert diff.has_changes
