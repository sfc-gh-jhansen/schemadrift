"""Shared test fixtures for the schemadrift test suite."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

import pytest

from schemadrift.core.base_object import ObjectScope, SnowflakeObject


# =============================================================================
# Concrete SnowflakeObject subclasses for testing base-class behaviour
# =============================================================================


@dataclass
class AccountObject(SnowflakeObject):
    """ACCOUNT-scoped test object."""

    OBJECT_TYPE: ClassVar[str] = "TEST_ACCOUNT_OBJ"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.ACCOUNT

    name: str
    comment: str | None = None


@dataclass
class DatabaseObject(SnowflakeObject):
    """DATABASE-scoped test object."""

    OBJECT_TYPE: ClassVar[str] = "TEST_DB_OBJ"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.DATABASE

    name: str
    database_name: str = ""
    comment: str | None = None


@dataclass
class SchemaObject(SnowflakeObject):
    """SCHEMA-scoped test object."""

    OBJECT_TYPE: ClassVar[str] = "TEST_SCHEMA_OBJ"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.SCHEMA

    name: str
    database_name: str = ""
    schema_name: str = ""
    query: str = ""
    comment: str | None = None


# =============================================================================
# MockConnection fixture
# =============================================================================


@pytest.fixture()
def mock_conn():
    """Return a fresh MockConnection instance."""
    from schemadrift.connection.snowflake_impl import MockConnection

    return MockConnection()


# =============================================================================
# FakeDispatcher (lightweight stand-in for PluginDispatcher)
# =============================================================================


class FakeDispatcher:
    """Minimal PluginDispatcher replacement that avoids importing real plugins."""

    _scope_map = {
        "VIEW": ObjectScope.SCHEMA,
        "SCHEMA": ObjectScope.DATABASE,
        "DATABASE": ObjectScope.ACCOUNT,
    }

    def __init__(self, *, list_responses=None, extract_responses=None):
        self._list_responses: dict[tuple[str, str], list[str]] = list_responses or {}
        self._extract_responses: dict[tuple[str, str], object] = extract_responses or {}

    def get_object_types(self) -> list[str]:
        return ["DATABASE", "SCHEMA", "VIEW"]

    def has_object_type(self, object_type: str) -> bool:
        return object_type.upper() in self._scope_map

    def get_scope(self, object_type: str) -> ObjectScope:
        return self._scope_map[object_type.upper()]

    def extract_object(self, object_type, connection, identifier):
        key = (object_type.upper(), identifier.upper())
        if key in self._extract_responses:
            return self._extract_responses[key]
        from schemadrift.core.base_object import ObjectNotFoundError

        raise ObjectNotFoundError(object_type, identifier)

    def list_objects(self, object_type, connection, scope):
        key = (object_type.upper(), scope.upper() if scope else "")
        return list(self._list_responses.get(key, []))

    def compare_objects(self, target_obj, source_obj):
        @dataclass
        class _FakeDiff:
            has_changes: bool = False

        return _FakeDiff(has_changes=target_obj.name != source_obj.name)

    def generate_dict(self, obj):
        return {"name": obj.name}

    def generate_ddl(self, obj):
        return f"-- DDL for {obj.name}"

    def object_from_dict(self, object_type, data, context=None):
        return None

    def object_from_ddl(self, object_type, sql):
        return None


# =============================================================================
# Fake SnowflakeObjects for service-level tests
# =============================================================================


@dataclass
class FakeView:
    """Minimal stand-in for a SCHEMA-scoped SnowflakeObject."""

    OBJECT_TYPE: ClassVar[str] = "VIEW"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.SCHEMA

    name: str
    database_name: str
    schema_name: str

    @property
    def object_type(self) -> str:
        return self.OBJECT_TYPE

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.database_name}.{self.schema_name}.{self.name}"

    def get_database(self) -> str | None:
        return self.database_name

    def get_schema(self) -> str | None:
        return self.schema_name


@dataclass
class FakeDatabase:
    """Minimal stand-in for an ACCOUNT-scoped SnowflakeObject."""

    OBJECT_TYPE: ClassVar[str] = "DATABASE"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.ACCOUNT

    name: str

    @property
    def object_type(self) -> str:
        return self.OBJECT_TYPE

    @property
    def fully_qualified_name(self) -> str:
        return self.name

    def get_database(self) -> str | None:
        return None

    def get_schema(self) -> str | None:
        return None


@dataclass
class FakeSchema:
    """Minimal stand-in for a DATABASE-scoped SnowflakeObject."""

    OBJECT_TYPE: ClassVar[str] = "SCHEMA"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.DATABASE

    name: str
    database_name: str

    @property
    def object_type(self) -> str:
        return self.OBJECT_TYPE

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.database_name}.{self.name}"

    def get_database(self) -> str | None:
        return self.database_name

    def get_schema(self) -> str | None:
        return None
