"""Tests for the Role plugin.

Run with: pytest src/schemadrift/plugins/builtin/role/tests.py -v
"""

import json

import pytest

from schemadrift.core.base_object import ObjectExtractionError, ObjectNotFoundError
from schemadrift.plugins.builtin.role.model import Role
from schemadrift.connection.snowflake_impl import MockConnection


class TestRoleModel:
    """Test the Role model class directly."""

    def test_create_role(self):
        """Test creating a Role instance."""
        role = Role(
            name="DATA_ENGINEER",
            comment="Data engineering team role",
        )

        assert role.name == "DATA_ENGINEER"
        assert role.object_type == "ROLE"
        assert role.fully_qualified_name == "DATA_ENGINEER"
        assert role.comment == "Data engineering team role"

    def test_to_ddl_raises(self):
        """to_ddl() is a stub and should raise NotImplementedError."""
        role = Role(name="MY_ROLE")
        with pytest.raises(NotImplementedError):
            role.to_ddl()

    def test_from_ddl_raises(self):
        """from_ddl() is a stub and should raise NotImplementedError."""
        with pytest.raises(NotImplementedError):
            Role.from_ddl("CREATE ROLE MY_ROLE")

    def test_to_dict(self):
        """Test dictionary export excludes contextual/read-only fields."""
        role = Role(
            name="ANALYST",
            comment="Analyst role",
        )

        data = role.to_dict()
        assert data["name"] == "ANALYST"
        assert data["comment"] == "Analyst role"
        assert "object_type" not in data

    def test_from_dict(self):
        """Test loading from dictionary."""
        data = {
            "name": "MY_ROLE",
            "comment": "Production role",
        }

        role = Role.from_dict(data)
        assert role.name == "MY_ROLE"
        assert role.comment == "Production role"

    def test_dict_round_trip(self):
        """Test that to_dict -> from_dict produces an equivalent object."""
        original = Role(
            name="ROUND_TRIP",
            comment="round trip test",
        )
        restored = Role.from_dict(original.to_dict())
        assert restored == original

    def test_compare_identical(self):
        """Test comparing identical roles."""
        role1 = Role(name="R")
        role2 = Role(name="R")

        diff = role1.compare(role2)
        assert not diff.has_changes

    def test_compare_comment_change(self):
        """Test comparing roles with comment change."""
        role1 = Role(name="R", comment="old")
        role2 = Role(name="R", comment="new")

        diff = role1.compare(role2)
        assert diff.has_changes
        assert "comment" in diff.modified


class TestRoleWithMockConnection:
    """Test Role extraction with mock connection."""

    def _mock_describe_response(self, mock: MockConnection, identifier: str, data: dict) -> None:
        """Register a DESCRIBE AS RESOURCE ROLE mock response."""
        mock.add_response(
            f"DESCRIBE AS RESOURCE ROLE {identifier}",
            [{"resource": json.dumps(data)}],
        )

    def test_extract(self):
        """Test extracting a role via DESCRIBE AS RESOURCE."""
        mock = MockConnection()
        self._mock_describe_response(mock, "DATA_ENGINEER", {
            "name": "DATA_ENGINEER",
            "comment": "Data engineering team role",
            "created_on": "2024-01-01T00:00:00Z",
            "owner": "USERADMIN",
            "assigned_to_users": 5,
            "granted_roles": 3,
            "granted_to_roles": 1,
            "is_current": False,
            "is_default": False,
            "is_inherited": False,
        })

        role = Role.extract(mock, "DATA_ENGINEER")
        assert role.name == "DATA_ENGINEER"
        assert role.comment == "Data engineering team role"

    def test_extract_not_found(self):
        """Test that extracting a missing role raises ObjectNotFoundError."""
        mock = MockConnection()
        with pytest.raises(ObjectNotFoundError) as exc_info:
            Role.extract(mock, "NONEXISTENT")
        assert exc_info.value.object_type == "ROLE"

    def test_extract_malformed_json(self):
        """Test that malformed JSON raises ObjectExtractionError."""
        mock = MockConnection()
        mock.add_response(
            "DESCRIBE AS RESOURCE ROLE BAD",
            [{"resource": "NOT VALID JSON {{{"}],
        )
        with pytest.raises(ObjectExtractionError) as exc_info:
            Role.extract(mock, "BAD")
        assert exc_info.value.object_type == "ROLE"

    def test_list_roles(self):
        """Test listing roles returns all results unfiltered."""
        mock = MockConnection()
        mock.add_response(
            "SHOW AS RESOURCE TERSE ROLES",
            [
                {"resource": '{"name": "SYSADMIN"}'},
                {"resource": '{"name": "DATA_ENGINEER"}'},
                {"resource": '{"name": "ANALYST"}'},
            ],
        )

        roles = Role.list_objects(mock, "")
        assert len(roles) == 3
        assert "SYSADMIN" in roles
        assert "DATA_ENGINEER" in roles
        assert "ANALYST" in roles


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
