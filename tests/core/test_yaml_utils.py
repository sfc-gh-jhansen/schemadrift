"""Tests for YAML serialization utilities."""

import pytest

from schemadrift.core.yaml_utils import dump_yaml


class TestBlockScalarDumper:
    """Test that dump_yaml renders multiline strings as YAML block scalars."""

    def test_multiline_uses_block_scalar(self):
        data = {"query": "SELECT a\nFROM table1\nWHERE x > 1"}
        result = dump_yaml(data)
        assert result.startswith("query: |-\n")
        assert "\\n" not in result

    def test_single_line_stays_plain(self):
        data = {"name": "MY_VIEW"}
        result = dump_yaml(data)
        assert result.strip() == "name: MY_VIEW"

    def test_tabs_expanded_to_spaces(self):
        """Tab characters must be expanded so PyYAML emits block scalar style."""
        data = {"query": "SELECT\n\tA,\n\tB\nFROM T"}
        result = dump_yaml(data)
        assert result.startswith("query: |-\n")
        assert "\t" not in result
        assert "\\t" not in result
        assert "    A," in result

    def test_tabs_with_snowflake_view_query(self):
        """Reproduce the exact pattern returned by DESCRIBE AS RESOURCE for a view."""
        query = (
            "CREATE OR REPLACE VIEW LOCATION_VIEW\n"
            "AS\n"
            "SELECT\n"
            "\tLOCATION_ID,\n"
            "\tCITY,\n"
            "\tREGION\n"
            "FROM LOCATION\n"
            ";"
        )
        data = {"query": query}
        result = dump_yaml(data)
        assert "query: |" in result
        assert "\\n" not in result
        assert "\\t" not in result
        assert "    LOCATION_ID," in result
        assert "    CITY," in result

    def test_preserves_key_order(self):
        data = {"name": "V", "query": "SELECT 1\nFROM T", "secure": False}
        result = dump_yaml(data)
        lines = result.strip().splitlines()
        assert lines[0] == "name: V"
        assert lines[-1] == "secure: false"
