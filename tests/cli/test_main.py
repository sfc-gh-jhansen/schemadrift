"""Tests for schemadrift.cli.main."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from schemadrift.cli.main import app, _resolve_config


runner = CliRunner()


# =============================================================================
# Version
# =============================================================================


class TestVersion:
    def test_version_flag(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "schemadrift version" in result.output


# =============================================================================
# Extract command -- argument validation
# =============================================================================


class TestExtractValidation:
    def test_no_output_or_stdout_errors(self):
        result = runner.invoke(app, [
            "extract", "view", "DB.SCH.V",
            "--connection", "fake",
        ])
        assert result.exit_code != 0
        assert "Must specify --output or --stdout" in result.output

    def test_identifier_without_object_type_errors(self):
        """Typer won't allow this ordering, but the guard exists."""
        result = runner.invoke(app, [
            "extract",
            "--stdout",
            "--connection", "fake",
        ])
        # Without object_type AND without config file -> error
        assert result.exit_code != 0

    def test_no_identifier_no_config_errors(self):
        result = runner.invoke(app, [
            "extract",
            "--stdout",
            "--connection", "fake",
        ])
        assert result.exit_code != 0


# =============================================================================
# Compare command -- argument validation
# =============================================================================


class TestCompareValidation:
    def test_no_object_type_no_config_errors(self):
        result = runner.invoke(app, [
            "compare",
            "--source", ".",
            "--connection", "fake",
        ])
        assert result.exit_code != 0


# =============================================================================
# _resolve_config
# =============================================================================


class TestResolveConfig:
    def test_explicit_path_not_found(self, tmp_path):
        import typer
        with pytest.raises(typer.Exit):
            _resolve_config(config_path=tmp_path / "nope.toml")

    def test_explicit_path_valid(self, tmp_path):
        cfg_file = tmp_path / "test.toml"
        cfg_file.write_text('[[targets]]\ndatabase = "DB"\n')
        config = _resolve_config(config_path=cfg_file)
        assert len(config.targets) == 1

    def test_default_file_fallback(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        default = tmp_path / "schemadrift.toml"
        default.write_text('[[targets]]\ndatabase = "X"\n')
        config = _resolve_config()
        assert config.targets[0].database == "X"

    def test_no_config_anywhere(self, tmp_path, monkeypatch):
        import typer
        monkeypatch.chdir(tmp_path)
        with pytest.raises(typer.Exit):
            _resolve_config()
