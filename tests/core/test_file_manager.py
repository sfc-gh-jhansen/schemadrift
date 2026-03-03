"""Tests for schemadrift.core.file_manager."""

from __future__ import annotations

from pathlib import Path

import pytest

from schemadrift.core.base_object import ObjectScope
from schemadrift.core.file_manager import FileManager, FileStructure, SourceEntry


# =============================================================================
# FileStructure -- static helpers
# =============================================================================


class TestFileStructureGetTypeDirectory:
    def test_default_pluralization(self):
        assert FileStructure.get_type_directory("VIEW") == "views"
        assert FileStructure.get_type_directory("DATABASE") == "databases"
        assert FileStructure.get_type_directory("SCHEMA") == "schemas"

    def test_lowercase_input(self):
        assert FileStructure.get_type_directory("view") == "views"


class TestFileStructureNormalizeName:
    def test_uppercase(self):
        assert FileStructure.normalize_name("myview") == "MYVIEW"


class TestFileStructureGetFileName:
    def test_format(self):
        assert FileStructure.get_file_name("myview", "yaml") == "MYVIEW.yaml"
        assert FileStructure.get_file_name("myview", "sql") == "MYVIEW.sql"


class TestFileStructureBuildPath:
    def test_account_level(self, tmp_path):
        path = FileStructure.build_path(tmp_path, "DATABASE", name="MYDB")
        assert path == tmp_path / "databases" / "MYDB.yaml"

    def test_database_level(self, tmp_path):
        path = FileStructure.build_path(tmp_path, "SCHEMA", database="MYDB", name="RAW")
        assert path == tmp_path / "MYDB" / "schemas" / "RAW.yaml"

    def test_schema_level(self, tmp_path):
        path = FileStructure.build_path(
            tmp_path, "VIEW", database="MYDB", schema="RAW", name="V",
        )
        assert path == tmp_path / "MYDB" / "RAW" / "views" / "V.yaml"

    def test_directory_only_no_name(self, tmp_path):
        path = FileStructure.build_path(tmp_path, "VIEW", database="DB", schema="SCH")
        assert path == tmp_path / "DB" / "SCH" / "views"

    def test_custom_extension(self, tmp_path):
        path = FileStructure.build_path(
            tmp_path, "VIEW", database="DB", schema="SCH", name="V", extension="sql",
        )
        assert path.suffix == ".sql"


class TestFileStructureParseHierarchyFromPath:
    def test_schema_level(self, tmp_path):
        file_path = tmp_path / "MYDB" / "RAW" / "views" / "V.yaml"
        result = FileStructure.parse_hierarchy_from_path(file_path, tmp_path)
        assert result["database"] == "MYDB"
        assert result["schema"] == "RAW"
        assert result["name"] == "V"

    def test_database_level(self, tmp_path):
        file_path = tmp_path / "MYDB" / "schemas" / "RAW.yaml"
        result = FileStructure.parse_hierarchy_from_path(file_path, tmp_path)
        assert result["database"] == "MYDB"
        assert result["name"] == "RAW"
        assert "schema" not in result

    def test_account_level(self, tmp_path):
        file_path = tmp_path / "databases" / "MYDB.yaml"
        result = FileStructure.parse_hierarchy_from_path(file_path, tmp_path)
        assert result["name"] == "MYDB"
        assert "database" not in result


class TestFileStructureBuildIdentifier:
    def test_account_scope(self):
        ident = FileStructure.build_identifier(
            ObjectScope.ACCOUNT, {"name": "MYDB"}, "IGNORED",
        )
        assert ident == "MYDB"

    def test_database_scope(self):
        ident = FileStructure.build_identifier(
            ObjectScope.DATABASE, {"name": "RAW", "database": "DB"}, "DB",
        )
        assert ident == "DB.RAW"

    def test_schema_scope(self):
        ident = FileStructure.build_identifier(
            ObjectScope.SCHEMA, {"name": "V", "database": "DB", "schema": "SCH"}, "DB",
        )
        assert ident == "DB.SCH.V"

    def test_schema_scope_missing_schema_raises(self):
        with pytest.raises(ValueError, match="missing schema"):
            FileStructure.build_identifier(
                ObjectScope.SCHEMA, {"name": "V"}, "DB",
            )


class TestFileStructureParseIdentifier:
    def test_account_scope(self):
        result = FileStructure.parse_identifier(ObjectScope.ACCOUNT, "MYDB")
        assert result == {"name": "MYDB"}

    def test_database_scope(self):
        result = FileStructure.parse_identifier(ObjectScope.DATABASE, "MYDB.SCH")
        assert result == {"database": "MYDB", "name": "SCH"}

    def test_schema_scope(self):
        result = FileStructure.parse_identifier(ObjectScope.SCHEMA, "MYDB.SCH.V")
        assert result == {"database": "MYDB", "schema": "SCH", "name": "V"}

    def test_database_scope_insufficient_parts(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            FileStructure.parse_identifier(ObjectScope.DATABASE, "MYDB")

    def test_schema_scope_insufficient_parts(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            FileStructure.parse_identifier(ObjectScope.SCHEMA, "MYDB.SCH")


# =============================================================================
# FileManager
# =============================================================================


class TestFileManagerWriteAndRead:
    def test_write_creates_dirs_and_file(self, tmp_path):
        fm = FileManager(tmp_path)
        path = fm.write_object(
            content="name: V\nquery: SELECT 1\n",
            object_type="VIEW",
            database="DB",
            schema="SCH",
            name="V",
        )
        assert path.exists()
        assert path.read_text() == "name: V\nquery: SELECT 1\n"

    def test_read_existing(self, tmp_path):
        fm = FileManager(tmp_path)
        fm.write_object("content", "VIEW", database="DB", schema="SCH", name="V")
        result = fm.read_object("VIEW", database="DB", schema="SCH", name="V")
        assert result == "content"

    def test_read_missing_returns_none(self, tmp_path):
        fm = FileManager(tmp_path)
        result = fm.read_object("VIEW", database="DB", schema="SCH", name="NOPE")
        assert result is None


class TestFileManagerReadFile:
    def test_read_existing_file(self, tmp_path):
        p = tmp_path / "test.txt"
        p.write_text("hello")
        fm = FileManager(tmp_path)
        assert fm.read_file(p) == "hello"

    def test_read_missing_file(self, tmp_path):
        fm = FileManager(tmp_path)
        assert fm.read_file(tmp_path / "nope.txt") is None


class TestFileManagerListObjects:
    def test_list_matching(self, tmp_path):
        fm = FileManager(tmp_path)
        fm.write_object("a", "VIEW", database="DB", schema="SCH", name="V1")
        fm.write_object("b", "VIEW", database="DB", schema="SCH", name="V2")
        paths = fm.list_objects("VIEW", database="DB", schema="SCH")
        assert len(paths) == 2
        names = {p.stem for p in paths}
        assert names == {"V1", "V2"}

    def test_list_empty_directory(self, tmp_path):
        fm = FileManager(tmp_path)
        assert fm.list_objects("VIEW", database="DB", schema="SCH") == []

    def test_list_filters_by_extension(self, tmp_path):
        fm = FileManager(tmp_path)
        fm.write_object("a", "VIEW", database="DB", schema="SCH", name="V1", extension="yaml")
        fm.write_object("b", "VIEW", database="DB", schema="SCH", name="V2", extension="sql")
        yaml_paths = fm.list_objects("VIEW", database="DB", schema="SCH", extension="yaml")
        assert len(yaml_paths) == 1
        assert yaml_paths[0].stem == "V1"


class TestFileManagerIdentifierFromFilePath:
    def test_schema_scope(self, tmp_path):
        fm = FileManager(tmp_path)
        file_path = tmp_path / "MYDB" / "SCH" / "views" / "V.yaml"
        ident = fm.identifier_from_file_path(ObjectScope.SCHEMA, file_path, "MYDB")
        assert ident == "MYDB.SCH.V"

    def test_account_scope(self, tmp_path):
        fm = FileManager(tmp_path)
        file_path = tmp_path / "databases" / "MYDB.yaml"
        ident = fm.identifier_from_file_path(ObjectScope.ACCOUNT, file_path, "")
        assert ident == "MYDB"


# =============================================================================
# FileManager -- external object (directory) detection
# =============================================================================


class TestFileManagerIsExternalObject:
    def test_true_when_directory_exists(self, tmp_path):
        fm = FileManager(tmp_path)
        proc_dir = tmp_path / "DB" / "SCH" / "procedures"
        proc_dir.mkdir(parents=True)
        (proc_dir / "ML_INFERENCE").mkdir()

        assert fm.is_external_object("PROCEDURE", "ML_INFERENCE", database="DB", schema="SCH") is True

    def test_false_when_only_file_exists(self, tmp_path):
        fm = FileManager(tmp_path)
        proc_dir = tmp_path / "DB" / "SCH" / "procedures"
        proc_dir.mkdir(parents=True)
        (proc_dir / "GET_CUSTOMER.yaml").write_text("name: GET_CUSTOMER")

        assert fm.is_external_object("PROCEDURE", "GET_CUSTOMER", database="DB", schema="SCH") is False

    def test_false_when_nothing_exists(self, tmp_path):
        fm = FileManager(tmp_path)
        assert fm.is_external_object("VIEW", "NOPE", database="DB", schema="SCH") is False

    def test_case_insensitive_via_normalize(self, tmp_path):
        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "MY_VIEW").mkdir()

        assert fm.is_external_object("VIEW", "my_view", database="DB", schema="SCH") is True


# =============================================================================
# FileManager -- list_source_entries (unified scan)
# =============================================================================


class TestFileManagerListSourceEntries:
    def test_returns_files_and_directories(self, tmp_path):
        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "NORMAL_VIEW.yaml").write_text("name: NORMAL_VIEW")
        (views_dir / "CLI_VIEW").mkdir()

        entries = fm.list_source_entries("VIEW", database="DB", schema="SCH")
        assert len(entries) == 2

        by_name = {e.path.stem: e for e in entries}
        assert by_name["CLI_VIEW"].is_external is True
        assert by_name["NORMAL_VIEW"].is_external is False

    def test_excludes_hidden_directories(self, tmp_path):
        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / ".hidden").mkdir()
        (views_dir / "V.yaml").write_text("x")

        entries = fm.list_source_entries("VIEW", database="DB", schema="SCH")
        assert len(entries) == 1
        assert entries[0].is_external is False

    def test_filters_by_extension(self, tmp_path):
        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "V.yaml").write_text("x")
        (views_dir / "V.sql").write_text("x")

        yaml_entries = fm.list_source_entries("VIEW", database="DB", schema="SCH", extension="yaml")
        assert len(yaml_entries) == 1
        assert yaml_entries[0].path.suffix == ".yaml"

    def test_empty_when_path_missing(self, tmp_path):
        fm = FileManager(tmp_path)
        assert fm.list_source_entries("VIEW", database="DB", schema="SCH") == []

    def test_sorted_by_name(self, tmp_path):
        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "ZEBRA.yaml").write_text("x")
        (views_dir / "ALPHA").mkdir()
        (views_dir / "MIDDLE.yaml").write_text("x")

        entries = fm.list_source_entries("VIEW", database="DB", schema="SCH")
        names = [e.path.stem for e in entries]
        assert names == ["ALPHA", "MIDDLE", "ZEBRA"]
