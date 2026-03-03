"""Tests for schemadrift.core.service.DriftService."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from schemadrift.core.base_object import ObjectNotFoundError, ObjectScope
from schemadrift.core.comparison import ComparisonStatus
from schemadrift.core.config import (
    ObjectRenamer,
    ProjectConfig,
    TargetConfig,
)
from schemadrift.core.service import DriftService

from tests.conftest import FakeDatabase, FakeDispatcher, FakeSchema, FakeView


# =============================================================================
# Properties
# =============================================================================


class TestServiceProperties:
    def test_object_renamer_identity_without_config(self):
        svc = DriftService(connection=None, config=None, dispatcher=FakeDispatcher())
        assert svc.object_renamer.is_identity

    def test_object_renamer_from_config(self):
        cfg = ProjectConfig(
            object_renamer=ObjectRenamer.from_config(
                account={"A": "B"}, schemas={}
            )
        )
        svc = DriftService(connection=None, config=cfg, dispatcher=FakeDispatcher())
        assert not svc.object_renamer.is_identity

    def test_file_manager_raises_when_not_set(self):
        svc = DriftService(connection=None, dispatcher=FakeDispatcher())
        with pytest.raises(ValueError, match="FileManager not configured"):
            _ = svc.file_manager

    def test_file_manager_returns_when_set(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        svc = DriftService(connection=None, file_manager=fm, dispatcher=FakeDispatcher())
        assert svc.file_manager is fm

    def test_get_object_types(self):
        svc = DriftService(connection=None, dispatcher=FakeDispatcher())
        assert "DATABASE" in svc.get_object_types()

    def test_has_object_type(self):
        svc = DriftService(connection=None, dispatcher=FakeDispatcher())
        assert svc.has_object_type("VIEW") is True
        assert svc.has_object_type("UNKNOWN") is False


# =============================================================================
# Object renaming integration (absorbed from test_object_renamer.py)
# =============================================================================


class TestServiceObjectRenaming:
    @pytest.fixture()
    def config(self):
        return ProjectConfig(
            targets=[TargetConfig(database="SOURCE", schemas=["RAW"])],
            object_renamer=ObjectRenamer.from_config(
                account={"SOURCE": "SOURCE_DEV"},
                schemas={"SOURCE": {"RAW": "RAW_DEV"}},
            ),
        )

    @pytest.fixture()
    def dispatcher(self):
        return FakeDispatcher(
            list_responses={
                ("VIEW", "SOURCE_DEV.RAW_DEV"): ["SOURCE_DEV.RAW_DEV.CUSTOMER_VIEW"],
            },
            extract_responses={
                ("VIEW", "SOURCE_DEV.RAW_DEV.CUSTOMER_VIEW"): FakeView(
                    name="CUSTOMER_VIEW",
                    database_name="SOURCE_DEV",
                    schema_name="RAW_DEV",
                ),
            },
        )

    @pytest.fixture()
    def service(self, config, dispatcher):
        return DriftService(
            connection=None, config=config, dispatcher=dispatcher,
        )

    def test_extract_object_resolves_and_unresolves(self, service):
        obj = service.extract_object("VIEW", "SOURCE.RAW.CUSTOMER_VIEW")
        assert obj.database_name == "SOURCE"
        assert obj.schema_name == "RAW"
        assert obj.name == "CUSTOMER_VIEW"

    def test_list_objects_unresolves(self, service):
        views = service.list_objects("VIEW", "SOURCE_DEV.RAW_DEV")
        assert "SOURCE.RAW.CUSTOMER_VIEW" in views

    def test_get_scopes_for_type_resolves(self, service, config):
        scopes = service.get_scopes_for_type("VIEW", config.targets[0])
        assert scopes == ["SOURCE_DEV.RAW_DEV"]

    def test_get_identifiers_from_snowflake_returns_logical(self, service, config):
        ids = service.get_identifiers_from_snowflake("VIEW", config.targets[0])
        assert "SOURCE.RAW.CUSTOMER_VIEW" in ids

    def test_get_identifiers_from_snowflake_account_scope(self, service, config):
        ids = service.get_identifiers_from_snowflake("DATABASE", config.targets[0])
        assert ids == ["SOURCE"]

    def test_get_scopes_includes_unmapped(self, service, config):
        scopes = service.get_scopes_for_type(
            "VIEW", config.targets[0], include_unmapped=True,
        )
        assert "SOURCE_DEV.RAW_DEV" in scopes
        assert "SOURCE_DEV.RAW" in scopes

    def test_get_scopes_no_unmapped_by_default(self, service, config):
        scopes = service.get_scopes_for_type("VIEW", config.targets[0])
        assert scopes == ["SOURCE_DEV.RAW_DEV"]

    def test_to_logical_object_fields_account_scope(self):
        renamer = ObjectRenamer.from_config(account={"SOURCE": "SOURCE_DEV"}, schemas={})
        obj = FakeDatabase(name="SOURCE_DEV")
        DriftService._to_logical_object_fields(obj, renamer)
        assert obj.name == "SOURCE"

    def test_to_logical_object_fields_schema_scope(self):
        renamer = ObjectRenamer.from_config(
            account={"SOURCE": "SOURCE_DEV"},
            schemas={"SOURCE": {"RAW": "RAW_DEV"}},
        )
        obj = FakeView(name="MYVIEW", database_name="SOURCE_DEV", schema_name="RAW_DEV")
        DriftService._to_logical_object_fields(obj, renamer)
        assert obj.database_name == "SOURCE"
        assert obj.schema_name == "RAW"
        assert obj.name == "MYVIEW"


# =============================================================================
# Unmapped / all-schemas scoping tests (absorbed from test_object_renamer.py)
# =============================================================================


class TestScopingEdgeCases:
    def test_get_identifiers_from_snowflake_with_unmapped(self):
        dispatcher = FakeDispatcher(
            list_responses={
                ("VIEW", "SOURCE_DEV.RAW_DEV"): ["SOURCE_DEV.RAW_DEV.MAPPED_VIEW"],
                ("VIEW", "SOURCE_DEV.RAW"): ["SOURCE_DEV.RAW.ORPHAN_VIEW"],
            },
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="SOURCE", schemas=["RAW"])],
            object_renamer=ObjectRenamer.from_config(
                account={"SOURCE": "SOURCE_DEV"},
                schemas={"SOURCE": {"RAW": "RAW_DEV"}},
            ),
        )
        service = DriftService(
            connection=None, config=config, dispatcher=dispatcher,
        )
        ids = service.get_identifiers_from_snowflake(
            "VIEW", config.targets[0], include_unmapped=True,
        )
        id_set = {i.upper() for i in ids}
        assert "SOURCE.RAW.MAPPED_VIEW" in id_set
        assert "SOURCE.RAW.ORPHAN_VIEW" in id_set

    def test_get_scopes_includes_unmapped_all_schemas(self):
        dispatcher = FakeDispatcher(
            list_responses={
                ("SCHEMA", "SOURCE_DEV"): ["SOURCE_DEV.RAW_DEV", "SOURCE_DEV.OTHER"],
            },
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="SOURCE")],
            object_renamer=ObjectRenamer.from_config(
                account={"SOURCE": "SOURCE_DEV"},
                schemas={"SOURCE": {"RAW": "RAW_DEV"}},
            ),
        )
        service = DriftService(
            connection=None, config=config, dispatcher=dispatcher,
        )
        scopes = service.get_scopes_for_type(
            "VIEW", config.targets[0], include_unmapped=True,
        )
        assert "SOURCE_DEV.RAW_DEV" in scopes
        assert "SOURCE.RAW" in scopes
        assert "SOURCE_DEV.OTHER" in scopes

    def test_get_scopes_all_schemas_no_unmapped_by_default(self):
        dispatcher = FakeDispatcher(
            list_responses={
                ("SCHEMA", "SOURCE_DEV"): ["SOURCE_DEV.RAW_DEV", "SOURCE_DEV.OTHER"],
            },
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="SOURCE")],
            object_renamer=ObjectRenamer.from_config(
                account={"SOURCE": "SOURCE_DEV"},
                schemas={"SOURCE": {"RAW": "RAW_DEV"}},
            ),
        )
        service = DriftService(
            connection=None, config=config, dispatcher=dispatcher,
        )
        scopes = service.get_scopes_for_type("VIEW", config.targets[0])
        assert "SOURCE_DEV.RAW_DEV" in scopes
        assert "SOURCE_DEV.OTHER" in scopes
        assert "SOURCE.RAW" not in scopes

    def test_list_objects_no_filter_without_mapping(self):
        dispatcher = FakeDispatcher(
            list_responses={
                ("SCHEMA", "MYDB"): ["MYDB.RAW", "MYDB.CURATED"],
            },
        )
        service = DriftService(
            connection=None,
            config=ProjectConfig(targets=[TargetConfig(database="MYDB")]),
            dispatcher=dispatcher,
        )
        schemas = service.list_objects("SCHEMA", "MYDB")
        assert "MYDB.RAW" in schemas
        assert "MYDB.CURATED" in schemas


# =============================================================================
# Phantom collision tests (absorbed from test_object_renamer.py)
# =============================================================================


class TestPhantomCollisions:
    def test_phantom_collision_detected_as_missing_in_source(self):
        mapped_view = FakeView(
            name="MAPPED_VIEW", database_name="SOURCE_DEV", schema_name="RAW_DEV",
        )
        orphan_view = FakeView(
            name="ORPHAN_VIEW", database_name="SOURCE_DEV", schema_name="RAW",
        )
        dispatcher = FakeDispatcher(
            list_responses={
                ("VIEW", "SOURCE_DEV.RAW_DEV"): ["SOURCE_DEV.RAW_DEV.MAPPED_VIEW"],
                ("VIEW", "SOURCE_DEV.RAW"): ["SOURCE_DEV.RAW.ORPHAN_VIEW"],
            },
            extract_responses={
                ("VIEW", "SOURCE_DEV.RAW_DEV.MAPPED_VIEW"): mapped_view,
                ("VIEW", "SOURCE_DEV.RAW.ORPHAN_VIEW"): orphan_view,
            },
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="SOURCE", schemas=["RAW"])],
            object_renamer=ObjectRenamer.from_config(
                account={"SOURCE": "SOURCE_DEV"},
                schemas={"SOURCE": {"RAW": "RAW_DEV"}},
            ),
        )
        service = DriftService(
            connection=None, config=config, dispatcher=dispatcher,
        )
        source_view = FakeView(
            name="MAPPED_VIEW", database_name="SOURCE", schema_name="RAW",
        )
        with (
            patch.object(service, "_iter_source_entries",
                         return_value=[("SOURCE.RAW.MAPPED_VIEW", False)]),
            patch.object(service, "load_object_from_file", return_value=source_view),
        ):
            entries = service.compare_targets(config)

        missing = [e for e in entries if e.status == ComparisonStatus.MISSING_IN_SOURCE]
        assert len(missing) == 1
        assert "ORPHAN_VIEW" in missing[0].identifier.upper()

    def test_properly_mapped_not_flagged_as_phantom(self):
        mapped_view = FakeView(
            name="MAPPED_VIEW", database_name="SOURCE_DEV", schema_name="RAW_DEV",
        )
        dispatcher = FakeDispatcher(
            list_responses={
                ("VIEW", "SOURCE_DEV.RAW_DEV"): ["SOURCE_DEV.RAW_DEV.MAPPED_VIEW"],
                ("VIEW", "SOURCE_DEV.RAW"): [],
            },
            extract_responses={
                ("VIEW", "SOURCE_DEV.RAW_DEV.MAPPED_VIEW"): mapped_view,
            },
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="SOURCE", schemas=["RAW"])],
            object_renamer=ObjectRenamer.from_config(
                account={"SOURCE": "SOURCE_DEV"},
                schemas={"SOURCE": {"RAW": "RAW_DEV"}},
            ),
        )
        service = DriftService(
            connection=None, config=config, dispatcher=dispatcher,
        )
        source_view = FakeView(
            name="MAPPED_VIEW", database_name="SOURCE", schema_name="RAW",
        )
        with (
            patch.object(service, "_iter_source_entries",
                         return_value=[("SOURCE.RAW.MAPPED_VIEW", False)]),
            patch.object(service, "load_object_from_file", return_value=source_view),
        ):
            entries = service.compare_targets(config)

        missing = [e for e in entries if e.status == ComparisonStatus.MISSING_IN_SOURCE]
        assert len(missing) == 0


class TestAccountLevelPhantomCollision:
    def test_account_phantom_detected(self):
        mapped_db = FakeDatabase(name="SOURCE_DEV")
        orphan_db = FakeDatabase(name="SOURCE")
        dispatcher = FakeDispatcher(
            list_responses={("DATABASE", ""): ["SOURCE_DEV", "SOURCE"]},
            extract_responses={
                ("DATABASE", "SOURCE_DEV"): mapped_db,
                ("DATABASE", "SOURCE"): orphan_db,
            },
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="SOURCE", object_types=["DATABASE"])],
            object_renamer=ObjectRenamer.from_config(
                account={"SOURCE": "SOURCE_DEV"}, schemas={},
            ),
        )
        service = DriftService(
            connection=None, config=config, dispatcher=dispatcher,
        )
        source_db = FakeDatabase(name="SOURCE")
        with patch.object(service, "load_object_from_file", return_value=source_db):
            entries = service.compare_targets(config)

        missing = [e for e in entries if e.status == ComparisonStatus.MISSING_IN_SOURCE]
        assert len(missing) == 1
        assert missing[0].identifier.upper() == "SOURCE"

    def test_no_phantom_when_no_collision(self):
        mapped_db = FakeDatabase(name="SOURCE_DEV")
        dispatcher = FakeDispatcher(
            list_responses={("DATABASE", ""): ["SOURCE_DEV"]},
            extract_responses={("DATABASE", "SOURCE_DEV"): mapped_db},
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="SOURCE", object_types=["DATABASE"])],
            object_renamer=ObjectRenamer.from_config(
                account={"SOURCE": "SOURCE_DEV"}, schemas={},
            ),
        )
        service = DriftService(
            connection=None, config=config, dispatcher=dispatcher,
        )
        source_db = FakeDatabase(name="SOURCE")
        with patch.object(service, "load_object_from_file", return_value=source_db):
            entries = service.compare_targets(config)

        missing = [e for e in entries if e.status == ComparisonStatus.MISSING_IN_SOURCE]
        assert len(missing) == 0


# =============================================================================
# get_target_object_types
# =============================================================================


class TestGetTargetObjectTypes:
    @pytest.fixture()
    def service(self):
        return DriftService(
            connection=None, dispatcher=FakeDispatcher(),
        )

    def test_database_target_without_schemas(self, service):
        target = TargetConfig(database="DB")
        types = service.get_target_object_types(target)
        assert "DATABASE" in types
        assert "SCHEMA" in types
        assert "VIEW" in types

    def test_database_target_with_schemas(self, service):
        target = TargetConfig(database="DB", schemas=["SCH"])
        types = service.get_target_object_types(target)
        assert "VIEW" in types
        assert "DATABASE" not in types
        assert "SCHEMA" not in types

    def test_no_database_account_only(self, service):
        target = TargetConfig()
        types = service.get_target_object_types(target)
        assert "DATABASE" in types
        assert "SCHEMA" not in types
        assert "VIEW" not in types

    def test_explicit_object_types_filter(self, service):
        target = TargetConfig(database="DB", object_types=["VIEW"])
        types = service.get_target_object_types(target)
        assert types == ["VIEW"]


# =============================================================================
# _build_comparison_entry
# =============================================================================


class TestBuildComparisonEntry:
    @pytest.fixture()
    def service(self):
        return DriftService(
            connection=None, dispatcher=FakeDispatcher(),
        )

    def test_both_none_returns_none(self, service):
        result = service._build_comparison_entry("VIEW", "X", None, None)
        assert result is None

    def test_source_only_missing_in_target(self, service):
        src = FakeView(name="V", database_name="DB", schema_name="S")
        entry = service._build_comparison_entry("VIEW", "X", src, None)
        assert entry.status == ComparisonStatus.MISSING_IN_TARGET

    def test_target_only_missing_in_source(self, service):
        tgt = FakeView(name="V", database_name="DB", schema_name="S")
        entry = service._build_comparison_entry("VIEW", "X", None, tgt)
        assert entry.status == ComparisonStatus.MISSING_IN_SOURCE

    def test_equivalent(self, service):
        v = FakeView(name="V", database_name="DB", schema_name="S")
        entry = service._build_comparison_entry("VIEW", "X", v, v)
        assert entry.status == ComparisonStatus.EQUIVALENT

    def test_differs(self, service):
        src = FakeView(name="V1", database_name="DB", schema_name="S")
        tgt = FakeView(name="V2", database_name="DB", schema_name="S")
        entry = service._build_comparison_entry("VIEW", "X", src, tgt)
        assert entry.status == ComparisonStatus.DIFFERS


# =============================================================================
# compare_object
# =============================================================================


class TestCompareObject:
    def test_both_absent_returns_none(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        dispatcher = FakeDispatcher()
        fm = FileManager(tmp_path)
        svc = DriftService(connection=None, file_manager=fm, dispatcher=dispatcher)
        with patch.object(svc, "load_object_from_file", return_value=None):
            entry = svc.compare_object("VIEW", "DB.SCH.V")
        assert entry is None

    def test_missing_in_target(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        dispatcher = FakeDispatcher()
        fm = FileManager(tmp_path)
        svc = DriftService(connection=None, file_manager=fm, dispatcher=dispatcher)
        source = FakeView(name="V", database_name="DB", schema_name="SCH")
        with patch.object(svc, "load_object_from_file", return_value=source):
            entry = svc.compare_object("VIEW", "DB.SCH.V")
        assert entry.status == ComparisonStatus.MISSING_IN_TARGET


# =============================================================================
# get_scopes_for_type edge cases
# =============================================================================


class TestGetScopesForType:
    def test_account_scope(self):
        svc = DriftService(connection=None, dispatcher=FakeDispatcher())
        target = TargetConfig()
        scopes = svc.get_scopes_for_type("DATABASE", target)
        assert scopes == [""]

    def test_database_scope_with_database(self):
        svc = DriftService(connection=None, dispatcher=FakeDispatcher())
        target = TargetConfig(database="MYDB")
        scopes = svc.get_scopes_for_type("SCHEMA", target)
        assert scopes == ["MYDB"]

    def test_account_scope_with_database_target(self):
        """ACCOUNT-scope (DATABASE type) with a database target returns the resolved db."""
        svc = DriftService(connection=None, dispatcher=FakeDispatcher())
        target = TargetConfig(database="DB")
        scopes = svc.get_scopes_for_type("DATABASE", target)
        assert scopes == [""]


# =============================================================================
# save_object_to_file
# =============================================================================


class TestSaveObjectToFile:
    def test_yaml_format(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        svc = DriftService(connection=None, file_manager=fm, dispatcher=FakeDispatcher())
        obj = FakeView(name="V", database_name="DB", schema_name="SCH")
        path = svc.save_object_to_file(obj, format="yaml")
        assert path.suffix == ".yaml"
        assert path.exists()

    def test_sql_format(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        svc = DriftService(connection=None, file_manager=fm, dispatcher=FakeDispatcher())
        obj = FakeView(name="V", database_name="DB", schema_name="SCH")
        path = svc.save_object_to_file(obj, format="sql")
        assert path.suffix == ".sql"
        assert path.exists()


# =============================================================================
# Externally managed object coexistence
# =============================================================================


class TestExternallyManaged:
    def test_is_externally_managed_true(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "ML_INFERENCE").mkdir()

        svc = DriftService(connection=None, file_manager=fm, dispatcher=FakeDispatcher())
        assert svc._is_externally_managed("VIEW", "DB.SCH.ML_INFERENCE") is True

    def test_is_externally_managed_false(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "NORMAL.yaml").write_text("name: NORMAL")

        svc = DriftService(connection=None, file_manager=fm, dispatcher=FakeDispatcher())
        assert svc._is_externally_managed("VIEW", "DB.SCH.NORMAL") is False

    def test_extract_to_file_raises_for_external(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "ML_VIEW").mkdir()

        svc = DriftService(connection=None, file_manager=fm, dispatcher=FakeDispatcher())
        with pytest.raises(ValueError, match="externally managed"):
            svc.extract_to_file("VIEW", "DB.SCH.ML_VIEW")

    def test_detect_missing_skips_external_in_source_ids(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "CLI_VIEW").mkdir()

        sf_view = FakeView(name="CLI_VIEW", database_name="DB", schema_name="SCH")
        dispatcher = FakeDispatcher(
            list_responses={("VIEW", "DB.SCH"): ["DB.SCH.CLI_VIEW"]},
            extract_responses={("VIEW", "DB.SCH.CLI_VIEW"): sf_view},
        )
        svc = DriftService(connection=None, file_manager=fm, dispatcher=dispatcher)

        entries: list = []
        target = TargetConfig(database="DB", schemas=["SCH"])
        svc._detect_missing_in_source(
            "VIEW", target, {"DB.SCH.CLI_VIEW"}, entries,
        )

        assert len(entries) == 0

    def test_detect_missing_normal_still_works(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)

        sf_view = FakeView(name="ORPHAN", database_name="DB", schema_name="SCH")
        dispatcher = FakeDispatcher(
            list_responses={("VIEW", "DB.SCH"): ["DB.SCH.ORPHAN"]},
            extract_responses={("VIEW", "DB.SCH.ORPHAN"): sf_view},
        )
        svc = DriftService(connection=None, file_manager=fm, dispatcher=dispatcher)

        entries: list = []
        target = TargetConfig(database="DB", schemas=["SCH"])
        svc._detect_missing_in_source("VIEW", target, set(), entries)

        assert len(entries) == 1
        assert entries[0].status == ComparisonStatus.MISSING_IN_SOURCE

    def test_compare_targets_shows_external_directory(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "CLI_VIEW").mkdir()
        (views_dir / "NORMAL.yaml").write_text("name: NORMAL")

        sf_normal = FakeView(name="NORMAL", database_name="DB", schema_name="SCH")
        sf_cli = FakeView(name="CLI_VIEW", database_name="DB", schema_name="SCH")
        dispatcher = FakeDispatcher(
            list_responses={("VIEW", "DB.SCH"): ["DB.SCH.NORMAL", "DB.SCH.CLI_VIEW"]},
            extract_responses={
                ("VIEW", "DB.SCH.NORMAL"): sf_normal,
                ("VIEW", "DB.SCH.CLI_VIEW"): sf_cli,
            },
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="DB", schemas=["SCH"], object_types=["VIEW"])],
        )
        svc = DriftService(
            connection=None, file_manager=fm, dispatcher=dispatcher, config=config,
        )
        source_view = FakeView(name="NORMAL", database_name="DB", schema_name="SCH")
        with patch.object(svc, "load_object_from_file", return_value=source_view):
            entries = svc.compare_targets(config)

        external = [e for e in entries if e.status == ComparisonStatus.EXTERNALLY_MANAGED]
        assert len(external) == 1
        assert "CLI_VIEW" in external[0].identifier

        missing = [e for e in entries if e.status == ComparisonStatus.MISSING_IN_SOURCE]
        assert len(missing) == 0

    def test_compare_targets_external_not_in_snowflake(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "LOCAL_ONLY").mkdir()

        dispatcher = FakeDispatcher(
            list_responses={("VIEW", "DB.SCH"): []},
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="DB", schemas=["SCH"], object_types=["VIEW"])],
        )
        svc = DriftService(
            connection=None, file_manager=fm, dispatcher=dispatcher, config=config,
        )
        entries = svc.compare_targets(config)

        assert len(entries) == 1
        assert entries[0].status == ComparisonStatus.EXTERNALLY_MANAGED
        assert "LOCAL_ONLY" in entries[0].identifier


# =============================================================================
# _build_context_from_parts
# =============================================================================


class TestBuildContextFromParts:
    def test_empty_parts(self):
        ctx = DriftService._build_context_from_parts({"name": "V"})
        assert ctx == {}

    def test_database_only(self):
        ctx = DriftService._build_context_from_parts({"database": "DB", "name": "SCH"})
        assert ctx == {"database_name": "DB"}

    def test_database_and_schema(self):
        ctx = DriftService._build_context_from_parts(
            {"database": "DB", "schema": "SCH", "name": "V"}
        )
        assert ctx == {"database_name": "DB", "schema_name": "SCH"}


# =============================================================================
# _to_logical_object_fields -- DATABASE scope
# =============================================================================


class TestToLogicalObjectFieldsDatabaseScope:
    def test_database_scope_translates_both_fields(self):
        renamer = ObjectRenamer.from_config(
            account={"SOURCE": "SOURCE_DEV"},
            schemas={"SOURCE": {"RAW": "RAW_DEV"}},
        )
        obj = FakeSchema(name="RAW_DEV", database_name="SOURCE_DEV")
        DriftService._to_logical_object_fields(obj, renamer)
        assert obj.database_name == "SOURCE"
        assert obj.name == "RAW"

    def test_database_scope_identity_is_noop(self):
        renamer = ObjectRenamer.identity()
        obj = FakeSchema(name="RAW", database_name="SOURCE")
        DriftService._to_logical_object_fields(obj, renamer)
        assert obj.database_name == "SOURCE"
        assert obj.name == "RAW"


# =============================================================================
# load_object_from_file
# =============================================================================


class TestLoadObjectFromFile:
    def test_returns_none_when_file_missing(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        svc = DriftService(connection=None, file_manager=fm, dispatcher=FakeDispatcher())
        result = svc.load_object_from_file("VIEW", "DB.SCH.NOEXIST")
        assert result is None

    def test_yaml_round_trip(self, tmp_path):
        from schemadrift.core.file_manager import FileManager
        from schemadrift.plugins.manager import PluginDispatcher

        fm = FileManager(tmp_path)
        dispatcher = PluginDispatcher()
        svc = DriftService(connection=None, file_manager=fm, dispatcher=dispatcher)

        from schemadrift.plugins.builtin.view.model import View

        original = View(
            name="MYVIEW",
            database_name="DB",
            schema_name="SCH",
            query="SELECT 1",
        )
        svc.save_object_to_file(original, format="yaml")
        loaded = svc.load_object_from_file("VIEW", "DB.SCH.MYVIEW")

        assert loaded is not None
        assert loaded.name == "MYVIEW"
        assert loaded.database_name == "DB"
        assert loaded.schema_name == "SCH"


# =============================================================================
# extract_targets
# =============================================================================


class TestExtractTargets:
    def test_extract_targets_writes_files(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        view = FakeView(name="V1", database_name="DB", schema_name="SCH")
        dispatcher = FakeDispatcher(
            list_responses={("VIEW", "DB.SCH"): ["DB.SCH.V1"]},
            extract_responses={("VIEW", "DB.SCH.V1"): view},
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="DB", schemas=["SCH"], object_types=["VIEW"])],
        )
        svc = DriftService(
            connection=None, file_manager=fm, dispatcher=dispatcher, config=config,
        )
        paths = svc.extract_targets(config)
        assert len(paths) == 1
        assert paths[0].exists()

    def test_extract_targets_skips_external(self, tmp_path):
        from schemadrift.core.file_manager import FileManager

        fm = FileManager(tmp_path)
        views_dir = tmp_path / "DB" / "SCH" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "CLI_VIEW").mkdir()

        view = FakeView(name="CLI_VIEW", database_name="DB", schema_name="SCH")
        dispatcher = FakeDispatcher(
            list_responses={("VIEW", "DB.SCH"): ["DB.SCH.CLI_VIEW"]},
            extract_responses={("VIEW", "DB.SCH.CLI_VIEW"): view},
        )
        config = ProjectConfig(
            targets=[TargetConfig(database="DB", schemas=["SCH"], object_types=["VIEW"])],
        )
        svc = DriftService(
            connection=None, file_manager=fm, dispatcher=dispatcher, config=config,
        )
        paths = svc.extract_targets(config)
        assert len(paths) == 0
