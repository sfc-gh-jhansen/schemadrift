"""Tests for schemadrift.core.config."""

from __future__ import annotations

import pytest

from schemadrift.core.base_object import ObjectScope
from schemadrift.core.config import (
    DEFAULT_OBJECT_TYPE_CONFIGS,
    ObjectRenamer,
    ObjectTypeConfig,
    ProjectConfig,
    TargetConfig,
    is_managed,
    load_config,
    _parse_targets,
    _parse_object_types,
    _parse_name_mapping,
)


# =============================================================================
# ObjectRenamer -- identity
# =============================================================================


class TestObjectRenamerIdentity:
    def test_identity_passthrough(self):
        r = ObjectRenamer.identity()
        assert r.to_physical_account("SOURCE") == "SOURCE"
        assert r.to_logical_account("SOURCE") == "SOURCE"
        assert r.to_physical_schema("SOURCE", "RAW") == "RAW"
        assert r.to_logical_schema("SOURCE", "RAW") == "RAW"
        assert r.is_identity is True

    def test_identity_to_physical_identifier(self):
        r = ObjectRenamer.identity()
        assert r.to_physical_identifier(ObjectScope.ACCOUNT, "SOURCE") == "SOURCE"
        assert r.to_physical_identifier(ObjectScope.DATABASE, "SOURCE.RAW") == "SOURCE.RAW"
        assert r.to_physical_identifier(ObjectScope.SCHEMA, "SOURCE.RAW.V1") == "SOURCE.RAW.V1"

    def test_identity_to_logical_identifier(self):
        r = ObjectRenamer.identity()
        assert r.to_logical_identifier(ObjectScope.ACCOUNT, "SOURCE") == "SOURCE"
        assert r.to_logical_identifier(ObjectScope.DATABASE, "SOURCE.RAW") == "SOURCE.RAW"
        assert r.to_logical_identifier(ObjectScope.SCHEMA, "SOURCE.RAW.V1") == "SOURCE.RAW.V1"


# =============================================================================
# ObjectRenamer -- account-only mapping
# =============================================================================


class TestObjectRenamerAccountOnly:
    @pytest.fixture()
    def renamer(self):
        return ObjectRenamer.from_config(
            account={"SOURCE": "SOURCE_DEV", "ANALYTICS": "ANALYTICS_DEV"},
            schemas={},
        )

    def test_is_not_identity(self, renamer):
        assert renamer.is_identity is False

    def test_to_physical_account(self, renamer):
        assert renamer.to_physical_account("SOURCE") == "SOURCE_DEV"
        assert renamer.to_physical_account("ANALYTICS") == "ANALYTICS_DEV"
        assert renamer.to_physical_account("UNMAPPED") == "UNMAPPED"

    def test_to_logical_account(self, renamer):
        assert renamer.to_logical_account("SOURCE_DEV") == "SOURCE"
        assert renamer.to_logical_account("ANALYTICS_DEV") == "ANALYTICS"
        assert renamer.to_logical_account("UNMAPPED") == "UNMAPPED"

    def test_case_insensitive(self, renamer):
        assert renamer.to_physical_account("source") == "SOURCE_DEV"
        assert renamer.to_logical_account("source_dev") == "SOURCE"

    def test_schema_passthrough(self, renamer):
        assert renamer.to_physical_schema("SOURCE", "RAW") == "RAW"
        assert renamer.to_logical_schema("SOURCE", "RAW") == "RAW"

    def test_to_physical_identifier_account_scope(self, renamer):
        assert renamer.to_physical_identifier(ObjectScope.ACCOUNT, "SOURCE") == "SOURCE_DEV"

    def test_to_physical_identifier_database_scope(self, renamer):
        assert renamer.to_physical_identifier(ObjectScope.DATABASE, "SOURCE.RAW") == "SOURCE_DEV.RAW"

    def test_to_physical_identifier_schema_scope(self, renamer):
        assert renamer.to_physical_identifier(ObjectScope.SCHEMA, "SOURCE.RAW.V1") == "SOURCE_DEV.RAW.V1"

    def test_to_logical_identifier_roundtrip(self, renamer):
        for scope, ident in [
            (ObjectScope.ACCOUNT, "SOURCE"),
            (ObjectScope.DATABASE, "SOURCE.RAW"),
            (ObjectScope.SCHEMA, "SOURCE.RAW.MYVIEW"),
        ]:
            resolved = renamer.to_physical_identifier(scope, ident)
            assert renamer.to_logical_identifier(scope, resolved) == ident


# =============================================================================
# ObjectRenamer -- account + schema mapping
# =============================================================================


class TestObjectRenamerWithSchemas:
    @pytest.fixture()
    def renamer(self):
        return ObjectRenamer.from_config(
            account={"SOURCE": "SOURCE_DEV"},
            schemas={
                "SOURCE": {"RAW": "RAW_DEV", "CURATED": "CURATED_DEV"},
            },
        )

    def test_to_physical_schema(self, renamer):
        assert renamer.to_physical_schema("SOURCE", "RAW") == "RAW_DEV"
        assert renamer.to_physical_schema("SOURCE", "CURATED") == "CURATED_DEV"
        assert renamer.to_physical_schema("SOURCE", "UNMAPPED") == "UNMAPPED"

    def test_to_logical_schema(self, renamer):
        assert renamer.to_logical_schema("SOURCE", "RAW_DEV") == "RAW"
        assert renamer.to_logical_schema("SOURCE", "CURATED_DEV") == "CURATED"

    def test_schema_scoped_to_database(self, renamer):
        assert renamer.to_physical_schema("OTHER_DB", "RAW") == "RAW"

    def test_to_physical_identifier_database_scope(self, renamer):
        assert renamer.to_physical_identifier(ObjectScope.DATABASE, "SOURCE.RAW") == "SOURCE_DEV.RAW_DEV"

    def test_to_physical_identifier_schema_scope(self, renamer):
        result = renamer.to_physical_identifier(ObjectScope.SCHEMA, "SOURCE.RAW.MYVIEW")
        assert result == "SOURCE_DEV.RAW_DEV.MYVIEW"

    def test_leaf_name_never_mapped(self, renamer):
        result = renamer.to_physical_identifier(ObjectScope.SCHEMA, "SOURCE.CURATED.RAW")
        assert result == "SOURCE_DEV.CURATED_DEV.RAW"

    def test_to_logical_identifier_roundtrip(self, renamer):
        for scope, ident in [
            (ObjectScope.ACCOUNT, "SOURCE"),
            (ObjectScope.DATABASE, "SOURCE.RAW"),
            (ObjectScope.DATABASE, "SOURCE.CURATED"),
            (ObjectScope.SCHEMA, "SOURCE.RAW.MYVIEW"),
            (ObjectScope.SCHEMA, "SOURCE.CURATED.REPORT"),
        ]:
            resolved = renamer.to_physical_identifier(scope, ident)
            assert renamer.to_logical_identifier(scope, resolved) == ident


# =============================================================================
# ObjectRenamer -- multiple databases
# =============================================================================


class TestObjectRenamerMultipleDatabases:
    @pytest.fixture()
    def renamer(self):
        return ObjectRenamer.from_config(
            account={"SOURCE": "SOURCE_DEV", "ANALYTICS": "ANALYTICS_DEV"},
            schemas={
                "SOURCE": {"RAW": "RAW_LANDING"},
                "ANALYTICS": {"RAW": "RAW_PROCESSED"},
            },
        )

    def test_same_schema_different_mapping(self, renamer):
        assert renamer.to_physical_schema("SOURCE", "RAW") == "RAW_LANDING"
        assert renamer.to_physical_schema("ANALYTICS", "RAW") == "RAW_PROCESSED"

    def test_identifiers(self, renamer):
        assert renamer.to_physical_identifier(
            ObjectScope.SCHEMA, "SOURCE.RAW.V1"
        ) == "SOURCE_DEV.RAW_LANDING.V1"
        assert renamer.to_physical_identifier(
            ObjectScope.SCHEMA, "ANALYTICS.RAW.V1"
        ) == "ANALYTICS_DEV.RAW_PROCESSED.V1"


# =============================================================================
# ObjectRenamer -- bijective validation
# =============================================================================


class TestObjectRenamerValidation:
    def test_rejects_non_bijective_account_mapping(self):
        with pytest.raises(ValueError, match="Non-bijective account name mapping"):
            ObjectRenamer.from_config(
                account={"DB_A": "PROD_DB", "DB_B": "PROD_DB"},
                schemas={},
            )

    def test_rejects_non_bijective_schema_mapping(self):
        with pytest.raises(ValueError, match="Non-bijective schema name mapping"):
            ObjectRenamer.from_config(
                account={},
                schemas={"MY_DB": {"SCHEMA_A": "TARGET", "SCHEMA_B": "TARGET"}},
            )


# =============================================================================
# ObjectTypeConfig / DEFAULT_OBJECT_TYPE_CONFIGS
# =============================================================================


class TestObjectTypeConfig:
    def test_default_database_exclusions(self):
        cfg = DEFAULT_OBJECT_TYPE_CONFIGS["DATABASE"]
        assert "SNOWFLAKE" in cfg.exclude_names
        assert "SNOWFLAKE_SAMPLE_DATA" in cfg.exclude_names
        assert cfg.exclude_prefixes == ("USER$",)

    def test_default_schema_exclusions(self):
        cfg = DEFAULT_OBJECT_TYPE_CONFIGS["SCHEMA"]
        assert "INFORMATION_SCHEMA" in cfg.exclude_names
        assert "PUBLIC" in cfg.exclude_names

    def test_empty_config(self):
        cfg = ObjectTypeConfig()
        assert cfg.exclude_names == set()
        assert cfg.exclude_prefixes == ()


# =============================================================================
# is_managed
# =============================================================================


class TestIsManaged:
    def test_excluded_by_name_defaults(self):
        assert is_managed("DATABASE", "SNOWFLAKE") is False
        assert is_managed("DATABASE", "SNOWFLAKE_SAMPLE_DATA") is False
        assert is_managed("SCHEMA", "DB.INFORMATION_SCHEMA") is False
        assert is_managed("SCHEMA", "DB.PUBLIC") is False

    def test_excluded_by_prefix_defaults(self):
        assert is_managed("DATABASE", "USER$TEMP_DB") is False

    def test_managed_object(self):
        assert is_managed("DATABASE", "ANALYTICS") is True
        assert is_managed("SCHEMA", "DB.MY_SCHEMA") is True
        assert is_managed("VIEW", "DB.SCH.MY_VIEW") is True

    def test_unknown_type_is_managed(self):
        assert is_managed("WAREHOUSE", "MY_WH") is True

    def test_with_custom_config(self):
        config = ProjectConfig(
            object_types={
                "VIEW": ObjectTypeConfig(
                    exclude_names={"TEMP_VIEW"},
                    exclude_prefixes=("_INTERNAL",),
                ),
            }
        )
        assert is_managed("VIEW", "DB.SCH.TEMP_VIEW", config) is False
        assert is_managed("VIEW", "DB.SCH._INTERNAL_V", config) is False
        assert is_managed("VIEW", "DB.SCH.MY_VIEW", config) is True

    def test_extracts_last_component(self):
        assert is_managed("DATABASE", "ORG.SNOWFLAKE") is False
        assert is_managed("DATABASE", "SNOWFLAKE.SOMETHING") is True


# =============================================================================
# TargetConfig / ProjectConfig defaults
# =============================================================================


class TestTargetConfig:
    def test_defaults(self):
        t = TargetConfig()
        assert t.database is None
        assert t.schemas is None
        assert t.object_types is None


class TestProjectConfig:
    def test_defaults(self):
        p = ProjectConfig()
        assert p.targets == []
        assert p.object_types == {}
        assert p.object_renamer.is_identity


# =============================================================================
# load_config
# =============================================================================


class TestLoadConfig:
    def test_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.toml")

    def test_valid_config(self, tmp_path):
        config_file = tmp_path / "test.toml"
        config_file.write_text(
            '[[targets]]\ndatabase = "MYDB"\nschemas = ["RAW", "curated"]\n'
            'object_types = ["view", "schema"]\n'
        )
        cfg = load_config(config_file)
        assert len(cfg.targets) == 1
        assert cfg.targets[0].database == "MYDB"
        assert cfg.targets[0].schemas == ["RAW", "CURATED"]
        assert cfg.targets[0].object_types == ["VIEW", "SCHEMA"]

    def test_no_name_mapping_produces_identity(self, tmp_path):
        config_file = tmp_path / "test.toml"
        config_file.write_text('[[targets]]\ndatabase = "DB"\n')
        cfg = load_config(config_file)
        assert cfg.object_renamer.is_identity

    def test_account_only_mapping(self, tmp_path):
        config_file = tmp_path / "test.toml"
        config_file.write_text(
            '[[targets]]\ndatabase = "SOURCE"\n\n'
            '[name_mapping.account]\nSOURCE = "SOURCE_DEV"\n'
        )
        cfg = load_config(config_file)
        assert not cfg.object_renamer.is_identity
        assert cfg.object_renamer.to_physical_account("SOURCE") == "SOURCE_DEV"

    def test_account_and_schemas_mapping(self, tmp_path):
        config_file = tmp_path / "test.toml"
        config_file.write_text(
            '[[targets]]\ndatabase = "SOURCE"\n\n'
            '[name_mapping.account]\nSOURCE = "SOURCE_DEV"\n\n'
            '[name_mapping.schemas.SOURCE]\nRAW = "RAW_DEV"\n'
        )
        cfg = load_config(config_file)
        r = cfg.object_renamer
        assert r.to_physical_account("SOURCE") == "SOURCE_DEV"
        assert r.to_physical_schema("SOURCE", "RAW") == "RAW_DEV"


# =============================================================================
# _parse_targets
# =============================================================================


class TestParseTargets:
    def test_empty_targets_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            _parse_targets([])

    def test_non_dict_entry_raises(self):
        with pytest.raises(ValueError, match="must be a table"):
            _parse_targets(["not a dict"])

    def test_uppercase_normalization(self):
        targets = _parse_targets([{
            "database": "mydb",
            "schemas": ["raw", "curated"],
            "object_types": ["view"],
        }])
        assert targets[0].database == "MYDB"
        assert targets[0].schemas == ["RAW", "CURATED"]
        assert targets[0].object_types == ["VIEW"]

    def test_none_fields_preserved(self):
        targets = _parse_targets([{"database": "db"}])
        assert targets[0].schemas is None
        assert targets[0].object_types is None

    def test_multiple_targets(self):
        targets = _parse_targets([
            {"database": "db1"},
            {"database": "db2"},
        ])
        assert len(targets) == 2


# =============================================================================
# _parse_object_types
# =============================================================================


class TestParseObjectTypes:
    def test_empty_merges_defaults(self):
        result = _parse_object_types({})
        assert "DATABASE" in result
        assert "SCHEMA" in result
        assert "SNOWFLAKE" in result["DATABASE"].exclude_names

    def test_additive_merge(self):
        result = _parse_object_types({
            "DATABASE": {"exclude_names": ["EXTRA_DB"]},
        })
        assert "SNOWFLAKE" in result["DATABASE"].exclude_names
        assert "EXTRA_DB" in result["DATABASE"].exclude_names

    def test_new_type(self):
        result = _parse_object_types({
            "WAREHOUSE": {"exclude_names": ["SYSTEM_WH"]},
        })
        assert "WAREHOUSE" in result
        assert "SYSTEM_WH" in result["WAREHOUSE"].exclude_names

    def test_prefix_appended(self):
        result = _parse_object_types({
            "DATABASE": {"exclude_prefixes": ["TMP_"]},
        })
        assert "TMP_" in result["DATABASE"].exclude_prefixes
        assert "USER$" in result["DATABASE"].exclude_prefixes


# =============================================================================
# _parse_name_mapping
# =============================================================================


class TestParseNameMapping:
    def test_empty_returns_identity(self):
        r = _parse_name_mapping({})
        assert r.is_identity

    def test_empty_account_and_schemas_returns_identity(self):
        r = _parse_name_mapping({"account": {}, "schemas": {}})
        assert r.is_identity

    def test_account_non_string_raises(self):
        with pytest.raises(ValueError, match="must be strings"):
            _parse_name_mapping({"account": {"DB": 42}})

    def test_schemas_non_dict_raises(self):
        with pytest.raises(ValueError, match="must be a table"):
            _parse_name_mapping({"schemas": {"DB": "not a dict"}})

    def test_schemas_non_string_value_raises(self):
        with pytest.raises(ValueError, match="must be strings"):
            _parse_name_mapping({"schemas": {"DB": {"SCH": 99}}})

    def test_valid_mapping(self):
        r = _parse_name_mapping({
            "account": {"SRC": "SRC_DEV"},
            "schemas": {"SRC": {"RAW": "RAW_DEV"}},
        })
        assert not r.is_identity
        assert r.to_physical_account("SRC") == "SRC_DEV"
        assert r.to_physical_schema("SRC", "RAW") == "RAW_DEV"
