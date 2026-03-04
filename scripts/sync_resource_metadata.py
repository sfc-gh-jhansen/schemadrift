#!/usr/bin/env python3
"""Fetch Snowflake OpenAPI specs and generate resource_metadata.py.

Usage:
    python scripts/sync_resource_metadata.py

Fetches every resource spec from the snowflakedb/snowflake-rest-api-specs
GitHub repo, parses schema definitions, and writes a static Python module
at src/schemadrift/core/resource_metadata.py.
"""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any

import yaml

GITHUB_API_URL = (
    "https://api.github.com/repos/snowflakedb/snowflake-rest-api-specs"
    "/contents/specifications"
)
RAW_BASE_URL = (
    "https://raw.githubusercontent.com/snowflakedb/snowflake-rest-api-specs"
    "/main/specifications"
)

SKIP_FILES = frozenset({
    "common.yaml",
    "common-cortex-agent.yaml",
    "common-cortex-analyst.yaml",
    "common-cortex-tool.yaml",
    "cortex-analyst.yaml",
    "cortex-embed.yaml",
    "cortex-generic-anthropic.yaml",
    "cortex-generic-openai.yaml",
    "cortex-inference.yaml",
    "result.yaml",
    "sqlapi.yaml",
    "spark-connect.yaml",
})

OUTPUT_PATH = (
    Path(__file__).resolve().parent
    / "resource_metadata.py"
)


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------

def filename_to_object_type(filename: str) -> str:
    """Convert spec filename to Snowflake OBJECT_TYPE.

    Examples: 'database.yaml' -> 'DATABASE',
              'dynamic-table.yaml' -> 'DYNAMIC TABLE'
    """
    stem = filename.removesuffix(".yaml")
    return stem.replace("-", " ").upper()


def filename_to_schema_name(filename: str) -> str:
    """Convert spec filename to expected PascalCase schema name.

    Examples: 'database.yaml' -> 'Database',
              'dynamic-table.yaml' -> 'DynamicTable'
    """
    stem = filename.removesuffix(".yaml")
    return "".join(part.capitalize() for part in stem.split("-"))


# ---------------------------------------------------------------------------
# OpenAPI parsing
# ---------------------------------------------------------------------------

def resolve_type(prop: dict[str, Any]) -> str:
    """Extract the type string from an OpenAPI property definition."""
    if "$ref" in prop:
        ref = prop["$ref"]
        if "common.yaml" in ref:
            return "string"
        return ref.split("/")[-1]
    t = prop.get("type", "unknown")
    if t == "array" and "items" in prop:
        inner = resolve_type(prop["items"])
        return f"array[{inner}]"
    return t


def extract_fields(
    schema: dict[str, Any],
    required_fields: list[str],
) -> dict[str, dict[str, Any]]:
    """Extract field metadata from an OpenAPI schema's properties."""
    properties = schema.get("properties", {})
    fields: dict[str, dict[str, Any]] = {}
    for name, prop in sorted(properties.items()):
        fields[name] = {
            "type": resolve_type(prop),
            "read_only": prop.get("readOnly", False),
            "required": name in required_fields,
        }
    return fields


def find_primary_schema(
    spec: dict[str, Any],
    expected_name: str,
) -> dict[str, Any] | None:
    """Find the primary resource schema in components.schemas.

    Prefers an exact match on *expected_name*; falls back to a
    case-insensitive comparison.
    """
    schemas = spec.get("components", {}).get("schemas", {})
    if expected_name in schemas:
        return schemas[expected_name]
    for name, schema in schemas.items():
        if name.lower() == expected_name.lower():
            return schema
    return None


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def fetch_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "schemadrift"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "schemadrift"})
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode()


# ---------------------------------------------------------------------------
# Spec discovery & processing
# ---------------------------------------------------------------------------

def list_spec_files() -> list[str]:
    """Get the list of spec filenames from the GitHub API."""
    entries = fetch_json(GITHUB_API_URL)
    return [
        entry["name"]
        for entry in entries
        if entry["name"].endswith(".yaml") and entry["name"] not in SKIP_FILES
    ]


def process_spec(filename: str) -> tuple[str, dict[str, Any]] | None:
    """Fetch and process a single spec file.

    Returns (object_type, metadata_dict) or None if the spec was skipped.
    """
    url = f"{RAW_BASE_URL}/{filename}"
    print(f"  Fetching {filename}...")
    raw = fetch_text(url)
    spec = yaml.safe_load(raw)

    schema_name = filename_to_schema_name(filename)
    schema = find_primary_schema(spec, schema_name)

    if schema is None:
        print(f"    WARNING: No schema '{schema_name}' found, skipping")
        return None

    if "properties" not in schema:
        print(f"    WARNING: Schema '{schema_name}' has no properties, skipping")
        return None

    required_list = schema.get("required", [])
    fields = extract_fields(schema, required_list)
    object_type = filename_to_object_type(filename)

    return object_type, {"fields": fields}


# ---------------------------------------------------------------------------
# Code generation
# ---------------------------------------------------------------------------

def generate_module(metadata: dict[str, dict[str, Any]]) -> str:
    """Generate the Python module source code."""
    lines: list[str] = [
        '"""Snowflake resource field metadata -- AUTO-GENERATED.',
        "",
        "Re-generate by running:  python scripts/sync_resource_metadata.py",
        "Source: https://github.com/snowflakedb/snowflake-rest-api-specs",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "",
        "RESOURCE_METADATA: dict[str, dict] = {",
    ]

    for obj_type in sorted(metadata):
        entry = metadata[obj_type]
        lines.append(f'    "{obj_type}": {{')
        lines.append('        "fields": {')
        for fname in sorted(entry["fields"]):
            fmeta = entry["fields"][fname]
            ro = fmeta["read_only"]
            req = fmeta["required"]
            ftype = fmeta["type"]
            lines.append(
                f'            "{fname}": '
                f'{{"type": "{ftype}", "read_only": {ro}, "required": {req}}},'
            )
        lines.append("        },")
        lines.append("    },")

    lines.append("}")
    lines.append("")
    lines.append("")
    lines.extend([
        "def get_read_only_fields(object_type: str) -> frozenset[str]:",
        '    """Return the set of read-only field names for an object type."""',
        '    entry = RESOURCE_METADATA.get(object_type.upper())',
        "    if entry is None:",
        "        return frozenset()",
        "    return frozenset(",
        '        name for name, meta in entry["fields"].items() if meta["read_only"]',
        "    )",
        "",
        "",
        "def get_writable_fields(object_type: str) -> frozenset[str]:",
        '    """Return the set of writable (non-read-only) field names for an object type."""',
        '    entry = RESOURCE_METADATA.get(object_type.upper())',
        "    if entry is None:",
        "        return frozenset()",
        "    return frozenset(",
        '        name for name, meta in entry["fields"].items() if not meta["read_only"]',
        "    )",
        "",
        "",
        "def get_required_fields(object_type: str) -> frozenset[str]:",
        '    """Return the set of required field names for an object type."""',
        '    entry = RESOURCE_METADATA.get(object_type.upper())',
        "    if entry is None:",
        "        return frozenset()",
        "    return frozenset(",
        '        name for name, meta in entry["fields"].items() if meta["required"]',
        "    )",
        "",
    ])

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Discovering spec files from GitHub...")
    filenames = list_spec_files()
    print(f"Found {len(filenames)} spec files")

    metadata: dict[str, dict[str, Any]] = {}
    skipped: list[str] = []

    print("\nProcessing specs...")
    for filename in sorted(filenames):
        result = process_spec(filename)
        if result is not None:
            obj_type, entry = result
            metadata[obj_type] = entry
            field_count = len(entry["fields"])
            ro_count = sum(1 for f in entry["fields"].values() if f["read_only"])
            print(f"    -> {obj_type}: {field_count} fields ({ro_count} read-only)")
        else:
            skipped.append(filename)

    print(f"\nExtracted {len(metadata)} object types")
    if skipped:
        print(f"Skipped {len(skipped)} files: {', '.join(skipped)}")

    source = generate_module(metadata)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(source)
    print(f"\nWrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
