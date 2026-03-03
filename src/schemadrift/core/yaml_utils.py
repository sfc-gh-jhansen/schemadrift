"""YAML serialization utilities for Snowflake object definitions."""

from __future__ import annotations

from typing import Any

import yaml


class BlockScalarDumper(yaml.SafeDumper):
    """YAML dumper that uses literal block style (|) for multiline strings.

    This makes embedded SQL queries, stored procedure bodies, and other
    multiline content readable in YAML files.
    """

    pass


def _str_representer(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    if "\n" in data:
        # PyYAML's emitter rejects block scalar style for strings containing
        # tab characters.  Expand tabs to spaces so the literal style works.
        clean = data.expandtabs(4)
        return dumper.represent_scalar("tag:yaml.org,2002:str", clean, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


BlockScalarDumper.add_representer(str, _str_representer)


def dump_yaml(data: Any) -> str:
    """Dump a Python object to a YAML string with block scalar support.

    Multiline strings are rendered using literal block style (|) for
    readability.  Keys retain their insertion order.
    """
    return yaml.dump(
        data,
        Dumper=BlockScalarDumper,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )
