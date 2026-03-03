#!/usr/bin/env python3
"""Test creating a Snowflake object via the REST API v2 from a YAML file.

Uses the snowflake-connector-python session token to authenticate REST
API calls directly, bypassing the CLI.  Useful for verifying which
payloads the API accepts or rejects.

Usage:
    python scripts/test_rest_api.py VIEW DEMO_DB DEV_SCHEMA \\
        temp/out/DEMO_DB/DEV_SCHEMA/views/LOCATION_VIEW.yaml \\
        --connection defaultold

    # Dry-run: just print the JSON payload without sending
    python scripts/test_rest_api.py VIEW DEMO_DB DEV_SCHEMA \\
        temp/out/DEMO_DB/DEV_SCHEMA/views/LOCATION_VIEW.yaml \\
        --dry-run

    # Include read-only fields to see what the API rejects
    python scripts/test_rest_api.py VIEW DEMO_DB DEV_SCHEMA \\
        temp/out/DEMO_DB/DEV_SCHEMA/views/LOCATION_VIEW.yaml \\
        --connection defaultold --keep-read-only
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import urllib.request
from typing import Any

import yaml


def load_yaml(path: str) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def strip_read_only_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove common read-only fields from the top-level payload."""
    from schemadrift.core.resource_metadata import get_read_only_fields

    object_type = payload.get("_object_type", "")
    ro = get_read_only_fields(object_type)
    return {k: v for k, v in payload.items() if k not in ro and k != "_object_type"}


def build_url(host: str, object_type: str, database: str, schema: str | None) -> str:
    """Build the REST API v2 URL for creating an object."""
    base = f"https://{host}/api/v2"
    otype = object_type.lower().replace(" ", "-")

    if otype == "database":
        return f"{base}/databases"
    elif otype == "schema":
        return f"{base}/databases/{database}/schemas"
    else:
        plural = otype + "s"
        return f"{base}/databases/{database}/schemas/{schema}/{plural}"


def create_object(
    host: str,
    token: str,
    object_type: str,
    database: str,
    schema: str | None,
    payload: dict[str, Any],
    create_mode: str = "orReplace",
) -> tuple[int, dict[str, Any] | str]:
    """POST to the REST API v2 and return (status_code, response_body)."""
    url = build_url(host, object_type, database, schema)
    url += f"?createMode={create_mode}"

    body = json.dumps(payload).encode()

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f'Snowflake Token="{token}"',
    }

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    ctx = ssl.create_default_context()

    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            resp_body = resp.read().decode()
            try:
                return resp.status, json.loads(resp_body)
            except json.JSONDecodeError:
                return resp.status, resp_body
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode()
        try:
            return e.code, json.loads(resp_body)
        except json.JSONDecodeError:
            return e.code, resp_body


def get_session(connection_name: str | None) -> tuple[str, str]:
    """Connect via snowflake-connector-python and return (host, token)."""
    import snowflake.connector

    params: dict[str, Any] = {}
    if connection_name:
        params["connection_name"] = connection_name

    conn = snowflake.connector.connect(**params)
    rest = conn.rest
    return rest._host, rest._token


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test creating a Snowflake object via the REST API v2",
    )
    parser.add_argument("object_type", help="Object type (e.g., VIEW, DATABASE, SCHEMA)")
    parser.add_argument("database", help="Target database")
    parser.add_argument("schema", nargs="?", default=None, help="Target schema")
    parser.add_argument("yaml_file", help="Path to the YAML definition file")
    parser.add_argument("--connection", "-c", default=None, help="connections.toml name")
    parser.add_argument(
        "--create-mode", default="orReplace",
        choices=["orReplace", "ifNotExists", "errorIfExists"],
        help="Create mode (default: orReplace)",
    )
    parser.add_argument(
        "--keep-read-only", action="store_true",
        help="Keep read-only fields in the payload (to test what the API rejects)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Just print the JSON payload and URL without sending",
    )
    args = parser.parse_args()

    payload = load_yaml(args.yaml_file)

    if not args.keep_read_only:
        payload["_object_type"] = args.object_type.upper()
        payload = strip_read_only_fields(payload)

    print("=== Payload ===")
    print(json.dumps(payload, indent=2))
    print()

    if args.dry_run:
        host = "<account>.snowflakecomputing.com"
        url = build_url(host, args.object_type, args.database, args.schema)
        print(f"=== Would POST to ===")
        print(f"{url}?createMode={args.create_mode}")
        return

    print("Connecting to Snowflake...")
    host, token = get_session(args.connection)

    url = build_url(host, args.object_type, args.database, args.schema)
    print(f"=== POST {url}?createMode={args.create_mode} ===")
    print()

    status, response = create_object(
        host, token, args.object_type, args.database, args.schema,
        payload, args.create_mode,
    )

    if status in (200, 201, 202):
        print(f"SUCCESS ({status})")
    else:
        print(f"FAILED ({status})")

    print(json.dumps(response, indent=2) if isinstance(response, dict) else response)


if __name__ == "__main__":
    main()
