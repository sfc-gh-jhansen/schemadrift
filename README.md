# schemadrift

Detect and manage Snowflake schema drift.

schemadrift keeps Snowflake object definitions in source control in sync with a live Snowflake account. It extracts object definitions, stores them as YAML files, and performs bidirectional comparison to detect drift. The output is a structured changeset that AI agents or other tools can consume to generate SQL DDL scripts.

## How it differs from traditional DCM tools

Tools like Flyway, schemachange, Liquibase, and the Snowflake Terraform provider are **deployment-first**: their primary job is to apply changes from a repository to Snowflake. schemadrift inverts this relationship.

schemadrift is **comparison-first**. It does not deploy anything. Instead, it:

1. Extracts the current state of objects from Snowflake into YAML definitions.
2. Compares those definitions against what is stored in source control.
3. Produces a structured YAML changeset describing the differences.

The changeset is the handoff point. An AI agent, a CI pipeline, or a human developer can consume it to decide what SQL to generate and when to apply it. schemadrift separates the concern of "what changed" from "how to apply it."

## Why YAML, not SQL

Most database change management tools store object definitions as SQL DDL (`CREATE OR REPLACE VIEW ...`). This is natural but creates real problems:

- **Parsing SQL is hard.** Snowflake DDL includes non-standard clauses (`WITH MANAGED ACCESS`, `DATA_RETENTION_TIME_IN_DAYS`, `CLUSTER BY`, etc.) that generic SQL parsers don't handle well. Every object type needs its own parser, and edge cases accumulate.
- **Diffing SQL is unreliable.** Whitespace, quoting, keyword casing, and clause ordering all vary between what a human writes and what Snowflake returns from `GET_DDL()`. Meaningful comparison requires normalization that is fragile and object-type-specific.
- **Round-tripping is lossy.** SQL captures some attributes (the query body of a view) but not others (comments, retention policies, managed access). A complete definition requires supplementing DDL with `SHOW` metadata, then stitching them back together.

schemadrift stores definitions as **YAML attribute dictionaries** instead. Each file contains the writable attributes of a single object:

```yaml
# ANALYTICS_DB/REPORTING/views/DAILY_REVENUE.yaml
name: DAILY_REVENUE
query: SELECT date, SUM(amount) AS revenue FROM orders GROUP BY date
columns:
  - name: DATE
  - name: REVENUE
kind: PERMANENT
```

```yaml
# ANALYTICS_DB/schemas/REPORTING.yaml
name: REPORTING
kind: PERMANENT
managed_access: false
data_retention_time_in_days: 1
```

This is simpler to parse, trivial to diff, and captures every attribute in one place.

### Alignment with the Snowflake resource model

The field names and structure in these YAML files are not invented by schemadrift. They follow the **Snowflake resource model** — the same schema used by:

- The [Snowflake REST API](https://docs.snowflake.com/en/developer-guide/snowflake-rest-api/snowflake-rest-api) (`POST /api/v2/databases/{database}/schemas/{schema}/views`)
- The [Snowflake Python API](https://docs.snowflake.com/en/developer-guide/snowflake-python-api/snowflake-python-overview) (`ViewResource`, `SchemaResource`, etc.)
- The [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (`snow object create view --json '{...}'`)
- The `DESCRIBE ... AS RESOURCE` SQL command

This means a YAML file produced by schemadrift can be converted to JSON and passed directly to `snow object create` or the REST API without any transformation:

```bash
# Convert schemadrift YAML to JSON and deploy via Snowflake CLI
snow object create view \
  --database ANALYTICS_DB --schema REPORTING --replace \
  --json "$(python -c \
    "import yaml, json, sys; print(json.dumps(yaml.safe_load(open(sys.argv[1]))))" \
    repo/ANALYTICS_DB/REPORTING/views/DAILY_REVENUE.yaml)"
```

By aligning with the resource model, schemadrift avoids inventing its own schema and stays compatible with the broader Snowflake toolchain.

## Installation

```bash
pip install schemadrift
```

Requires Python 3.10+.

## Quick start

### Extract objects from Snowflake

```bash
# Extract a single view to the file system
schemadrift extract view MYDB.MYSCHEMA.MYVIEW --output ./repo

# Extract all objects defined in the config file
schemadrift extract --config schemadrift.toml --output ./repo

# Print a view definition to stdout as YAML
schemadrift extract view MYDB.MYSCHEMA.MYVIEW --stdout
```

### Compare source control against Snowflake

```bash
# Human-readable summary
schemadrift compare --source ./repo

# Structured YAML changeset to stdout
schemadrift compare --source ./repo --changeset

# Write changeset to a file
schemadrift compare --source ./repo --output changes.yaml
```

### List objects in Snowflake

```bash
schemadrift list databases
schemadrift list schemas MYDB
schemadrift list views MYDB.MYSCHEMA
```

### Connection options

schemadrift connects to Snowflake using `snowflake-connector-python`. Credentials can be supplied via CLI flags, environment variables, or a named connection from `connections.toml`:

```bash
# Named connection
schemadrift extract view MYDB.MYSCHEMA.MYVIEW --output ./repo --connection myconn

# Explicit credentials
schemadrift extract view MYDB.MYSCHEMA.MYVIEW --output ./repo \
  --account myaccount --user myuser --authenticator externalbrowser
```

Environment variables `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, and `SNOWFLAKE_AUTHENTICATOR` are also supported.

## Configuration

schemadrift uses a TOML configuration file (`schemadrift.toml`) to define which objects are under management.

```toml
# Targets define the scope of objects to manage.

# Manage all schemas in ANALYTICS_DB
[[targets]]
database = "ANALYTICS_DB"
object_types = ["SCHEMA"]

# Manage all objects in ANALYTICS_DB.REPORTING
[[targets]]
database = "ANALYTICS_DB"
schemas = ["REPORTING"]

# Name mapping for multi-environment support.
# Logical names (used in source control) map to physical Snowflake names.
[name_mapping.account]
ANALYTICS_DB = "ANALYTICS_DB_DEV"

[name_mapping.schemas.ANALYTICS_DB]
REPORTING = "REPORTING_V2"

# Per-object-type exclusion rules (merged with built-in defaults).
[object_types.DATABASE]
exclude_names = ["SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"]
exclude_prefixes = ["USER$"]

[object_types.SCHEMA]
exclude_names = ["INFORMATION_SCHEMA", "PUBLIC"]
```

### Targets

Each `[[targets]]` entry defines a scope:

| Fields | Behavior |
|--------|----------|
| (no database) | Account-level objects only (DATABASE, WAREHOUSE, ROLE) |
| `database` | The database itself + all schemas + all schema-level objects |
| `database` + `schemas` | Only schema-level objects within those schemas |
| `object_types` | Additional filter at any level |

### Name mapping

The `[name_mapping]` section translates between **logical names** (used in source control and file paths) and **physical names** (the actual object names in Snowflake). This enables a single repository to target multiple environments by swapping only the config file.

The mapping is bidirectional:

- **Extract**: Objects are fetched from Snowflake using physical names, then the returned definitions are translated back to logical names before writing to disk.
- **Compare**: Source files use logical names. When comparing against Snowflake, logical names are resolved to physical names for the query, then results are translated back.

Two levels of mapping are supported:

- `[name_mapping.account]` renames account-level objects (databases, warehouses, roles).
- `[name_mapping.schemas.<LOGICAL_DB>]` renames schemas within a specific logical database.

Leaf object names (views, tables, etc.) are never renamed — only their container names (database and schema) are mapped.

**Example:** A team maintains a single repo with logical names `ANALYTICS_DB` and `REPORTING`. Each environment has its own `schemadrift.toml`:

```toml
# schemadrift.toml (dev environment)
[[targets]]
database = "ANALYTICS_DB"
schemas = ["REPORTING"]

[name_mapping.account]
ANALYTICS_DB = "ANALYTICS_DB_DEV"

[name_mapping.schemas.ANALYTICS_DB]
REPORTING = "REPORTING_DEV"
```

```toml
# schemadrift.toml (production — no mapping needed)
[[targets]]
database = "ANALYTICS_DB"
schemas = ["REPORTING"]
```

The file system always uses logical names regardless of environment:

```
repo/ANALYTICS_DB/REPORTING/views/DAILY_REVENUE.yaml
```

When running against dev, schemadrift resolves `ANALYTICS_DB` to `ANALYTICS_DB_DEV` and `REPORTING` to `REPORTING_DEV` for all Snowflake queries. The view `DAILY_REVENUE` keeps its name — only the database and schema are remapped.

The mapping must be bijective (one-to-one). schemadrift raises an error if multiple logical names map to the same physical name.

### Built-in exclusions

These objects are excluded by default and do not need to be configured:

- **DATABASE**: `SNOWFLAKE`, `SNOWFLAKE_SAMPLE_DATA`, names starting with `USER$`
- **SCHEMA**: `INFORMATION_SCHEMA`, `PUBLIC`

User-defined exclusions in `[object_types.*]` are merged additively with these defaults.

## Filesystem layout

Object definitions are stored in a directory hierarchy that mirrors Snowflake's object hierarchy:

```
repo/
  databases/
    ANALYTICS_DB.yaml
  ANALYTICS_DB/
    schemas/
      REPORTING.yaml
    REPORTING/
      views/
        DAILY_REVENUE.yaml
        CUSTOMER_SUMMARY.yaml
```

The convention is:

| Scope | Path |
|-------|------|
| Account-level | `<root>/<type>s/<NAME>.yaml` |
| Database-level | `<root>/<DATABASE>/<type>s/<NAME>.yaml` |
| Schema-level | `<root>/<DATABASE>/<SCHEMA>/<type>s/<NAME>.yaml` |

Object names and directory names are uppercased. The type directory is the lowercase plural of the object type (`databases/`, `schemas/`, `views/`).

### External object support

If a directory exists where a YAML file would be (e.g., `views/DAILY_REVENUE/` instead of `views/DAILY_REVENUE.yaml`), schemadrift treats that object as **externally managed**. This allows other tools, such as the Snowflake CLI (`snow`), to own specific objects within the same repository. Externally managed objects are reported as `EXTERNALLY_MANAGED` in comparison results and are skipped during extraction.

This makes it possible to integrate schemadrift into a Snowflake CLI project where some objects are deployed via `snow` and others are tracked for drift detection.

## Comparison and changesets

The `compare` command performs a bidirectional comparison. For each object in scope, it determines one of:

| Status | Meaning |
|--------|---------|
| `EQUIVALENT` | Source and Snowflake definitions match |
| `DIFFERS` | Both exist but definitions don't match (drift detected) |
| `MISSING_IN_TARGET` | Exists in source control but not in Snowflake |
| `MISSING_IN_SOURCE` | Exists in Snowflake but not in source control |
| `EXTERNALLY_MANAGED` | Managed by an external tool (directory detected) |

The `--changeset` flag (or `--output`) produces a structured YAML changeset containing only actionable entries (everything except `EQUIVALENT` and `EXTERNALLY_MANAGED`):

```yaml
changes:
- status: DIFFERS
  object_type: VIEW
  identifier: ANALYTICS_DB.REPORTING.DAILY_REVENUE
  definition:
    name: DAILY_REVENUE
    query: SELECT date, SUM(amount) AS revenue FROM orders GROUP BY date
  diff:
    modified:
      query:
        current: SELECT date, SUM(amount) AS revenue FROM sales GROUP BY date
        desired: SELECT date, SUM(amount) AS revenue FROM orders GROUP BY date
- status: MISSING_IN_SOURCE
  object_type: VIEW
  identifier: ANALYTICS_DB.REPORTING.TEMP_DEBUG
  definition:
    name: TEMP_DEBUG
    query: SELECT 1
```

This changeset is designed to be consumed by LLMs, AI agents, or automation pipelines that generate the appropriate SQL DDL.

## Python API

The `DriftService` class is the primary programmatic interface. The CLI is a thin wrapper around it.

```python
from schemadrift.core.service import DriftService
from schemadrift.core.file_manager import FileManager
from schemadrift.connection import SnowflakeConnection

conn = SnowflakeConnection(connection_name="myconn")
fm = FileManager("./repo")
service = DriftService(conn, fm)

# Extract a single object
obj = service.extract_object("VIEW", "MYDB.MYSCHEMA.MYVIEW")

# Extract to file
path = service.extract_to_file("VIEW", "MYDB.MYSCHEMA.MYVIEW")

# Compare a single object
entry = service.compare_object("VIEW", "MYDB.MYSCHEMA.MYVIEW")
print(entry.status)  # ComparisonStatus.EQUIVALENT, DIFFERS, etc.

# Batch comparison via config
from schemadrift.core.config import load_config
config = load_config(Path("schemadrift.toml"))
entries = service.compare_targets(config)

# Serialize as YAML changeset
from schemadrift.core.comparison import to_changeset_yaml
print(to_changeset_yaml(entries))
```

## Architecture

```
+---------------------------------------------------------------+
|                           CLI                                 |
|                      (typer app)                              |
|              extract / compare / list                         |
+-------------------------------+-------------------------------+
                                |
+-------------------------------v-------------------------------+
|                       DriftService                            |
|                                                               |
|  Orchestrates extraction, comparison, batch operations.       |
|  Coordinates all subsystems. Primary API for integrations.    |
+---------+-----------------+-------------------+---------------+
          |                 |                   |
+---------v-------+ +-------v---------+ +-------v---------------+
| PluginDispatcher| |   FileManager   | | SnowflakeConnection   |
|                 | |                 | | Interface (Protocol)   |
| Routes calls by | | Reads/writes    | |                       |
| object type to  | | YAML/SQL files  | | execute()             |
| the correct     | | using           | | execute_scalar()      |
| plugin          | | FileStructure   | | get_current_database()|
+---------+-------+ | conventions     | | close()               |
          |         +-----------------+ +-----------+-----------+
+---------v-----------------------------------------+           |
|                  Object Model Layer               |           |
|                                                   |           |
|  SnowflakeObject (ABC)                            |           |
|    +-- Database  (ACCOUNT scope)                  |           |
|    +-- Schema    (DATABASE scope)                 |           |
|    +-- View      (SCHEMA scope)                   |           |
|                                                   |           |
|  ObjectScope: ORGANIZATION > ACCOUNT > DATABASE   |           |
|               > SCHEMA                            |           |
|                                                   |           |
|  ObjectDiff: added / removed / modified           |           |
|  DiffStrategy: pluggable comparison logic         |           |
+---------------------------------------------------+           |
                                                                |
+---------------------------------------------------------------+
|                  snowflake-connector-python                    |
+---------------------------------------------------------------+
```

### Key classes

| Class | Module | Role |
|-------|--------|------|
| `DriftService` | `core.service` | Top-level orchestrator. Coordinates plugins, file manager, and connection. |
| `PluginDispatcher` | `plugins.manager` | Routes operations to the correct plugin by object type. |
| `FileManager` | `core.file_manager` | Reads and writes object definition files. Uses `FileStructure` for path conventions. |
| `SnowflakeConnectionInterface` | `connection.interface` | Protocol that any connection implementation must satisfy. |
| `SnowflakeObject` | `core.base_object` | Abstract dataclass base for all object types. Provides generic `extract()`, `to_dict()`, `from_dict()`, and `compare()`. |
| `ObjectRenamer` | `core.config` | Bidirectional logical-to-physical name translation for multi-environment support. |
| `ComparisonEntry` | `core.comparison` | Result of comparing a single object. Carries status, definition, and diff. |
| `HookAdapter` | `plugins.hook_adapter` | Bridges any `SnowflakeObject` subclass to pluggy hooks without per-plugin boilerplate. |
| `DiffStrategy` | `core.diff_strategy` | Strategy pattern for object comparison. Supports custom normalizers (e.g., SQL whitespace normalization). |

### Object extraction methods

schemadrift supports multiple methods for extracting object definitions from Snowflake:

| Method | Used by | Description |
|--------|---------|-------------|
| `DESCRIBE AS RESOURCE` | Schema, Database | Returns a JSON object with all attributes in a single query. Used by the generic `SnowflakeObject.extract()`. |
| `SHOW AS RESOURCE TERSE` | All (listing) | Lists objects within a scope, returning JSON rows. Used by `SnowflakeObject.list_objects()`. |
| Custom SQL | View | Overrides `extract()` for objects that need post-processing (e.g., stripping DDL wrappers from view queries). |

Most object types can use the generic extraction from `SnowflakeObject` without any custom code. The base class introspects its own dataclass fields, calls `DESCRIBE AS RESOURCE`, filters out read-only fields, and constructs the instance.

## Plugin system

schemadrift uses [pluggy](https://pluggy.readthedocs.io/) for its plugin architecture. Each object type (DATABASE, SCHEMA, VIEW) is implemented as a plugin.

### Hook specification

Every plugin must implement the hooks defined in `hookspecs.SnowflakeObjectSpec`:

| Hook | Returns | Purpose |
|------|---------|---------|
| `get_object_type()` | `str` | Object type name (e.g., `"VIEW"`) |
| `get_scope()` | `ObjectScope` | Scope level (ACCOUNT, DATABASE, SCHEMA) |
| `extract_object(connection, identifier)` | `SnowflakeObject` | Extract from Snowflake |
| `list_objects(connection, scope)` | `list[str]` | List objects in a scope |
| `object_from_dict(data, context)` | `SnowflakeObject` | Deserialize from dict |
| `generate_dict(obj)` | `dict` | Serialize to dict |
| `compare_objects(source, target)` | `ObjectDiff` | Compare two objects |

### Writing a plugin

Most plugins require only a model class and a one-line registration. The `HookAdapter` eliminates per-plugin boilerplate by bridging any `SnowflakeObject` subclass to the pluggy hooks.

**1. Define the model:**

```python
# my_plugin/model.py
from dataclasses import dataclass
from typing import ClassVar
from schemadrift.core.base_object import ObjectScope, SnowflakeObject

@dataclass
class Table(SnowflakeObject):
    OBJECT_TYPE: ClassVar[str] = "TABLE"
    SCOPE: ClassVar[ObjectScope] = ObjectScope.SCHEMA

    name: str
    database_name: str = ""
    schema_name: str = ""
    kind: str | None = "PERMANENT"
    cluster_by: list[str] | None = None
    comment: str | None = None
```

**2. Register the plugin:**

```python
# my_plugin/__init__.py
from my_plugin.model import Table
from schemadrift.plugins.hook_adapter import HookAdapter

plugin = HookAdapter(Table)
```

**3. Declare the entry point** in `pyproject.toml`:

```toml
[project.entry-points.schemadrift]
table = "my_plugin"
```

The generic `extract()`, `to_dict()`, `from_dict()`, and `compare()` methods on `SnowflakeObject` use field introspection, so most plugins need zero custom serialization code. Override `extract()` only when the Snowflake response requires special mapping (as the View plugin does to strip DDL wrappers).

### Built-in plugins

| Plugin | Scope | Module |
|--------|-------|--------|
| DATABASE | ACCOUNT | `schemadrift.plugins.builtin.database` |
| SCHEMA | DATABASE | `schemadrift.plugins.builtin.schema` |
| VIEW | SCHEMA | `schemadrift.plugins.builtin.view` |

External plugins are discovered automatically via setuptools entry points under the `schemadrift` group.

## Development

```bash
# Clone and install in editable mode
git clone https://github.com/your-org/schemadrift.git
cd schemadrift
pip install -e ".[dev]"

# Run all tests
pytest

# Run a specific plugin's tests
pytest src/schemadrift/plugins/builtin/view/tests.py -v

# Lint and format
ruff check src/
black src/
mypy src/
```

### Project structure

```
src/schemadrift/
  __init__.py
  hookspecs.py                 # Pluggy hook specifications
  cli/
    main.py                    # Typer CLI (extract, compare, list)
  core/
    service.py                 # DriftService (main orchestrator)
    config.py                  # TOML config loader, ObjectRenamer
    base_object.py             # SnowflakeObject ABC, ObjectScope, ObjectDiff
    file_manager.py            # FileManager, FileStructure
    comparison.py              # ComparisonEntry, changeset serialization
    diff_strategy.py           # DiffStrategy, DefaultDiffStrategy
    yaml_utils.py              # YAML serialization helpers
    resource_metadata.py       # Generated read-only field metadata
  connection/
    interface.py               # SnowflakeConnectionInterface (Protocol)
    snowflake_impl.py          # Concrete implementation + MockConnection
  plugins/
    manager.py                 # PluginDispatcher, plugin manager setup
    hook_adapter.py            # Generic HookAdapter
    builtin/
      database/                # DATABASE plugin (model + tests)
      schema/                  # SCHEMA plugin (model + tests)
      view/                    # VIEW plugin (model + tests)
```

## License

Apache-2.0
