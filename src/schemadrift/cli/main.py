"""CLI for schemadrift.

Provides commands for extracting and comparing Snowflake objects.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Optional

import typer

from schemadrift import __version__
from schemadrift.core.file_manager import FileManager
from schemadrift.core.service import DriftService

if TYPE_CHECKING:
    from schemadrift.connection.interface import SnowflakeConnectionInterface
    from schemadrift.core.comparison import ComparisonEntry
    from schemadrift.core.config import ProjectConfig

# Create the main app
app = typer.Typer(
    name="schemadrift",
    help="Detect and manage Snowflake schema drift.",
    add_completion=False,
)

# Configure logging - set root to WARNING to suppress noisy dependency logs,
# but set our own logger to INFO
logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        typer.echo(f"schemadrift version {__version__}")
        raise typer.Exit()


def _create_connection(
    *,
    connection_name: str | None = None,
    account: str | None = None,
    user: str | None = None,
    password: str | None = None,
    authenticator: str | None = None,
) -> SnowflakeConnectionInterface:
    """Create a Snowflake connection from CLI parameters.

    Centralises connection construction so that every command uses the
    same logic and error handling.

    Raises:
        typer.Exit: If the connection cannot be established.
    """
    try:
        from schemadrift.connection import SnowflakeConnection

        return SnowflakeConnection(
            connection_name=connection_name,
            account=account,
            user=user,
            password=password,
            authenticator=authenticator,
        )
    except Exception as e:
        typer.echo(f"Error connecting to Snowflake: {e}", err=True)
        raise typer.Exit(1)


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            help="Show version and exit.",
            callback=version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """schemadrift - Manage Snowflake objects in the file system."""
    pass


# ============================================================================
# Extract Command
# ============================================================================


@app.command()
def extract(
    object_type: Annotated[
        Optional[str],
        typer.Argument(help="Object type to extract (database, schema, view)"),
    ] = None,
    identifier: Annotated[
        Optional[str],
        typer.Argument(help="Fully qualified object name (e.g., MYDB.MYSCHEMA.MYVIEW)"),
    ] = None,
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output",
            "-o",
            help="Output directory for extracted definitions",
        ),
    ] = None,
    stdout: Annotated[
        bool,
        typer.Option(
            "--stdout",
            help="Print to stdout instead of writing to file",
        ),
    ] = False,
    config: Annotated[
        Optional[Path],
        typer.Option(
            "--config",
            help="Path to schemadrift.toml config file (for batch extraction)",
        ),
    ] = None,
    format: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help="Output format (yaml or sql)",
        ),
    ] = "yaml",
    account: Annotated[
        Optional[str],
        typer.Option(
            "--account",
            envvar="SNOWFLAKE_ACCOUNT",
            help="Snowflake account identifier",
        ),
    ] = None,
    user: Annotated[
        Optional[str],
        typer.Option(
            "--user",
            envvar="SNOWFLAKE_USER",
            help="Snowflake username",
        ),
    ] = None,
    password: Annotated[
        Optional[str],
        typer.Option(
            "--password",
            envvar="SNOWFLAKE_PASSWORD",
            help="Snowflake password",
            hide_input=True,
        ),
    ] = None,
    authenticator: Annotated[
        Optional[str],
        typer.Option(
            "--authenticator",
            envvar="SNOWFLAKE_AUTHENTICATOR",
            help="Authentication method (externalbrowser, snowflake, etc.)",
        ),
    ] = None,
    connection_name: Annotated[
        Optional[str],
        typer.Option(
            "--connection",
            "-c",
            help="Connection name from connections.toml",
        ),
    ] = None,
) -> None:
    """Extract object definitions from Snowflake and write to the file system.

    Single-object mode requires both object_type and identifier.
    Batch mode uses a config file (--config or schemadrift.toml).

    Examples:
        # Extract a single view
        schemadrift extract view MYDB.MYSCHEMA.MYVIEW --output ./repo

        # Extract using a config file
        schemadrift extract --config schemadrift.toml --output ./repo

        # Print to stdout
        schemadrift extract view MYDB.MYSCHEMA.MYVIEW --stdout
    """
    if not stdout and not output:
        typer.echo("Error: Must specify --output or --stdout", err=True)
        raise typer.Exit(1)

    if identifier and not object_type:
        typer.echo("Error: object_type is required when specifying an identifier", err=True)
        raise typer.Exit(1)

    if not identifier and not config:
        default_config = Path("schemadrift.toml")
        if not default_config.exists():
            typer.echo(
                "Error: Must specify object_type and identifier, "
                "or use --config / schemadrift.toml for batch extraction",
                err=True,
            )
            raise typer.Exit(1)

    conn = _create_connection(
        connection_name=connection_name,
        account=account,
        user=user,
        password=password,
        authenticator=authenticator,
    )

    try:
        project_config = None if (object_type and identifier) else _resolve_config(config_path=config)
        fm = FileManager(output) if output else None
        service = DriftService(conn, fm, config=project_config)

        if object_type and identifier:
            obj_type = object_type.upper().rstrip("S")
            _extract_single_object(service, obj_type, identifier, stdout, format)
        else:
            typer.echo("Extracting objects...")
            paths = service.extract_targets(project_config, format=format)
            typer.echo(f"Extracted {len(paths)} object(s)")
            for p in paths:
                typer.echo(f"  Written to {p}")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    finally:
        conn.close()


def _extract_single_object(
    service: DriftService,
    obj_type: str,
    identifier: str,
    stdout: bool,
    format: str,
) -> None:
    """Extract a single object."""
    typer.echo(f"Extracting {obj_type} {identifier}...")

    if stdout:
        if format == "sql":
            typer.echo(service.extract_as_ddl(obj_type, identifier))
        else:
            from schemadrift.core.yaml_utils import dump_yaml

            typer.echo(dump_yaml(service.extract_as_dict(obj_type, identifier)))
    else:
        path = service.extract_to_file(obj_type, identifier, format)
        typer.echo(f"Written to {path}")


# ============================================================================
# Shared Config Resolution
# ============================================================================


def _resolve_config(
    config_path: Optional[Path] = None,
) -> ProjectConfig:
    """Load a ProjectConfig from a config file.

    If config_path is provided, loads from that file. Otherwise falls back
    to schemadrift.toml in the current directory.

    Args:
        config_path: Path to a TOML config file.

    Returns:
        A ProjectConfig instance.

    Raises:
        typer.Exit: If config can't be resolved.
    """
    from schemadrift.core.config import load_config

    if config_path:
        try:
            return load_config(config_path)
        except (FileNotFoundError, ValueError) as e:
            typer.echo(f"Error loading config: {e}", err=True)
            raise typer.Exit(1)

    default_config = Path("schemadrift.toml")
    if default_config.exists():
        try:
            return load_config(default_config)
        except ValueError as e:
            typer.echo(f"Error loading config: {e}", err=True)
            raise typer.Exit(1)

    typer.echo(
        "Error: Must specify --config "
        "or have a schemadrift.toml in the current directory",
        err=True,
    )
    raise typer.Exit(1)


# ============================================================================
# Compare Command
# ============================================================================


@app.command()
def compare(
    object_type: Annotated[
        Optional[str],
        typer.Argument(help="Object type to compare (database, schema, view)"),
    ] = None,
    identifier: Annotated[
        Optional[str],
        typer.Argument(help="Fully qualified object name"),
    ] = None,
    source: Annotated[
        Path,
        typer.Option(
            "--source",
            "-s",
            help="Source directory containing object definitions",
        ),
    ] = Path("."),
    config: Annotated[
        Optional[Path],
        typer.Option(
            "--config",
            help="Path to schemadrift.toml config file (for batch comparison)",
        ),
    ] = None,
    changeset: Annotated[
        bool,
        typer.Option(
            "--changeset",
            help="Output a structured YAML changeset instead of a human-readable summary",
        ),
    ] = False,
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output",
            "-o",
            help="Write changeset YAML to a file (implies --changeset)",
        ),
    ] = None,
    account: Annotated[
        Optional[str],
        typer.Option(
            "--account",
            envvar="SNOWFLAKE_ACCOUNT",
            help="Snowflake account identifier",
        ),
    ] = None,
    user: Annotated[
        Optional[str],
        typer.Option(
            "--user",
            envvar="SNOWFLAKE_USER",
            help="Snowflake username",
        ),
    ] = None,
    password: Annotated[
        Optional[str],
        typer.Option(
            "--password",
            envvar="SNOWFLAKE_PASSWORD",
            help="Snowflake password",
            hide_input=True,
        ),
    ] = None,
    authenticator: Annotated[
        Optional[str],
        typer.Option(
            "--authenticator",
            envvar="SNOWFLAKE_AUTHENTICATOR",
            help="Authentication method",
        ),
    ] = None,
    connection_name: Annotated[
        Optional[str],
        typer.Option(
            "--connection",
            "-c",
            help="Connection name from connections.toml",
        ),
    ] = None,
    format: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help="Input file format (yaml or sql)",
        ),
    ] = "yaml",
) -> None:
    """Compare source control definitions with Snowflake.

    Performs a bidirectional comparison: finds objects in source that differ
    from Snowflake (drift / creates) and objects in Snowflake that are absent
    from source (drop candidates).

    Single-object mode requires both object_type and identifier.
    Batch mode uses a config file (--config or schemadrift.toml).

    By default a human-readable summary is printed. Use --changeset to emit
    a structured YAML changeset, or --output FILE to write it to a file.

    Examples:
        # Compare a single view
        schemadrift compare view MYDB.MYSCHEMA.MYVIEW --source ./repo

        # Compare using a config file
        schemadrift compare --source ./repo --config schemadrift.toml

        # Generate a changeset YAML to stdout
        schemadrift compare --changeset --source ./repo

        # Write changeset to a file
        schemadrift compare --output changes.yaml --source ./repo
    """
    if identifier and not object_type:
        typer.echo("Error: object_type is required when specifying an identifier", err=True)
        raise typer.Exit(1)

    if not object_type and not config:
        default_config = Path("schemadrift.toml")
        if not default_config.exists():
            typer.echo(
                "Error: Must specify object_type and identifier, "
                "or use --config / schemadrift.toml for batch comparison",
                err=True,
            )
            raise typer.Exit(1)

    emit_changeset = changeset or output is not None

    conn = _create_connection(
        connection_name=connection_name,
        account=account,
        user=user,
        password=password,
        authenticator=authenticator,
    )

    try:
        project_config = None if (object_type and identifier) else _resolve_config(config_path=config)
        fm = FileManager(source)
        service = DriftService(conn, fm, config=project_config)

        if object_type and identifier:
            obj_type = object_type.upper().rstrip("S")
            _compare_single_object(service, obj_type, identifier, format)
        else:
            entries = service.compare_targets(project_config, format=format)
            if emit_changeset:
                _output_changeset(entries, output)
            else:
                from schemadrift.core.comparison import format_summary_report

                typer.echo("Comparing objects...")
                typer.echo(format_summary_report(entries))

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    finally:
        conn.close()


def _compare_single_object(
    service: DriftService,
    obj_type: str,
    identifier: str,
    format: str = "sql",
) -> None:
    """Compare a single object and display the result."""
    typer.echo(f"Comparing {obj_type} {identifier}...")
    entry = service.compare_object(obj_type, identifier, format)
    if entry is None:
        typer.echo(f"  {obj_type} {identifier}: not found in source or Snowflake")
    else:
        typer.echo(entry.format_summary())


def _output_changeset(
    entries: list[ComparisonEntry],
    output: Optional[Path],
) -> None:
    """Output a structured YAML changeset from comparison results."""
    from schemadrift.core.comparison import to_changeset_yaml

    yaml_output = to_changeset_yaml(entries)

    actionable = [e for e in entries if e.has_changes]
    if not actionable:
        typer.echo("No changes detected.")
        return

    if output:
        output.write_text(yaml_output)
        typer.echo(f"Changeset written to {output} ({len(actionable)} change(s))")
    else:
        typer.echo(yaml_output)


# ============================================================================
# List Command
# ============================================================================


@app.command("list")
def list_objects(
    object_type: Annotated[
        str,
        typer.Argument(help="Object type to list (database, schema, view)"),
    ],
    scope: Annotated[
        Optional[str],
        typer.Argument(help="Scope to list in (database.schema or database)"),
    ] = None,
    account: Annotated[
        Optional[str],
        typer.Option(
            "--account",
            envvar="SNOWFLAKE_ACCOUNT",
            help="Snowflake account identifier",
        ),
    ] = None,
    user: Annotated[
        Optional[str],
        typer.Option(
            "--user",
            envvar="SNOWFLAKE_USER",
            help="Snowflake username",
        ),
    ] = None,
    password: Annotated[
        Optional[str],
        typer.Option(
            "--password",
            envvar="SNOWFLAKE_PASSWORD",
            help="Snowflake password",
            hide_input=True,
        ),
    ] = None,
    authenticator: Annotated[
        Optional[str],
        typer.Option(
            "--authenticator",
            envvar="SNOWFLAKE_AUTHENTICATOR",
            help="Authentication method",
        ),
    ] = None,
    connection_name: Annotated[
        Optional[str],
        typer.Option(
            "--connection",
            "-c",
            help="Connection name from connections.toml",
        ),
    ] = None,
) -> None:
    """List objects in Snowflake.

    Examples:
        # List all views in a schema
        schemadrift list views MYDB.MYSCHEMA

        # List all databases
        schemadrift list databases
    """
    obj_type = object_type.upper().rstrip("S")

    conn = _create_connection(
        connection_name=connection_name,
        account=account,
        user=user,
        password=password,
        authenticator=authenticator,
    )

    try:
        service = DriftService(conn)
        objects = service.list_objects(obj_type, scope or "")

        typer.echo(f"Found {len(objects)} {obj_type.lower()}(s):")
        for obj in objects:
            typer.echo(f"  {obj}")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    app()
