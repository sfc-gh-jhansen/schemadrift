"""Snowflake connector implementation.

This module provides the concrete implementation of the connection
interface using snowflake-connector-python.
"""

from __future__ import annotations

import os
from typing import Any

from schemadrift.connection.interface import BaseConnection


class SnowflakeConnection(BaseConnection):
    """Snowflake connection implementation using snowflake-connector-python.

    Supports multiple authentication methods:
    - Username/password
    - SSO (externalbrowser)
    - Key pair authentication
    - Environment variables

    Example:
        # Using explicit credentials
        conn = SnowflakeConnection(
            account="myaccount",
            user="myuser",
            password="mypassword",
            database="mydb",
            schema="myschema",
        )

        # Using environment variables
        conn = SnowflakeConnection.from_env()

        # Using with context manager
        with SnowflakeConnection(...) as conn:
            results = conn.execute("SELECT 1")
    """

    def __init__(
        self,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
        authenticator: str | None = None,
        private_key_path: str | None = None,
        private_key_passphrase: str | None = None,
        connection_name: str | None = None,
        **kwargs: Any,
    ):
        """Initialize a Snowflake connection.

        Args:
            account: Snowflake account identifier.
            user: Username.
            password: Password (if using password auth).
            database: Default database.
            schema: Default schema.
            warehouse: Default warehouse.
            role: Default role.
            authenticator: Authentication method ('externalbrowser', 'snowflake', etc.)
            private_key_path: Path to private key file (for key pair auth).
            private_key_passphrase: Passphrase for private key.
            **kwargs: Additional arguments passed to snowflake.connector.connect()
        """
        # Import here to make it optional for testing
        import snowflake.connector

        connect_params = {
            "connection_name": connection_name,
            "account": account,
            "user": user,
            "database": database,
            "schema": schema,
            "warehouse": warehouse,
            "role": role,
        }

        # Handle authentication method
        if authenticator:
            connect_params["authenticator"] = authenticator
        elif private_key_path:
            connect_params["private_key_path"] = private_key_path
            if private_key_passphrase:
                connect_params["private_key_passphrase"] = private_key_passphrase
        elif password:
            connect_params["password"] = password

        # Remove None values
        connect_params = {k: v for k, v in connect_params.items() if v is not None}

        # Add any additional kwargs
        connect_params.update(kwargs)

        self._conn = snowflake.connector.connect(**connect_params)
        self._account = account or ""

    @classmethod
    def from_env(cls) -> "SnowflakeConnection":
        """Create a connection from environment variables.

        Supported environment variables:
        - SNOWFLAKE_ACCOUNT
        - SNOWFLAKE_USER
        - SNOWFLAKE_PASSWORD
        - SNOWFLAKE_DATABASE
        - SNOWFLAKE_SCHEMA
        - SNOWFLAKE_WAREHOUSE
        - SNOWFLAKE_ROLE
        - SNOWFLAKE_AUTHENTICATOR
        - SNOWFLAKE_PRIVATE_KEY_PATH
        - SNOWFLAKE_PRIVATE_KEY_PASSPHRASE

        Returns:
            A configured SnowflakeConnection.
        """
        return cls(
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            user=os.environ.get("SNOWFLAKE_USER"),
            password=os.environ.get("SNOWFLAKE_PASSWORD"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            role=os.environ.get("SNOWFLAKE_ROLE"),
            authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR"),
            private_key_path=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
            private_key_passphrase=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        )

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL statement and return results as dicts.

        Args:
            sql: The SQL statement to execute.

        Returns:
            List of dictionaries with column names as keys.
        """
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

    def execute_scalar(self, sql: str) -> Any:
        """Execute and return a single scalar value.

        Args:
            sql: The SQL statement to execute.

        Returns:
            The first column of the first row, or None.
        """
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()

    def get_current_database(self) -> str | None:
        """Get the current database context."""
        return self.execute_scalar("SELECT CURRENT_DATABASE()")

    def get_current_schema(self) -> str | None:
        """Get the current schema context."""
        return self.execute_scalar("SELECT CURRENT_SCHEMA()")

    def get_current_account(self) -> str:
        """Get the current account identifier."""
        result = self.execute_scalar("SELECT CURRENT_ACCOUNT()")
        return result or self._account

    def close(self) -> None:
        """Close the connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_native_connection(self):
        """Get the underlying snowflake-connector connection.

        This can be used with the Snowflake Python APIs (snowflake.core)
        to create a Root object for high-level resource management.

        Returns:
            The native snowflake.connector connection.
        """
        return self._conn


class MockConnection(BaseConnection):
    """Mock connection for testing.

    Allows predefined responses for specific SQL queries.

    Example:
        mock = MockConnection()
        mock.add_response("SELECT 1", [{"1": 1}])
        mock.add_response(
            "SHOW VIEWS IN SCHEMA MYDB.MYSCHEMA",
            [{"name": "VIEW1"}, {"name": "VIEW2"}]
        )
    """

    def __init__(
        self,
        account: str = "test_account",
        database: str | None = "test_db",
        schema: str | None = "test_schema",
    ):
        """Initialize a mock connection.

        Args:
            account: Mock account name.
            database: Mock current database.
            schema: Mock current schema.
        """
        self._account = account
        self._database = database
        self._schema = schema
        self._responses: dict[str, list[dict[str, Any]]] = {}

    def add_response(self, sql_pattern: str, response: list[dict[str, Any]]) -> None:
        """Add a mock response for a SQL pattern.

        Args:
            sql_pattern: SQL pattern to match (case-insensitive, whitespace-normalized).
            response: The response to return.
        """
        normalized = " ".join(sql_pattern.upper().split())
        self._responses[normalized] = response

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL statement against mock responses."""
        normalized = " ".join(sql.upper().split())

        # Check for exact match
        if normalized in self._responses:
            return self._responses[normalized]

        # Check for partial match
        for pattern, response in self._responses.items():
            if pattern in normalized or normalized in pattern:
                return response

        return []

    def get_current_database(self) -> str | None:
        """Get the mock current database."""
        return self._database

    def get_current_schema(self) -> str | None:
        """Get the mock current schema."""
        return self._schema

    def get_current_account(self) -> str:
        """Get the mock account."""
        return self._account

    def close(self) -> None:
        """No-op for mock connection."""
        pass
