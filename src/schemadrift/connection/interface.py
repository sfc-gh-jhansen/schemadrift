"""Abstract connection interface for Snowflake.

This module defines the protocol that all connection implementations
must follow, allowing for different connection sources (direct connector,
external tools, mocked connections, etc.).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SnowflakeConnectionInterface(Protocol):
    """Protocol defining the interface for Snowflake connections.

    This allows the library to work with different connection sources:
    - Direct snowflake-connector-python connections
    - Connections from external tools (e.g., dbt, Airflow)
    - Mock connections for testing

    Implementations must provide all methods defined here.
    """

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL statement and return results as a list of dicts.

        Args:
            sql: The SQL statement to execute.

        Returns:
            List of dictionaries, one per row, with column names as keys.
        """
        ...

    def execute_scalar(self, sql: str) -> Any:
        """Execute a SQL statement and return a single scalar value.

        Args:
            sql: The SQL statement to execute.

        Returns:
            The first column of the first row, or None if no results.
        """
        ...

    def get_current_database(self) -> str | None:
        """Get the current database context.

        Returns:
            Current database name or None if not set.
        """
        ...

    def get_current_schema(self) -> str | None:
        """Get the current schema context.

        Returns:
            Current schema name or None if not set.
        """
        ...

    def get_current_account(self) -> str:
        """Get the current Snowflake account identifier.

        Returns:
            The account identifier.
        """
        ...

    def close(self) -> None:
        """Close the connection."""
        ...

    def get_native_connection(self) -> Any:
        """Get the underlying native snowflake-connector connection.

        This is used by the Snowflake Python APIs (snowflake.core) to
        create a Root object for high-level resource management.

        Returns:
            The native snowflake.connector connection, or None for mock connections.
        """
        ...


class BaseConnection(ABC):
    """Abstract base class for connection implementations.

    Provides common functionality and ensures the interface is followed.
    """

    @abstractmethod
    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL statement and return results."""
        pass

    def execute_scalar(self, sql: str) -> Any:
        """Execute and return single value (default implementation)."""
        results = self.execute(sql)
        if results and results[0]:
            return next(iter(results[0].values()))
        return None

    @abstractmethod
    def get_current_database(self) -> str | None:
        """Get the current database context."""
        pass

    @abstractmethod
    def get_current_schema(self) -> str | None:
        """Get the current schema context."""
        pass

    @abstractmethod
    def get_current_account(self) -> str:
        """Get the current account identifier."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection."""
        pass

    def get_native_connection(self) -> Any:
        """Get the underlying native connection (default: None)."""
        return None

    def __enter__(self) -> "BaseConnection":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
