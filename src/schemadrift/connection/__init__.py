"""Connection module for Snowflake connectivity."""

from schemadrift.connection.interface import SnowflakeConnectionInterface
from schemadrift.connection.snowflake_impl import SnowflakeConnection

__all__ = ["SnowflakeConnectionInterface", "SnowflakeConnection"]
