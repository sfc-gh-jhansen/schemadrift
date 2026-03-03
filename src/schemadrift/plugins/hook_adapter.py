"""Generic hook adapter that bridges any SnowflakeObject subclass to pluggy.

Instead of writing a separate hooks.py for every object type plugin,
instantiate HookAdapter(ModelClass) and register it with pluggy.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import schemadrift
from schemadrift.core.base_object import (
    ObjectExtractionError,
    ObjectNotFoundError,
    ObjectParseError,
)

if TYPE_CHECKING:
    from schemadrift.core.base_object import ObjectDiff, ObjectScope, SnowflakeObject


class HookAdapter:
    """Pluggy-compatible hook implementation for any SnowflakeObject subclass.

    All the error-wrapping and delegation logic that was previously
    copy-pasted in every plugin's hooks.py lives here once.
    """

    def __init__(self, model_class: type[SnowflakeObject]):
        self._model = model_class

    @schemadrift.hookimpl
    def get_object_type(self) -> str:
        return self._model.OBJECT_TYPE

    @schemadrift.hookimpl
    def get_scope(self) -> ObjectScope:
        return self._model.SCOPE

    @schemadrift.hookimpl
    def extract_object(self, connection, identifier: str) -> SnowflakeObject:
        obj_type = self._model.OBJECT_TYPE
        try:
            return self._model.extract(connection, identifier)
        except ObjectNotFoundError:
            raise
        except ValueError as e:
            if "not found" in str(e).lower():
                raise ObjectNotFoundError(obj_type, identifier) from e
            raise ObjectExtractionError(obj_type, identifier, e) from e
        except Exception as e:
            raise ObjectExtractionError(obj_type, identifier, e) from e

    @schemadrift.hookimpl
    def list_objects(self, connection, scope: str) -> list[str]:
        return self._model.list_objects(connection, scope)

    @schemadrift.hookimpl
    def object_from_ddl(self, sql: str) -> SnowflakeObject:
        try:
            return self._model.from_ddl(sql)
        except Exception as e:
            raise ObjectParseError(self._model.OBJECT_TYPE, "DDL", e) from e

    @schemadrift.hookimpl
    def object_from_dict(
        self, data: dict, context: dict | None = None,
    ) -> SnowflakeObject:
        try:
            return self._model.from_dict(data, context=context)
        except Exception as e:
            raise ObjectParseError(self._model.OBJECT_TYPE, "dict", e) from e

    @schemadrift.hookimpl
    def generate_ddl(self, obj: SnowflakeObject) -> str:
        return obj.to_ddl()

    @schemadrift.hookimpl
    def generate_dict(self, obj: SnowflakeObject) -> dict:
        return obj.to_dict()

    @schemadrift.hookimpl
    def compare_objects(self, source: SnowflakeObject, target: SnowflakeObject) -> ObjectDiff:
        return source.compare(target)
