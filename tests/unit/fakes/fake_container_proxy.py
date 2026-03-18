"""Fake in-memory ContainerProxy for unit-testing PackedContainerProxy.

Mimics relevant ``azure.cosmos.aio.ContainerProxy`` behaviour:
- ``read_item(item, partition_key=...) -> dict``
- ``upsert_item(body, partition_key=...) -> dict``
- ``replace_item(item, body, partition_key=..., if_match=...) -> dict``
- ``create_item(body, ...) -> dict``
- ``patch_item(item, partition_key=..., patch_operations=..., ...) -> dict``
- ``query_items(query=..., parameters=..., enable_cross_partition_query=...) -> async iterable``
- Raises ``CosmosResourceNotFoundError`` on read miss
- Raises ``CosmosAccessConditionFailedError`` on ETag mismatch
- Raises ``CosmosResourceExistsError`` on duplicate create

All methods are **async** to match the async ContainerProxy.
"""

from __future__ import annotations

import copy
import re
import uuid
from typing import Any, Dict, List, Optional

from azure.cosmos.exceptions import (
    CosmosAccessConditionFailedError,
    CosmosHttpResponseError,
    CosmosResourceExistsError,
    CosmosResourceNotFoundError,
)


class _AsyncList:
    """Minimal async iterable wrapper around a list, mimicking ``AsyncItemPaged``."""

    def __init__(self, items: list) -> None:
        self._items = items

    def __aiter__(self):
        return _AsyncListIterator(self._items)


class _AsyncListIterator:
    def __init__(self, items: list) -> None:
        self._items = items
        self._index = 0

    async def __anext__(self):
        if self._index >= len(self._items):
            raise StopAsyncIteration
        item = self._items[self._index]
        self._index += 1
        return item


class FakeContainerProxy:
    """In-memory substitute for ``azure.cosmos.ContainerProxy``.

    Parameters
    ----------
    conflict_schedule:
        A list of booleans controlling when ``replace_item`` should raise
        ``CosmosAccessConditionFailedError`` to simulate ETag conflicts.
    """

    def __init__(
        self,
        conflict_schedule: Optional[List[bool]] = None,
    ) -> None:
        # {(document_id, partition_key): dict}
        self._store: Dict[tuple, Dict[str, Any]] = {}
        self._conflict_schedule = list(conflict_schedule) if conflict_schedule else []

    # -- helpers -----------------------------------------------------------

    @property
    def items(self) -> Dict[tuple, Dict[str, Any]]:
        """Raw view of the store (for test assertions)."""
        return self._store

    def _next_etag(self) -> str:
        return str(uuid.uuid4())

    # -- ContainerProxy-compatible methods ---------------------------------

    async def read_item(self, item: str, *, partition_key: Any, **kwargs: Any) -> dict:
        key = (str(item), str(partition_key))
        doc = self._store.get(key)
        if doc is None:
            raise CosmosResourceNotFoundError(
                status_code=404,
                message=f"Entity with the specified id does not exist: {item}",
            )
        return copy.deepcopy(doc)

    async def upsert_item(self, body: dict, *, partition_key: Any | None = None, **kwargs: Any) -> dict:
        pk = str(partition_key) if partition_key is not None else str(body.get("pk", ""))
        key = (str(body["id"]), pk)
        stored = copy.deepcopy(body)
        stored["_etag"] = self._next_etag()
        stored["_rid"] = "fake_rid"
        stored["_self"] = "fake_self"
        stored["_attachments"] = "attachments/"
        stored["_ts"] = 1234567890
        self._store[key] = stored
        return copy.deepcopy(stored)

    async def replace_item(
        self,
        item: str,
        body: dict,
        *,
        partition_key: Any | None = None,
        if_match: str | None = None,
        **kwargs: Any,
    ) -> dict:
        # Resolve PK: explicit > body["pk"]
        pk = str(partition_key) if partition_key is not None else str(body.get("pk", ""))
        # Check conflict schedule first
        if self._conflict_schedule:
            should_conflict = self._conflict_schedule.pop(0)
            if should_conflict:
                # Bump etag so the next retry sees a different one
                key = (str(item), pk)
                if key in self._store:
                    self._store[key]["_etag"] = self._next_etag()
                raise CosmosAccessConditionFailedError(
                    status_code=412,
                    message="Precondition Failed",
                )

        key = (str(item), pk)
        existing = self._store.get(key)
        if existing is None:
            raise CosmosResourceNotFoundError(
                status_code=404,
                message=f"Entity with the specified id does not exist: {item}",
            )
        if if_match and existing.get("_etag") != if_match:
            raise CosmosAccessConditionFailedError(
                status_code=412,
                message="Precondition Failed",
            )
        stored = copy.deepcopy(body)
        stored["_etag"] = self._next_etag()
        stored["_rid"] = existing.get("_rid", "fake_rid")
        stored["_self"] = existing.get("_self", "fake_self")
        stored["_attachments"] = "attachments/"
        stored["_ts"] = 1234567890
        self._store[key] = stored
        return copy.deepcopy(stored)

    async def create_item(self, body: dict, *, partition_key: Any | None = None, **kwargs: Any) -> dict:
        """Create a new item.  Raises ``CosmosResourceExistsError`` if it already exists."""
        pk = str(partition_key) if partition_key is not None else str(body.get("pk", ""))
        key = (str(body["id"]), pk)
        if key in self._store:
            raise CosmosResourceExistsError(
                status_code=409,
                message=f"Entity with the specified id already exists: {body['id']}",
            )
        stored = copy.deepcopy(body)
        stored["_etag"] = self._next_etag()
        stored["_rid"] = "fake_rid"
        stored["_self"] = "fake_self"
        stored["_attachments"] = "attachments/"
        stored["_ts"] = 1234567890
        self._store[key] = stored
        return copy.deepcopy(stored)

    async def patch_item(
        self,
        item: str,
        *,
        partition_key: Any,
        patch_operations: list,
        filter_predicate: str | None = None,
        **kwargs: Any,
    ) -> dict:
        """Apply partial update operations to an existing item."""
        pk = str(partition_key)
        key = (str(item), pk)
        existing = self._store.get(key)
        if existing is None:
            raise CosmosResourceNotFoundError(
                status_code=404,
                message=f"Entity with the specified id does not exist: {item}",
            )

        doc = existing  # mutate in-place

        # Evaluate filter predicate
        if filter_predicate is not None:
            if not self._evaluate_filter(doc, filter_predicate):
                raise CosmosAccessConditionFailedError(
                    status_code=412,
                    message="Precondition Failed (filter predicate)",
                )

        # Apply operations
        for op_spec in patch_operations:
            op = op_spec["op"].lower()
            path = op_spec["path"]
            value = op_spec.get("value")
            segments = [s for s in path.split("/") if s]
            if not segments:
                raise CosmosHttpResponseError(
                    status_code=400,
                    message=f"Invalid path: {path}",
                )

            if op in ("set", "add"):
                self._set_nested(doc, segments, value)
            elif op == "replace":
                if not self._path_exists(doc, segments):
                    raise CosmosHttpResponseError(
                        status_code=400,
                        message=f"Path '{path}' does not exist for replace",
                    )
                self._set_nested(doc, segments, value)
            elif op == "remove":
                if not self._path_exists(doc, segments):
                    raise CosmosHttpResponseError(
                        status_code=400,
                        message=f"Path '{path}' does not exist for remove",
                    )
                self._remove_nested(doc, segments)
            elif op == "incr":
                current = self._get_nested(doc, segments)
                if current is None:
                    self._set_nested(doc, segments, value)
                else:
                    self._set_nested(doc, segments, current + value)
            else:
                raise CosmosHttpResponseError(
                    status_code=400, message=f"Unknown op: {op}"
                )

        doc["_etag"] = self._next_etag()
        return copy.deepcopy(doc)

    # -- Patch helpers -----------------------------------------------------

    @staticmethod
    def _set_nested(doc: dict, segments: list, value: Any) -> None:
        obj = doc
        for seg in segments[:-1]:
            if seg not in obj:
                obj[seg] = {}
            obj = obj[seg]
        obj[segments[-1]] = copy.deepcopy(value) if isinstance(value, (dict, list)) else value

    @staticmethod
    def _get_nested(doc: dict, segments: list) -> Any:
        obj = doc
        for seg in segments:
            if not isinstance(obj, dict) or seg not in obj:
                return None
            obj = obj[seg]
        return obj

    @staticmethod
    def _path_exists(doc: dict, segments: list) -> bool:
        obj = doc
        for seg in segments:
            if not isinstance(obj, dict) or seg not in obj:
                return False
            obj = obj[seg]
        return True

    @staticmethod
    def _remove_nested(doc: dict, segments: list) -> None:
        obj = doc
        for seg in segments[:-1]:
            if not isinstance(obj, dict) or seg not in obj:
                return
            obj = obj[seg]
        if isinstance(obj, dict):
            obj.pop(segments[-1], None)

    def _evaluate_filter(self, doc: dict, predicate: str) -> bool:
        """Evaluate a simple filter predicate against a document."""
        m = re.search(r"where\s+(.+)", predicate, re.IGNORECASE)
        if not m:
            return True
        condition = m.group(1).strip()

        # NOT IS_DEFINED(c.path)
        m_not = re.match(r'NOT\s+IS_DEFINED\(c\.(.+)\)', condition, re.IGNORECASE)
        if m_not:
            path_str = m_not.group(1)
            return not self._resolve_path_exists(doc, path_str)

        # IS_DEFINED(c.path)
        m_def = re.match(r'IS_DEFINED\(c\.(.+)\)', condition, re.IGNORECASE)
        if m_def:
            path_str = m_def.group(1)
            return self._resolve_path_exists(doc, path_str)

        return True

    @staticmethod
    def _resolve_path_exists(doc: dict, path_str: str) -> bool:
        normalised = re.sub(r'\["([^"]+)"\]', r'.\1', path_str)
        segments = normalised.split(".")
        obj = doc
        for seg in segments:
            if not isinstance(obj, dict) or seg not in obj:
                return False
            obj = obj[seg]
        return True

    def query_items(
        self,
        query: str,
        *,
        parameters: list | None = None,
        enable_cross_partition_query: bool = False,
        partition_key: Any | None = None,
        **kwargs: Any,
    ) -> _AsyncList:
        """Very simple query implementation for testing.

        Supports ``STARTSWITH(c.id, @prefix)`` and ``c.docType = '...'``
        which is what ``PackedContainerProxy._all_bucket_ids`` uses.
        Also supports ``SELECT c.bucketId FROM c ...`` projection.

        Returns an async iterable to match ``azure.cosmos.aio.ContainerProxy``.
        """
        # Collect candidate docs
        docs: List[dict] = []
        for (_doc_id, _pk), doc in self._store.items():
            if partition_key is not None and _pk != str(partition_key):
                continue
            docs.append(copy.deepcopy(doc))

        # Filter by WHERE clauses (minimal)
        results = self._apply_where(docs, query, parameters)

        # Projection: SELECT c.field FROM c
        results = self._apply_projection(results, query)

        return _AsyncList(results)

    # -- Query helpers (minimal) -------------------------------------------

    @staticmethod
    def _apply_where(
        docs: List[dict], query: str, parameters: list | None
    ) -> List[dict]:
        upper = query.upper()
        where_idx = upper.find("WHERE")
        if where_idx < 0:
            return docs

        where_clause = query[where_idx + 5 :].strip()
        conditions = re.split(r"\bAND\b", where_clause, flags=re.IGNORECASE)

        for cond in conditions:
            cond = cond.strip()

            # STARTSWITH(c.field, value)
            m = re.match(
                r"STARTSWITH\(c\.(\w+),\s*(@\w+|'[^']*')\)",
                cond,
                re.IGNORECASE,
            )
            if m:
                field = m.group(1)
                raw_val = m.group(2)
                val = FakeContainerProxy._resolve_param(raw_val, parameters)
                docs = [d for d in docs if str(d.get(field, "")).startswith(val)]
                continue

            # c.field = value
            m = re.match(
                r"c\.(\w+)\s*=\s*(@\w+|'[^']*'|\"[^\"]*\"|\d+)",
                cond,
                re.IGNORECASE,
            )
            if m:
                field = m.group(1)
                raw_val = m.group(2)
                val = FakeContainerProxy._resolve_param(raw_val, parameters)
                docs = [d for d in docs if d.get(field) == val]
                continue

        return docs

    @staticmethod
    def _apply_projection(docs: List[dict], query: str) -> List[dict]:
        """Handle ``SELECT c.field FROM c ...`` projection."""
        m = re.match(r"SELECT\s+c\.(\w+)\s+FROM", query, re.IGNORECASE)
        if m:
            field = m.group(1)
            return [{field: d.get(field)} for d in docs]
        return docs

    @staticmethod
    def _resolve_param(raw: str, parameters: list | None) -> Any:
        if raw.startswith("@") and parameters:
            for p in parameters:
                if p.get("name") == raw:
                    return p["value"]
            return raw
        if (raw.startswith("'") and raw.endswith("'")) or (
            raw.startswith('"') and raw.endswith('"')
        ):
            return raw[1:-1]
        try:
            return int(raw)
        except ValueError:
            return raw

    # -- Test helpers ------------------------------------------------------

    def inject_conflicts(self, schedule: List[bool]) -> None:
        """Replace the remaining conflict schedule."""
        self._conflict_schedule = list(schedule)

    def clear(self) -> None:
        """Remove all stored items."""
        self._store.clear()
        self._conflict_schedule.clear()
