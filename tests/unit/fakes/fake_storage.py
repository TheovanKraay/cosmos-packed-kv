"""Fake in-memory storage implementation for unit tests.

Supports:
- Point reads / upserts / conditional replaces
- Patch operations (set, replace, remove, incr)
- Conditional create (409 on duplicate)
- ETag simulation (incremented on each write)
- Optional ETag conflict injection for contention testing

All methods are **async** to match the async :class:`StorageInterface`.
"""

from __future__ import annotations

import copy
import re
import uuid
from typing import Any, Callable, Dict, List, Optional

from packed_kv_cosmos.storage import (
    ETagMismatchError,
    ItemAlreadyExistsError,
    PatchOperationFailedError,
    PatchPreconditionFailedError,
    PatchTargetNotFoundError,
    StorageInterface,
)


class FakeStorage(StorageInterface):
    """In-memory storage backend for deterministic unit testing.

    Parameters
    ----------
    conflict_schedule:
        Optional list of booleans.  When provided, each ``replace_item``
        call pops the first element; if ``True`` the call raises
        :class:`ETagMismatchError` to simulate a conflict.  When the list
        is exhausted normal behaviour resumes.
    on_write:
        Optional callback invoked after every successful write with the
        item dict.  Useful for assertions or side-effects in tests.
    """

    def __init__(
        self,
        conflict_schedule: Optional[List[bool]] = None,
        on_write: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        # {(document_id, partition_key): item_dict}
        self._store: Dict[tuple, Dict[str, Any]] = {}
        self._conflict_schedule = list(conflict_schedule) if conflict_schedule else []
        self._on_write = on_write

    # -- helpers -----------------------------------------------------------

    @property
    def items(self) -> Dict[tuple, Dict[str, Any]]:
        """Raw view of the in-memory store (for test assertions)."""
        return self._store

    def _next_etag(self) -> str:
        return str(uuid.uuid4())

    # -- StorageInterface --------------------------------------------------

    async def read_item(self, document_id: str, partition_key: str) -> Optional[Dict[str, Any]]:
        item = self._store.get((document_id, partition_key))
        if item is None:
            return None
        return copy.deepcopy(item)

    async def upsert_item(self, body: Dict[str, Any], partition_key: str) -> Dict[str, Any]:
        key = (body["id"], partition_key)
        etag = self._next_etag()
        stored = copy.deepcopy(body)
        stored["_etag"] = etag
        self._store[key] = stored
        if self._on_write:
            self._on_write(stored)
        return copy.deepcopy(stored)

    async def replace_item(
        self,
        document_id: str,
        body: Dict[str, Any],
        partition_key: str,
        if_match: str,
    ) -> Dict[str, Any]:
        # Check conflict schedule first
        if self._conflict_schedule:
            should_conflict = self._conflict_schedule.pop(0)
            if should_conflict:
                # Simulate another writer slipping in: update the stored etag
                key = (document_id, partition_key)
                if key in self._store:
                    self._store[key]["_etag"] = self._next_etag()
                raise ETagMismatchError(
                    f"Simulated ETag conflict for '{document_id}'"
                )

        key = (document_id, partition_key)
        existing = self._store.get(key)
        if existing is None:
            raise ETagMismatchError(f"Document '{document_id}' does not exist")
        if existing.get("_etag") != if_match:
            raise ETagMismatchError(
                f"ETag mismatch for '{document_id}': "
                f"expected '{if_match}', got '{existing.get('_etag')}'"
            )

        etag = self._next_etag()
        stored = copy.deepcopy(body)
        stored["_etag"] = etag
        self._store[key] = stored
        if self._on_write:
            self._on_write(stored)
        return copy.deepcopy(stored)

    async def create_item(self, body: Dict[str, Any], partition_key: str) -> Dict[str, Any]:
        key = (body["id"], partition_key)
        if key in self._store:
            raise ItemAlreadyExistsError(
                f"Item with id '{body['id']}' already exists"
            )
        etag = self._next_etag()
        stored = copy.deepcopy(body)
        stored["_etag"] = etag
        self._store[key] = stored
        if self._on_write:
            self._on_write(stored)
        return copy.deepcopy(stored)

    async def patch_item(
        self,
        document_id: str,
        partition_key: str,
        operations: List[Dict[str, Any]],
        *,
        filter_predicate: Optional[str] = None,
    ) -> Dict[str, Any]:
        key = (document_id, partition_key)
        existing = self._store.get(key)
        if existing is None:
            raise PatchTargetNotFoundError(
                f"Document '{document_id}' does not exist"
            )

        doc = existing  # mutate in-place then update etag

        # Evaluate filter predicate if present
        if filter_predicate is not None:
            if not self._evaluate_filter(doc, filter_predicate):
                raise PatchPreconditionFailedError(
                    f"Filter predicate failed for '{document_id}'"
                )

        # Apply operations
        for op_spec in operations:
            op = op_spec["op"].lower()
            path = op_spec["path"]  # e.g. "/entries/abc123"
            value = op_spec.get("value")

            segments = [s for s in path.split("/") if s]
            if not segments:
                raise PatchOperationFailedError(f"Invalid path: {path}")

            if op in ("set", "add"):
                self._set_nested(doc, segments, value)
            elif op == "replace":
                if not self._path_exists(doc, segments):
                    raise PatchOperationFailedError(
                        f"Path '{path}' does not exist (replace requires existing path)"
                    )
                self._set_nested(doc, segments, value)
            elif op == "remove":
                if not self._path_exists(doc, segments):
                    raise PatchOperationFailedError(
                        f"Path '{path}' does not exist (cannot remove)"
                    )
                self._remove_nested(doc, segments)
            elif op == "incr":
                current = self._get_nested(doc, segments)
                if current is None:
                    self._set_nested(doc, segments, value)
                else:
                    self._set_nested(doc, segments, current + value)
            else:
                raise PatchOperationFailedError(f"Unknown op: {op}")

        doc["_etag"] = self._next_etag()
        if self._on_write:
            self._on_write(doc)
        return copy.deepcopy(doc)

    # -- Patch helpers -----------------------------------------------------

    @staticmethod
    def _set_nested(doc: dict, segments: list, value: Any) -> None:
        """Set a nested value given path segments like ['entries', 'abc']."""
        obj = doc
        for seg in segments[:-1]:
            if seg not in obj:
                obj[seg] = {}
            obj = obj[seg]
        obj[segments[-1]] = copy.deepcopy(value) if isinstance(value, (dict, list)) else value

    @staticmethod
    def _get_nested(doc: dict, segments: list) -> Any:
        """Get a nested value, returning None if path doesn't exist."""
        obj = doc
        for seg in segments:
            if not isinstance(obj, dict) or seg not in obj:
                return None
            obj = obj[seg]
        return obj

    @staticmethod
    def _path_exists(doc: dict, segments: list) -> bool:
        """Check if a nested path exists."""
        obj = doc
        for seg in segments:
            if not isinstance(obj, dict) or seg not in obj:
                return False
            obj = obj[seg]
        return True

    @staticmethod
    def _remove_nested(doc: dict, segments: list) -> None:
        """Remove a nested key."""
        obj = doc
        for seg in segments[:-1]:
            if not isinstance(obj, dict) or seg not in obj:
                return
            obj = obj[seg]
        if isinstance(obj, dict):
            obj.pop(segments[-1], None)

    def _evaluate_filter(self, doc: dict, predicate: str) -> bool:
        """Evaluate a simple filter predicate against a document.

        Supports patterns like:
        - ``from c where NOT IS_DEFINED(c.entries["key"])``
        - ``from c where IS_DEFINED(c.entries["key"])``
        """
        # Extract the WHERE clause
        m = re.search(r"where\s+(.+)", predicate, re.IGNORECASE)
        if not m:
            return True  # no condition → passes
        condition = m.group(1).strip()

        # NOT IS_DEFINED(c.path.to.prop) or NOT IS_DEFINED(c.path["key"])
        m_not = re.match(
            r'NOT\s+IS_DEFINED\(c\.(.+)\)', condition, re.IGNORECASE
        )
        if m_not:
            path_str = m_not.group(1)
            return not self._resolve_path_exists(doc, path_str)

        # IS_DEFINED(c.path.to.prop) or IS_DEFINED(c.path["key"])
        m_def = re.match(
            r'IS_DEFINED\(c\.(.+)\)', condition, re.IGNORECASE
        )
        if m_def:
            path_str = m_def.group(1)
            return self._resolve_path_exists(doc, path_str)

        # Unknown predicate → passes (conservative)
        return True

    @staticmethod
    def _resolve_path_exists(doc: dict, path_str: str) -> bool:
        """Resolve a dot/bracket path like ``entries["abc"]`` or ``entries.abc``."""
        # Normalise bracket notation: entries["abc"] → entries.abc
        normalised = re.sub(r'\["([^"]+)"\]', r'.\1', path_str)
        segments = normalised.split(".")
        obj = doc
        for seg in segments:
            if not isinstance(obj, dict) or seg not in obj:
                return False
            obj = obj[seg]
        return True

    # -- Test helpers ------------------------------------------------------

    def inject_conflicts(self, schedule: List[bool]) -> None:
        """Replace the remaining conflict schedule."""
        self._conflict_schedule = list(schedule)

    def clear(self) -> None:
        """Remove all stored items."""
        self._store.clear()
        self._conflict_schedule.clear()
