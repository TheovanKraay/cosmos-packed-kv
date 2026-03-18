# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""PackedContainerProxy — ContainerProxy-style wrapper with transparent packing.

Wraps a standard :class:`azure.cosmos.aio.ContainerProxy` and exposes a similar
(but simplified) method surface while transparently packing many small
logical items into fewer physical Cosmos "bucket documents".

**Bucket routing is derived solely from the item id**::

    bucketId  = sha256(item_id)[0:prefix]
    entryId   = sha256(item_id)

This guarantees that every ``read_item`` is a **single Cosmos point read**
because the bin document ``id`` and ``pk`` are both derivable from just the
item's ``id``.

**Writes use Cosmos partial document update (patch)** to add, update, or
remove entries inside bucket documents without reading the full document
first.  This eliminates the read-before-write round-trip and provides
server-side conflict resolution — two concurrent patches to different
entries in the same bucket are serialised automatically with no 412
retries required.

All public methods are **async** coroutines.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, AsyncIterator, Dict, List, Optional

from azure.cosmos.aio import ContainerProxy
from azure.cosmos.exceptions import (
    CosmosAccessConditionFailedError,
    CosmosHttpResponseError,
    CosmosResourceExistsError,
    CosmosResourceNotFoundError,
)

from packed_kv_cosmos.config import PackingConfig
from packed_kv_cosmos.hashing import compute_bucket_id, compute_entry_id
from packed_kv_cosmos.model import DOC_TYPE_ROOT
from packed_kv_cosmos.packing import (
    make_partition_key,
    make_root_document_id,
)
from packed_kv_cosmos.results import BulkError, BulkResult

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_ENTRIES_PER_PATCH = 9
"""Max entries per Cosmos patch call (Cosmos limit is 10 ops; 1 reserved for updatedAt)."""

_RETRY_BASE_DELAY = 0.5
"""Base delay in seconds for exponential backoff on transient errors."""

_RETRY_MAX_DELAY = 30.0
"""Maximum delay in seconds between retries."""


def _is_transient(exc: Exception) -> bool:
    """Return True if *exc* is a transient Cosmos error worth retrying."""
    if isinstance(exc, CosmosHttpResponseError):
        return exc.status_code in (429, 449, 500, 503)
    # Connection-level errors (reset, timeout, etc.)
    return isinstance(exc, (ConnectionError, TimeoutError, OSError))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COSMOS_SYSTEM_PROPS = {"_rid", "_self", "_etag", "_attachments", "_ts"}
"""Cosmos system properties stripped from returned items."""


def _strip_system_props(item: dict) -> dict:
    """Return a shallow copy of *item* without Cosmos system properties."""
    return {k: v for k, v in item.items() if k not in _COSMOS_SYSTEM_PROPS}


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# PackedContainerProxy
# ---------------------------------------------------------------------------

class PackedContainerProxy:
    """ContainerProxy-style wrapper with transparent bin packing.

    Writes use Cosmos partial document update (patch) operations to
    modify entries in-place, eliminating the read-before-write cycle.

    All public methods are **async** coroutines.

    Parameters
    ----------
    container:
        The underlying async Cosmos ``ContainerProxy`` used for physical reads/writes.
    config:
        A :class:`PackingConfig` controlling namespace, routing, and limits.
    """

    def __init__(self, container: ContainerProxy, config: PackingConfig | None = None) -> None:
        self._container = container
        self._config = config or PackingConfig()

    # ------------------------------------------------------------------
    # Internal: routing helpers
    # ------------------------------------------------------------------

    def _bucket_id(self, item_id: str) -> str:
        """Compute bucket id from item id alone: ``sha256(item_id)[:prefix]``."""
        return compute_bucket_id(
            compute_entry_id(item_id), self._config.bucket_prefix_length
        )

    def _entry_id(self, item_id: str) -> str:
        """Compute the entry id (full SHA-256 hex) from an item ``id``."""
        return compute_entry_id(item_id)

    def _root_id(self, bucket_id: str) -> str:
        return make_root_document_id(self._config.namespace, bucket_id)

    def _cosmos_pk(self, bucket_id: str) -> str:
        return make_partition_key(self._config.namespace, bucket_id)

    # ------------------------------------------------------------------
    # Internal: bucket read primitive (still needed for reads/queries)
    # ------------------------------------------------------------------

    async def _read_bucket_doc(self, doc_id: str, cosmos_pk: str) -> Optional[dict]:
        """Point-read a bucket document.  Return ``None`` if not found."""
        try:
            return await self._container.read_item(doc_id, partition_key=cosmos_pk)
        except CosmosResourceNotFoundError:
            return None

    # ------------------------------------------------------------------
    # Internal: make a new empty root document dict
    # ------------------------------------------------------------------

    def _new_root_doc(self, root_id: str, cosmos_pk: str, bucket_id: str) -> dict:
        return {
            "id": root_id,
            "pk": cosmos_pk,
            "schemaVersion": self._config.schema_version,
            "docType": DOC_TYPE_ROOT,
            "bucketId": bucket_id,
            "entries": {},
            "tailPage": None,
            "pageCount": 0,
            "updatedAt": _now_iso(),
        }

    # ------------------------------------------------------------------
    # Internal: create bucket with first entry (handles race)
    # ------------------------------------------------------------------

    async def _create_bucket_with_entry(
        self,
        bucket_id: str,
        entry_id: str,
        item_body: dict,
        patch_ops: list,
    ) -> dict:
        """Create a new bucket document with *item_body* as its first entry.

        Handles the race condition where another thread creates the same
        bucket concurrently:  ``create_item`` → 409 → retry with patch.
        """
        root_id = self._root_id(bucket_id)
        cosmos_pk = self._cosmos_pk(bucket_id)
        root_doc = self._new_root_doc(root_id, cosmos_pk, bucket_id)
        root_doc["entries"][entry_id] = item_body
        root_doc["updatedAt"] = _now_iso()
        try:
            await self._container.create_item(root_doc)
            return item_body
        except CosmosResourceExistsError:
            # Another thread created the bucket first — add entry via patch
            await self._container.patch_item(
                item=root_id,
                partition_key=cosmos_pk,
                patch_operations=patch_ops,
            )
            return item_body

    # ------------------------------------------------------------------
    # Internal: put entry using patch
    # ------------------------------------------------------------------

    async def _put_entry(
        self,
        bucket_id: str,
        entry_id: str,
        item_body: dict,
        *,
        must_not_exist: bool = False,
        must_exist: bool = False,
    ) -> dict:
        """Insert or update an entry inside a bucket document using patch.

        Parameters
        ----------
        must_not_exist:
            If ``True``, raise 409 if entry already exists (for ``create_item``).
        must_exist:
            If ``True``, raise 404 if entry not found (for ``replace_item``).
        """
        root_id = self._root_id(bucket_id)
        cosmos_pk = self._cosmos_pk(bucket_id)

        if must_not_exist:
            # create_item: use filter to ensure entry doesn't exist
            ops = [
                {"op": "set", "path": f"/entries/{entry_id}", "value": item_body},
                {"op": "set", "path": "/updatedAt", "value": _now_iso()},
            ]
            filter_pred = f'from c where NOT IS_DEFINED(c.entries["{entry_id}"])'
            try:
                await self._container.patch_item(
                    item=root_id,
                    partition_key=cosmos_pk,
                    patch_operations=ops,
                    filter_predicate=filter_pred,
                )
                return item_body
            except CosmosResourceNotFoundError:
                # Bucket doesn't exist → create with entry
                return await self._create_bucket_with_entry(
                    bucket_id, entry_id, item_body, ops,
                )
            except CosmosAccessConditionFailedError:
                # 412 → filter failed → entry already exists
                raise CosmosResourceExistsError(
                    status_code=409,
                    message=f"Item with id '{item_body.get('id')}' already exists.",
                )

        elif must_exist:
            # replace_item: use 'replace' op which fails if path doesn't exist
            ops = [
                {"op": "replace", "path": f"/entries/{entry_id}", "value": item_body},
                {"op": "set", "path": "/updatedAt", "value": _now_iso()},
            ]
            try:
                await self._container.patch_item(
                    item=root_id,
                    partition_key=cosmos_pk,
                    patch_operations=ops,
                )
                return item_body
            except CosmosResourceNotFoundError:
                # Bucket doesn't exist → entry doesn't exist
                raise CosmosResourceNotFoundError(
                    status_code=404,
                    message=f"Item with id '{item_body.get('id')}' not found.",
                )
            except CosmosHttpResponseError as exc:
                if exc.status_code == 400:
                    # 'replace' on non-existent path → entry not found
                    raise CosmosResourceNotFoundError(
                        status_code=404,
                        message=f"Item with id '{item_body.get('id')}' not found.",
                    )
                raise

        else:
            # upsert_item: use 'set' op (creates or updates)
            ops = [
                {"op": "set", "path": f"/entries/{entry_id}", "value": item_body},
                {"op": "set", "path": "/updatedAt", "value": _now_iso()},
            ]
            try:
                await self._container.patch_item(
                    item=root_id,
                    partition_key=cosmos_pk,
                    patch_operations=ops,
                )
                return item_body
            except CosmosResourceNotFoundError:
                # Bucket doesn't exist → create with entry
                return await self._create_bucket_with_entry(
                    bucket_id, entry_id, item_body, ops,
                )

    # ------------------------------------------------------------------
    # Internal: find and return an entry (single point read)
    # ------------------------------------------------------------------

    async def _find_entry(self, bucket_id: str, entry_id: str) -> Optional[dict]:
        """Return the item dict for *entry_id* in the given bucket, or ``None``.

        This is always a single Cosmos point read.
        """
        root_id = self._root_id(bucket_id)
        cosmos_pk = self._cosmos_pk(bucket_id)
        root_raw = await self._read_bucket_doc(root_id, cosmos_pk)
        if root_raw is None:
            return None

        entries = root_raw.get("entries", {})
        return entries.get(entry_id)

    # ------------------------------------------------------------------
    # Internal: delete an entry using patch
    # ------------------------------------------------------------------

    async def _delete_entry(self, bucket_id: str, entry_id: str) -> None:
        """Remove *entry_id* from its bucket using a patch ``remove``.

        Raises 404 if the bucket or entry does not exist.
        No read-before-write is needed.
        """
        root_id = self._root_id(bucket_id)
        cosmos_pk = self._cosmos_pk(bucket_id)
        ops = [
            {"op": "remove", "path": f"/entries/{entry_id}"},
            {"op": "set", "path": "/updatedAt", "value": _now_iso()},
        ]
        try:
            await self._container.patch_item(
                item=root_id,
                partition_key=cosmos_pk,
                patch_operations=ops,
            )
        except CosmosResourceNotFoundError:
            raise CosmosResourceNotFoundError(
                status_code=404, message="Item not found."
            )
        except CosmosHttpResponseError as exc:
            if exc.status_code == 400:
                # 'remove' on non-existent path → entry not found
                raise CosmosResourceNotFoundError(
                    status_code=404, message="Item not found."
                )
            raise

    # ------------------------------------------------------------------
    # Internal: iterate all entries in a single bucket
    # ------------------------------------------------------------------

    async def _iter_bucket_entries(self, bucket_id: str) -> AsyncIterator[dict]:
        """Yield all item dicts packed in the bucket identified by *bucket_id*."""
        root_id = self._root_id(bucket_id)
        cosmos_pk = self._cosmos_pk(bucket_id)
        root_raw = await self._read_bucket_doc(root_id, cosmos_pk)
        if root_raw is None:
            return

        for entry in root_raw.get("entries", {}).values():
            yield entry

    # ------------------------------------------------------------------
    # Internal: iterate ALL entries across all buckets
    # ------------------------------------------------------------------

    async def _all_bucket_ids(self) -> List[str]:
        """Return unique bucket ids from all root documents belonging to this namespace."""
        prefix = f"{self._config.namespace}:"
        query = (
            "SELECT c.bucketId FROM c "
            "WHERE c.docType = 'bucketRoot' AND STARTSWITH(c.id, @prefix)"
        )
        params: List[Dict[str, Any]] = [{"name": "@prefix", "value": prefix}]
        results = self._container.query_items(
            query=query,
            parameters=params,
        )
        seen: set[str] = set()
        bucket_ids: list[str] = []
        async for row in results:
            bid = row.get("bucketId")
            if bid and bid not in seen:
                seen.add(bid)
                bucket_ids.append(bid)
        return bucket_ids

    async def _iter_all_entries(self) -> AsyncIterator[dict]:
        """Yield every item dict from every bucket in this namespace."""
        for bucket_id in await self._all_bucket_ids():
            async for entry in self._iter_bucket_entries(bucket_id):
                yield entry

    # ==================================================================
    # Public API — ContainerProxy-compatible methods
    # ==================================================================

    async def create_item(self, body: dict, **kwargs: Any) -> dict:
        """Insert a new item.  Raise ``CosmosResourceExistsError`` (409) if it exists."""
        item_id = str(body["id"])
        bucket_id = self._bucket_id(item_id)
        entry_id = self._entry_id(item_id)
        return await self._put_entry(bucket_id, entry_id, body, must_not_exist=True)

    async def read_item(self, item: str, **kwargs: Any) -> dict:
        """Read a single item by id.

        This always executes a single Cosmos point read.

        Raises ``CosmosResourceNotFoundError`` (404) if not found.
        """
        item_id = str(item)
        bucket_id = self._bucket_id(item_id)
        entry_id = self._entry_id(item_id)
        result = await self._find_entry(bucket_id, entry_id)
        if result is None:
            raise CosmosResourceNotFoundError(
                status_code=404,
                message=f"Item with id '{item}' not found.",
            )
        return result

    async def upsert_item(self, body: dict, **kwargs: Any) -> dict:
        """Create or replace an item."""
        item_id = str(body["id"])
        bucket_id = self._bucket_id(item_id)
        entry_id = self._entry_id(item_id)
        return await self._put_entry(bucket_id, entry_id, body)

    async def replace_item(self, item: str, body: dict, **kwargs: Any) -> dict:
        """Replace an existing item.  Raise 404 if not found."""
        item_id = str(body["id"])
        bucket_id = self._bucket_id(item_id)
        entry_id = self._entry_id(item_id)
        return await self._put_entry(bucket_id, entry_id, body, must_exist=True)

    async def delete_item(self, item: str, **kwargs: Any) -> None:
        """Delete an item by id.  Raise 404 if not found."""
        item_id = str(item)
        bucket_id = self._bucket_id(item_id)
        entry_id = self._entry_id(item_id)
        await self._delete_entry(bucket_id, entry_id)

    async def query_items(
        self,
        query: str,
        *,
        parameters: list | None = None,
        **kwargs: Any,
    ) -> list[dict]:
        """Execute a query over packed items, evaluated **client-side**.

        All queries require a cross-partition scan of every bucket in the
        namespace.

        .. note::

           The *query* string and *parameters* are parsed very minimally:
           only ``SELECT * FROM c WHERE ...`` style queries are handled by
           evaluating the ``WHERE`` predicate client-side using simple
           field comparison.  Complex SQL predicates are **not** pushed down.
           As a pragmatic compromise the implementation returns **all**
           items when it cannot parse the predicate, which is correct but
           potentially slower than a server-side query.
        """
        items: list[dict] = []
        async for entry in self._iter_all_entries():
            items.append(entry)

        # Client-side WHERE filtering (best-effort)
        predicate = _parse_where_predicate(query, parameters)
        if predicate is not None:
            items = [i for i in items if predicate(i)]
        return items

    async def read_all_items(self, **kwargs: Any) -> list[dict]:
        """Return all packed items across all buckets in this namespace."""
        items: list[dict] = []
        async for entry in self._iter_all_entries():
            items.append(entry)
        return items

    # ==================================================================
    # Public API — Bulk operations
    # ==================================================================

    async def bulk_upsert_items(
        self,
        items: list[dict],
        *,
        max_concurrency: int = 50,
        patch_first: bool = False,
        max_retries: int | None = None,
    ) -> BulkResult:
        """Upsert many items in bulk with cross-bucket parallelism.

        Items are grouped by their deterministic bucket id, then written
        concurrently.  New buckets are created in a single ``create_item``
        call containing all their entries.  Existing buckets are updated
        via batched patch operations (up to 9 entries per patch call,
        respecting the Cosmos 10-operation-per-patch limit).

        Parameters
        ----------
        items:
            List of item dicts, each containing at least an ``"id"`` key.
        max_concurrency:
            Maximum number of concurrent bucket-level Cosmos operations.
        patch_first:
            When ``True``, try patching entries into an existing bucket
            first and fall back to ``create_item`` on 404.  This is the
            optimal strategy when most target buckets already exist (e.g.
            streaming waves after the first batch).  When ``False``
            (default), the library tries ``create_item`` first and falls
            back to patch on 409 — optimal for initial loads into empty
            containers.
        max_retries:
            Maximum number of retries per bucket on transient errors
            (429, 503, timeouts, connection resets).  ``None`` (default)
            means retry indefinitely — bulk jobs will slow down under
            throttling but never drop items.

        Returns
        -------
        BulkResult
            Aggregate counts and per-item error details.
        """
        if not items:
            return BulkResult()

        # ----- group items by bucket -----
        bucket_groups: dict[str, list[tuple[str, dict]]] = defaultdict(list)
        for item in items:
            item_id = str(item["id"])
            bucket_id = self._bucket_id(item_id)
            entry_id = self._entry_id(item_id)
            bucket_groups[bucket_id].append((entry_id, item))

        # ----- process each bucket concurrently -----
        sem = asyncio.Semaphore(max_concurrency)

        async def _process(bid: str, entries: list[tuple[str, dict]]) -> BulkResult:
            async with sem:
                attempt = 0
                while True:
                    try:
                        return await self._bulk_upsert_bucket(bid, entries, patch_first=patch_first)
                    except Exception as exc:
                        if _is_transient(exc) and (max_retries is None or attempt < max_retries):
                            delay = min(_RETRY_BASE_DELAY * (2 ** attempt), _RETRY_MAX_DELAY)
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue
                        return BulkResult(
                            succeeded=0,
                            failed=len(entries),
                            errors=[
                                BulkError(item_id=body.get("id", ""), error=str(exc))
                                for _, body in entries
                            ],
                        )

        bucket_results = await asyncio.gather(
            *(_process(bid, ents) for bid, ents in bucket_groups.items())
        )

        # ----- aggregate -----
        return BulkResult(
            succeeded=sum(r.succeeded for r in bucket_results),
            failed=sum(r.failed for r in bucket_results),
            errors=[e for r in bucket_results for e in r.errors],
        )

    # ------------------------------------------------------------------
    # Internal: bulk-write a single bucket
    # ------------------------------------------------------------------

    async def _bulk_upsert_bucket(
        self,
        bucket_id: str,
        entries: list[tuple[str, dict]],
        *,
        patch_first: bool = False,
    ) -> BulkResult:
        """Write all *entries* into a single bucket document.

        Strategy selection per path:

        * **Wave 1** (``patch_first=False``): ``create_item`` packs all
          entries in 1 call.  On 409, falls back to patch or
          read-merge-upsert depending on batch size.

        * **Waves 2+** (``patch_first=True``):

          - **≤9 entries** — single atomic patch (1 Cosmos call).
          - **≥10 entries** — read + merge + upsert (2 Cosmos calls),
            which is both faster *and* cheaper in RU than
            ``ceil(N/9)`` sequential patches (each patch reads the
            full document server-side).
        """
        root_id = self._root_id(bucket_id)
        cosmos_pk = self._cosmos_pk(bucket_id)
        entries_dict = {eid: body for eid, body in entries}
        use_patch = len(entries) <= _MAX_ENTRIES_PER_PATCH

        if patch_first:
            if use_patch:
                # Small batch — single atomic patch (1 Cosmos call)
                try:
                    return await self._bulk_patch_entries(bucket_id, entries)
                except CosmosResourceNotFoundError:
                    pass  # Bucket doesn't exist — fall through to create
            else:
                # Large batch — read-merge-upsert (2 calls instead of ceil(N/9) sequential patches)
                existing = await self._read_bucket_doc(root_id, cosmos_pk)
                if existing is not None:
                    existing["entries"].update(entries_dict)
                    existing["updatedAt"] = _now_iso()
                    await self._container.upsert_item(existing)
                    return BulkResult(succeeded=len(entries))
                # Bucket doesn't exist — fall through to create

            root_doc = self._new_root_doc(root_id, cosmos_pk, bucket_id)
            root_doc["entries"] = entries_dict
            await self._container.create_item(root_doc)
            return BulkResult(succeeded=len(entries))

        # patch_first=False — try create first (wave 1 / new buckets)
        root_doc = self._new_root_doc(root_id, cosmos_pk, bucket_id)
        root_doc["entries"] = entries_dict

        try:
            await self._container.create_item(root_doc)
            return BulkResult(succeeded=len(entries))
        except CosmosResourceExistsError:
            if use_patch:
                return await self._bulk_patch_entries(bucket_id, entries)
            # Large batch — read, merge, upsert (2 calls)
            existing = await self._read_bucket_doc(root_id, cosmos_pk)
            if existing is not None:
                existing["entries"].update(entries_dict)
                existing["updatedAt"] = _now_iso()
                await self._container.upsert_item(existing)
                return BulkResult(succeeded=len(entries))
            # Race: bucket deleted between 409 and read — create fresh
            await self._container.create_item(root_doc)
            return BulkResult(succeeded=len(entries))

    async def _bulk_patch_entries(
        self,
        bucket_id: str,
        entries: list[tuple[str, dict]],
    ) -> BulkResult:
        """Patch *entries* into an existing bucket in batches of ``_MAX_ENTRIES_PER_PATCH``.

        Raises ``CosmosResourceNotFoundError`` if the bucket document does
        not exist (the caller may fall back to ``create_item``).
        """
        root_id = self._root_id(bucket_id)
        cosmos_pk = self._cosmos_pk(bucket_id)

        succeeded = 0
        failed = 0
        errors: list[BulkError] = []

        for i in range(0, len(entries), _MAX_ENTRIES_PER_PATCH):
            batch = entries[i : i + _MAX_ENTRIES_PER_PATCH]
            ops: list[dict] = [
                {"op": "set", "path": f"/entries/{eid}", "value": body}
                for eid, body in batch
            ]
            ops.append({"op": "set", "path": "/updatedAt", "value": _now_iso()})
            try:
                await self._container.patch_item(
                    item=root_id,
                    partition_key=cosmos_pk,
                    patch_operations=ops,
                )
                succeeded += len(batch)
            except CosmosResourceNotFoundError:
                raise  # bucket doesn't exist — let caller handle
            except Exception as exc:
                failed += len(batch)
                for _, body in batch:
                    errors.append(
                        BulkError(item_id=body.get("id", ""), error=str(exc))
                    )

        return BulkResult(succeeded=succeeded, failed=failed, errors=errors)


# ---------------------------------------------------------------------------
# Minimal client-side WHERE predicate parser
# ---------------------------------------------------------------------------

def _parse_where_predicate(query: str, parameters: list | None = None):
    """Attempt to extract a simple ``c.field = value`` predicate from *query*.

    Returns a callable ``predicate(item) -> bool`` or ``None`` if the query
    cannot be parsed (in which case the caller should return all items).

    Supported patterns::

        SELECT * FROM c WHERE c.field = 'value'
        SELECT * FROM c WHERE c.field = @param  (with *parameters*)
        SELECT * FROM c WHERE c.field = 123
        SELECT * FROM c WHERE c.field > 70

    Multiple ``AND`` clauses are supported.
    """
    import re

    if not query:
        return None

    upper = query.upper()
    where_idx = upper.find("WHERE")
    if where_idx < 0:
        return None

    where_clause = query[where_idx + 5 :].strip()
    if not where_clause:
        return None

    # Split by AND
    parts = re.split(r"\bAND\b", where_clause, flags=re.IGNORECASE)
    predicates = []
    for part in parts:
        pred = _parse_single_predicate(part.strip(), parameters)
        if pred is None:
            # Can't fully parse → fall back to returning everything
            return None
        predicates.append(pred)

    def combined(item: dict) -> bool:
        return all(p(item) for p in predicates)

    return combined


def _parse_single_predicate(expr: str, parameters: list | None = None):
    """Parse a single ``c.field op value`` expression."""
    import re

    m = re.match(
        r"c\.(\w+)\s*(=|!=|<>|>=|<=|>|<)\s*(.+)", expr.strip(), re.IGNORECASE
    )
    if not m:
        return None

    field_name = m.group(1)
    op = m.group(2)
    raw_value = m.group(3).strip()

    value = _resolve_value(raw_value, parameters)

    def predicate(item: dict) -> bool:
        if field_name not in item:
            return False
        item_val = item[field_name]
        try:
            if op == "=":
                return item_val == value
            if op in ("!=", "<>"):
                return item_val != value
            if op == ">":
                return item_val > value
            if op == ">=":
                return item_val >= value
            if op == "<":
                return item_val < value
            if op == "<=":
                return item_val <= value
        except TypeError:
            return False
        return False

    return predicate


def _resolve_value(raw: str, parameters: list | None):
    """Resolve a literal or @-parameter reference."""
    raw = raw.strip().rstrip(";").strip()

    # @param reference
    if raw.startswith("@") and parameters:
        name = raw
        for p in parameters:
            if p.get("name") == name:
                return p["value"]
        return raw  # unresolved param — treat as string

    # String literal
    if (raw.startswith("'") and raw.endswith("'")) or (
        raw.startswith('"') and raw.endswith('"')
    ):
        return raw[1:-1]

    # Numeric
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        pass

    # Boolean / null
    lower = raw.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if lower == "null":
        return None

    return raw
