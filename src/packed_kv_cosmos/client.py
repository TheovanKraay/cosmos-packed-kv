# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""Public client façade – Put / Get / Delete on packed KV buckets.

This module is the primary entry-point for callers.  It orchestrates
hashing, bucket resolution, packing, and single-document bin logic.

Writes use **Cosmos DB partial document update (patch)** to add, update,
or remove entries inside bucket documents without reading the full
document first.  This eliminates the read-before-write round-trip and
provides server-side conflict resolution — two concurrent patches to
different entries in the same bucket are serialised automatically with
no 412 retries required.

Every read is guaranteed to be a single Cosmos point read (one document
lookup).  There are no overflow pages — each bucket is a single document.

All public methods are **async** — the library uses the ``azure.cosmos.aio``
async SDK under the hood.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone

from packed_kv_cosmos.config import PackedKvConfig
from packed_kv_cosmos.hashing import compute_bucket_id, compute_entry_id
from packed_kv_cosmos.model import BucketRootDocument
from packed_kv_cosmos.packing import (
    decode_value,
    encode_value,
    make_partition_key,
    make_root_document_id,
)
from packed_kv_cosmos.results import (
    ERR_CONFLICT_RETRY_EXHAUSTED,
    ERR_INVALID_INPUT,
    ERR_TOO_LARGE,
    STATUS_CONFLICT_RETRY_EXHAUSTED,
    STATUS_INVALID_INPUT,
    STATUS_NOT_FOUND,
    STATUS_SUCCESS,
    STATUS_TOO_LARGE,
    BulkError,
    BulkResult,
    Diagnostics,
    OperationResult,
)
from packed_kv_cosmos.storage import (
    CosmosStorage,
    ETagMismatchError,
    ItemAlreadyExistsError,
    PatchOperationFailedError,
    PatchPreconditionFailedError,
    PatchTargetNotFoundError,
    StorageInterface,
)


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
    """Return True if *exc* is a transient error worth retrying."""
    # Storage layer wraps Cosmos errors; check for status_code attribute
    status = getattr(exc, "status_code", None)
    if status in (429, 449, 500, 503):
        return True
    # Connection-level errors (reset, timeout, etc.)
    return isinstance(exc, (ConnectionError, TimeoutError, OSError))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# PackedKvClient
# ---------------------------------------------------------------------------

class PackedKvClient:
    """High-level packed key-value client for Cosmos DB.

    Use :meth:`from_connection_string` for the convenience constructor,
    or inject a :class:`StorageInterface` directly for testing.

    Writes use Cosmos partial document update (patch) to modify entries
    in-place without a read-before-write cycle.  Every Get is a single
    Cosmos point read.  Buckets are single documents with no overflow
    pages.

    All public methods are **async** coroutines.
    """

    def __init__(self, storage: StorageInterface, config: PackedKvConfig) -> None:
        self._storage = storage
        self._config = config

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_connection_string(
        cls,
        connection_string: str,
        database: str,
        container: str,
        config: PackedKvConfig,
    ) -> "PackedKvClient":
        """Convenience constructor from a Cosmos connection string."""
        storage = CosmosStorage.from_connection_string(connection_string, database, container)
        return cls(storage=storage, config=config)

    @classmethod
    def from_credential(
        cls,
        endpoint: str,
        credential: object,
        database: str,
        container: str,
        config: PackedKvConfig,
    ) -> "PackedKvClient":
        """Convenience constructor using a token credential (Entra ID).

        *credential* can be any ``TokenCredential``, e.g.
        ``DefaultAzureCredential`` from the ``azure-identity`` package.
        """
        storage = CosmosStorage.from_credential(endpoint, credential, database, container)
        return cls(storage=storage, config=config)

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------

    async def put(self, key: str, value: bytes) -> OperationResult:
        """Store *value* under *key*.  Creates or updates the entry.

        Uses a single Cosmos patch operation when the bucket already
        exists, falling back to ``create_item`` for new buckets.
        """
        if not key:
            return OperationResult(
                status=STATUS_INVALID_INPUT,
                error_code=ERR_INVALID_INPUT,
                message="key must be a non-empty string",
            )
        if not isinstance(value, (bytes, bytearray)):
            return OperationResult(
                status=STATUS_INVALID_INPUT,
                error_code=ERR_INVALID_INPUT,
                message="value must be bytes",
            )

        entry_id = compute_entry_id(key)
        bucket_id = compute_bucket_id(entry_id, self._config.bucket_prefix_length)
        encoded = encode_value(value)
        pk = make_partition_key(self._config.namespace, bucket_id)
        root_id = make_root_document_id(self._config.namespace, bucket_id)

        diag = Diagnostics()
        ops = [
            {"op": "set", "path": f"/entries/{entry_id}", "value": encoded},
            {"op": "set", "path": "/updatedAt", "value": _now_iso()},
        ]

        try:
            await self._storage.patch_item(root_id, pk, ops)
            diag.documents_written += 1
            return OperationResult(status=STATUS_SUCCESS, diagnostics=diag)
        except PatchTargetNotFoundError:
            # Bucket doesn't exist yet — create it with the entry
            return await self._create_bucket_and_put(
                root_id, pk, bucket_id, entry_id, encoded, diag, ops,
            )

    async def get(self, key: str) -> OperationResult:
        """Retrieve the value associated with *key*.

        This always executes a single Cosmos point read.
        """
        if not key:
            return OperationResult(
                status=STATUS_INVALID_INPUT,
                error_code=ERR_INVALID_INPUT,
                message="key must be a non-empty string",
            )

        entry_id = compute_entry_id(key)
        bucket_id = compute_bucket_id(entry_id, self._config.bucket_prefix_length)
        pk = make_partition_key(self._config.namespace, bucket_id)
        root_id = make_root_document_id(self._config.namespace, bucket_id)

        diag = Diagnostics()

        raw = await self._storage.read_item(root_id, pk)
        diag.documents_read += 1
        if raw is None:
            return OperationResult(status=STATUS_NOT_FOUND, diagnostics=diag)

        root = BucketRootDocument.from_dict(raw)

        if entry_id in root.entries:
            return OperationResult(
                status=STATUS_SUCCESS,
                value=decode_value(root.entries[entry_id]),
                diagnostics=diag,
            )

        return OperationResult(status=STATUS_NOT_FOUND, diagnostics=diag)

    async def delete(self, key: str) -> OperationResult:
        """Remove the entry for *key* if present.

        Uses a single Cosmos patch ``remove`` operation — no read needed.
        """
        if not key:
            return OperationResult(
                status=STATUS_INVALID_INPUT,
                error_code=ERR_INVALID_INPUT,
                message="key must be a non-empty string",
            )

        entry_id = compute_entry_id(key)
        bucket_id = compute_bucket_id(entry_id, self._config.bucket_prefix_length)
        pk = make_partition_key(self._config.namespace, bucket_id)
        root_id = make_root_document_id(self._config.namespace, bucket_id)

        diag = Diagnostics()
        ops = [
            {"op": "remove", "path": f"/entries/{entry_id}"},
            {"op": "set", "path": "/updatedAt", "value": _now_iso()},
        ]

        try:
            await self._storage.patch_item(root_id, pk, ops)
            diag.documents_written += 1
            return OperationResult(status=STATUS_SUCCESS, diagnostics=diag)
        except (PatchTargetNotFoundError, PatchOperationFailedError):
            # Document doesn't exist or entry path doesn't exist → not found
            return OperationResult(status=STATUS_NOT_FOUND, diagnostics=diag)

    # ------------------------------------------------------------------
    # Internal: create new bucket with first entry
    # ------------------------------------------------------------------

    async def _create_bucket_and_put(
        self,
        root_id: str,
        pk: str,
        bucket_id: str,
        entry_id: str,
        encoded: str,
        diag: Diagnostics,
        patch_ops: list,
    ) -> OperationResult:
        """Create a new bucket document containing the entry.

        Handles the race condition where another thread creates the same
        bucket concurrently:  create → 409 → retry with patch.
        """
        root = BucketRootDocument(
            id=root_id,
            pk=pk,
            schema_version=self._config.schema_version,
            bucket_id=bucket_id,
            entries={entry_id: encoded},
            tail_page=None,
            page_count=0,
        )
        try:
            await self._storage.create_item(root.to_dict(), pk)
            diag.documents_written += 1
            return OperationResult(status=STATUS_SUCCESS, diagnostics=diag)
        except ItemAlreadyExistsError:
            # Another thread created the bucket first — add entry via patch
            await self._storage.patch_item(root_id, pk, patch_ops)
            diag.documents_written += 1
            return OperationResult(status=STATUS_SUCCESS, diagnostics=diag)

    # ==================================================================
    # Public: bulk operations
    # ==================================================================

    async def bulk_put(
        self,
        entries: list[tuple[str, bytes]],
        *,
        max_concurrency: int = 50,
        patch_first: bool = False,
        max_retries: int | None = None,
    ) -> BulkResult:
        """Store many key-value pairs in bulk with cross-bucket parallelism.

        Entries are grouped by their deterministic bucket id, then written
        concurrently.  New buckets are created in a single ``create_item``
        call containing all their entries.  Existing buckets are updated
        via batched patch operations (up to 9 entries per patch call).

        Parameters
        ----------
        entries:
            List of ``(key, value_bytes)`` tuples.
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
        if not entries:
            return BulkResult()

        # ----- validate & group by bucket -----
        bucket_groups: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        input_errors: list[BulkError] = []

        for key, value in entries:
            if not key:
                input_errors.append(
                    BulkError(item_id="", error="key must be a non-empty string")
                )
                continue
            if not isinstance(value, (bytes, bytearray)):
                input_errors.append(
                    BulkError(item_id=key, error="value must be bytes")
                )
                continue
            entry_id = compute_entry_id(key)
            bucket_id = compute_bucket_id(entry_id, self._config.bucket_prefix_length)
            encoded = encode_value(value)
            bucket_groups[bucket_id].append((entry_id, encoded, key))

        # ----- process each bucket concurrently -----
        sem = asyncio.Semaphore(max_concurrency)

        async def _process(
            bid: str, kv_entries: list[tuple[str, str, str]]
        ) -> BulkResult:
            async with sem:
                attempt = 0
                while True:
                    try:
                        return await self._bulk_put_bucket(bid, kv_entries, patch_first=patch_first)
                    except Exception as exc:
                        if _is_transient(exc) and (max_retries is None or attempt < max_retries):
                            delay = min(_RETRY_BASE_DELAY * (2 ** attempt), _RETRY_MAX_DELAY)
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue
                        return BulkResult(
                            succeeded=0,
                            failed=len(kv_entries),
                            errors=[
                                BulkError(item_id=k, error=str(exc))
                                for _, _, k in kv_entries
                            ],
                        )

        bucket_results = list(
            await asyncio.gather(
                *(_process(bid, ents) for bid, ents in bucket_groups.items())
            )
        )

        # ----- aggregate -----
        return BulkResult(
            succeeded=sum(r.succeeded for r in bucket_results),
            failed=sum(r.failed for r in bucket_results) + len(input_errors),
            errors=input_errors + [e for r in bucket_results for e in r.errors],
        )

    # ------------------------------------------------------------------
    # Internal: bulk-write a single bucket (KV)
    # ------------------------------------------------------------------

    async def _bulk_put_bucket(
        self,
        bucket_id: str,
        kv_entries: list[tuple[str, str, str]],
        *,
        patch_first: bool = False,
    ) -> BulkResult:
        """Write all *kv_entries* into one bucket document.

        Strategy selection per path:

        * **Wave 1** (``patch_first=False``): ``create_item`` packs all
          entries in 1 call.  On 409, falls back to patch or
          read-merge-upsert depending on batch size.

        * **Waves 2+** (``patch_first=True``):

          - **≤9 entries** — single atomic patch (1 Cosmos call).
          - **≥10 entries** — read + merge + upsert (2 Cosmos calls),
            which is both faster *and* cheaper in RU than
            ``ceil(N/9)`` sequential patches.
        """
        pk = make_partition_key(self._config.namespace, bucket_id)
        root_id = make_root_document_id(self._config.namespace, bucket_id)
        entries_dict = {eid: enc for eid, enc, _ in kv_entries}
        use_patch = len(kv_entries) <= _MAX_ENTRIES_PER_PATCH

        if patch_first:
            if use_patch:
                # Small batch — single atomic patch (1 Cosmos call)
                try:
                    return await self._bulk_patch_kv_entries(bucket_id, kv_entries)
                except PatchTargetNotFoundError:
                    pass  # Bucket doesn't exist — fall through to create
            else:
                # Large batch — read-merge-upsert (2 calls instead of ceil(N/9) sequential patches)
                existing = await self._storage.read_item(root_id, pk)
                if existing is not None:
                    existing["entries"].update(entries_dict)
                    existing["updatedAt"] = _now_iso()
                    await self._storage.upsert_item(existing, pk)
                    return BulkResult(succeeded=len(kv_entries))
                # Bucket doesn't exist — fall through to create

            root = BucketRootDocument(
                id=root_id,
                pk=pk,
                schema_version=self._config.schema_version,
                bucket_id=bucket_id,
                entries=entries_dict,
                tail_page=None,
                page_count=0,
            )
            await self._storage.create_item(root.to_dict(), pk)
            return BulkResult(succeeded=len(kv_entries))

        # patch_first=False — try create first (wave 1 / new buckets)
        root = BucketRootDocument(
            id=root_id,
            pk=pk,
            schema_version=self._config.schema_version,
            bucket_id=bucket_id,
            entries=entries_dict,
            tail_page=None,
            page_count=0,
        )

        try:
            await self._storage.create_item(root.to_dict(), pk)
            return BulkResult(succeeded=len(kv_entries))
        except ItemAlreadyExistsError:
            if use_patch:
                return await self._bulk_patch_kv_entries(bucket_id, kv_entries)
            # Large batch — read, merge, upsert (2 calls)
            existing = await self._storage.read_item(root_id, pk)
            if existing is not None:
                existing["entries"].update(entries_dict)
                existing["updatedAt"] = _now_iso()
                await self._storage.upsert_item(existing, pk)
                return BulkResult(succeeded=len(kv_entries))
            # Race: bucket deleted between 409 and read — create fresh
            await self._storage.create_item(root.to_dict(), pk)
            return BulkResult(succeeded=len(kv_entries))

    async def _bulk_patch_kv_entries(
        self,
        bucket_id: str,
        kv_entries: list[tuple[str, str, str]],
    ) -> BulkResult:
        """Patch *kv_entries* into an existing bucket in batches.

        Raises ``PatchTargetNotFoundError`` if the bucket document does
        not exist (the caller may fall back to ``create_item``).
        """
        pk = make_partition_key(self._config.namespace, bucket_id)
        root_id = make_root_document_id(self._config.namespace, bucket_id)

        succeeded = 0
        failed = 0
        errors: list[BulkError] = []

        for i in range(0, len(kv_entries), _MAX_ENTRIES_PER_PATCH):
            batch = kv_entries[i : i + _MAX_ENTRIES_PER_PATCH]
            ops: list[dict] = [
                {"op": "set", "path": f"/entries/{eid}", "value": enc}
                for eid, enc, _ in batch
            ]
            ops.append({"op": "set", "path": "/updatedAt", "value": _now_iso()})
            try:
                await self._storage.patch_item(root_id, pk, ops)
                succeeded += len(batch)
            except PatchTargetNotFoundError:
                raise  # bucket doesn't exist — let caller handle
            except Exception as exc:
                failed += len(batch)
                for _, _, key in batch:
                    errors.append(BulkError(item_id=key, error=str(exc)))

        return BulkResult(succeeded=succeeded, failed=failed, errors=errors)
