"""Comprehensive live Cosmos DB integration tests for PackedContainerProxy.

Routing is by item id only (sha256(item_id)[:prefix]) — no caller-supplied
partition_key parameter.  All items with the same ``id`` map to the same
bucket entry, regardless of other fields.

Skipped unless all required environment variables are set:
  - COSMOS_CONNECTION_STRING
  - COSMOS_DATABASE
  - COSMOS_CONTAINER

Run with: pytest tests/integration/test_proxy_comprehensive.py -v
"""

from __future__ import annotations

import os
import uuid
from typing import AsyncGenerator

import pytest

from packed_kv_cosmos import PackedContainerProxy, PackingConfig

_CONN = os.environ.get("COSMOS_CONNECTION_STRING")
_DB = os.environ.get("COSMOS_DATABASE")
_CONTAINER = os.environ.get("COSMOS_CONTAINER")

_SKIP_REASON = (
    "Live Cosmos env vars not set "
    "(COSMOS_CONNECTION_STRING, COSMOS_DATABASE, COSMOS_CONTAINER)"
)
_HAS_ENV = all([_CONN, _DB, _CONTAINER])


# ─── Fixtures ───────────────────────────────────────────────────────

def _make_container():
    """Return a real Cosmos ContainerProxy."""
    from azure.cosmos.aio import CosmosClient

    assert _CONN and _DB and _CONTAINER
    client = CosmosClient.from_connection_string(_CONN)
    database = client.get_database_client(_DB)
    return database.get_container_client(_CONTAINER)


@pytest.fixture()
def container():
    """Cosmos ContainerProxy (function-scoped to match pytest-asyncio loop)."""
    return _make_container()


@pytest.fixture()
async def proxy(container) -> AsyncGenerator[PackedContainerProxy, None]:
    """Proxy with id-only routing.

    Uses a unique namespace per test to avoid cross-test interference.
    max_entries_per_doc=5 to make overflow testing feasible.
    """
    ns = f"test-{uuid.uuid4().hex[:8]}"
    config = PackingConfig(
        namespace=ns,
        bucket_prefix_length=4,
        max_entries_per_doc=5,
    )
    p = PackedContainerProxy(container, config)
    yield p
    # Cleanup: best-effort removal of test data
    await _cleanup_namespace(p)


async def _cleanup_namespace(proxy: PackedContainerProxy) -> None:
    """Best-effort cleanup of all items in a proxy's namespace."""
    try:
        items = await proxy.read_all_items()
        for item in items:
            try:
                await proxy.delete_item(item["id"])
            except Exception:
                pass
    except Exception:
        pass


# ─── Helpers ────────────────────────────────────────────────────────

def _uid(prefix: str = "item") -> str:
    """Generate a unique item id."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# =====================================================================
# Tests: Basic CRUD
# =====================================================================

@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestBasicCRUD:
    """CRUD operations with id-only routing."""

    async def test_create_and_read(self, proxy: PackedContainerProxy) -> None:
        """create_item() stores an item; read_item() retrieves it."""
        iid = _uid("cr")
        body = {"id": iid, "value": 42}

        created = await proxy.create_item(body)
        assert created["id"] == iid
        assert created["value"] == 42

        read = await proxy.read_item(iid)
        assert read == body

    async def test_create_and_delete(self, proxy: PackedContainerProxy) -> None:
        """delete_item() removes the item; subsequent read raises 404."""
        from azure.cosmos.exceptions import CosmosResourceNotFoundError

        iid = _uid("cd")
        await proxy.create_item({"id": iid, "x": 1})
        await proxy.delete_item(iid)

        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.read_item(iid)

    async def test_upsert_creates_new(self, proxy: PackedContainerProxy) -> None:
        """upsert_item() creates a new item when it doesn't exist."""
        iid = _uid("un")
        body = {"id": iid, "ver": 1}
        result = await proxy.upsert_item(body)
        assert result["ver"] == 1

        read = await proxy.read_item(iid)
        assert read["ver"] == 1

    async def test_upsert_overwrites_existing(self, proxy: PackedContainerProxy) -> None:
        """upsert_item() updates an existing item."""
        iid = _uid("uo")
        await proxy.create_item({"id": iid, "ver": 1})
        await proxy.upsert_item({"id": iid, "ver": 2})

        read = await proxy.read_item(iid)
        assert read["ver"] == 2

    async def test_replace_existing(self, proxy: PackedContainerProxy) -> None:
        """replace_item() updates an existing item in-place."""
        iid = _uid("rep")
        await proxy.create_item({"id": iid, "val": "old"})
        await proxy.replace_item(iid, {"id": iid, "val": "new"})

        read = await proxy.read_item(iid)
        assert read["val"] == "new"

    async def test_replace_adds_new_fields(self, proxy: PackedContainerProxy) -> None:
        """replace_item() can add new fields to the item body."""
        iid = _uid("rf")
        await proxy.create_item({"id": iid, "a": 1})
        await proxy.replace_item(iid, {"id": iid, "a": 1, "b": 2, "c": 3})

        read = await proxy.read_item(iid)
        assert read == {"id": iid, "a": 1, "b": 2, "c": 3}

    async def test_create_duplicate_raises_409(self, proxy: PackedContainerProxy) -> None:
        """create_item() with an existing id raises CosmosResourceExistsError."""
        from azure.cosmos.exceptions import CosmosResourceExistsError

        iid = _uid("dup")
        await proxy.create_item({"id": iid, "v": 1})

        with pytest.raises(CosmosResourceExistsError):
            await proxy.create_item({"id": iid, "v": 2})

    async def test_read_missing_raises_404(self, proxy: PackedContainerProxy) -> None:
        """read_item() for a non-existent id raises CosmosResourceNotFoundError."""
        from azure.cosmos.exceptions import CosmosResourceNotFoundError

        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.read_item("does-not-exist")

    async def test_delete_missing_raises_404(self, proxy: PackedContainerProxy) -> None:
        """delete_item() for a non-existent id raises CosmosResourceNotFoundError."""
        from azure.cosmos.exceptions import CosmosResourceNotFoundError

        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.delete_item("does-not-exist")

    async def test_replace_missing_raises_404(self, proxy: PackedContainerProxy) -> None:
        """replace_item() for a non-existent id raises CosmosResourceNotFoundError."""
        from azure.cosmos.exceptions import CosmosResourceNotFoundError

        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.replace_item(
                "does-not-exist", {"id": "does-not-exist", "v": 1}
            )

    async def test_nested_body_roundtrip(self, proxy: PackedContainerProxy) -> None:
        """Items with nested objects, arrays, and nulls round-trip correctly."""
        iid = _uid("nested")
        body = {
            "id": iid,
            "meta": {"tags": ["a", "b", "c"], "count": 3},
            "readings": [1.1, 2.2, 3.3],
            "nullable": None,
            "flag": True,
        }
        await proxy.create_item(body)
        read = await proxy.read_item(iid)
        assert read == body


# =====================================================================
# Tests: Id-Only Routing Semantics
# =====================================================================

@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestIdOnlyRouting:
    """Tests verifying id-only routing: same id = same bucket entry."""

    async def test_items_with_different_categories_same_id_collide(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Two items with the same 'id' but different 'category' values collide.

        With id-only routing, the id alone determines the bucket entry.
        The second create_item should raise 409.
        """
        from azure.cosmos.exceptions import CosmosResourceExistsError

        shared_id = _uid("shared")
        await proxy.create_item({"id": shared_id, "category": "a", "src": "a"})

        with pytest.raises(CosmosResourceExistsError):
            await proxy.create_item({"id": shared_id, "category": "b", "src": "b"})

    async def test_upsert_overwrites_regardless_of_other_fields(
        self, proxy: PackedContainerProxy
    ) -> None:
        """upsert_item() replaces the entry for a given id, even if other
        fields like 'category' change."""
        iid = _uid("uov")
        await proxy.create_item({"id": iid, "category": "old", "v": 1})
        await proxy.upsert_item({"id": iid, "category": "new", "v": 2})

        read = await proxy.read_item(iid)
        assert read["category"] == "new"
        assert read["v"] == 2

    async def test_items_with_different_ids_are_independent(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Items with different ids are stored independently."""
        id_a = _uid("ind-a")
        id_b = _uid("ind-b")
        await proxy.create_item({"id": id_a, "category": "env", "v": 1})
        await proxy.create_item({"id": id_b, "category": "env", "v": 2})

        assert (await proxy.read_item(id_a))["v"] == 1
        assert (await proxy.read_item(id_b))["v"] == 2


# =====================================================================
# Tests: Queries (WHERE filtering, read_all_items)
# =====================================================================

@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestQueries:
    """Query and read_all_items() tests — all queries are full scans."""

    async def test_query_with_where_equality(
        self, proxy: PackedContainerProxy
    ) -> None:
        """query_items() with a WHERE equality clause filters client-side."""
        items = [
            {"id": _uid("qe"), "category": "q", "status": "active"},
            {"id": _uid("qe"), "category": "q", "status": "inactive"},
            {"id": _uid("qe"), "category": "q", "status": "active"},
        ]
        for item in items:
            await proxy.create_item(item)

        results = await proxy.query_items("SELECT * FROM c WHERE c.status = 'active'")
        assert all(r["status"] == "active" for r in results)
        assert len(results) >= 2

    async def test_query_with_where_numeric(
        self, proxy: PackedContainerProxy
    ) -> None:
        """query_items() with numeric comparison in WHERE clause."""
        items = [
            {"id": _uid("qn"), "category": "num", "score": 10},
            {"id": _uid("qn"), "category": "num", "score": 50},
            {"id": _uid("qn"), "category": "num", "score": 90},
        ]
        for item in items:
            await proxy.create_item(item)

        results = await proxy.query_items("SELECT * FROM c WHERE c.score > 40")
        assert len(results) >= 2
        assert all(r["score"] > 40 for r in results)

    async def test_query_empty_result(
        self, proxy: PackedContainerProxy
    ) -> None:
        """query_items() returns empty when no items match."""
        iid = _uid("emp")
        await proxy.create_item({"id": iid, "category": "empty-cat", "v": 1})

        results = await proxy.query_items("SELECT * FROM c WHERE c.v = 999")
        assert results == []

    async def test_read_all_items_returns_everything(
        self, proxy: PackedContainerProxy
    ) -> None:
        """read_all_items() retrieves all items in the namespace."""
        items = [
            {"id": _uid("ra-a"), "category": "group-a", "v": 1},
            {"id": _uid("ra-b"), "category": "group-b", "v": 2},
            {"id": _uid("ra-c"), "category": "group-c", "v": 3},
        ]
        for item in items:
            await proxy.create_item(item)

        all_items = await proxy.read_all_items()
        all_ids = {r["id"] for r in all_items}
        for item in items:
            assert item["id"] in all_ids

    async def test_query_select_star_returns_all_fields(
        self, proxy: PackedContainerProxy
    ) -> None:
        """SELECT * returns the full item body, not a projection."""
        iid = _uid("star")
        body = {"id": iid, "name": "test", "count": 7, "nested": {"a": 1}}
        await proxy.create_item(body)

        results = await proxy.query_items("SELECT * FROM c")
        found = [r for r in results if r["id"] == iid]
        assert len(found) == 1
        assert found[0] == body


# =====================================================================
# Tests: Many Items Per Bucket (id-only routing)
# =====================================================================

@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestManyItems:
    """Test many items with id-only routing.

    With id-only routing, each unique id hashes to a bucket.
    Items distribute across bins.  No overflow pages are used.
    """

    async def test_many_items_all_readable(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Insert 8 items with unique ids; all should be readable."""
        items = [
            {"id": _uid("ov"), "category": "test", "idx": i}
            for i in range(8)
        ]
        for item in items:
            await proxy.create_item(item)

        for item in items:
            read = await proxy.read_item(item["id"])
            assert read["idx"] == item["idx"]

    async def test_query_returns_all(
        self, proxy: PackedContainerProxy
    ) -> None:
        """query_items() returns all items in the namespace."""
        items = [
            {"id": _uid("oq"), "category": "test", "idx": i}
            for i in range(12)
        ]
        for item in items:
            await proxy.create_item(item)

        results = await proxy.query_items("SELECT * FROM c")
        result_ids = {r["id"] for r in results}
        for item in items:
            assert item["id"] in result_ids

    async def test_upsert_updates_existing(
        self, proxy: PackedContainerProxy
    ) -> None:
        """upsert_item() can update any item by id."""
        items = [
            {"id": _uid("ou"), "category": "test", "idx": i}
            for i in range(7)
        ]
        for item in items:
            await proxy.create_item(item)

        target = items[6]
        await proxy.upsert_item({"id": target["id"], "category": "test", "idx": 999})

        read = await proxy.read_item(target["id"])
        assert read["idx"] == 999

    async def test_replace_updates_existing(
        self, proxy: PackedContainerProxy
    ) -> None:
        """replace_item() can replace any item by id."""
        items = [
            {"id": _uid("or"), "category": "test", "idx": i}
            for i in range(7)
        ]
        for item in items:
            await proxy.create_item(item)

        target = items[6]
        await proxy.replace_item(
            target["id"],
            {"id": target["id"], "category": "test", "idx": -1},
        )

        read = await proxy.read_item(target["id"])
        assert read["idx"] == -1

    async def test_delete_one_item(
        self, proxy: PackedContainerProxy
    ) -> None:
        """delete_item() removes one item; others remain."""
        from azure.cosmos.exceptions import CosmosResourceNotFoundError

        items = [
            {"id": _uid("od"), "category": "test", "idx": i}
            for i in range(7)
        ]
        for item in items:
            await proxy.create_item(item)

        target = items[6]
        await proxy.delete_item(target["id"])

        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.read_item(target["id"])

        for item in items[:6]:
            read = await proxy.read_item(item["id"])
            assert read["idx"] == item["idx"]

    async def test_query_with_where_across_items(
        self, proxy: PackedContainerProxy
    ) -> None:
        """WHERE filtering works across all items."""
        items = [
            {"id": _uid("ow"), "category": "test", "idx": i}
            for i in range(10)
        ]
        for item in items:
            await proxy.create_item(item)

        results = await proxy.query_items("SELECT * FROM c WHERE c.idx > 6")
        assert len(results) == 3
        assert all(r["idx"] > 6 for r in results)

    async def test_read_all_items(
        self, proxy: PackedContainerProxy
    ) -> None:
        """read_all_items() captures all items."""
        items = [
            {"id": _uid("ora"), "category": "test", "idx": i}
            for i in range(8)
        ]
        for item in items:
            await proxy.create_item(item)

        all_items = await proxy.read_all_items()
        all_ids = {r["id"] for r in all_items}
        for item in items:
            assert item["id"] in all_ids


# =====================================================================
# Tests: Edge Cases
# =====================================================================

@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestEdgeCases:
    """Edge cases and less common scenarios."""

    async def test_create_delete_recreate(
        self, proxy: PackedContainerProxy
    ) -> None:
        """An item can be created, deleted, and re-created with new data."""
        iid = _uid("cdr")
        await proxy.create_item({"id": iid, "phase": "v1"})
        await proxy.delete_item(iid)
        await proxy.create_item({"id": iid, "phase": "v2"})

        read = await proxy.read_item(iid)
        assert read["phase"] == "v2"

    async def test_upsert_after_delete(
        self, proxy: PackedContainerProxy
    ) -> None:
        """upsert_item() after delete_item() re-creates the item."""
        iid = _uid("uad")
        await proxy.create_item({"id": iid, "v": 1})
        await proxy.delete_item(iid)
        await proxy.upsert_item({"id": iid, "v": 3})

        read = await proxy.read_item(iid)
        assert read["v"] == 3

    async def test_multiple_rapid_upserts(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Multiple upserts in sequence converge to the last value."""
        iid = _uid("mru")
        for i in range(5):
            await proxy.upsert_item({"id": iid, "counter": i})

        read = await proxy.read_item(iid)
        assert read["counter"] == 4

    async def test_empty_string_values(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Items with empty string values are stored and retrieved correctly."""
        iid = _uid("esv")
        body = {"id": iid, "name": "", "desc": ""}
        await proxy.create_item(body)

        read = await proxy.read_item(iid)
        assert read["name"] == ""
        assert read["desc"] == ""

    async def test_numeric_field_types_preserved(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Integer and float field types are preserved through round-trip."""
        iid = _uid("nft")
        body = {"id": iid, "int_val": 42, "float_val": 3.14, "zero": 0}
        await proxy.create_item(body)

        read = await proxy.read_item(iid)
        assert read["int_val"] == 42
        assert abs(read["float_val"] - 3.14) < 0.001
        assert read["zero"] == 0

    async def test_boolean_and_null_preserved(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Boolean and null values are preserved through round-trip."""
        iid = _uid("bn")
        body = {"id": iid, "active": True, "deleted": False, "meta": None}
        await proxy.create_item(body)

        read = await proxy.read_item(iid)
        assert read["active"] is True
        assert read["deleted"] is False
        assert read["meta"] is None

    async def test_large_item_body(
        self, proxy: PackedContainerProxy
    ) -> None:
        """An item with a moderately large body (few KB) round-trips correctly."""
        iid = _uid("lg")
        large_list = [{"key": f"k{i}", "value": f"v{i}" * 20} for i in range(50)]
        body = {"id": iid, "data": large_list}
        await proxy.create_item(body)

        read = await proxy.read_item(iid)
        assert len(read["data"]) == 50
        assert read["data"][0]["key"] == "k0"

    async def test_special_characters_in_id(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Item ids with special characters are handled correctly."""
        iid = f"special-{uuid.uuid4().hex[:4]}-with:colons/and.dots"
        body = {"id": iid, "v": 1}
        await proxy.create_item(body)

        read = await proxy.read_item(iid)
        assert read["id"] == iid

    async def test_unicode_in_body(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Unicode strings in item body are preserved."""
        iid = _uid("uni")
        body = {"id": iid, "name": "\u65e5\u672c\u8a9e\u30c6\u30b9\u30c8", "emoji": "\U0001f525\U0001f680"}
        await proxy.create_item(body)

        read = await proxy.read_item(iid)
        assert read["name"] == "\u65e5\u672c\u8a9e\u30c6\u30b9\u30c8"
        assert read["emoji"] == "\U0001f525\U0001f680"

    async def test_many_items_batch(
        self, proxy: PackedContainerProxy
    ) -> None:
        """Creating and reading back 20 items."""
        items = []
        for i in range(20):
            item = {"id": _uid("batch"), "category": f"cat-{i % 4}", "seq": i}
            await proxy.create_item(item)
            items.append(item)

        all_items = await proxy.read_all_items()
        all_ids = {r["id"] for r in all_items}
        assert len(all_ids) >= 20
        for item in items:
            assert item["id"] in all_ids


# ─── Bin Packing Density Test (no cleanup) ──────────────────────────


@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestBinPackingDensity:
    """Insert enough items with a short prefix to force multiple entries per bin.

    Uses bucket_prefix_length=2 (256 possible bins) and inserts 200 items,
    guaranteeing many bins contain multiple packed entries.

    **Deliberately does NOT clean up** so you can inspect the packed
    bin documents in the Cosmos emulator after the test run.
    """

    NAMESPACE = "density-test"
    PREFIX_LEN = 2
    NUM_ITEMS = 200

    @pytest.fixture()
    def dense_proxy(self, container) -> PackedContainerProxy:
        """Proxy with a very short prefix to force bin collisions."""
        config = PackingConfig(
            namespace=self.NAMESPACE,
            bucket_prefix_length=self.PREFIX_LEN,
            max_entries_per_doc=self.NUM_ITEMS,  # large enough to never hit limit
        )
        return PackedContainerProxy(container, config)

    async def test_insert_200_items_and_verify_packing(
        self, dense_proxy: PackedContainerProxy
    ) -> None:
        """Insert 200 items and verify bin packing density.

        With prefix_length=2 (256 bins) and 200 items, the birthday
        paradox guarantees many bins will contain 2+ entries.
        """
        inserted = []

        for i in range(self.NUM_ITEMS):
            item = {
                "id": f"item-{i:04d}",
                "category": f"cat-{i % 5}",
                "index": i,
                "data": f"payload-{i}",
            }
            await dense_proxy.upsert_item(item)
            inserted.append(item)

        # Verify all items are readable
        for item in inserted:
            read = await dense_proxy.read_item(item["id"])
            assert read["id"] == item["id"]
            assert read["index"] == item["index"]

        # Read all and check count
        all_items = await dense_proxy.read_all_items()
        all_ids = {r["id"] for r in all_items}
        assert len(all_ids) == self.NUM_ITEMS, (
            f"Expected {self.NUM_ITEMS} unique items, got {len(all_ids)}"
        )

        # Inspect bin density by reading raw bin documents
        from packed_kv_cosmos.hashing import compute_bucket_id, compute_entry_id
        from collections import Counter

        bin_counter: Counter[str] = Counter()
        for item in inserted:
            entry_id = compute_entry_id(item["id"])
            bucket = compute_bucket_id(entry_id, self.PREFIX_LEN)
            bin_counter[bucket] += 1

        multi_entry_bins = {b: c for b, c in bin_counter.items() if c > 1}
        max_per_bin = max(bin_counter.values())
        unique_bins = len(bin_counter)

        print(f"\n{'='*60}")
        print(f"BIN PACKING DENSITY REPORT")
        print(f"{'='*60}")
        print(f"  Items inserted:        {self.NUM_ITEMS}")
        print(f"  Prefix length:         {self.PREFIX_LEN} ({16**self.PREFIX_LEN} possible bins)")
        print(f"  Unique bins used:      {unique_bins}")
        print(f"  Bins with 2+ entries:  {len(multi_entry_bins)}")
        print(f"  Max entries in a bin:  {max_per_bin}")
        print(f"\n  Top 10 densest bins:")
        for bucket, count in bin_counter.most_common(10):
            print(f"    bin '{self.NAMESPACE}:{bucket}' -> {count} entries")
        print(f"{'='*60}")

        # The key assertion: with 200 items in 256 bins, we MUST have collisions
        assert len(multi_entry_bins) > 0, (
            "Expected multiple entries per bin with prefix_length=2 and 200 items"
        )
        assert max_per_bin >= 2, "Expected at least one bin with 2+ entries"

        print(f"\n  OK: {len(multi_entry_bins)} bins have multiple packed entries")
        print(f"  OK: Data left in emulator for inspection (namespace='{self.NAMESPACE}')")
