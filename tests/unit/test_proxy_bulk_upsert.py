"""Unit tests for PackedContainerProxy.bulk_upsert_items.

All tests use FakeContainerProxy — no live Cosmos needed.
"""

import pytest
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from packed_kv_cosmos.config import PackingConfig
from packed_kv_cosmos.packed_container import PackedContainerProxy
from packed_kv_cosmos.results import BulkResult
from tests.unit.fakes.fake_container_proxy import FakeContainerProxy


def _make_proxy(**cfg_overrides) -> PackedContainerProxy:
    defaults = dict(
        namespace="test",
        bucket_prefix_length=4,
        max_entries_per_doc=200,
    )
    defaults.update(cfg_overrides)
    config = PackingConfig(**defaults)
    return PackedContainerProxy(FakeContainerProxy(), config)


# ------------------------------------------------------------------
# Happy path
# ------------------------------------------------------------------


class TestBulkUpsertBasic:
    """Basic bulk upsert operations."""

    async def test_empty_list(self) -> None:
        proxy = _make_proxy()
        result = await proxy.bulk_upsert_items([])
        assert isinstance(result, BulkResult)
        assert result.succeeded == 0
        assert result.failed == 0
        assert result.errors == []

    async def test_single_item(self) -> None:
        proxy = _make_proxy()
        result = await proxy.bulk_upsert_items([{"id": "a", "val": 1}])
        assert result.succeeded == 1
        assert result.failed == 0

        item = await proxy.read_item("a")
        assert item["id"] == "a"
        assert item["val"] == 1

    async def test_multiple_items(self) -> None:
        proxy = _make_proxy()
        items = [{"id": f"k{i}", "val": i} for i in range(20)]
        result = await proxy.bulk_upsert_items(items)
        assert result.succeeded == 20
        assert result.failed == 0

        # Verify all items are readable
        for i in range(20):
            item = await proxy.read_item(f"k{i}")
            assert item["val"] == i

    async def test_large_batch(self) -> None:
        """100 items, some sharing buckets (prefix_length=2 → 256 possible buckets)."""
        proxy = _make_proxy(bucket_prefix_length=2)
        items = [{"id": f"item-{i:04d}", "data": f"payload-{i}"} for i in range(100)]
        result = await proxy.bulk_upsert_items(items)
        assert result.succeeded == 100
        assert result.failed == 0

        # Spot check a few
        item = await proxy.read_item("item-0000")
        assert item["data"] == "payload-0"
        item = await proxy.read_item("item-0099")
        assert item["data"] == "payload-99"


class TestBulkUpsertSemanticsAreUpsert:
    """bulk_upsert_items uses upsert semantics (create-or-replace)."""

    async def test_upsert_overwrites_existing(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "x", "ver": 1})

        result = await proxy.bulk_upsert_items([{"id": "x", "ver": 2}])
        assert result.succeeded == 1

        item = await proxy.read_item("x")
        assert item["ver"] == 2

    async def test_upsert_creates_new_and_overwrites_existing(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "existing", "v": "old"})

        result = await proxy.bulk_upsert_items([
            {"id": "existing", "v": "updated"},
            {"id": "brand-new", "v": "fresh"},
        ])
        assert result.succeeded == 2
        assert result.failed == 0

        assert (await proxy.read_item("existing"))["v"] == "updated"
        assert (await proxy.read_item("brand-new"))["v"] == "fresh"


class TestBulkUpsertWithExistingBuckets:
    """Verify bulk handles the create→409→patch fallback for existing buckets."""

    async def test_adds_to_existing_bucket(self) -> None:
        proxy = _make_proxy(bucket_prefix_length=1)
        # Pre-populate some items so buckets exist
        for i in range(5):
            await proxy.create_item({"id": f"pre-{i}", "v": True})

        # Bulk upsert more items — some will land in existing buckets
        new_items = [{"id": f"post-{i}", "v": False} for i in range(10)]
        result = await proxy.bulk_upsert_items(new_items)
        assert result.succeeded == 10
        assert result.failed == 0

        # Verify pre-existing items are still there
        for i in range(5):
            item = await proxy.read_item(f"pre-{i}")
            assert item["v"] is True

        # Verify new items
        for i in range(10):
            item = await proxy.read_item(f"post-{i}")
            assert item["v"] is False


class TestBulkUpsertDuplicateIds:
    """When the same id appears multiple times, later entry wins (dict overwrite)."""

    async def test_duplicate_ids_last_wins(self) -> None:
        proxy = _make_proxy()
        items = [
            {"id": "dup", "version": 1},
            {"id": "dup", "version": 2},
            {"id": "dup", "version": 3},
        ]
        result = await proxy.bulk_upsert_items(items)
        # All 3 "logical items" are counted as succeeded even though
        # 2 are overwritten — this is consistent with upsert semantics
        assert result.succeeded >= 1
        assert result.failed == 0

        item = await proxy.read_item("dup")
        assert item["version"] == 3


class TestBulkUpsertReadAllAfterBulk:
    """read_all_items returns everything that was bulk-upserted."""

    async def test_read_all_after_bulk(self) -> None:
        proxy = _make_proxy()
        items = [{"id": f"item-{i}", "x": i} for i in range(15)]
        await proxy.bulk_upsert_items(items)

        all_items = await proxy.read_all_items()
        ids = {item["id"] for item in all_items}
        assert ids == {f"item-{i}" for i in range(15)}


class TestBulkUpsertMaxConcurrency:
    """Verify the max_concurrency parameter is accepted."""

    async def test_low_concurrency(self) -> None:
        proxy = _make_proxy()
        items = [{"id": f"k{i}"} for i in range(10)]
        result = await proxy.bulk_upsert_items(items, max_concurrency=1)
        assert result.succeeded == 10

    async def test_high_concurrency(self) -> None:
        proxy = _make_proxy()
        items = [{"id": f"k{i}"} for i in range(10)]
        result = await proxy.bulk_upsert_items(items, max_concurrency=100)
        assert result.succeeded == 10


# ------------------------------------------------------------------
# patch_first mode
# ------------------------------------------------------------------


class TestBulkUpsertPatchFirst:
    """Tests for the patch_first=True bulk ingestion strategy."""

    async def test_patch_first_into_empty_container(self) -> None:
        """patch_first on first wave — buckets don't exist → 404 → create fallback."""
        proxy = _make_proxy()
        items = [{"id": f"k{i}", "val": i} for i in range(20)]
        result = await proxy.bulk_upsert_items(items, patch_first=True)
        assert result.succeeded == 20
        assert result.failed == 0

        for i in range(20):
            item = await proxy.read_item(f"k{i}")
            assert item["val"] == i

    async def test_patch_first_into_existing_buckets(self) -> None:
        """patch_first on subsequent waves — buckets exist → patch succeeds directly."""
        proxy = _make_proxy(bucket_prefix_length=1)
        # Wave 1: create-first (default) to populate buckets
        wave1 = [{"id": f"w1-{i}", "wave": 1} for i in range(10)]
        r1 = await proxy.bulk_upsert_items(wave1)
        assert r1.succeeded == 10

        # Wave 2: patch-first into existing buckets
        wave2 = [{"id": f"w2-{i}", "wave": 2} for i in range(10)]
        r2 = await proxy.bulk_upsert_items(wave2, patch_first=True)
        assert r2.succeeded == 10
        assert r2.failed == 0

        # Verify both waves readable
        for i in range(10):
            assert (await proxy.read_item(f"w1-{i}"))["wave"] == 1
            assert (await proxy.read_item(f"w2-{i}"))["wave"] == 2

    async def test_patch_first_overwrites_existing_entries(self) -> None:
        """patch_first uses upsert semantics — existing entries are overwritten."""
        proxy = _make_proxy()
        await proxy.create_item({"id": "x", "ver": 1})

        result = await proxy.bulk_upsert_items(
            [{"id": "x", "ver": 2}], patch_first=True,
        )
        assert result.succeeded == 1
        assert (await proxy.read_item("x"))["ver"] == 2

    async def test_patch_first_mixed_new_and_existing_buckets(self) -> None:
        """Some buckets exist and some don't — patch_first handles both."""
        proxy = _make_proxy(bucket_prefix_length=2)
        # Pre-populate some buckets
        for i in range(5):
            await proxy.create_item({"id": f"pre-{i}", "src": "pre"})

        # Bulk upsert with patch_first — items will land in a mix of
        # existing and new buckets
        items = [{"id": f"new-{i}", "src": "new"} for i in range(20)]
        result = await proxy.bulk_upsert_items(items, patch_first=True)
        assert result.succeeded == 20
        assert result.failed == 0

        # Pre-existing items still readable
        for i in range(5):
            assert (await proxy.read_item(f"pre-{i}"))["src"] == "pre"

        # New items readable
        for i in range(20):
            assert (await proxy.read_item(f"new-{i}"))["src"] == "new"
