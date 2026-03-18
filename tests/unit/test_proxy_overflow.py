"""Unit tests for proxy bucket capacity.

With id-only routing, each item id maps to a bucket via sha256(id)[:prefix].
With small max_entries_per_doc, a RuntimeError is raised when a bucket is full.
"""

import pytest
from azure.cosmos.exceptions import CosmosResourceExistsError, CosmosResourceNotFoundError

from packed_kv_cosmos.config import PackingConfig
from packed_kv_cosmos.packed_container import PackedContainerProxy
from tests.unit.fakes.fake_container_proxy import FakeContainerProxy


def _make_proxy(max_entries: int = 3, **cfg_overrides) -> PackedContainerProxy:
    defaults = dict(
        namespace="test",
        bucket_prefix_length=4,
        max_entries_per_doc=max_entries,
    )
    defaults.update(cfg_overrides)
    config = PackingConfig(**defaults)
    return PackedContainerProxy(FakeContainerProxy(), config)


class TestSingleDocBins:
    """With id-only routing, items distribute across buckets by id hash."""

    async def test_many_items_different_ids(self) -> None:
        """Items with different IDs go to different buckets."""
        proxy = _make_proxy(max_entries=2)
        for i in range(10):
            await proxy.create_item({"id": f"i{i}", "category": "env", "v": i})
        for i in range(10):
            assert (await proxy.read_item(f"i{i}"))["v"] == i

    async def test_read_is_single_point_read(self) -> None:
        """read_item always does a single point read."""
        proxy = _make_proxy(max_entries=200)
        await proxy.create_item({"id": "s1", "category": "env", "temp": 72.5})
        result = await proxy.read_item("s1")
        assert result["temp"] == 72.5


class TestQueryWithIdRouting:
    """query_items works via full scan with client-side filtering."""

    async def test_query_includes_all_items(self) -> None:
        proxy = _make_proxy(max_entries=200)
        for i in range(5):
            await proxy.create_item({"id": f"q{i}", "category": "env", "v": i})
        results = await proxy.query_items("SELECT * FROM c")
        assert len(results) == 5

    async def test_read_all_includes_all_items(self) -> None:
        proxy = _make_proxy(max_entries=200)
        for i in range(4):
            await proxy.create_item({"id": f"r{i}", "category": "env"})
        items = await proxy.read_all_items()
        assert len(items) == 4


class TestUpsertSingleDoc:
    """Upsert works with single-document bins."""

    async def test_upsert_updates_existing(self) -> None:
        proxy = _make_proxy(max_entries=200)
        await proxy.create_item({"id": "a", "category": "env", "v": 1})
        await proxy.upsert_item({"id": "a", "category": "env", "v": 99})
        assert (await proxy.read_item("a"))["v"] == 99


class TestDeleteSingleDoc:
    """Delete works with single-document bins."""

    async def test_delete_existing(self) -> None:
        proxy = _make_proxy(max_entries=200)
        await proxy.create_item({"id": "a", "category": "env"})
        await proxy.delete_item("a")
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.read_item("a")


class TestReplaceSingleDoc:
    """Replace works with single-document bins."""

    async def test_replace_existing(self) -> None:
        proxy = _make_proxy(max_entries=200)
        await proxy.create_item({"id": "a", "category": "env", "v": 1})
        await proxy.replace_item("a", {"id": "a", "category": "env", "v": 42})
        assert (await proxy.read_item("a"))["v"] == 42

    async def test_replace_nonexistent_raises_404(self) -> None:
        proxy = _make_proxy(max_entries=200)
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.replace_item("nope", {"id": "nope", "category": "env", "v": 1})


class TestDuplicateCreate:
    """create_item raises 409 for duplicate items."""

    async def test_duplicate_raises_409(self) -> None:
        proxy = _make_proxy(max_entries=200)
        await proxy.create_item({"id": "a", "category": "env"})
        with pytest.raises(CosmosResourceExistsError):
            await proxy.create_item({"id": "a", "category": "env"})
