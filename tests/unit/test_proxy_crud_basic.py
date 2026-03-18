"""Unit tests for PackedContainerProxy CRUD happy path (T055).

All tests use FakeContainerProxy - no live Cosmos needed.
"""

import pytest
from azure.cosmos.exceptions import (
    CosmosResourceExistsError,
    CosmosResourceNotFoundError,
)

from packed_kv_cosmos.config import PackingConfig
from packed_kv_cosmos.packed_container import PackedContainerProxy
from tests.unit.fakes.fake_container_proxy import FakeContainerProxy


def _make_proxy(**cfg_overrides) -> PackedContainerProxy:
    defaults = dict(
        namespace="test",
        bucket_prefix_length=4,
        max_entries_per_doc=5,
    )
    defaults.update(cfg_overrides)
    config = PackingConfig(**defaults)
    return PackedContainerProxy(FakeContainerProxy(), config)


class TestCreateItem:
    """create_item inserts a new item."""

    async def test_create_and_read(self) -> None:
        proxy = _make_proxy()
        body = {"id": "s1", "category": "env", "temp": 72.5}
        result = await proxy.create_item(body)
        assert result["id"] == "s1"
        assert result["temp"] == 72.5

        read = await proxy.read_item("s1")
        assert read["id"] == "s1"
        assert read["temp"] == 72.5

    async def test_create_duplicate_raises_409(self) -> None:
        proxy = _make_proxy()
        body = {"id": "s1", "category": "env", "temp": 72.5}
        await proxy.create_item(body)
        with pytest.raises(CosmosResourceExistsError):
            await proxy.create_item(body)

    async def test_create_returns_full_body(self) -> None:
        proxy = _make_proxy()
        body = {"id": "x", "category": "cfg", "nested": {"a": 1}}
        result = await proxy.create_item(body)
        assert result["nested"] == {"a": 1}


class TestReadItem:
    """read_item returns item by id."""

    async def test_read_existing(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "r1", "category": "env", "val": 42})
        r = await proxy.read_item("r1")
        assert r["val"] == 42

    async def test_read_missing_raises_404(self) -> None:
        proxy = _make_proxy()
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.read_item("nope")


class TestUpsertItem:
    """upsert_item creates or replaces."""

    async def test_upsert_new(self) -> None:
        proxy = _make_proxy()
        body = {"id": "u1", "category": "env", "v": 1}
        result = await proxy.upsert_item(body)
        assert result["v"] == 1

    async def test_upsert_overwrite(self) -> None:
        proxy = _make_proxy()
        await proxy.upsert_item({"id": "u2", "category": "env", "v": 1})
        await proxy.upsert_item({"id": "u2", "category": "env", "v": 2})
        r = await proxy.read_item("u2")
        assert r["v"] == 2


class TestReplaceItem:
    """replace_item updates existing or raises 404."""

    async def test_replace_existing(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "rp1", "category": "env", "v": 1})
        await proxy.replace_item("rp1", {"id": "rp1", "category": "env", "v": 99})
        r = await proxy.read_item("rp1")
        assert r["v"] == 99

    async def test_replace_missing_raises_404(self) -> None:
        proxy = _make_proxy()
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.replace_item("nope", {"id": "nope", "category": "env", "v": 1})


class TestDeleteItem:
    """delete_item removes an entry."""

    async def test_delete_existing(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "d1", "category": "env"})
        await proxy.delete_item("d1")
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.read_item("d1")

    async def test_delete_missing_raises_404(self) -> None:
        proxy = _make_proxy()
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.delete_item("nope")

    async def test_delete_then_recreate(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "d2", "category": "env", "v": 1})
        await proxy.delete_item("d2")
        await proxy.create_item({"id": "d2", "category": "env", "v": 2})
        r = await proxy.read_item("d2")
        assert r["v"] == 2


class TestSameIdCollision:
    """Items with same id but different category now collide (same bucket)."""

    async def test_same_id_different_category_upsert_overwrites(self) -> None:
        proxy = _make_proxy()
        await proxy.upsert_item({"id": "x", "category": "env", "source": "env"})
        await proxy.upsert_item({"id": "x", "category": "cfg", "source": "cfg"})
        r = await proxy.read_item("x")
        assert r["source"] == "cfg"  # second upsert wins

    async def test_same_id_create_twice_raises_409(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "y", "category": "a"})
        with pytest.raises(CosmosResourceExistsError):
            await proxy.create_item({"id": "y", "category": "b"})
