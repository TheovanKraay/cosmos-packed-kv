"""Unit tests for read_all_items (T057).

Routing is by item id only — no partition_key parameter.
"""

from packed_kv_cosmos.config import PackingConfig
from packed_kv_cosmos.packed_container import PackedContainerProxy
from tests.unit.fakes.fake_container_proxy import FakeContainerProxy


def _make_proxy(**cfg_overrides) -> PackedContainerProxy:
    defaults = dict(
        namespace="test",
        bucket_prefix_length=4,
        max_entries_per_doc=50,
    )
    defaults.update(cfg_overrides)
    config = PackingConfig(**defaults)
    return PackedContainerProxy(FakeContainerProxy(), config)


class TestReadAllItems:
    """read_all_items scans all buckets and returns every item."""

    async def test_empty(self) -> None:
        proxy = _make_proxy()
        assert await proxy.read_all_items() == []

    async def test_single_item(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "a", "category": "env"})
        items = await proxy.read_all_items()
        assert len(items) == 1
        assert items[0]["id"] == "a"

    async def test_multiple_categories(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "a", "category": "env"})
        await proxy.create_item({"id": "b", "category": "cfg"})
        await proxy.create_item({"id": "c", "category": "env"})
        items = await proxy.read_all_items()
        ids = {i["id"] for i in items}
        assert ids == {"a", "b", "c"}

    async def test_after_delete(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "x", "category": "env"})
        await proxy.create_item({"id": "y", "category": "env"})
        await proxy.delete_item("x")
        items = await proxy.read_all_items()
        ids = {i["id"] for i in items}
        assert ids == {"y"}

    async def test_after_upsert(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "z", "category": "env", "v": 1})
        await proxy.upsert_item({"id": "z", "category": "env", "v": 2})
        items = await proxy.read_all_items()
        assert len(items) == 1
        assert items[0]["v"] == 2
