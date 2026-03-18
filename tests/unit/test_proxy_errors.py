"""Unit tests for proxy error handling - 409/404 (T058)."""

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


class TestCreate409:
    """create_item raises 409 when the item already exists."""

    async def test_duplicate_create(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "dup", "category": "env"})
        with pytest.raises(CosmosResourceExistsError):
            await proxy.create_item({"id": "dup", "category": "env"})

    async def test_duplicate_id_different_category_raises_409(self) -> None:
        """Same id with different category is the same item - raises 409."""
        proxy = _make_proxy()
        await proxy.create_item({"id": "x", "category": "a"})
        with pytest.raises(CosmosResourceExistsError):
            await proxy.create_item({"id": "x", "category": "b"})


class TestRead404:
    """read_item raises 404 when item not found."""

    async def test_read_nonexistent(self) -> None:
        proxy = _make_proxy()
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.read_item("ghost")

    async def test_read_after_delete(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "del", "category": "env"})
        await proxy.delete_item("del")
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.read_item("del")


class TestReplace404:
    """replace_item raises 404 when item not found."""

    async def test_replace_nonexistent(self) -> None:
        proxy = _make_proxy()
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.replace_item("rp", {"id": "rp", "category": "env", "v": 1})


class TestDelete404:
    """delete_item raises 404 when item not found."""

    async def test_delete_nonexistent(self) -> None:
        proxy = _make_proxy()
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.delete_item("ghost")

    async def test_double_delete(self) -> None:
        proxy = _make_proxy()
        await proxy.create_item({"id": "dd", "category": "env"})
        await proxy.delete_item("dd")
        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.delete_item("dd")
