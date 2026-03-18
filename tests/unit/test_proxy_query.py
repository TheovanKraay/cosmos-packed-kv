"""Unit tests for query_items (T056)."""

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


async def _seed(proxy: PackedContainerProxy) -> None:
    """Insert test items across two categories."""
    await proxy.create_item({"id": "s1", "category": "env", "temp": 72.5})
    await proxy.create_item({"id": "s2", "category": "env", "temp": 68.0})
    await proxy.create_item({"id": "c1", "category": "cfg", "key": "timeout", "val": 30})
    await proxy.create_item({"id": "c2", "category": "cfg", "key": "retries", "val": 3})


class TestQueryAllItems:
    """query_items scans all buckets and returns matching entries."""

    async def test_select_all_returns_everything(self) -> None:
        proxy = _make_proxy()
        await _seed(proxy)
        results = await proxy.query_items("SELECT * FROM c")
        ids = {r["id"] for r in results}
        assert ids == {"s1", "s2", "c1", "c2"}

    async def test_empty_container(self) -> None:
        proxy = _make_proxy()
        results = await proxy.query_items("SELECT * FROM c")
        assert results == []


class TestWhereClauseFiltering:
    """Client-side WHERE filtering."""

    async def test_simple_equality(self) -> None:
        proxy = _make_proxy()
        await _seed(proxy)
        results = await proxy.query_items(
            "SELECT * FROM c WHERE c.key = 'timeout'"
        )
        assert len(results) == 1
        assert results[0]["id"] == "c1"

    async def test_numeric_comparison(self) -> None:
        proxy = _make_proxy()
        await _seed(proxy)
        results = await proxy.query_items(
            "SELECT * FROM c WHERE c.temp > 70"
        )
        assert len(results) == 1
        assert results[0]["id"] == "s1"

    async def test_parameterised_query(self) -> None:
        proxy = _make_proxy()
        await _seed(proxy)
        results = await proxy.query_items(
            "SELECT * FROM c WHERE c.category = @cat",
            parameters=[{"name": "@cat", "value": "env"}],
        )
        assert len(results) == 2

    async def test_multiple_and_conditions(self) -> None:
        proxy = _make_proxy()
        await _seed(proxy)
        results = await proxy.query_items(
            "SELECT * FROM c WHERE c.category = 'env' AND c.temp >= 70"
        )
        assert len(results) == 1
        assert results[0]["id"] == "s1"

    async def test_filter_by_category(self) -> None:
        """Category is now regular data, filterable via WHERE clause."""
        proxy = _make_proxy()
        await _seed(proxy)
        results = await proxy.query_items(
            "SELECT * FROM c WHERE c.category = 'cfg'"
        )
        ids = {r["id"] for r in results}
        assert ids == {"c1", "c2"}

    async def test_no_where_returns_all(self) -> None:
        proxy = _make_proxy()
        await _seed(proxy)
        results = await proxy.query_items("SELECT * FROM c")
        assert len(results) == 4
