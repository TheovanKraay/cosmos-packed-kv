"""Live Cosmos DB integration tests for bulk operations.

Tests both PackedContainerProxy.bulk_upsert_items and PackedKvClient.bulk_put
against a real (or emulator) Cosmos container.

Skipped unless all required environment variables are set:
  - COSMOS_CONNECTION_STRING
  - COSMOS_DATABASE
  - COSMOS_CONTAINER

Run with: pytest tests/integration/test_bulk_live.py -v
"""

import json
import os
import uuid

import pytest

from packed_kv_cosmos import (
    BulkResult,
    PackedContainerProxy,
    PackedKvClient,
    PackedKvConfig,
    PackingConfig,
)

_CONN = os.environ.get("COSMOS_CONNECTION_STRING")
_DB = os.environ.get("COSMOS_DATABASE")
_CONTAINER = os.environ.get("COSMOS_CONTAINER")

_SKIP_REASON = (
    "Live Cosmos env vars not set "
    "(COSMOS_CONNECTION_STRING, COSMOS_DATABASE, COSMOS_CONTAINER)"
)
_HAS_ENV = all([_CONN, _DB, _CONTAINER])


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture()
def proxy() -> PackedContainerProxy:
    assert _CONN and _DB and _CONTAINER
    from azure.cosmos.aio import CosmosClient

    client = CosmosClient.from_connection_string(_CONN)
    database = client.get_database_client(_DB)
    container = database.get_container_client(_CONTAINER)
    ns = f"bulk-proxy-{uuid.uuid4().hex[:8]}"
    config = PackingConfig(
        namespace=ns,
        bucket_prefix_length=4,
        max_entries_per_doc=200,
    )
    return PackedContainerProxy(container, config)


@pytest.fixture()
def kv_client() -> PackedKvClient:
    assert _CONN and _DB and _CONTAINER
    ns = f"bulk-kv-{uuid.uuid4().hex[:8]}"
    config = PackedKvConfig(
        namespace=ns,
        bucket_prefix_length=4,
        max_entries_per_doc=200,
    )
    return PackedKvClient.from_connection_string(
        connection_string=_CONN,
        database=_DB,
        container=_CONTAINER,
        config=config,
    )


# ==================================================================
# PackedContainerProxy.bulk_upsert_items
# ==================================================================


@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestProxyBulkLive:
    """Live tests for PackedContainerProxy.bulk_upsert_items."""

    async def test_bulk_small_batch(self, proxy: PackedContainerProxy) -> None:
        items = [{"id": f"bs-{i}", "val": i} for i in range(5)]
        result = await proxy.bulk_upsert_items(items)
        assert isinstance(result, BulkResult)
        assert result.succeeded == 5
        assert result.failed == 0

        for i in range(5):
            item = await proxy.read_item(f"bs-{i}")
            assert item["val"] == i

    async def test_bulk_medium_batch(self, proxy: PackedContainerProxy) -> None:
        """50 items — exercises multi-entry patch batching."""
        items = [{"id": f"bm-{i:03d}", "data": f"payload-{i}"} for i in range(50)]
        result = await proxy.bulk_upsert_items(items)
        assert result.succeeded == 50
        assert result.failed == 0

        # Spot check
        item = await proxy.read_item("bm-000")
        assert item["data"] == "payload-0"
        item = await proxy.read_item("bm-049")
        assert item["data"] == "payload-49"

    async def test_bulk_then_read_all(self, proxy: PackedContainerProxy) -> None:
        items = [{"id": f"ra-{i}", "v": i} for i in range(10)]
        await proxy.bulk_upsert_items(items)

        all_items = await proxy.read_all_items()
        ids = {item["id"] for item in all_items}
        for i in range(10):
            assert f"ra-{i}" in ids

    async def test_bulk_upsert_overwrites(self, proxy: PackedContainerProxy) -> None:
        """Create-then-bulk-upsert overwrites existing items."""
        await proxy.create_item({"id": "overwrite-test", "ver": 1})

        result = await proxy.bulk_upsert_items([
            {"id": "overwrite-test", "ver": 2},
            {"id": "new-item", "ver": 1},
        ])
        assert result.succeeded == 2

        assert (await proxy.read_item("overwrite-test"))["ver"] == 2
        assert (await proxy.read_item("new-item"))["ver"] == 1

    async def test_bulk_with_nested_json(self, proxy: PackedContainerProxy) -> None:
        items = [
            {
                "id": "nested-1",
                "meta": {"tags": ["a", "b"], "count": 42},
                "flags": [True, False],
            },
            {
                "id": "nested-2",
                "meta": {"tags": ["c"], "count": 7},
                "flags": [False],
            },
        ]
        result = await proxy.bulk_upsert_items(items)
        assert result.succeeded == 2

        item = await proxy.read_item("nested-1")
        assert item["meta"]["tags"] == ["a", "b"]
        assert item["flags"] == [True, False]


# ==================================================================
# PackedKvClient.bulk_put
# ==================================================================


@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestKvBulkLive:
    """Live tests for PackedKvClient.bulk_put."""

    async def test_bulk_small_batch(self, kv_client: PackedKvClient) -> None:
        entries = [(f"k{i}", f"v{i}".encode()) for i in range(5)]
        result = await kv_client.bulk_put(entries)
        assert isinstance(result, BulkResult)
        assert result.succeeded == 5
        assert result.failed == 0

        for i in range(5):
            res = await kv_client.get(f"k{i}")
            assert res.value == f"v{i}".encode()

    async def test_bulk_medium_batch(self, kv_client: PackedKvClient) -> None:
        entries = [(f"km-{i:03d}", f"data-{i}".encode()) for i in range(50)]
        result = await kv_client.bulk_put(entries)
        assert result.succeeded == 50
        assert result.failed == 0

        assert (await kv_client.get("km-000")).value == b"data-0"
        assert (await kv_client.get("km-049")).value == b"data-49"

    async def test_bulk_put_overwrites(self, kv_client: PackedKvClient) -> None:
        await kv_client.put("existing", b"old-val")

        result = await kv_client.bulk_put([
            ("existing", b"new-val"),
            ("brand-new", b"fresh"),
        ])
        assert result.succeeded == 2

        assert (await kv_client.get("existing")).value == b"new-val"
        assert (await kv_client.get("brand-new")).value == b"fresh"

    async def test_bulk_put_binary(self, kv_client: PackedKvClient) -> None:
        entries = [
            ("bin1", bytes(range(256))),
            ("json1", json.dumps({"key": "value"}).encode()),
            ("empty", b""),
        ]
        result = await kv_client.bulk_put(entries)
        assert result.succeeded == 3

        assert (await kv_client.get("bin1")).value == bytes(range(256))
        assert json.loads((await kv_client.get("json1")).value) == {"key": "value"}
        assert (await kv_client.get("empty")).value == b""

    async def test_bulk_put_validation(self, kv_client: PackedKvClient) -> None:
        result = await kv_client.bulk_put([
            ("", b"bad-key"),
            ("good-key", b"good-val"),
        ])
        assert result.succeeded == 1
        assert result.failed == 1
        assert len(result.errors) == 1
