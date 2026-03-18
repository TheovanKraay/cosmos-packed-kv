"""Live Cosmos DB smoke test for PackedContainerProxy.

Routing is by item id only -- no caller-supplied partition_key.

Skipped unless all required environment variables are set:
  - COSMOS_CONNECTION_STRING
  - COSMOS_DATABASE
  - COSMOS_CONTAINER

Run with: pytest tests/integration/ -v
"""

import os
import uuid

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


@pytest.fixture()
def proxy() -> PackedContainerProxy:
    assert _CONN and _DB and _CONTAINER
    from azure.cosmos.aio import CosmosClient

    client = CosmosClient.from_connection_string(_CONN)
    database = client.get_database_client(_DB)
    container = database.get_container_client(_CONTAINER)
    # Use a unique namespace per run to avoid cross-test interference
    ns = f"proxy-test-{uuid.uuid4().hex[:8]}"
    config = PackingConfig(
        namespace=ns,
        bucket_prefix_length=4,
        max_entries_per_doc=5,
    )
    return PackedContainerProxy(container, config)


@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestProxyLiveSmoke:
    """Basic CRUD against a real Cosmos container via PackedContainerProxy."""

    async def test_create_read_delete(self, proxy: PackedContainerProxy) -> None:
        item_id = f"smoke-{uuid.uuid4().hex[:8]}"
        body = {"id": item_id, "val": 42}

        created = await proxy.create_item(body)
        assert created["id"] == item_id

        read = await proxy.read_item(item_id)
        assert read["val"] == 42

        await proxy.delete_item(item_id)
        from azure.cosmos.exceptions import CosmosResourceNotFoundError

        with pytest.raises(CosmosResourceNotFoundError):
            await proxy.read_item(item_id)

    async def test_upsert_and_query(self, proxy: PackedContainerProxy) -> None:
        item_id = f"upsert-{uuid.uuid4().hex[:8]}"
        body = {"id": item_id, "v": 1}

        await proxy.upsert_item(body)
        await proxy.upsert_item({**body, "v": 2})

        read = await proxy.read_item(item_id)
        assert read["v"] == 2

        results = await proxy.query_items("SELECT * FROM c")
        found = [r for r in results if r["id"] == item_id]
        assert len(found) == 1
        assert found[0]["v"] == 2

        # cleanup
        await proxy.delete_item(item_id)
