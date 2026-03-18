"""Live Cosmos DB smoke test (T048).

Skipped unless all required environment variables are set:
  - COSMOS_CONNECTION_STRING
  - COSMOS_DATABASE
  - COSMOS_CONTAINER

Run with: pytest tests/integration/ -v
"""

import os

import pytest

from packed_kv_cosmos import PackedKvClient, PackedKvConfig

_CONN = os.environ.get("COSMOS_CONNECTION_STRING")
_DB = os.environ.get("COSMOS_DATABASE")
_CONTAINER = os.environ.get("COSMOS_CONTAINER")

_SKIP_REASON = (
    "Live Cosmos env vars not set "
    "(COSMOS_CONNECTION_STRING, COSMOS_DATABASE, COSMOS_CONTAINER)"
)
_HAS_ENV = all([_CONN, _DB, _CONTAINER])


@pytest.fixture()
def client() -> PackedKvClient:
    assert _CONN and _DB and _CONTAINER
    config = PackedKvConfig(
        namespace="integration-test",
        bucket_prefix_length=4,
        max_entries_per_doc=5,
    )
    return PackedKvClient.from_connection_string(
        connection_string=_CONN,
        database=_DB,
        container=_CONTAINER,
        config=config,
    )


@pytest.mark.skipif(not _HAS_ENV, reason=_SKIP_REASON)
class TestLiveSmoke:
    """Basic put / get / delete against a real Cosmos container."""

    async def test_put_get_delete(self, client: PackedKvClient) -> None:
        key = "integration-test:smoke:1"
        value = b"hello-cosmos"

        put_res = await client.put(key, value)
        assert put_res.status == "success"

        get_res = await client.get(key)
        assert get_res.status == "success"
        assert get_res.value == value

        del_res = await client.delete(key)
        assert del_res.status == "success"

        get_after = await client.get(key)
        assert get_after.status == "notFound"

    async def test_overwrite(self, client: PackedKvClient) -> None:
        key = "integration-test:smoke:overwrite"
        await client.put(key, b"v1")
        await client.put(key, b"v2")

        res = await client.get(key)
        assert res.status == "success"
        assert res.value == b"v2"

        # Cleanup
        await client.delete(key)
