"""Tests for delete in single-document bins (replaced overflow delete tests).

Verify that deleting a key works correctly in a single-document bucket
and does not corrupt other entries.
"""

from packed_kv_cosmos.client import PackedKvClient
from packed_kv_cosmos.config import PackedKvConfig
from packed_kv_cosmos.results import STATUS_NOT_FOUND, STATUS_SUCCESS
from tests.unit.fakes.fake_storage import FakeStorage
from tests.unit.test_overflow_pages_put_get import _find_keys_same_bucket


class TestDeleteInBucket:
    """Delete entries from single-document buckets."""

    async def test_delete_from_bucket(self) -> None:
        """Delete a key from a bucket with multiple entries."""
        keys = _find_keys_same_bucket(3)
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=5,
        )
        client = PackedKvClient(storage=storage, config=config)

        for i, k in enumerate(keys):
            await client.put(k, f"v{i}".encode())

        # Delete the last key
        res = await client.delete(keys[-1])
        assert res.status == STATUS_SUCCESS

        assert (await client.get(keys[-1])).status == STATUS_NOT_FOUND

        # Other keys still intact
        for i, k in enumerate(keys[:-1]):
            assert (await client.get(k)).value == f"v{i}".encode()

    async def test_delete_all_keys(self) -> None:
        """Delete every key in a bucket."""
        keys = _find_keys_same_bucket(4)
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=5,
        )
        client = PackedKvClient(storage=storage, config=config)

        for i, k in enumerate(keys):
            await client.put(k, f"v{i}".encode())

        for k in keys:
            res = await client.delete(k)
            assert res.status == STATUS_SUCCESS

        for k in keys:
            assert (await client.get(k)).status == STATUS_NOT_FOUND

    async def test_delete_does_not_affect_other_bucket_keys(self) -> None:
        """Deleting from one bucket doesn't affect another bucket."""
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=200,
        )
        client = PackedKvClient(storage=storage, config=config)

        await client.put("alpha", b"1")
        await client.put("beta", b"2")
        await client.delete("alpha")
        assert (await client.get("beta")).value == b"2"
