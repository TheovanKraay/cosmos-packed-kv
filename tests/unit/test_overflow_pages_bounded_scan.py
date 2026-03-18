"""Tests for single-document bin point reads (replaced bounded scan tests).

Verify that every read is a single point read — no overflow scanning.
"""

from packed_kv_cosmos.client import PackedKvClient
from packed_kv_cosmos.config import PackedKvConfig
from packed_kv_cosmos.results import STATUS_NOT_FOUND, STATUS_SUCCESS
from tests.unit.fakes.fake_storage import FakeStorage
from tests.unit.test_overflow_pages_put_get import _find_keys_same_bucket


class TestPointReadGuarantee:
    """Every read is exactly 1 Cosmos point read."""

    async def test_get_existing_key_one_read(self) -> None:
        """Get an existing key reads exactly 1 document."""
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=200,
        )
        client = PackedKvClient(storage=storage, config=config)

        await client.put("k1", b"v1")
        res = await client.get("k1")
        assert res.status == STATUS_SUCCESS
        assert res.diagnostics.documents_read == 1
        assert res.diagnostics.pages_scanned == 0
        assert res.diagnostics.used_overflow is False

    async def test_get_missing_key_one_read(self) -> None:
        """Get a missing key (bucket exists) reads exactly 1 document."""
        keys = _find_keys_same_bucket(2)
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=200,
        )
        client = PackedKvClient(storage=storage, config=config)

        await client.put(keys[0], b"val")
        res = await client.get(keys[1])
        assert res.status == STATUS_NOT_FOUND
        assert res.diagnostics.documents_read == 1

    async def test_get_empty_bucket_one_read(self) -> None:
        """Get a key whose bucket doesn't exist reads exactly 1 document."""
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=200,
        )
        client = PackedKvClient(storage=storage, config=config)

        res = await client.get("nonexistent")
        assert res.status == STATUS_NOT_FOUND
        assert res.diagnostics.documents_read == 1

    async def test_multiple_keys_same_bucket_all_point_reads(self) -> None:
        """Multiple keys in the same bucket are each found with 1 read."""
        keys = _find_keys_same_bucket(4)
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=10,
        )
        client = PackedKvClient(storage=storage, config=config)

        for i, k in enumerate(keys):
            await client.put(k, f"v{i}".encode())

        for i, k in enumerate(keys):
            res = await client.get(k)
            assert res.status == STATUS_SUCCESS
            assert res.value == f"v{i}".encode()
            assert res.diagnostics.documents_read == 1
