"""Tests that multiple keys sharing a bucket are stored and readable.

With patch-based writes, the client no longer reads the document before
writing, so the ``max_entries_per_doc`` capacity guard is removed from the
write path.  Cosmos DB's own 2 MB document size limit is the ultimate
safety net.  The tests below verify that many keys coexist correctly in a
single bucket.
"""

from packed_kv_cosmos.client import PackedKvClient
from packed_kv_cosmos.config import PackedKvConfig
from packed_kv_cosmos.hashing import compute_bucket_id, compute_entry_id
from packed_kv_cosmos.results import STATUS_SUCCESS
from tests.unit.fakes.fake_storage import FakeStorage


def _find_keys_same_bucket(n: int, prefix_length: int = 4) -> list[str]:
    """Find *n* keys that all hash to the same bucket."""
    buckets: dict[str, list[str]] = {}
    for i in range(500_000):
        key = f"overflow-{i}"
        eid = compute_entry_id(key)
        bid = compute_bucket_id(eid, prefix_length)
        buckets.setdefault(bid, []).append(key)
        if len(buckets[bid]) >= n:
            return buckets[bid][:n]
    raise RuntimeError(f"Could not find {n} same-bucket keys")


class TestBucketFull:
    """With patch-based writes, all puts succeed as long as the Cosmos 2 MB
    document limit is not exceeded."""

    async def test_put_beyond_max_entries_succeeds_with_patch(self) -> None:
        """Writes beyond max_entries_per_doc succeed because patch does not
        perform a client-side capacity check."""
        keys = _find_keys_same_bucket(5)
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=2,
        )
        client = PackedKvClient(storage=storage, config=config)

        # All 5 should succeed — no client-side capacity check with patch
        for i in range(5):
            res = await client.put(keys[i], f"val-{i}".encode())
            assert res.status == STATUS_SUCCESS, f"Put failed for key #{i}"

        # All 5 are readable
        for i in range(5):
            res = await client.get(keys[i])
            assert res.status == STATUS_SUCCESS
            assert res.value == f"val-{i}".encode()

    async def test_existing_keys_within_capacity(self) -> None:
        """Keys within capacity are all readable."""
        keys = _find_keys_same_bucket(3)
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=3,
        )
        client = PackedKvClient(storage=storage, config=config)

        for i, k in enumerate(keys):
            res = await client.put(k, f"val-{i}".encode())
            assert res.status == STATUS_SUCCESS

        for i, k in enumerate(keys):
            res = await client.get(k)
            assert res.status == STATUS_SUCCESS
            assert res.value == f"val-{i}".encode()

    async def test_overwrite_existing_key_in_full_bucket(self) -> None:
        """Overwriting an existing key in a full bucket still works."""
        keys = _find_keys_same_bucket(3)
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=3,
        )
        client = PackedKvClient(storage=storage, config=config)

        for i, k in enumerate(keys):
            await client.put(k, f"old-{i}".encode())

        # Overwrite existing key — should work even though bucket is full
        res = await client.put(keys[0], b"updated")
        assert res.status == STATUS_SUCCESS
        assert (await client.get(keys[0])).value == b"updated"


class TestSingleDocPointRead:
    """Every Get is exactly 1 document read (no overflow scanning)."""

    async def test_get_reads_exactly_one_document(self) -> None:
        """A Get for a key that exists reads exactly 1 document."""
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=200,
        )
        client = PackedKvClient(storage=storage, config=config)

        await client.put("mykey", b"myvalue")
        res = await client.get("mykey")
        assert res.status == STATUS_SUCCESS
        assert res.diagnostics.documents_read == 1
        assert res.diagnostics.used_overflow is False

    async def test_get_not_found_reads_one_document(self) -> None:
        """A Get for a missing key in an existing bucket reads 1 document."""
        keys = _find_keys_same_bucket(2)
        storage = FakeStorage()
        config = PackedKvConfig(
            namespace="test",
            bucket_prefix_length=4,
            max_entries_per_doc=200,
        )
        client = PackedKvClient(storage=storage, config=config)

        await client.put(keys[0], b"val")
        # keys[1] is in the same bucket but was never stored
        res = await client.get(keys[1])
        assert res.diagnostics.documents_read == 1
