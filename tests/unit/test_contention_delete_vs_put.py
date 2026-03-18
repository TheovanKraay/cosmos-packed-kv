"""Concurrent Delete vs Put on the same bucket (T029, US2).

With patch-based writes, concurrent operations on different entries in
the same bucket succeed without conflict — no 412 retries are needed.
The tests below verify that interleaved deletes and puts remain correct.
"""

from packed_kv_cosmos.client import PackedKvClient
from packed_kv_cosmos.config import PackedKvConfig
from packed_kv_cosmos.results import (
    STATUS_NOT_FOUND,
    STATUS_SUCCESS,
)
from tests.unit.fakes.fake_storage import FakeStorage


def _make_client(
    storage: FakeStorage | None = None,
    max_retries: int = 5,
) -> PackedKvClient:
    config = PackedKvConfig(namespace="test", bucket_prefix_length=4, max_retries=max_retries)
    return PackedKvClient(storage=storage or FakeStorage(), config=config)


class TestDeleteVsPutContention:
    """Delete and Put on the same bucket succeed independently with patch."""

    async def test_delete_succeeds_no_conflict(self) -> None:
        """Patch-based delete succeeds on the first attempt (no ETag conflict)."""
        storage = FakeStorage()
        client = _make_client(storage=storage)

        await client.put("k", b"v")
        res = await client.delete("k")
        assert res.status == STATUS_SUCCESS
        assert res.diagnostics.retries == 0
        assert (await client.get("k")).status == STATUS_NOT_FOUND

    async def test_delete_missing_key(self) -> None:
        """Deleting a key that was never written returns NOT_FOUND."""
        client = _make_client()
        res = await client.delete("nonexistent")
        assert res.status == STATUS_NOT_FOUND

    async def test_put_after_delete(self) -> None:
        """After a successful delete, a new put works."""
        storage = FakeStorage()
        client = _make_client(storage=storage)

        await client.put("k", b"old")
        await client.delete("k")

        res = await client.put("k", b"new")
        assert res.status == STATUS_SUCCESS
        assert (await client.get("k")).value == b"new"

    async def test_delete_key_a_while_putting_key_b(self) -> None:
        """Delete(A) and put(B) on the same bucket both succeed.

        With patch, these touch different entries and don't conflict.
        """
        storage = FakeStorage()
        client = _make_client(storage=storage)

        await client.put("a", b"1")
        await client.put("b", b"2")

        del_res = await client.delete("a")
        assert del_res.status == STATUS_SUCCESS
        assert del_res.diagnostics.retries == 0

        # b should still be intact
        assert (await client.get("b")).value == b"2"
