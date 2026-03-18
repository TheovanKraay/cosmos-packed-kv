"""Unit tests for Delete missing-key behaviour (T020, US1).

Covers: deleting a key that was never put, deleting twice, etc.
"""

from packed_kv_cosmos.client import PackedKvClient
from packed_kv_cosmos.config import PackedKvConfig
from packed_kv_cosmos.results import STATUS_INVALID_INPUT, STATUS_NOT_FOUND, STATUS_SUCCESS
from tests.unit.fakes.fake_storage import FakeStorage


def _make_client(**cfg_overrides) -> PackedKvClient:
    defaults = dict(namespace="test", bucket_prefix_length=4)
    defaults.update(cfg_overrides)
    config = PackedKvConfig(**defaults)
    return PackedKvClient(storage=FakeStorage(), config=config)


class TestDeleteMissing:
    """Delete of a non-existent key returns notFound."""

    async def test_delete_never_put(self) -> None:
        client = _make_client()
        res = await client.delete("never-stored")
        assert res.status == STATUS_NOT_FOUND

    async def test_delete_twice(self) -> None:
        client = _make_client()
        await client.put("k", b"v")
        res1 = await client.delete("k")
        assert res1.status == STATUS_SUCCESS

        res2 = await client.delete("k")
        assert res2.status == STATUS_NOT_FOUND

    async def test_delete_empty_key(self) -> None:
        """Empty key is rejected as invalidInput."""
        client = _make_client()
        res = await client.delete("")
        assert res.status == STATUS_INVALID_INPUT

    async def test_get_after_delete_not_found(self) -> None:
        client = _make_client()
        await client.put("k", b"data")
        await client.delete("k")
        assert (await client.get("k")).status == STATUS_NOT_FOUND

    async def test_delete_does_not_affect_other_keys(self) -> None:
        client = _make_client()
        await client.put("a", b"1")
        await client.put("b", b"2")
        await client.delete("a")
        assert (await client.get("b")).value == b"2"
