"""Unit tests for Put / Get / Delete happy-path (T019, US1).

All tests use FakeStorage – no live Cosmos account needed.
"""

from packed_kv_cosmos.client import PackedKvClient
from packed_kv_cosmos.config import PackedKvConfig
from packed_kv_cosmos.results import STATUS_NOT_FOUND, STATUS_SUCCESS
from tests.unit.fakes.fake_storage import FakeStorage


def _make_client(storage: FakeStorage | None = None, **cfg_overrides) -> PackedKvClient:
    defaults = dict(namespace="test", bucket_prefix_length=4)
    defaults.update(cfg_overrides)
    config = PackedKvConfig(**defaults)
    return PackedKvClient(storage=storage or FakeStorage(), config=config)


class TestPutGetHappyPath:
    """Basic Put -> Get returns the stored value."""

    async def test_put_then_get(self) -> None:
        client = _make_client()
        res = await client.put("user:1", b"hello")
        assert res.status == STATUS_SUCCESS

        res = await client.get("user:1")
        assert res.status == STATUS_SUCCESS
        assert res.value == b"hello"

    async def test_put_overwrite(self) -> None:
        """Second Put to the same key overwrites the value."""
        client = _make_client()
        await client.put("k", b"v1")
        await client.put("k", b"v2")

        res = await client.get("k")
        assert res.status == STATUS_SUCCESS
        assert res.value == b"v2"

    async def test_put_multiple_keys(self) -> None:
        client = _make_client()
        await client.put("a", b"1")
        await client.put("b", b"2")
        await client.put("c", b"3")

        assert (await client.get("a")).value == b"1"
        assert (await client.get("b")).value == b"2"
        assert (await client.get("c")).value == b"3"

    async def test_put_empty_value(self) -> None:
        client = _make_client()
        await client.put("empty", b"")
        res = await client.get("empty")
        assert res.status == STATUS_SUCCESS
        assert res.value == b""

    async def test_put_binary_value(self) -> None:
        client = _make_client()
        val = bytes(range(256))
        await client.put("bin", val)
        assert (await client.get("bin")).value == val


class TestDeleteHappyPath:
    """Delete removes the entry."""

    async def test_delete_existing(self) -> None:
        client = _make_client()
        await client.put("x", b"val")
        res = await client.delete("x")
        assert res.status == STATUS_SUCCESS

        res = await client.get("x")
        assert res.status == STATUS_NOT_FOUND

    async def test_delete_then_reput(self) -> None:
        client = _make_client()
        await client.put("k", b"v1")
        await client.delete("k")
        await client.put("k", b"v2")
        assert (await client.get("k")).value == b"v2"


class TestDiagnostics:
    """Verify diagnostics are populated on basic operations."""

    async def test_put_diagnostics(self) -> None:
        client = _make_client()
        res = await client.put("d", b"x")
        # Patch-based writes do not read the document first
        assert res.diagnostics.documents_read == 0
        assert res.diagnostics.documents_written >= 1
        assert res.diagnostics.retries == 0

    async def test_get_diagnostics(self) -> None:
        client = _make_client()
        await client.put("d", b"x")
        res = await client.get("d")
        assert res.diagnostics.documents_read >= 1

    async def test_get_not_found_diagnostics(self) -> None:
        client = _make_client()
        res = await client.get("missing")
        assert res.status == STATUS_NOT_FOUND
        assert res.diagnostics.documents_read >= 1
