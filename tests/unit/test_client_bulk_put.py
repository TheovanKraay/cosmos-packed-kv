"""Unit tests for PackedKvClient.bulk_put.

All tests use FakeStorage — no live Cosmos needed.
"""

import json

from packed_kv_cosmos.client import PackedKvClient
from packed_kv_cosmos.config import PackedKvConfig
from packed_kv_cosmos.results import STATUS_SUCCESS, BulkResult
from tests.unit.fakes.fake_storage import FakeStorage


def _make_client(storage: FakeStorage | None = None, **cfg_overrides) -> PackedKvClient:
    defaults = dict(namespace="test", bucket_prefix_length=4)
    defaults.update(cfg_overrides)
    config = PackedKvConfig(**defaults)
    return PackedKvClient(storage=storage or FakeStorage(), config=config)


# ------------------------------------------------------------------
# Happy path
# ------------------------------------------------------------------


class TestBulkPutBasic:
    """Basic bulk put operations."""

    async def test_empty_list(self) -> None:
        client = _make_client()
        result = await client.bulk_put([])
        assert isinstance(result, BulkResult)
        assert result.succeeded == 0
        assert result.failed == 0

    async def test_single_entry(self) -> None:
        client = _make_client()
        result = await client.bulk_put([("key1", b"value1")])
        assert result.succeeded == 1
        assert result.failed == 0

        get_res = await client.get("key1")
        assert get_res.status == STATUS_SUCCESS
        assert get_res.value == b"value1"

    async def test_multiple_entries(self) -> None:
        client = _make_client()
        entries = [(f"k{i}", f"v{i}".encode()) for i in range(20)]
        result = await client.bulk_put(entries)
        assert result.succeeded == 20
        assert result.failed == 0

        for i in range(20):
            res = await client.get(f"k{i}")
            assert res.value == f"v{i}".encode()

    async def test_large_batch(self) -> None:
        """100 entries with short prefix → many per bucket → tests batched patching."""
        client = _make_client(bucket_prefix_length=2)
        entries = [(f"item-{i:04d}", f"data-{i}".encode()) for i in range(100)]
        result = await client.bulk_put(entries)
        assert result.succeeded == 100
        assert result.failed == 0

        # Spot check
        assert (await client.get("item-0000")).value == b"data-0"
        assert (await client.get("item-0099")).value == b"data-99"


class TestBulkPutUpsertSemantics:
    """bulk_put uses upsert semantics (create-or-replace)."""

    async def test_overwrites_existing(self) -> None:
        client = _make_client()
        await client.put("x", b"old")

        result = await client.bulk_put([("x", b"new")])
        assert result.succeeded == 1

        assert (await client.get("x")).value == b"new"

    async def test_creates_and_overwrites(self) -> None:
        client = _make_client()
        await client.put("existing", b"v1")

        result = await client.bulk_put([
            ("existing", b"v2"),
            ("brand-new", b"fresh"),
        ])
        assert result.succeeded == 2
        assert (await client.get("existing")).value == b"v2"
        assert (await client.get("brand-new")).value == b"fresh"


class TestBulkPutWithExistingBuckets:
    """Verify bulk handles the create→409→patch fallback."""

    async def test_adds_to_existing_bucket(self) -> None:
        client = _make_client(bucket_prefix_length=1)
        # Pre-populate
        for i in range(5):
            await client.put(f"pre-{i}", f"old-{i}".encode())

        # Bulk put more — some will land in existing buckets
        new_entries = [(f"post-{i}", f"new-{i}".encode()) for i in range(10)]
        result = await client.bulk_put(new_entries)
        assert result.succeeded == 10
        assert result.failed == 0

        # Verify pre-existing entries still there
        for i in range(5):
            assert (await client.get(f"pre-{i}")).value == f"old-{i}".encode()

        # Verify new entries
        for i in range(10):
            assert (await client.get(f"post-{i}")).value == f"new-{i}".encode()


class TestBulkPutDuplicateKeys:
    """When the same key appears multiple times, last value wins."""

    async def test_last_value_wins(self) -> None:
        client = _make_client()
        result = await client.bulk_put([
            ("dup", b"first"),
            ("dup", b"second"),
            ("dup", b"third"),
        ])
        assert result.succeeded >= 1
        assert result.failed == 0

        assert (await client.get("dup")).value == b"third"


# ------------------------------------------------------------------
# Validation
# ------------------------------------------------------------------


class TestBulkPutValidation:
    """Input validation for bulk_put."""

    async def test_empty_key_rejected(self) -> None:
        client = _make_client()
        result = await client.bulk_put([
            ("", b"value"),
            ("good-key", b"value"),
        ])
        assert result.succeeded == 1
        assert result.failed == 1
        assert len(result.errors) == 1
        assert "non-empty" in result.errors[0].error

    async def test_non_bytes_value_rejected(self) -> None:
        client = _make_client()
        result = await client.bulk_put([
            ("bad-val", "not-bytes"),  # type: ignore[arg-type]
            ("good-val", b"bytes"),
        ])
        assert result.succeeded == 1
        assert result.failed == 1
        assert "bytes" in result.errors[0].error


class TestBulkPutConcurrency:
    """Verify the max_concurrency parameter is accepted."""

    async def test_low_concurrency(self) -> None:
        client = _make_client()
        entries = [(f"k{i}", b"v") for i in range(10)]
        result = await client.bulk_put(entries, max_concurrency=1)
        assert result.succeeded == 10

    async def test_high_concurrency(self) -> None:
        client = _make_client()
        entries = [(f"k{i}", b"v") for i in range(10)]
        result = await client.bulk_put(entries, max_concurrency=100)
        assert result.succeeded == 10


class TestBulkPutBinaryValues:
    """Bulk put with various binary values."""

    async def test_binary_values(self) -> None:
        client = _make_client()
        entries = [
            ("bin1", bytes(range(256))),
            ("bin2", b"\x00\xff\x00\xff"),
            ("empty", b""),
            ("json", json.dumps({"key": "value"}).encode()),
        ]
        result = await client.bulk_put(entries)
        assert result.succeeded == 4

        assert (await client.get("bin1")).value == bytes(range(256))
        assert (await client.get("bin2")).value == b"\x00\xff\x00\xff"
        assert (await client.get("empty")).value == b""
        assert json.loads((await client.get("json")).value) == {"key": "value"}


# ------------------------------------------------------------------
# patch_first mode
# ------------------------------------------------------------------


class TestBulkPutPatchFirst:
    """Tests for the patch_first=True bulk ingestion strategy."""

    async def test_patch_first_into_empty_container(self) -> None:
        """patch_first on first wave — buckets don't exist → 404 → create fallback."""
        client = _make_client()
        entries = [(f"k{i}", f"v{i}".encode()) for i in range(20)]
        result = await client.bulk_put(entries, patch_first=True)
        assert result.succeeded == 20
        assert result.failed == 0

        for i in range(20):
            assert (await client.get(f"k{i}")).value == f"v{i}".encode()

    async def test_patch_first_into_existing_buckets(self) -> None:
        """patch_first on subsequent waves — buckets exist → patch succeeds directly."""
        client = _make_client(bucket_prefix_length=1)
        # Wave 1: create-first (default)
        wave1 = [(f"w1-{i}", b"wave1") for i in range(10)]
        r1 = await client.bulk_put(wave1)
        assert r1.succeeded == 10

        # Wave 2: patch-first into existing buckets
        wave2 = [(f"w2-{i}", b"wave2") for i in range(10)]
        r2 = await client.bulk_put(wave2, patch_first=True)
        assert r2.succeeded == 10
        assert r2.failed == 0

        # Both waves readable
        for i in range(10):
            assert (await client.get(f"w1-{i}")).value == b"wave1"
            assert (await client.get(f"w2-{i}")).value == b"wave2"

    async def test_patch_first_overwrites_existing_entries(self) -> None:
        """patch_first uses upsert semantics — existing entries are overwritten."""
        client = _make_client()
        await client.put("x", b"old")

        result = await client.bulk_put([("x", b"new")], patch_first=True)
        assert result.succeeded == 1
        assert (await client.get("x")).value == b"new"

    async def test_patch_first_mixed_new_and_existing_buckets(self) -> None:
        """Some buckets exist and some don't — patch_first handles both."""
        client = _make_client(bucket_prefix_length=2)
        # Pre-populate some buckets
        for i in range(5):
            await client.put(f"pre-{i}", b"pre")

        entries = [(f"new-{i}", b"new") for i in range(20)]
        result = await client.bulk_put(entries, patch_first=True)
        assert result.succeeded == 20
        assert result.failed == 0

        for i in range(5):
            assert (await client.get(f"pre-{i}")).value == b"pre"
        for i in range(20):
            assert (await client.get(f"new-{i}")).value == b"new"
