"""Two keys mapping to the same bucket (T028, US2).

With patch-based writes, concurrent puts to different entries in the
same bucket are serialised server-side and both succeed without 412
retries.  The tests below verify that both keys preserve their values
when they share a bucket.
"""

from packed_kv_cosmos.client import PackedKvClient
from packed_kv_cosmos.config import PackedKvConfig
from packed_kv_cosmos.hashing import compute_bucket_id, compute_entry_id
from packed_kv_cosmos.results import STATUS_SUCCESS
from tests.unit.fakes.fake_storage import FakeStorage


def _find_two_keys_same_bucket(prefix_length: int = 4) -> tuple[str, str]:
    """Brute-force find two keys that hash to the same bucket.

    With prefix_length=4 (65536 buckets), collisions are very common among
    a small set of sequential strings.
    """
    buckets: dict[str, str] = {}
    for i in range(200_000):
        key = f"key-{i}"
        eid = compute_entry_id(key)
        bid = compute_bucket_id(eid, prefix_length)
        if bid in buckets:
            return buckets[bid], key
        buckets[bid] = key
    raise RuntimeError("Could not find two same-bucket keys")


class TestTwoKeysSameBucket:
    """Putting two keys in the same bucket succeeds without retries
    now that writes use Cosmos partial document update (patch)."""

    async def test_both_values_survive(self) -> None:
        key_a, key_b = _find_two_keys_same_bucket()

        storage = FakeStorage()
        config = PackedKvConfig(namespace="test", bucket_prefix_length=4, max_retries=5)
        client = PackedKvClient(storage=storage, config=config)

        res_a = await client.put(key_a, b"val-a")
        assert res_a.status == STATUS_SUCCESS
        assert res_a.diagnostics.retries == 0

        res_b = await client.put(key_b, b"val-b")
        assert res_b.status == STATUS_SUCCESS
        assert res_b.diagnostics.retries == 0

        assert (await client.get(key_a)).value == b"val-a"
        assert (await client.get(key_b)).value == b"val-b"

    async def test_multiple_same_bucket_keys(self) -> None:
        """Many keys sharing one bucket all survive."""
        # Use a very short prefix to force everything into one bucket
        config = PackedKvConfig(namespace="test", bucket_prefix_length=1, max_retries=5)
        storage = FakeStorage()
        client = PackedKvClient(storage=storage, config=config)

        # Find several keys in the same 1-char-prefix bucket
        key_a, key_b = _find_two_keys_same_bucket(prefix_length=1)
        await client.put(key_a, b"a")
        await client.put(key_b, b"b")

        assert (await client.get(key_a)).value == b"a"
        assert (await client.get(key_b)).value == b"b"

    async def test_overwrite_same_key_succeeds(self) -> None:
        """Overwriting the same key succeeds on the first attempt with patch."""
        storage = FakeStorage()
        config = PackedKvConfig(namespace="test", bucket_prefix_length=4, max_retries=5)
        client = PackedKvClient(storage=storage, config=config)

        await client.put("k", b"old")
        res = await client.put("k", b"new")
        assert res.status == STATUS_SUCCESS
        assert res.diagnostics.retries == 0
        assert (await client.get("k")).value == b"new"

    async def test_delete_one_key_keeps_other(self) -> None:
        """Deleting one key in a shared bucket keeps the other intact."""
        key_a, key_b = _find_two_keys_same_bucket()

        storage = FakeStorage()
        config = PackedKvConfig(namespace="test", bucket_prefix_length=4, max_retries=5)
        client = PackedKvClient(storage=storage, config=config)

        await client.put(key_a, b"a")
        await client.put(key_b, b"b")
        await client.delete(key_a)

        assert (await client.get(key_b)).value == b"b"
