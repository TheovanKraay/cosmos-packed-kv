"""Unit tests for PK-based bucket routing (T054)."""

import hashlib

from packed_kv_cosmos.hashing import (
    compute_bucket_id,
    compute_bucket_id_from_pk_and_id,
    compute_bucket_id_from_value,
    compute_entry_id,
)


class TestComputeBucketIdFromValue:
    """Verify compute_bucket_id_from_value correctness."""

    def test_returns_hex_prefix(self) -> None:
        result = compute_bucket_id_from_value("env", 4)
        assert len(result) == 4
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self) -> None:
        a = compute_bucket_id_from_value("env", 4)
        b = compute_bucket_id_from_value("env", 4)
        assert a == b

    def test_different_values_differ(self) -> None:
        a = compute_bucket_id_from_value("env", 4)
        b = compute_bucket_id_from_value("config", 4)
        # They could collide in theory but won't for these two
        assert a != b

    def test_prefix_length_1(self) -> None:
        result = compute_bucket_id_from_value("test", 1)
        assert len(result) == 1

    def test_prefix_length_64(self) -> None:
        result = compute_bucket_id_from_value("test", 64)
        assert len(result) == 64

    def test_same_as_manual_hash(self) -> None:
        """bucket_id_from_value(x, n) == sha256(utf8(x)).hex()[:n]."""
        value = "my-partition"
        expected = hashlib.sha256(value.encode("utf-8")).hexdigest()[:6]
        assert compute_bucket_id_from_value(value, 6) == expected


class TestComputeBucketIdFromPkAndId:
    """Verify compute_bucket_id_from_pk_and_id correctness."""

    def test_returns_hex_prefix(self) -> None:
        result = compute_bucket_id_from_pk_and_id("env", "sensor:001", 4)
        assert len(result) == 4
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self) -> None:
        a = compute_bucket_id_from_pk_and_id("env", "sensor:001", 4)
        b = compute_bucket_id_from_pk_and_id("env", "sensor:001", 4)
        assert a == b

    def test_same_pk_different_id_different_bucket(self) -> None:
        """Same PK + different ID → (usually) different bucket."""
        a = compute_bucket_id_from_pk_and_id("env", "sensor:001", 8)
        b = compute_bucket_id_from_pk_and_id("env", "sensor:002", 8)
        # With 8 hex chars (4.3B buckets), collision is extremely unlikely
        assert a != b

    def test_different_pk_same_id_different_bucket(self) -> None:
        """Different PK + same ID → (usually) different bucket."""
        a = compute_bucket_id_from_pk_and_id("env", "x", 8)
        b = compute_bucket_id_from_pk_and_id("cfg", "x", 8)
        assert a != b

    def test_matches_manual_hash(self) -> None:
        """bucket = sha256(utf8(pk + '|' + id)).hex()[:prefix]."""
        pk, item_id, prefix = "env", "sensor:001", 6
        combined = f"{pk}|{item_id}"
        expected = hashlib.sha256(combined.encode("utf-8")).hexdigest()[:prefix]
        assert compute_bucket_id_from_pk_and_id(pk, item_id, prefix) == expected

    def test_prefix_length_1(self) -> None:
        result = compute_bucket_id_from_pk_and_id("env", "x", 1)
        assert len(result) == 1

    def test_prefix_length_64(self) -> None:
        result = compute_bucket_id_from_pk_and_id("env", "x", 64)
        assert len(result) == 64


class TestKvVsProxyRouting:
    """KV routing vs ContainerProxy routing use different functions."""

    def test_kv_routes_by_entry_id(self) -> None:
        """KV API: bucket = entry_id[:prefix] where entry_id = hash(key)."""
        entry_id = compute_entry_id("sensor:001")
        bucket = compute_bucket_id(entry_id, 4)
        assert bucket == entry_id[:4]

    def test_proxy_routes_by_pk_and_id(self) -> None:
        """ContainerProxy API: bucket = hash(pk_value + '|' + item_id)[:prefix]."""
        bucket = compute_bucket_id_from_pk_and_id("env", "sensor:001", 4)
        entry = compute_entry_id("sensor:001")
        assert len(bucket) == 4
        assert len(entry) == 64

    def test_same_pk_and_id_same_bucket(self) -> None:
        """Same pk + same id always produces the same bucket."""
        b1 = compute_bucket_id_from_pk_and_id("env", "s1", 4)
        b2 = compute_bucket_id_from_pk_and_id("env", "s1", 4)
        assert b1 == b2
