"""Deterministic unit tests for hashing and bucket mapping (T015)."""

from packed_kv_cosmos.hashing import compute_bucket_id, compute_entry_id


class TestComputeEntryId:
    """Tests for compute_entry_id."""

    def test_returns_64_char_hex(self) -> None:
        eid = compute_entry_id("user:123")
        assert len(eid) == 64
        assert all(c in "0123456789abcdef" for c in eid)

    def test_lowercase_hex(self) -> None:
        eid = compute_entry_id("SomeKey")
        assert eid == eid.lower()

    def test_deterministic(self) -> None:
        """Same key always produces the same entry id."""
        assert compute_entry_id("hello") == compute_entry_id("hello")

    def test_different_keys_produce_different_ids(self) -> None:
        assert compute_entry_id("a") != compute_entry_id("b")

    def test_utf8_encoding(self) -> None:
        """Non-ASCII characters are hashed via UTF-8 bytes."""
        eid = compute_entry_id("日本語")
        assert len(eid) == 64

    def test_empty_string(self) -> None:
        """Empty string is a valid key for hashing purposes."""
        eid = compute_entry_id("")
        assert len(eid) == 64

    def test_known_vector(self) -> None:
        """Verify against a precomputed SHA-256 value.

        SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        """
        eid = compute_entry_id("abc")
        assert eid == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


class TestComputeBucketId:
    """Tests for compute_bucket_id."""

    def test_prefix_4(self) -> None:
        eid = compute_entry_id("user:123")
        bid = compute_bucket_id(eid, 4)
        assert bid == eid[:4]
        assert len(bid) == 4

    def test_prefix_1(self) -> None:
        eid = compute_entry_id("x")
        bid = compute_bucket_id(eid, 1)
        assert len(bid) == 1

    def test_prefix_64(self) -> None:
        eid = compute_entry_id("x")
        bid = compute_bucket_id(eid, 64)
        assert bid == eid

    def test_same_prefix_for_same_key(self) -> None:
        eid = compute_entry_id("key")
        assert compute_bucket_id(eid, 4) == compute_bucket_id(eid, 4)
