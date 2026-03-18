"""Unit tests for PackingConfig validation (T053)."""

import pytest

from packed_kv_cosmos.config import PackedKvConfig, PackingConfig


class TestPackingConfigDefaults:
    """Default values are reasonable."""

    def test_defaults(self) -> None:
        cfg = PackingConfig()
        assert cfg.namespace == "default"
        assert cfg.bucket_prefix_length == 4
        assert cfg.max_entries_per_doc == 200
        assert cfg.max_retries == 10
        assert cfg.schema_version == 1

    def test_custom_values(self) -> None:
        cfg = PackingConfig(
            namespace="myns",
            bucket_prefix_length=8,
            max_entries_per_doc=100,
            max_retries=5,
        )
        assert cfg.namespace == "myns"
        assert cfg.bucket_prefix_length == 8
        assert cfg.max_entries_per_doc == 100
        assert cfg.max_retries == 5

    def test_frozen(self) -> None:
        cfg = PackingConfig()
        with pytest.raises(AttributeError):
            cfg.namespace = "other"  # type: ignore[misc]


class TestPackingConfigValidation:
    """Invalid params raise ValueError."""

    def test_empty_namespace(self) -> None:
        with pytest.raises(ValueError, match="namespace"):
            PackingConfig(namespace="")

    def test_blank_namespace(self) -> None:
        with pytest.raises(ValueError, match="namespace"):
            PackingConfig(namespace="   ")

    def test_bucket_prefix_length_zero(self) -> None:
        with pytest.raises(ValueError, match="bucket_prefix_length"):
            PackingConfig(bucket_prefix_length=0)

    def test_bucket_prefix_length_too_large(self) -> None:
        with pytest.raises(ValueError, match="bucket_prefix_length"):
            PackingConfig(bucket_prefix_length=65)

    def test_max_entries_per_doc_zero(self) -> None:
        with pytest.raises(ValueError, match="max_entries_per_doc"):
            PackingConfig(max_entries_per_doc=0)

    def test_max_doc_bytes_too_small(self) -> None:
        with pytest.raises(ValueError, match="max_doc_bytes_approx"):
            PackingConfig(max_doc_bytes_approx=100)

    def test_max_retries_negative(self) -> None:
        with pytest.raises(ValueError, match="max_retries"):
            PackingConfig(max_retries=-1)


class TestRecommendedPrefixLength:
    """recommended_prefix_length gives reasonable values."""

    def test_small_dataset(self) -> None:
        result = PackedKvConfig.recommended_prefix_length(100, 200)
        assert result >= 1

    def test_large_dataset(self) -> None:
        result = PackedKvConfig.recommended_prefix_length(10_000_000, 200)
        assert result >= 5

    def test_zero_items(self) -> None:
        assert PackedKvConfig.recommended_prefix_length(0, 200) == 1

    def test_zero_max_items(self) -> None:
        assert PackedKvConfig.recommended_prefix_length(1000, 0) == 1
