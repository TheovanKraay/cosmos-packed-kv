# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""Configuration model and validation for PackedKvCosmos."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

_DEFAULT_BUCKET_PREFIX_LENGTH = 4
_DEFAULT_MAX_ENTRIES_PER_DOC = 200
_DEFAULT_MAX_DOC_BYTES_APPROX = 1_800_000  # ~1.8 MB safety margin under 2 MB Cosmos limit
_DEFAULT_MAX_RETRIES = 10
_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class PackedKvConfig:
    """Immutable configuration for a :class:`PackedKvClient`.

    Parameters
    ----------
    namespace:
        Logical isolation boundary within a Cosmos container.  Different
        namespaces never share bucket documents.
    bucket_prefix_length:
        Number of leading hex characters of the SHA-256 entry-id used as
        the bucket id.  Must be 1-64.  Use
        :meth:`recommended_prefix_length` to choose a value based on your
        expected data volume.
    max_entries_per_doc:
        Hard cap on entries in a single bucket document.  Writes that
        would exceed this limit return ``STATUS_TOO_LARGE``.
    max_doc_bytes_approx:
        Approximate byte-size safety margin for a single Cosmos item.
    max_retries:
        Maximum optimistic-concurrency retry attempts per operation.
    schema_version:
        Persisted schema version written into new documents.
    """

    namespace: str
    bucket_prefix_length: int = _DEFAULT_BUCKET_PREFIX_LENGTH
    max_entries_per_doc: int = _DEFAULT_MAX_ENTRIES_PER_DOC
    max_doc_bytes_approx: int = _DEFAULT_MAX_DOC_BYTES_APPROX
    max_retries: int = _DEFAULT_MAX_RETRIES
    schema_version: int = field(default=_SCHEMA_VERSION, repr=False)

    def __post_init__(self) -> None:
        _validate(self)

    @staticmethod
    def recommended_prefix_length(
        total_expected_items: int,
        max_items_per_bin: int = _DEFAULT_MAX_ENTRIES_PER_DOC,
        safety_factor: float = 2.0,
    ) -> int:
        """Return the minimum ``bucket_prefix_length`` for the given scale.

        Parameters
        ----------
        total_expected_items:
            Estimated total number of logical items across all bins.
        max_items_per_bin:
            Maximum entries per bin document (same as ``max_entries_per_doc``).
        safety_factor:
            Multiplier to account for hash non-uniformity (default 2×).
        """
        if total_expected_items <= 0 or max_items_per_bin <= 0:
            return 1
        desired_bins = total_expected_items / max_items_per_bin * safety_factor
        return max(1, math.ceil(math.log(max(desired_bins, 1)) / math.log(16)))


def _validate(cfg: PackedKvConfig) -> None:
    """Raise ``ValueError`` for any invalid configuration value."""
    if not cfg.namespace or not cfg.namespace.strip():
        raise ValueError("namespace must be a non-empty string")
    if not 1 <= cfg.bucket_prefix_length <= 64:
        raise ValueError(
            f"bucket_prefix_length must be 1-64, got {cfg.bucket_prefix_length}"
        )
    if cfg.max_entries_per_doc < 1:
        raise ValueError(
            f"max_entries_per_doc must be >= 1, got {cfg.max_entries_per_doc}"
        )
    if cfg.max_doc_bytes_approx < 1024:
        raise ValueError(
            f"max_doc_bytes_approx must be >= 1024, got {cfg.max_doc_bytes_approx}"
        )
    if cfg.max_retries < 0:
        raise ValueError(f"max_retries must be >= 0, got {cfg.max_retries}")


# ---------------------------------------------------------------------------
# PackingConfig — configuration for PackedContainerProxy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PackingConfig:
    """Immutable configuration for a :class:`PackedContainerProxy`.

    Parameters
    ----------
    namespace:
        Logical isolation boundary within a Cosmos container.
    bucket_prefix_length:
        Number of leading hex characters of the SHA-256 hash used as
        the bucket id.  Must be 1-64.  Use
        :meth:`PackedKvConfig.recommended_prefix_length` to choose a
        value based on your expected data volume.
    max_entries_per_doc:
        Hard cap on entries per bucket document.  Writes that would
        exceed this limit raise a ``RuntimeError``.
    max_doc_bytes_approx:
        Approximate byte-size safety margin per Cosmos item.
    max_retries:
        Maximum optimistic-concurrency retry attempts per operation.
    schema_version:
        Persisted schema version written into new documents.
    """

    namespace: str = "default"
    bucket_prefix_length: int = _DEFAULT_BUCKET_PREFIX_LENGTH
    max_entries_per_doc: int = _DEFAULT_MAX_ENTRIES_PER_DOC
    max_doc_bytes_approx: int = _DEFAULT_MAX_DOC_BYTES_APPROX
    max_retries: int = _DEFAULT_MAX_RETRIES
    schema_version: int = field(default=_SCHEMA_VERSION, repr=False)

    def __post_init__(self) -> None:
        _validate_packing(self)


def _validate_packing(cfg: PackingConfig) -> None:
    """Raise ``ValueError`` for any invalid :class:`PackingConfig` value."""
    if not cfg.namespace or not cfg.namespace.strip():
        raise ValueError("namespace must be a non-empty string")
    if not 1 <= cfg.bucket_prefix_length <= 64:
        raise ValueError(
            f"bucket_prefix_length must be 1-64, got {cfg.bucket_prefix_length}"
        )
    if cfg.max_entries_per_doc < 1:
        raise ValueError(
            f"max_entries_per_doc must be >= 1, got {cfg.max_entries_per_doc}"
        )
    if cfg.max_doc_bytes_approx < 1024:
        raise ValueError(
            f"max_doc_bytes_approx must be >= 1024, got {cfg.max_doc_bytes_approx}"
        )
    if cfg.max_retries < 0:
        raise ValueError(f"max_retries must be >= 0, got {cfg.max_retries}")
