# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""Stable, cross-language hashing utilities.

All implementations MUST produce identical outputs given the same logical key
and configuration so that bucket documents are interoperable across languages.
"""

from __future__ import annotations

import hashlib


def compute_entry_id(key: str) -> str:
    """Return the full SHA-256 hex digest (lowercase) of *key* encoded as UTF-8.

    >>> compute_entry_id("user:123")
    'b1d7...'  # 64-char hex string
    """
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def compute_bucket_id(entry_id: str, prefix_length: int) -> str:
    """Return the first *prefix_length* hex characters of *entry_id*.

    Parameters
    ----------
    entry_id:
        A 64-character lowercase hex string produced by :func:`compute_entry_id`.
    prefix_length:
        Number of leading hex chars (1-64).
    """
    return entry_id[:prefix_length]


def compute_bucket_id_from_value(value: str, prefix_length: int) -> str:
    """Compute a bucket id directly from a partition key value.

    This hashes *value* with SHA-256 and takes the first *prefix_length*
    hex characters.

    Parameters
    ----------
    value:
        The partition key value (e.g. ``"env"``).
    prefix_length:
        Number of leading hex chars (1-64).
    """
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return digest[:prefix_length]


def compute_bucket_id_from_pk_and_id(pk_value: str, item_id: str, prefix_length: int) -> str:
    """Compute a bucket id from both partition key value and item id.

    This combines *pk_value* and *item_id* before hashing so that items
    with the same partition key but different ids are distributed across
    many buckets.  This prevents any single partition key from creating an
    oversized bin document and guarantees that every ``read_item`` is a
    single Cosmos point read (both the bin document ``id`` and ``pk`` are
    derivable from the caller-supplied arguments).

    ::

        bucketId = sha256(utf8(pk_value + "|" + item_id)).hex()[0:prefix]

    Parameters
    ----------
    pk_value:
        The partition key value (e.g. ``"env"``).
    item_id:
        The logical item id (e.g. ``"sensor:001"``).
    prefix_length:
        Number of leading hex chars (1-64).
    """
    combined = f"{pk_value}|{item_id}"
    digest = hashlib.sha256(combined.encode("utf-8")).hexdigest()
    return digest[:prefix_length]
