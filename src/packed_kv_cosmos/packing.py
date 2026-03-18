# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""Value encoding helpers and deterministic document-id generation.

Value encoding
--------------
The core contract treats values as opaque bytes stored as base64 strings in
Cosmos JSON.  These helpers perform the round-trip encoding.

Document-id generation
----------------------
Bucket root and page document ids are deterministic from (namespace, bucketId)
and optionally (pageIndex).
"""

from __future__ import annotations

import base64

# ---------------------------------------------------------------------------
# Value encoding (bytes <-> base64 string)
# ---------------------------------------------------------------------------

def encode_value(value: bytes) -> str:
    """Encode opaque *value* bytes to a base64 string for JSON storage."""
    return base64.b64encode(value).decode("ascii")


def decode_value(encoded: str) -> bytes:
    """Decode a base64 string back to the original opaque bytes."""
    return base64.b64decode(encoded)


# ---------------------------------------------------------------------------
# Deterministic document-id generation
# ---------------------------------------------------------------------------

def make_root_document_id(namespace: str, bucket_id: str) -> str:
    """Return the deterministic ``id`` for a bucket root document.

    Format: ``{namespace}:{bucket_id}``
    """
    return f"{namespace}:{bucket_id}"


def make_page_document_id(namespace: str, bucket_id: str, page_index: int) -> str:
    """Return the deterministic ``id`` for a bucket page document.

    Format: ``{namespace}:{bucket_id}:p{page_index}``
    """
    return f"{namespace}:{bucket_id}:p{page_index}"


def make_partition_key(namespace: str, bucket_id: str) -> str:
    """Return the deterministic partition key value for a bucket.

    All root and page documents in the same bucket share the same pk so they
    reside in the same logical partition.

    Format: ``{namespace}:{bucket_id}``
    """
    return f"{namespace}:{bucket_id}"
