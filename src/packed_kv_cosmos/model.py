# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""In-memory bucket document models and serialization helpers.

These models mirror the persisted JSON schema defined in
``contracts/bucket-document.schema.json``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional

DOC_TYPE_ROOT = "bucketRoot"
DOC_TYPE_PAGE = "bucketPage"


# ---------------------------------------------------------------------------
# Bucket root document
# ---------------------------------------------------------------------------

@dataclass
class BucketRootDocument:
    """In-memory representation of a persisted bucket root document."""

    id: str
    pk: str
    schema_version: int
    bucket_id: str
    entries: Dict[str, str] = field(default_factory=dict)
    tail_page: Optional[str] = None
    page_count: int = 0
    updated_at: Optional[str] = None
    etag: Optional[str] = None  # Cosmos _etag; not persisted in the body

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialize to a Cosmos-compatible dict."""
        doc: dict = {
            "id": self.id,
            "pk": self.pk,
            "schemaVersion": self.schema_version,
            "docType": DOC_TYPE_ROOT,
            "bucketId": self.bucket_id,
            "entries": dict(self.entries),
            "tailPage": self.tail_page,
            "pageCount": self.page_count,
            "updatedAt": self.updated_at or _now_iso(),
        }
        return doc

    @classmethod
    def from_dict(cls, doc: dict) -> BucketRootDocument:
        """Deserialize a Cosmos item dict into a model instance."""
        return cls(
            id=doc["id"],
            pk=doc["pk"],
            schema_version=doc.get("schemaVersion", 1),
            bucket_id=doc["bucketId"],
            entries=dict(doc.get("entries", {})),
            tail_page=doc.get("tailPage"),
            page_count=doc.get("pageCount", 0),
            updated_at=doc.get("updatedAt"),
            etag=doc.get("_etag"),
        )


# ---------------------------------------------------------------------------
# Bucket page document
# ---------------------------------------------------------------------------

@dataclass
class BucketPageDocument:
    """In-memory representation of a persisted bucket page (overflow) document."""

    id: str
    pk: str
    schema_version: int
    bucket_id: str
    page_index: int
    entries: Dict[str, str] = field(default_factory=dict)
    next_page: Optional[str] = None
    updated_at: Optional[str] = None
    etag: Optional[str] = None

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialize to a Cosmos-compatible dict."""
        doc: dict = {
            "id": self.id,
            "pk": self.pk,
            "schemaVersion": self.schema_version,
            "docType": DOC_TYPE_PAGE,
            "bucketId": self.bucket_id,
            "pageIndex": self.page_index,
            "entries": dict(self.entries),
            "nextPage": self.next_page,
            "updatedAt": self.updated_at or _now_iso(),
        }
        return doc

    @classmethod
    def from_dict(cls, doc: dict) -> BucketPageDocument:
        """Deserialize a Cosmos item dict into a model instance."""
        return cls(
            id=doc["id"],
            pk=doc["pk"],
            schema_version=doc.get("schemaVersion", 1),
            bucket_id=doc["bucketId"],
            page_index=doc["pageIndex"],
            entries=dict(doc.get("entries", {})),
            next_page=doc.get("nextPage"),
            updated_at=doc.get("updatedAt"),
            etag=doc.get("_etag"),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()
