# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""Operation result types and diagnostics.

Every public operation returns an :class:`OperationResult` with a deterministic
status string and structured diagnostics suitable for both human developers and
automation agents.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Status constants (string enum values)
# ---------------------------------------------------------------------------

STATUS_SUCCESS = "success"
STATUS_NOT_FOUND = "notFound"
STATUS_CONFLICT_RETRY_EXHAUSTED = "conflictRetryExhausted"
STATUS_INVALID_INPUT = "invalidInput"
STATUS_TOO_LARGE = "tooLarge"
STATUS_ERROR = "error"

# Stable error codes for agent consumption
ERR_CONFLICT_RETRY_EXHAUSTED = "ERR_CONFLICT_RETRY_EXHAUSTED"
ERR_INVALID_INPUT = "ERR_INVALID_INPUT"
ERR_TOO_LARGE = "ERR_TOO_LARGE"
ERR_STORAGE = "ERR_STORAGE"


# ---------------------------------------------------------------------------
# Bulk operation results
# ---------------------------------------------------------------------------

@dataclass
class BulkError:
    """Error detail for a single item in a bulk operation."""

    item_id: str
    error: str


@dataclass
class BulkResult:
    """Aggregate result of a bulk operation.

    Attributes
    ----------
    succeeded:
        Number of items successfully written.
    failed:
        Number of items that could not be written.
    errors:
        Per-item error details for failed items.
    """

    succeeded: int = 0
    failed: int = 0
    errors: list[BulkError] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

@dataclass
class Diagnostics:
    """Structured diagnostics returned with every operation result."""

    documents_read: int = 0
    documents_written: int = 0
    retries: int = 0
    used_overflow: bool = False
    pages_scanned: int = 0


# ---------------------------------------------------------------------------
# OperationResult
# ---------------------------------------------------------------------------

@dataclass
class OperationResult:
    """Machine-readable result returned by Put / Get / Delete.

    Attributes
    ----------
    status:
        One of the ``STATUS_*`` constants.
    value:
        Present only for a successful Get (decoded bytes).
    error_code:
        Stable short code (e.g. ``ERR_CONFLICT_RETRY_EXHAUSTED``).
    message:
        Human-readable message (optional).
    diagnostics:
        Structured diagnostics about the operation.
    """

    status: str
    value: Optional[bytes] = None
    error_code: Optional[str] = None
    message: Optional[str] = None
    diagnostics: Diagnostics = field(default_factory=Diagnostics)
