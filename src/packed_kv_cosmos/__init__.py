# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""Packed KV CRUD Library for Azure Cosmos DB.

Public API surface:

- :class:`PackedContainerProxy` – drop-in ContainerProxy wrapper (primary API)
- :class:`PackingConfig` – configuration for PackedContainerProxy
- :class:`PackedKvClient` – high-level Put / Get / Delete client (KV API)
- :class:`PackedKvConfig` – configuration for PackedKvClient
- :class:`OperationResult` – machine-readable operation result
- :class:`Diagnostics` – structured diagnostics
- :class:`BulkResult` – aggregate bulk operation result
- :class:`BulkError` – per-item error detail for bulk operations
"""

from packed_kv_cosmos.client import PackedKvClient
from packed_kv_cosmos.config import PackedKvConfig, PackingConfig
from packed_kv_cosmos.packed_container import PackedContainerProxy
from packed_kv_cosmos.results import BulkError, BulkResult, Diagnostics, OperationResult

__all__ = [
    "PackedContainerProxy",
    "PackingConfig",
    "PackedKvClient",
    "PackedKvConfig",
    "OperationResult",
    "Diagnostics",
    "BulkResult",
    "BulkError",
]
