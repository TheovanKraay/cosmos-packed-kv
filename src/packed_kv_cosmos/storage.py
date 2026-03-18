# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""Storage abstraction for Cosmos DB point reads/writes and patch operations.

The abstraction exists so that:
1. Unit tests can inject a lightweight fake (no live Cosmos needed).
2. Future language ports keep the same contract even though the SDK differs.

All methods are **async** — the library is designed for ``asyncio`` from the
ground up and uses the ``azure.cosmos.aio`` async SDK under the hood.
"""

from __future__ import annotations

import abc
from typing import Any, Dict, List, Optional

from azure.cosmos.aio import ContainerProxy, CosmosClient
from azure.cosmos import exceptions as cosmos_exc

# ---------------------------------------------------------------------------
# Abstract storage interface
# ---------------------------------------------------------------------------

class StorageInterface(abc.ABC):
    """Minimal point-operation contract for bucket document persistence."""

    @abc.abstractmethod
    async def read_item(self, document_id: str, partition_key: str) -> Optional[Dict[str, Any]]:
        """Read a single item by *document_id* and *partition_key*.

        Returns the item dict (including ``_etag``) or ``None`` if not found.
        """

    @abc.abstractmethod
    async def upsert_item(self, body: Dict[str, Any], partition_key: str) -> Dict[str, Any]:
        """Create or replace an item.  Returns the persisted item dict.

        The implementation MUST NOT set ``if-match`` / ``if-none-match``.
        """

    @abc.abstractmethod
    async def replace_item(
        self,
        document_id: str,
        body: Dict[str, Any],
        partition_key: str,
        if_match: str,
    ) -> Dict[str, Any]:
        """Replace an existing item with an ETag pre-condition.

        Raises :class:`ETagMismatchError` when the pre-condition fails.
        Returns the persisted item dict on success.
        """

    @abc.abstractmethod
    async def create_item(self, body: Dict[str, Any], partition_key: str) -> Dict[str, Any]:
        """Create a new item.  Raises :class:`ItemAlreadyExistsError` if the
        item already exists."""

    @abc.abstractmethod
    async def patch_item(
        self,
        document_id: str,
        partition_key: str,
        operations: List[Dict[str, Any]],
        *,
        filter_predicate: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Apply partial update operations to an existing document.

        Parameters
        ----------
        operations:
            List of patch operations, each a dict with ``op``, ``path``,
            and (for non-remove ops) ``value``.
        filter_predicate:
            Optional SQL-like condition (``from c where ...``).  If the
            predicate evaluates to false, raises
            :class:`PatchPreconditionFailedError`.

        Raises
        ------
        PatchTargetNotFoundError:
            The document does not exist (HTTP 404).
        PatchPreconditionFailedError:
            The filter predicate evaluated to false (HTTP 412).
        PatchOperationFailedError:
            A patch operation failed, e.g. ``remove`` on a non-existent
            path (HTTP 400).
        """


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

class ETagMismatchError(Exception):
    """Raised when a conditional write fails due to an ETag conflict."""


class ItemAlreadyExistsError(Exception):
    """Raised when create_item finds the item already exists (409)."""


class PatchTargetNotFoundError(Exception):
    """The document to patch does not exist (404)."""


class PatchPreconditionFailedError(Exception):
    """Patch filter predicate evaluated to false (412)."""


class PatchOperationFailedError(Exception):
    """A patch operation failed (400), e.g. remove on a non-existent path."""


# ---------------------------------------------------------------------------
# Cosmos SDK-backed implementation
# ---------------------------------------------------------------------------

class CosmosStorage(StorageInterface):
    """Concrete storage implementation backed by the ``azure-cosmos`` async SDK."""

    def __init__(self, container: ContainerProxy) -> None:
        self._container = container

    # -- Factory helpers ---------------------------------------------------

    @classmethod
    def from_connection_string(
        cls,
        connection_string: str,
        database: str,
        container: str,
    ) -> "CosmosStorage":
        """Create a :class:`CosmosStorage` from a Cosmos connection string.

        This is a synchronous factory — the underlying async
        :class:`CosmosClient` is created eagerly and its async IO
        methods are awaited at call time.
        """
        cosmos_client = CosmosClient.from_connection_string(connection_string)
        db_proxy = cosmos_client.get_database_client(database)
        container_proxy = db_proxy.get_container_client(container)
        return cls(container_proxy)

    @classmethod
    def from_credential(
        cls,
        endpoint: str,
        credential: Any,
        database: str,
        container: str,
    ) -> "CosmosStorage":
        """Create a :class:`CosmosStorage` using a token credential.

        *credential* can be any ``TokenCredential`` implementation, e.g.
        ``DefaultAzureCredential`` from the ``azure-identity`` package.

        Example::

            from azure.identity.aio import DefaultAzureCredential
            storage = CosmosStorage.from_credential(
                endpoint="https://myaccount.documents.azure.com:443/",
                credential=DefaultAzureCredential(),
                database="mydb",
                container="mycontainer",
            )
        """
        cosmos_client = CosmosClient(endpoint, credential=credential)
        db_proxy = cosmos_client.get_database_client(database)
        container_proxy = db_proxy.get_container_client(container)
        return cls(container_proxy)

    # -- StorageInterface --------------------------------------------------

    async def read_item(self, document_id: str, partition_key: str) -> Optional[Dict[str, Any]]:
        try:
            item = await self._container.read_item(item=document_id, partition_key=partition_key)
            return dict(item)
        except cosmos_exc.CosmosResourceNotFoundError:
            return None

    async def upsert_item(self, body: Dict[str, Any], partition_key: str) -> Dict[str, Any]:
        result = await self._container.upsert_item(body=body)
        return dict(result)

    async def replace_item(
        self,
        document_id: str,
        body: Dict[str, Any],
        partition_key: str,
        if_match: str,
    ) -> Dict[str, Any]:
        try:
            result = await self._container.replace_item(
                item=document_id,
                body=body,
                headers={"If-Match": if_match},
            )
            return dict(result)
        except cosmos_exc.CosmosAccessConditionFailedError:
            raise ETagMismatchError(
                f"ETag mismatch for document '{document_id}' "
                f"(expected '{if_match}')"
            )

    async def create_item(self, body: Dict[str, Any], partition_key: str) -> Dict[str, Any]:
        try:
            result = await self._container.create_item(body=body)
            return dict(result)
        except cosmos_exc.CosmosResourceExistsError:
            raise ItemAlreadyExistsError(
                f"Item with id '{body.get('id')}' already exists"
            )

    async def patch_item(
        self,
        document_id: str,
        partition_key: str,
        operations: List[Dict[str, Any]],
        *,
        filter_predicate: Optional[str] = None,
    ) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        if filter_predicate is not None:
            kwargs["filter_predicate"] = filter_predicate
        try:
            result = await self._container.patch_item(
                item=document_id,
                partition_key=partition_key,
                patch_operations=operations,
                **kwargs,
            )
            return dict(result)
        except cosmos_exc.CosmosResourceNotFoundError:
            raise PatchTargetNotFoundError(
                f"Document '{document_id}' does not exist"
            )
        except cosmos_exc.CosmosAccessConditionFailedError:
            raise PatchPreconditionFailedError(
                f"Filter predicate failed for document '{document_id}'"
            )
        except cosmos_exc.CosmosHttpResponseError as exc:
            if exc.status_code == 400:
                raise PatchOperationFailedError(
                    f"Patch operation failed for '{document_id}': {exc.message}"
                ) from exc
            raise
