# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

import os
import sys

from packed_kv_cosmos import PackedKvClient, PackedKvConfig


def main() -> None:
    """Demonstrate basic Put / Get / Delete with live diagnostics."""
    connection_string = os.environ.get("COSMOS_CONNECTION_STRING")
    database = os.environ.get("COSMOS_DATABASE")
    container = os.environ.get("COSMOS_CONTAINER")

    if not all([connection_string, database, container]):
        print(
            "Set COSMOS_CONNECTION_STRING, COSMOS_DATABASE, and "
            "COSMOS_CONTAINER environment variables.",
            file=sys.stderr,
        )
        sys.exit(1)

    config = PackedKvConfig(namespace="example", bucket_prefix_length=4)
    client = PackedKvClient.from_connection_string(
        connection_string=connection_string,
        database=database,
        container=container,
        config=config,
    )

    # Put
    put_result = client.put("user:123", b"\x01")
    print(f"Put  -> status={put_result.status}, "
          f"docs_written={put_result.diagnostics.documents_written}")

    # Get
    get_result = client.get("user:123")
    print(f"Get  -> status={get_result.status}, "
          f"value={get_result.value!r}, "
          f"docs_read={get_result.diagnostics.documents_read}")

    # Delete
    del_result = client.delete("user:123")
    print(f"Del  -> status={del_result.status}, "
          f"retries={del_result.diagnostics.retries}")

    # Verify gone
    gone = client.get("user:123")
    print(f"Gone -> status={gone.status}")


if __name__ == "__main__":
    main()
