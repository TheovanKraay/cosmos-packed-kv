# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""Quick diagnostic: raw SDK point read vs packed proxy read latency.

Usage:
    python benchmarks/latency_diagnostic.py --account <your-cosmos-account>
    python benchmarks/latency_diagnostic.py --account <your-cosmos-account> --database mydb --container mycontainer
"""

import argparse
import asyncio
import sys
import pathlib
import time
import statistics

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

from azure.cosmos.aio import CosmosClient
from azure.identity.aio import DefaultAzureCredential
from packed_kv_cosmos import PackedContainerProxy, PackingConfig


async def main(account: str, database: str, container_name: str):
    endpoint = f"https://{account}.documents.azure.com:443/"
    cred = DefaultAzureCredential()
    client = CosmosClient(endpoint, credential=cred)

    try:
        db = client.get_database_client(database)
        container = db.get_container_client(container_name)

        # Use a known bucket doc — derive from a known item id
        # The benchmark used namespace="bench10k", prefix_length=3
        from packed_kv_cosmos.hashing import compute_entry_id, compute_bucket_id
        from packed_kv_cosmos.packing import make_root_document_id, make_partition_key

        test_item_id = "item-000000001"
        entry_id = compute_entry_id(test_item_id)
        bucket_id = compute_bucket_id(entry_id, 3)
        doc_id = make_root_document_id("bench10k", bucket_id)
        doc_pk = make_partition_key("bench10k", bucket_id)
        print(f"Bucket doc: id={doc_id}, pk={doc_pk}\n")

        # ------- Raw SDK point reads -------
        # Warm up (establish connection, TCP, TLS)
        for _ in range(10):
            await container.read_item(doc_id, partition_key=doc_pk)

        latencies = []
        for _ in range(100):
            t0 = time.perf_counter()
            await container.read_item(doc_id, partition_key=doc_pk)
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000)

        sorted_lat = sorted(latencies)
        print(f"Raw SDK point reads (100 reads, after warmup):")
        print(f"  Mean:   {statistics.mean(latencies):.1f}ms")
        print(f"  Median: {statistics.median(latencies):.1f}ms")
        print(f"  P95:    {sorted_lat[95]:.1f}ms")
        print(f"  Min:    {min(latencies):.1f}ms")
        print(f"  Max:    {max(latencies):.1f}ms")

        # ------- Packed proxy reads -------
        proxy = PackedContainerProxy(
            container=container,
            config=PackingConfig(
                namespace="bench10k",
                bucket_prefix_length=3,
                max_entries_per_doc=200,
            ),
        )

        # Warm up proxy
        for _ in range(10):
            await proxy.read_item("item-000000001")

        proxy_latencies = []
        for _ in range(100):
            t0 = time.perf_counter()
            await proxy.read_item("item-000000001")
            t1 = time.perf_counter()
            proxy_latencies.append((t1 - t0) * 1000)

        sorted_plat = sorted(proxy_latencies)
        print(f"\nProxy point reads (100 reads, after warmup):")
        print(f"  Mean:   {statistics.mean(proxy_latencies):.1f}ms")
        print(f"  Median: {statistics.median(proxy_latencies):.1f}ms")
        print(f"  P95:    {sorted_plat[95]:.1f}ms")
        print(f"  Min:    {min(proxy_latencies):.1f}ms")
        print(f"  Max:    {max(proxy_latencies):.1f}ms")

        overhead = statistics.median(proxy_latencies) - statistics.median(latencies)
        print(f"\nProxy overhead: {overhead:.1f}ms (median)")

    finally:
        await client.close()
        await cred.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Raw SDK vs packed proxy point read latency diagnostic",
    )
    parser.add_argument(
        "--account", required=True,
        help="Cosmos DB account name (e.g. my-cosmos-account)",
    )
    parser.add_argument(
        "--database", default="bin-pack-test",
        help="Database name (default: bin-pack-test)",
    )
    parser.add_argument(
        "--container", default="bulk-benchmark-10k",
        help="Container name (default: bulk-benchmark-10k)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.account, args.database, args.container))
