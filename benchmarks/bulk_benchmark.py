# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License. See LICENSE file in the project root for details.

"""Bulk ingestion + random-read benchmark for packed_kv_cosmos.

Uses PackedContainerProxy with DefaultAzureCredential against a live
Cosmos DB container.  Supports multiple scale tiers via command-line
argument.

Usage:
    python benchmarks/bulk_benchmark.py 10k
    python benchmarks/bulk_benchmark.py 1m
    python benchmarks/bulk_benchmark.py 10m
    python benchmarks/bulk_benchmark.py 300m

The script will:
  1. Create (or reuse) a container with the configured RU throughput.
  2. Bulk-upsert items (streamed in batches for large runs), measuring
     wall-clock time and throughput with periodic progress reports.
  3. Perform random point reads and report latency stats.
  4. Leave all data in place for manual inspection.
"""

import argparse
import asyncio
import random
import statistics
import time
from dataclasses import dataclass

from azure.cosmos.aio import CosmosClient
from azure.identity.aio import DefaultAzureCredential

# ---- add src/ to path so we can import the library directly ----
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

from packed_kv_cosmos import PackedContainerProxy, PackingConfig, BulkResult


# ==================================================================
# Scale profiles
# ==================================================================

@dataclass(frozen=True)
class BenchmarkProfile:
    name: str
    item_count: int
    container_name: str
    throughput_ru: int
    namespace: str
    bucket_prefix_length: int
    max_entries_per_doc: int
    max_concurrency: int
    batch_size: int              # items per bulk_upsert_items call
    random_read_count: int
    progress_interval: int       # print progress every N items


PROFILES: dict[str, BenchmarkProfile] = {
    "10k": BenchmarkProfile(
        name="10K",
        item_count=10_000,
        container_name="bulk-benchmark-10k",
        throughput_ru=10_000,
        namespace="bench10k",
        bucket_prefix_length=3,   # 4,096 buckets
        max_entries_per_doc=200,
        max_concurrency=100,
        batch_size=10_000,        # single batch
        random_read_count=200,
        progress_interval=10_000,
    ),
    "1m": BenchmarkProfile(
        name="1M",
        item_count=1_000_000,
        container_name="bulk-benchmark-1m",
        throughput_ru=100_000,
        namespace="bench1m",
        bucket_prefix_length=4,   # 65,536 buckets
        max_entries_per_doc=200,
        max_concurrency=200,
        batch_size=1_000_000,     # single batch — ~100MB, fits in 16GB RAM
        random_read_count=500,
        progress_interval=100_000,
    ),
    "10m": BenchmarkProfile(
        name="10M",
        item_count=10_000_000,
        container_name="bulk-benchmark-10m",
        throughput_ru=100_000,
        namespace="bench10m",
        bucket_prefix_length=4,   # 65,536 buckets (~153 items/bucket avg)
        max_entries_per_doc=200,
        max_concurrency=200,
        batch_size=10_000_000,    # single batch — ~1GB RAM, avoids 409 overhead on subsequent waves
        random_read_count=500,
        progress_interval=500_000,
    ),
    "300m": BenchmarkProfile(
        name="300M",
        item_count=300_000_000,
        container_name="bulk-benchmark-300m",
        throughput_ru=100_000,
        namespace="bench300m",
        bucket_prefix_length=4,   # 65,536 buckets — 153 entries/bucket/wave → 17 patches each → 1.1M calls/wave saturates RU
        max_entries_per_doc=5_000,
        max_concurrency=190,      # ~95% RU saturation (200 hit ~100% → 429 retries)
        batch_size=10_000_000,    # 30 waves of 10M — many patches per bucket keeps the pipe full
        random_read_count=1_000,
        progress_interval=10_000_000,
    ),
}

# ==================================================================
# Shared config
# ==================================================================

PARTITION_KEY_PATH = "/pk"


# ==================================================================
# Helpers
# ==================================================================

def generate_batch(start: int, count: int) -> list[dict]:
    """Generate a batch of items with deterministic ids and small payloads."""
    width = 9  # enough digits for 300M
    return [
        {
            "id": f"item-{i:0{width}d}",
            "pk": f"item-{i:0{width}d}",
            "payload": f"data-{i:0{width}d}",
            "seq": i,
        }
        for i in range(start, start + count)
    ]


async def ensure_container(database_client, name: str, pk_path: str, throughput: int):
    """Create the container if it doesn't exist; return its client."""
    try:
        container = database_client.get_container_client(name)
        await container.read()
        print(f"  Container '{name}' already exists — reusing.")
    except Exception:
        from azure.cosmos import PartitionKey
        container = await database_client.create_container(
            id=name,
            partition_key=PartitionKey(path=pk_path),
            offer_throughput=throughput,
        )
        print(f"  Container '{name}' created with {throughput:,} RU/s.")
    return container


# ==================================================================
# Main benchmark
# ==================================================================

async def run_benchmark(p: BenchmarkProfile, *, account: str, database: str) -> None:
    endpoint = f"https://{account}.documents.azure.com:443/"
    credential = DefaultAzureCredential()
    cosmos = CosmosClient(endpoint, credential=credential)

    try:
        db = cosmos.get_database_client(database)

        # ---- 1. Ensure container ----
        print(f"\n{'='*60}")
        print(f"  Benchmark: {p.item_count:,} items ({p.name}) → {p.container_name}")
        print(f"  Account:   {account}")
        print(f"  Database:  {database}")
        print(f"  Throughput: {p.throughput_ru:,} RU/s")
        print(f"  Batch size: {p.batch_size:,}")
        print(f"  Prefix len: {p.bucket_prefix_length} ({16**p.bucket_prefix_length:,} buckets)")
        print(f"  Concurrency: {p.max_concurrency}")
        print(f"{'='*60}\n")

        container = await ensure_container(db, p.container_name, PARTITION_KEY_PATH, p.throughput_ru)

        proxy = PackedContainerProxy(
            container=container,
            config=PackingConfig(
                namespace=p.namespace,
                bucket_prefix_length=p.bucket_prefix_length,
                max_entries_per_doc=p.max_entries_per_doc,
            ),
        )

        # ---- 2. Bulk ingest in waves ----
        print("Starting bulk ingestion...\n")
        total_succeeded = 0
        total_failed = 0
        all_errors: list = []
        t0 = time.perf_counter()
        last_progress = 0

        for wave_start in range(0, p.item_count, p.batch_size):
            wave_count = min(p.batch_size, p.item_count - wave_start)
            batch = generate_batch(wave_start, wave_count)

            # Use patch_first on waves after the first (buckets already exist)
            use_patch_first = wave_start > 0
            result: BulkResult = await proxy.bulk_upsert_items(
                batch, max_concurrency=p.max_concurrency,
                patch_first=use_patch_first,
            )
            total_succeeded += result.succeeded
            total_failed += result.failed
            if result.errors:
                all_errors.extend(result.errors[:10])

            # Progress reporting
            items_so_far = wave_start + wave_count
            if items_so_far - last_progress >= p.progress_interval or items_so_far >= p.item_count:
                elapsed_so_far = time.perf_counter() - t0
                rate = total_succeeded / elapsed_so_far if elapsed_so_far > 0 else 0
                pct = items_so_far / p.item_count * 100
                eta_secs = (p.item_count - items_so_far) / rate if rate > 0 else 0
                eta_min = eta_secs / 60
                print(
                    f"  [{pct:5.1f}%] {items_so_far:>13,} / {p.item_count:,} | "
                    f"{total_succeeded:,} ok, {total_failed:,} fail | "
                    f"{rate:,.0f} items/sec | "
                    f"ETA {eta_min:.1f}min"
                )
                last_progress = items_so_far

        t1 = time.perf_counter()
        elapsed = t1 - t0
        throughput = total_succeeded / elapsed if elapsed > 0 else 0

        print(f"\n  Ingestion complete:")
        print(f"    Succeeded:  {total_succeeded:,}")
        print(f"    Failed:     {total_failed:,}")
        print(f"    Wall time:  {elapsed:.1f}s ({elapsed/60:.1f}min)")
        print(f"    Throughput: {throughput:,.0f} items/sec")
        if all_errors:
            print(f"    First errors:")
            for err in all_errors[:10]:
                print(f"      - {err.item_id}: {err.error}")

        # ---- 3. Random reads ----
        print(f"\nStarting {p.random_read_count} random point reads...")
        sample_ids = [f"item-{random.randint(0, p.item_count - 1):09d}" for _ in range(p.random_read_count)]

        read_latencies: list[float] = []
        read_errors = 0

        for item_id in sample_ids:
            rt0 = time.perf_counter()
            try:
                doc = await proxy.read_item(item_id)
                rt1 = time.perf_counter()
                read_latencies.append((rt1 - rt0) * 1000)  # ms
                assert doc["id"] == item_id, f"Mismatch: expected {item_id}, got {doc['id']}"
            except Exception as exc:
                rt1 = time.perf_counter()
                read_latencies.append((rt1 - rt0) * 1000)
                read_errors += 1
                print(f"  READ ERROR for {item_id}: {exc}")

        total_read_time = sum(read_latencies)
        print(f"\n  Read results ({p.random_read_count} reads):")
        print(f"    Errors:       {read_errors}")
        print(f"    Total time:   {total_read_time:.1f}ms")
        if read_latencies:
            sorted_lat = sorted(read_latencies)
            print(f"    Mean latency: {statistics.mean(read_latencies):.1f}ms")
            print(f"    Median:       {statistics.median(read_latencies):.1f}ms")
            print(f"    P95:          {sorted_lat[int(len(sorted_lat)*0.95)]:.1f}ms")
            print(f"    P99:          {sorted_lat[int(len(sorted_lat)*0.99)]:.1f}ms")
            print(f"    Min:          {min(read_latencies):.1f}ms")
            print(f"    Max:          {max(read_latencies):.1f}ms")

        # ---- 4. Summary ----
        print(f"\n{'='*60}")
        print(f"  SUMMARY — {p.name}")
        print(f"{'='*60}")
        print(f"  Items ingested: {total_succeeded:,} / {p.item_count:,}")
        print(f"  Ingest time:    {elapsed:.1f}s = {elapsed/60:.1f}min ({throughput:,.0f} items/sec)")
        if read_latencies:
            print(f"  Read latency:   {statistics.mean(read_latencies):.1f}ms avg, "
                  f"{statistics.median(read_latencies):.1f}ms median")
        print(f"  Data retained:  YES (container: {p.container_name})")
        print(f"{'='*60}\n")

    finally:
        await cosmos.close()
        await credential.close()


# ==================================================================
# CLI
# ==================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Bulk ingestion benchmark for packed_kv_cosmos",
    )
    parser.add_argument(
        "profile",
        choices=list(PROFILES.keys()),
        help="Scale profile: 10k, 1m, 10m, or 300m",
    )
    parser.add_argument(
        "--account",
        required=True,
        help="Cosmos DB account name (e.g. my-cosmos-account)",
    )
    parser.add_argument(
        "--database",
        default="bin-pack-test",
        help="Database name (default: bin-pack-test)",
    )
    args = parser.parse_args()
    asyncio.run(run_benchmark(PROFILES[args.profile], account=args.account, database=args.database))


if __name__ == "__main__":
    main()
