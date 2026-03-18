# Benchmarking Guide

## Useful Cosmos DB Queries

After running a benchmark, use these queries in the Azure Portal **Data Explorer** to inspect physical vs logical item counts.

### Count physical bucket documents

Each physical Cosmos document is a "bucket" that holds many packed logical items.

```sql
SELECT VALUE COUNT(1) FROM c WHERE c.docType = 'bucketRoot'
```

To scope to a specific benchmark namespace:

```sql
SELECT VALUE COUNT(1) FROM c
WHERE c.docType = 'bucketRoot'
  AND STARTSWITH(c.id, 'bench10k:')
```

### Count logical (packed) items

Each bucket document has an `entries` object where every key is one logical item. Use this query to sum them across all buckets:

```sql
SELECT
    COUNT(1) AS physicalDocs,
    SUM(ARRAY_LENGTH(ObjectToArray(c.entries))) AS logicalItems
FROM c
WHERE c.docType = 'bucketRoot'
```

To scope to a specific namespace:

```sql
SELECT
    COUNT(1) AS physicalDocs,
    SUM(ARRAY_LENGTH(ObjectToArray(c.entries))) AS logicalItems
FROM c
WHERE c.docType = 'bucketRoot'
  AND STARTSWITH(c.id, 'bench10k:')
```

### Packing ratio

The ratio of logical items to physical documents shows how effective the packing is:

```sql
SELECT
    COUNT(1) AS physicalDocs,
    SUM(ARRAY_LENGTH(ObjectToArray(c.entries))) AS logicalItems,
    SUM(ARRAY_LENGTH(ObjectToArray(c.entries))) / COUNT(1) AS avgItemsPerBucket
FROM c
WHERE c.docType = 'bucketRoot'
```

### Bucket size distribution

See how entries are distributed across buckets (useful for detecting hot buckets):

```sql
SELECT TOP 20
    c.bucketId,
    ARRAY_LENGTH(ObjectToArray(c.entries)) AS entryCount
FROM c
WHERE c.docType = 'bucketRoot'
ORDER BY ARRAY_LENGTH(ObjectToArray(c.entries)) DESC
```

---

## Running the Benchmark

The benchmark requires a live Azure Cosmos DB account. You must provide your own
account name via the `--account` flag.

### Authentication

The benchmark authenticates using
[`DefaultAzureCredential`](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.aio.defaultazurecredential)
from the `azure-identity` package. This supports multiple authentication methods
automatically. The simplest way to authenticate for local runs:

```bash
# Install the Azure CLI if you haven't already
# https://learn.microsoft.com/en-us/cli/azure/install-azure-cli

az login
```

Your Azure AD identity must have the **Cosmos DB Built-in Data Contributor** role
on the target Cosmos DB account. See
[Configure role-based access control](https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-setup-rbac)
for details.

Alternatively, you can set environment variables for a service principal:

```bash
export AZURE_TENANT_ID="<your-tenant-id>"
export AZURE_CLIENT_ID="<your-client-id>"
export AZURE_CLIENT_SECRET="<your-client-secret>"
```

### Running

```bash
python benchmarks/bulk_benchmark.py 10k  --account <your-cosmos-account>
python benchmarks/bulk_benchmark.py 1m   --account <your-cosmos-account>
python benchmarks/bulk_benchmark.py 10m  --account <your-cosmos-account>
python benchmarks/bulk_benchmark.py 300m --account <your-cosmos-account>

# Optional: specify a different database name (default: bin-pack-test)
python benchmarks/bulk_benchmark.py 10k --account <your-cosmos-account> --database mydb
```

### Scale Profiles

| Profile | Items | Container | RU/s | Prefix | Buckets | Concurrency | Batch Size |
|---------|-------|-----------|------|--------|---------|-------------|------------|
| `10k` | 10,000 | `bulk-benchmark-10k` | 10,000 | 3 | 4,096 | 100 | 10K (single) |
| `1m` | 1,000,000 | `bulk-benchmark-1m` | 100,000 | 4 | 65,536 | 200 | 1M (single) |
| `10m` | 10,000,000 | `bulk-benchmark-10m` | 100,000 | 4 | 65,536 | 200 | 10M (single) |
| `300m` | 300,000,000 | `bulk-benchmark-300m` | 100,000 | 4 | 65,536 | 190 | 10M (30 waves, hybrid) |

### Configuration

| Parameter | How to set | Description |
|-----------|------------|-------------|
| `--account` | CLI flag (required) | Cosmos DB account name |
| `--database` | CLI flag (optional) | Database name (default: `bin-pack-test`) |

Per-profile settings are defined in the `PROFILES` dict.  Each profile controls:
container name, throughput, item count, batch size, concurrency, bucket prefix length,
max entries per doc, random read count, and progress interval.

### Sample Output

```
============================================================
  Benchmark: 10,000 items → bulk-benchmark-10k
  Account:   <your-cosmos-account>
  Database:  bin-pack-test
  Throughput: 10,000 RU/s
============================================================

  Ingestion complete:
    Succeeded:  10,000
    Failed:     0
    Wall time:  13.85s
    Throughput: 722 items/sec

  Read results (200 reads):
    Errors:       0
    Mean latency: 114.8ms
    Median:       109.7ms
    P95:          138.4ms
    P99:          186.6ms

============================================================
  SUMMARY
============================================================
  Items ingested: 10,000 / 10,000
  Ingest time:    13.85s (722 items/sec)
  Read latency:   114.8ms avg, 109.7ms median
  Data retained:  YES (container: bulk-benchmark-10k)
============================================================
```

### Data Retention

The benchmark does **not** delete data after completion. Use the queries above to inspect the physical bucket documents and logical item counts in the Azure Portal.

---

## Benchmark Results

Results from live benchmarks against an Azure Cosmos DB account (hub region: **UK South**). Client machine located in the UK. Python 3.12, async `azure-cosmos` SDK (Gateway mode), `DefaultAzureCredential` (Entra ID).

### Ingestion throughput

| Scale | Items | Container RU/s | Prefix length | Buckets | Time | Throughput | Failures |
|------:|------:|---------------:|--------------:|--------:|-----:|-----------:|---------:|
| **10K** | 10,000 | 10,000 | 3 | 4,096 | 28.0s | 357 items/sec | 0 |
| **1M** | 1,000,000 | 100,000 | 4 | 65,536 | 190.9s (3.2 min) | 5,239 items/sec | 0 |
| **10M** | 10,000,000 | 100,000 | 4 | 65,536 | 736.2s (12.3 min) | 13,583 items/sec | 0 |
| **300M** | ~74M (partial) | 100,000 | 4 | 65,536 | -- | ~15-17K items/sec | 0 |

### Point read latency

Random point reads via `PackedContainerProxy.read_item()` after ingestion:

| Scale | Reads | Mean | Median | P95 | P99 | Min | Max |
|------:|------:|-----:|-------:|----:|----:|----:|----:|
| **10K** | 200 | 23.8ms | 20.8ms | 33.2ms | 92.6ms | 15.4ms | 187.1ms |
| **1M** | 500 | 30.3ms | 26.5ms | -- | -- | 15.3ms | -- |
| **10M** | 500 | 25.1ms | 21.8ms | 38.6ms | 66.0ms | 15.7ms | 79.9ms |

### Proxy overhead analysis

A [latency diagnostic](latency_diagnostic.py) isolating raw Cosmos SDK point reads from packed proxy reads confirms **zero library overhead**:

| Method | Mean | Median | P95 | Min |
|--------|-----:|-------:|----:|----:|
| Raw SDK point read | 40.4ms | 34.0ms | 82.1ms | 32.1ms |
| Packed proxy read | 39.0ms | 34.1ms | 81.6ms | 31.9ms |
| **Overhead** | -- | **0.0ms** | -- | -- |

The ~34ms floor is inherent to the Python `azure-cosmos` SDK's **Gateway mode** (HTTP). The Python SDK does not support Direct/TCP mode -- the .NET SDK with Direct mode typically achieves 5-10ms point reads from the same region.

### 300M partial run details

The 300M benchmark was run with 30 waves of 10M items each (batch_size=10M, concurrency=190). The run was stopped after ~74M items were inserted across all 65,536 buckets (~1,126 entries/bucket average). Per-wave throughput:

| Wave | Strategy | Throughput | Failures |
|-----:|----------|------------|----------|
| 1 | `create_item` (1 call/bucket) | 17,193 items/sec | 0 |
| 2 | read-merge-upsert (2 calls/bucket) | 15,032 items/sec | 0 |
| 3+ | read-merge-upsert | ~13-15K items/sec | 0 |

### Notes

- All benchmark data is retained in the containers for inspection.
- Ingestion uses `bulk_upsert_items` with cross-bucket parallelism via `asyncio.Semaphore`.
- **Hybrid bulk strategy**: The library automatically selects the cheapest write path per bucket:
  - **Create** (wave 1, empty buckets): single `create_item` call
  - **Patch** (≤9 entries to add): single atomic patch operation (1 Cosmos call)
  - **Read-merge-upsert** (≥10 entries): read existing bucket, merge entries in memory, upsert (2 Cosmos calls) -- avoids the `ceil(N/9)` sequential-patch bottleneck
- **Retry with exponential backoff**: Transient errors (429, 449, 500, 503, connection errors) are retried indefinitely by default (configurable via `max_retries`). Bulk jobs slow down under pressure but never drop items.
- The 10M test achieved 13,583 items/sec at 100K RU/s. Throughput scales with RU provisioning.
- The 10M read latency (21.8ms median) was the best observed, benefiting from warmed connections and SDK connection pool reuse.
- Per-item throughput increases with denser packing: 10M items in 65,536 buckets (~153 items/bucket) is 3.4× faster per item than 1M items in 65,536 buckets (~15 items/bucket), because the fixed per-bucket Cosmos call overhead is amortised over more items.
- The 300M run at concurrency=190 targets ~95% normalized RU saturation. At concurrency=200, the account hit ~100% → 429 retry storms.
