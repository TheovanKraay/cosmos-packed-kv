# cosmos-packed-kv

> **SAMPLE CODE** 
> This project is provided as a **sample only**, under the
> [MIT License](LICENSE). It is **not** an officially supported Microsoft /
> Azure product and carries **no warranties or guarantees**. Use at your own
> risk. See [LICENSE](LICENSE) for the full terms.

A sample library for Azure Cosmos DB that **packs many small logical items into fewer physical Cosmos documents** ("bin documents"), dramatically reducing cost and storage overhead for massive key spaces with tiny per-item payloads.

Built on the **async** Azure Cosmos DB Python SDK (`azure.cosmos.aio`). All public methods are `async` coroutines -- use `await` for every call.

## Key Design Guarantees

1. **Every `read_item` is a single Cosmos point read.** The bin document `id` and partition key are derived deterministically from the caller-supplied arguments -- no scanning, no fan-out.
2. **Bin documents have bounded, configurable size.** The hash algorithm distributes items uniformly so that no single bin exceeds `max_entries_per_doc`. There are no overflow pages.
3. **Server-side conflict-free writes via Cosmos patch.** All writes use partial document update (patch) operations -- no read-before-write, no ETag conflicts. Concurrent writes to different entries in the same bin are serialised automatically by the server.
4. **Fully async.** Built on `azure.cosmos.aio` -- every I/O method is an `async` coroutine, ready for high-throughput `asyncio` workloads.

## Two API Variants

| API | Class | Use Case |
|-----|-------|----------|
| **ContainerProxy-Compatible** (Primary) | `PackedContainerProxy` | Familiar async `ContainerProxy`-style API (`create_item`, `read_item`, `bulk_upsert_items`, etc.) with transparent bin packing |
| **KV Interface** (Lower-Level) | `PackedKvClient` | Explicit async key→bytes CRUD (Put/Get/Delete/`bulk_put`) with machine-readable `OperationResult` diagnostics |

The **primary** API is `PackedContainerProxy`. It wraps an async Cosmos `ContainerProxy` (`azure.cosmos.aio`) and exposes familiar `async` methods (`create_item`, `read_item`, `upsert_item`, `replace_item`, `delete_item`, `query_items`, `read_all_items`). Routing is by item `id` only -- there is no caller-supplied `partition_key` parameter.

## How It Works

### Bucket routing

Both APIs use id-only routing to guarantee point reads and bounded bins:

```
bucketId  = sha256(item_id)[0:prefix]   # PackedContainerProxy
bucketId  = sha256(key)[0:prefix]        # PackedKvClient
entryId   = sha256(item_id)              # or sha256(key)
```

Both the bin document `id` and `pk` are deterministic: `{namespace}:{bucketId}`. This means every read resolves to exactly **one Cosmos point read**.

### Bin document structure

The resulting Cosmos document looks like this:

```jsonc
{
  "id": "default:a7f3",                         // namespace:bucketId
  "pk": "default:a7f3",                         // partition key (same as id)
  "docType": "bucketRoot",
  "schemaVersion": 1,
  "bucketId": "a7f3",
  "entries": {
    "de5b4d666d98d687...": {"id": "sensor:001", "temp": 72.5, "category": "env"},
    "7d588994f607a971...": {"id": "sensor:042", "temp": 68.1, "category": "env"}
  },
  "tailPage": null,
  "pageCount": 0,
  "updatedAt": "2026-02-25T12:00:00+00:00"
}
```

Each entry key is the full SHA-256 hex of the logical item `id`. In the ContainerProxy API, values are **full JSON document dicts** (not opaque bytes), so items you get back look identical to what you stored.

A single Cosmos item can hold hundreds of these entries. When a bin reaches `max_entries_per_doc`, further writes are rejected with a clear error suggesting a longer `bucket_prefix_length`.

## Sizing for Scale

Two configuration parameters control bin density, and they must be tuned together based on the expected volume and size of your data:

| Parameter | What it controls | Default |
|-----------|------------------|---------|
| `bucket_prefix_length` | Number of bins = 16^n. More bins → fewer items per bin. | 4 |
| `max_entries_per_doc` | Hard cap on entries in a single bin document. Writes that exceed it **fail** with a clear error. | 200 |

### How the two parameters interact

- **`bucket_prefix_length`** determines the *statistical* bin density. With `n` hex chars there are 16^n possible bins. Items are distributed uniformly across bins by SHA-256, so the *average* entries per bin ≈ total_items / 16^n.
- **`max_entries_per_doc`** is a *hard safety net*. Even with good hash distribution a few bins will have more than the average (birthday-paradox tail). If any bin exceeds this limit the write is rejected -- a signal that you need a longer prefix.

Setting the prefix too short means bins fill up and writes start failing.  
Setting it too long means you create billions of nearly-empty bin documents and lose the storage savings that packing provides.

### Use `recommended_prefix_length()` to pick the right prefix

```python
from packed_kv_cosmos import PackedKvConfig

prefix = PackedKvConfig.recommended_prefix_length(
    total_expected_items=1_000_000_000,  # 1 billion items
    max_items_per_bin=200,               # matches max_entries_per_doc
)
print(prefix)  # 6
```

The helper applies a 2× safety factor so that even the fullest bins stay well below `max_entries_per_doc`.

### Sizing reference table

| Expected items | Recommended prefix | Bins (16^n) | Avg items / bin | Suggested `max_entries_per_doc` |
|---:|:---:|---:|---:|---:|
| 1,000 | 1 | 16 | 62 | 200 |
| 10,000 | 2 | 256 | 39 | 200 |
| 100,000 | 3 | 4,096 | 24 | 200 |
| 1,000,000 | 4 | 65,536 | 15 | 200 |
| 10,000,000 | 5 | 1,048,576 | 10 | 200 |
| 100,000,000 | 5 | 1,048,576 | 95 | 200 |
| 1,000,000,000 | 6 | 16,777,216 | 60 | 200 |
| 10,000,000,000 | 7 | 268,435,456 | 37 | 200 |
| 100,000,000,000 | 8 | 4,294,967,296 | 23 | 200 |
| 500,000,000,000 | 9 | 68,719,476,736 | 7 | 200 |

### The 2 MB Cosmos document limit

A single Cosmos DB item cannot exceed **2 MB**. The bin document overhead (id, pk, metadata) is small (~200 bytes), so the practical limit depends on the size of your packed entries:

| Avg item size | Max entries before 2 MB |
|---:|---:|
| 100 bytes | ~18,000 |
| 500 bytes | ~3,600 |
| 1 KB | ~1,800 |
| 5 KB | ~360 |
| 10 KB | ~180 |

If your entries are large, set `max_entries_per_doc` lower to stay within the 2 MB ceiling. For very large items (>10 KB each), packing provides diminishing benefit -- consider using standard Cosmos documents directly.

> **Note:** `max_doc_bytes_approx` (default 1.8 MB) is reserved for future byte-size enforcement. Currently only `max_entries_per_doc` is enforced at write time.

### Rules of thumb

1. **Start with `recommended_prefix_length()`** -- it accounts for a 2× safety margin.
2. **Set `max_entries_per_doc`** to the lesser of 200 or `floor(1,800,000 / avg_item_bytes)`. This keeps bin documents well under the 2 MB Cosmos limit.
3. **If you see `TOO_LARGE` / `RuntimeError` in production**, increase `prefix_length` by 1 (16× more bins). This is a zero-downtime change -- new writes route to new bins, existing bins remain readable.
4. **If you only have a few thousand items**, prefix=1 or 2 is fine. Don't over-shard.
5. **For 100 billion+ items**, prefix=8+ is needed. At this scale each extra prefix character adds 16× more bins.

## Quickstart (Python)

### 1) Install

```bash
pip install -e .
```

### 2) Create a Cosmos container

This library expects a container dedicated to packed bin documents.

Minimum requirements:
- **NoSQL API** container
- **Partition key path**: `/pk`

### 3a) PackedContainerProxy usage (primary)

```python
import asyncio
from azure.cosmos.aio import CosmosClient
from azure.identity.aio import DefaultAzureCredential
from packed_kv_cosmos import PackedContainerProxy, PackingConfig

async def main():
    # Connect to Cosmos with Entra ID (recommended)
    credential = DefaultAzureCredential()
    cosmos = CosmosClient("<account-endpoint>", credential=credential)
    # Or with a connection string:
    # cosmos = CosmosClient("<connection-string>")
    database = cosmos.get_database_client("<db>")
    container = database.get_container_client("<container>")

    # Wrap it -- familiar methods, transparent packing
    proxy = PackedContainerProxy(
        container=container,
        config=PackingConfig(
            namespace="myapp",
            bucket_prefix_length=8,          # scale for large datasets
        ),
    )

    # Use it exactly like a normal async ContainerProxy
    await proxy.create_item({"id": "sensor:001", "temp": 72.5, "category": "env"})
    await proxy.create_item({"id": "sensor:042", "temp": 68.1, "category": "env"})

    # Read back -- always a single point read (routed by item id)
    item = await proxy.read_item("sensor:001")
    print(item)  # {"id": "sensor:001", "temp": 72.5, "category": "env"}

    # Upsert (create or replace)
    await proxy.upsert_item({"id": "sensor:001", "temp": 73.0, "category": "env"})

    # Replace (must exist)
    await proxy.replace_item("sensor:001", {"id": "sensor:001", "temp": 74.0, "category": "env"})

    # Delete
    await proxy.delete_item("sensor:001")

    # Query -- scans all bins, filters client-side
    results = await proxy.query_items("SELECT * FROM c WHERE c.temp > 70")

    # Read all items across all bins
    all_items = await proxy.read_all_items()

asyncio.run(main())
```

### 3b) PackedKvClient usage (lower-level KV)

```python
import asyncio
import json
from azure.identity.aio import DefaultAzureCredential
from packed_kv_cosmos import PackedKvClient, PackedKvConfig

async def main():
    config = PackedKvConfig(
        namespace="myapp",
        bucket_prefix_length=8,
        max_entries_per_doc=200,
    )

    # Entra ID authentication (recommended)
    client = PackedKvClient.from_credential(
        endpoint="<account-endpoint>",
        credential=DefaultAzureCredential(),
        database="<db>",
        container="<container>",
        config=config,
    )
    # Or with a connection string:
    # client = PackedKvClient.from_connection_string(
    #     connection_string="<cosmos-connection-string>",
    #     database="<db>",
    #     container="<container>",
    #     config=config,
    # )

    # Store/retrieve raw bytes
    res = await client.put("sensor:001", json.dumps({"temp": 72.5}).encode())
    res = await client.get("sensor:001")
    print(json.loads(res.value))  # {"temp": 72.5}

    await client.delete("sensor:001")

asyncio.run(main())
```

### 3c) Bulk ingestion

For loading large numbers of items at once, both APIs provide bulk methods that group items by bucket and write them concurrently. New buckets are created in a single `create_item` call (all entries at once). Existing buckets are updated via batched patch operations (up to 9 entries per patch call, respecting the Cosmos 10-operation-per-patch limit). Cross-bucket writes run in parallel using `asyncio.gather` with a configurable concurrency limit.

```python
import asyncio
from azure.cosmos.aio import CosmosClient
from azure.identity.aio import DefaultAzureCredential
from packed_kv_cosmos import PackedContainerProxy, PackingConfig, BulkResult

async def main():
    credential = DefaultAzureCredential()
    cosmos = CosmosClient("<account-endpoint>", credential=credential)
    database = cosmos.get_database_client("<db>")
    container = database.get_container_client("<container>")

    proxy = PackedContainerProxy(
        container=container,
        config=PackingConfig(namespace="myapp", bucket_prefix_length=8),
    )

    # Bulk upsert -- items are grouped by bucket automatically
    items = [{"id": f"sensor:{i:06d}", "temp": 70 + i * 0.1} for i in range(10_000)]
    result: BulkResult = await proxy.bulk_upsert_items(items, max_concurrency=50)

    print(f"Succeeded: {result.succeeded}, Failed: {result.failed}")
    for err in result.errors:
        print(f"  {err.item_id}: {err.error}")

asyncio.run(main())
```

The lower-level KV client has an equivalent `bulk_put`:

```python
result = await client.bulk_put(
    [("key1", b"val1"), ("key2", b"val2"), ...],
    max_concurrency=50,
)
```

**How it works under the hood:**
1. Items are grouped by `bucket_id` (derived from `sha256(item_id)[:prefix]`).
2. For each bucket, the library tries `create_item` with all entries -- if the bucket is new, this writes everything in **one** Cosmos call.
3. If the bucket already exists (409), entries are patched in batches of 9 (Cosmos allows max 10 operations per patch; 1 is reserved for the `updatedAt` timestamp).
4. Buckets are processed in parallel with `asyncio.Semaphore(max_concurrency)`.

**Streaming large datasets with `patch_first`:**

When ingesting datasets too large to fit in a single batch (e.g. hundreds of millions of items), you can stream in waves. Use `patch_first=True` on subsequent waves to avoid wasted 409 responses on buckets that already exist:

```python
WAVE_SIZE = 10_000_000
for i, start in enumerate(range(0, total_items, WAVE_SIZE)):
    wave = generate_items(start, start + WAVE_SIZE)
    await proxy.bulk_upsert_items(
        wave,
        patch_first=(i > 0),  # first wave creates buckets, rest patch into them
    )
```

With `patch_first=True`, the library tries to patch entries into existing buckets first (succeeds immediately) and only falls back to `create_item` on 404 (new bucket). This eliminates the 409→patch overhead that would otherwise double the Cosmos calls per bucket in every wave after the first. See [ARCHITECTURE.md -- Decision 11](ARCHITECTURE.md#11-patch-first-bulk-ingestion-strategy) for the full rationale and benchmark evidence.

### 4) Results and diagnostics

**PackedContainerProxy** raises the same exceptions as a standard `ContainerProxy`:
- `CosmosResourceExistsError` (409) for duplicate `create_item`
- `CosmosResourceNotFoundError` (404) for missing items on `read_item` / `replace_item` / `delete_item`

**PackedKvClient** returns a machine-readable `OperationResult` with diagnostics:
- `documentsRead`, `documentsWritten`
- `retries`

---

## Design Trade-offs & Quirks

### Every read is a single Cosmos point read

The bin document's `id` and `pk` are both derived from `{namespace}:{bucketId}` where `bucketId = sha256(...)[:prefix]`. Since both the partition key value and item id are known at read time, the bucket is fully deterministic -- no scanning, no fan-out, no overflow pages.

### Queries are evaluated client-side (not pushed down to Cosmos)

`query_items()` and `read_all_items()` do **not** push predicates to Cosmos. The library reads all bin documents in the namespace, unpacks every entry, and filters in memory.

**Query is a secondary concern.** This is primarily a library for using Cosmos DB as an efficiently bin-packed key-value store. Point reads (`read_item`) are the fast path.

### Hashing is deterministic and stateless

Keys are mapped to bins via `SHA-256(utf8(...))` -- a pure mathematical function. There is **no metadata store**, no lookup table, no state to maintain. The same key always maps to the same bin on any machine, after any restart, in any language implementation.

### `bucket_prefix_length` and `max_entries_per_doc` control bin behaviour

These are the most important tuning parameters, and they must be set together. See **Sizing for Scale** above for a full reference table and worked examples.

| Shorter prefix (e.g., 2) | Longer prefix (e.g., 8) |
|--------------------------|------------------------|
| Fewer bins, more items per bin | Many bins, fewer items per bin |
| Fewer physical documents (better packing ratio) | More physical documents (less packing savings) |
| Higher write contention (concurrent writes to same bin) | Lower write contention |
| Higher risk of hitting `max_entries_per_doc` | Lower risk of hitting the limit |

`max_entries_per_doc` acts as the hard ceiling -- if a bin is full the write fails. Set it based on your average item size to stay under the 2 MB Cosmos document limit.

### Items returned do not include Cosmos system properties

Items returned by `PackedContainerProxy` are the **exact JSON dicts you stored**. They do not include Cosmos system properties (`_rid`, `_self`, `_etag`, `_attachments`, `_ts`) because the items are packed inside bin documents.

### Server-side conflict-free writes via Cosmos patch

All writes (`put`, `delete`, `create_item`, `upsert_item`, `replace_item`, `delete_item`) use Cosmos DB **partial document update (patch)** operations. This means:

- **No read-before-write:** Each write is a single server-side operation (~1 RU) instead of the previous read-modify-replace cycle (~6-11 RU).
- **No ETag conflicts:** Concurrent patches to different entries in the same bin are serialised automatically by Cosmos DB -- no 412 retries needed.
- **Atomic create-if-not-exists:** `create_item` uses a `filter_predicate` to ensure the entry doesn't already exist, returning 409 if it does.
- **New bucket race handling:** When a patch targets a bucket that doesn't exist yet, the library creates it with `create_item` and handles the 409 race (another thread created it first) by retrying the patch.

### Bin capacity

With patch-based writes the client does not perform a client-side capacity check. Cosmos DB's own **2 MB document size limit** is the safety net. With a correctly sized `bucket_prefix_length`, bins should never approach that limit in practice.

### No cross-namespace reads

Each `namespace` is an isolated boundary. Documents written with `namespace="app-a"` are invisible to a client configured with `namespace="app-b"`, even in the same container.

### Counting logical items in your container

Because this library packs many logical items into fewer physical Cosmos documents, the standard "item count" in the Azure Portal shows the number of **physical bin documents**, not the number of logical items you stored. Use this Cosmos DB SQL query to get the true counts:

```sql
SELECT
    COUNT(1)                                         AS physicalDocs,
    SUM(ARRAY_LENGTH(ObjectToArray(c.entries)))       AS logicalItems
FROM c
WHERE c.docType = 'bucketRoot'
```

To scope to a specific namespace (replace `myapp` with your namespace):

```sql
SELECT
    COUNT(1)                                         AS physicalDocs,
    SUM(ARRAY_LENGTH(ObjectToArray(c.entries)))       AS logicalItems
FROM c
WHERE c.docType = 'bucketRoot'
  AND STARTSWITH(c.id, 'myapp:')
```

To see the packing ratio and spot hot buckets:

```sql
SELECT TOP 20
    c.bucketId,
    ARRAY_LENGTH(ObjectToArray(c.entries)) AS entryCount
FROM c
WHERE c.docType = 'bucketRoot'
ORDER BY ARRAY_LENGTH(ObjectToArray(c.entries)) DESC
```

### `delete_item` behavior for missing keys

- **PackedContainerProxy**: Raises `CosmosResourceNotFoundError` (404), matching standard `ContainerProxy` behavior.
- **PackedKvClient**: Returns `status="not_found"` without raising an exception.

---

## CI / Testing

### Unit tests

```bash
pip install -e ".[dev]"
pytest tests/unit/ -v
```

Unit tests use in-memory fakes (`FakeContainerProxy`, `FakeStorage`) -- no Cosmos needed.

| Test file | Coverage |
|-----------|----------|
| `test_proxy_crud_basic.py` | `create_item`, `read_item`, `upsert_item`, `replace_item`, `delete_item` |
| `test_proxy_bulk_upsert.py` | `bulk_upsert_items` -- empty list, single/multi/large batches, upsert semantics, duplicate ids, read-back, concurrency |
| `test_proxy_query.py` | `query_items` client-side filtering |
| `test_proxy_read_all.py` | `read_all_items` across bins |
| `test_proxy_errors.py` | 404/409 error paths |
| `test_proxy_overflow.py` | Bin capacity limits |
| `test_client_crud_basic.py` | `put`, `get`, `delete` |
| `test_client_bulk_put.py` | `bulk_put` -- empty list, single/multi/large batches, overwrites, duplicate keys, validation (empty key, non-bytes), concurrency, binary values |
| `test_client_delete_missing.py` | Delete non-existent keys |
| `test_hashing.py`, `test_hashing_pk_routing.py` | Deterministic SHA-256 routing |
| `test_model_serialization.py` | Bin document serialization |
| `test_packing_config.py` | Config validation, `recommended_prefix_length()` |
| `test_value_encoding.py` | Base64 encode/decode round-trips |
| `test_contention_*.py` | Concurrent writes to the same bucket |
| `test_overflow_pages_*.py` | Overflow page creation and deletion |

### Integration tests (Cosmos DB Emulator)

Integration tests run against the [Azure Cosmos DB Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/local-emulator). Set the following environment variables:

```bash
export COSMOS_CONNECTION_STRING="AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
export COSMOS_DATABASE="testdb"
export COSMOS_CONTAINER="testcontainer"
export AZURE_COSMOS_EMULATOR="true"
pytest tests/integration/ -v
```

| Test file | Coverage |
|-----------|----------|
| `test_proxy_live_smoke.py` | End-to-end CRUD via `PackedContainerProxy` |
| `test_proxy_comprehensive.py` | Full proxy lifecycle including queries and read-all |
| `test_live_cosmos_smoke.py` | End-to-end CRUD via `PackedKvClient` |
| `test_bulk_live.py` | `bulk_upsert_items` (small/medium batches, read-all after bulk, overwrites, nested JSON) and `bulk_put` (small/medium batches, overwrites, binary values, input validation) |

The CI pipeline (`.github/workflows/python-build-and-test.yml`) automatically:
1. Runs unit tests on Ubuntu
2. Starts the Cosmos DB Emulator on Windows
3. Creates the test database and container
4. Runs integration tests against the emulator

---

## Documentation

- **Architecture & Design Decisions**: [ARCHITECTURE.md](ARCHITECTURE.md)

---

## Disclaimer

This project is a **sample** provided for demonstration and educational purposes
only. It is licensed under the [MIT License](LICENSE) and is **not** an
officially supported Microsoft or Azure product. There are **no warranties**,
**no SLA**, and **no official support**. Use it at your own risk.

See [LICENSE](LICENSE) for the full license text.
