# Architecture & Design Decisions

This document records the key architectural decisions behind `cosmos-packed-kv`, the reasoning for each, the trade-offs accepted, and known areas for future improvement.

---

## 1. Core Concept: Bin-Packing Small Items into Fewer Cosmos Documents

### Decision

Many small logical items (e.g. feature flags, sensor readings, user settings) are packed into a smaller number of physical Cosmos DB documents called **bin documents**. Each bin document contains an `entries` map where the keys are SHA-256 hashes of the logical item IDs and the values are the full item payloads.

### Why

Azure Cosmos DB charges per document and has per-item overhead (~1 KB metadata including `_rid`, `_self`, `_etag`, `_ts`, indexing structures). For millions or billions of items that are each only 50-500 bytes, the overhead dominates. Packing many small items into a single document eliminates the per-item overhead and dramatically reduces both storage cost and RU consumption for point reads.

### Trade-offs

- **Write amplification**: Updating one entry currently requires reading and rewriting the entire bin document (see [Decision 6: Read-Modify-Write vs. Patch](#6-read-modify-write-vs-patch-operations)).
- **No server-side indexing of packed fields**: Cosmos indexes the bin document's top-level fields, not the nested entries. Queries must be evaluated client-side.
- **2 MB document size limit**: Cosmos DB items cannot exceed 2 MB. This constrains how many entries a single bin can hold, depending on entry size.

---

## 2. Hash-Based Bucket Routing (Stateless, Deterministic)

### Decision

Items are assigned to bins using a deterministic hash function -- no lookup table, no metadata store, no coordinator. Given the same inputs, any client on any machine will compute the same bin:

- **Both APIs**: `bucketId = sha256(item_id_or_key)[:prefix_length]`

The bin document's Cosmos `id` and `pk` are both `{namespace}:{bucketId}`. Both `PackedContainerProxy` and `PackedKvClient` use id-only routing -- the item id (or key) alone determines the bucket.

### Why

- **Zero state to manage**: No routing table to maintain, replicate, or lose. The hash function is the routing table.
- **Cross-language interoperability**: SHA-256 is universally available and produces identical results everywhere. A Python client and a future Java or .NET client will agree on which bin holds any given item.
- **Single point read guarantee**: Since both the document `id` and partition key are derivable from the caller's inputs, every `read_item` is exactly one Cosmos point read -- no fan-out, no scanning.

### Trade-offs

- **No range queries**: Items that are "close" by key (e.g. `sensor:001`, `sensor:002`) may land in different bins. There is no way to do a range scan without reading all bins.
- **Bin density is probabilistic**: SHA-256 distributes uniformly, but some bins will have more entries than others (birthday paradox). This is managed by `max_entries_per_doc` as a hard cap and `bucket_prefix_length` as the statistical lever.
- **Changing `prefix_length` is a migration**: Existing bin documents remain at the old prefix length. New writes go to new bins. A migration tool would be needed to rebalance, though the system remains correct (reads for old items still resolve to old bins).

---

## 3. Partition Key Is Hash-Derived -- Caller Does Not Supply One

### Decision

The Cosmos DB partition key for every bin document is `{namespace}:{bucketId}` -- derived entirely from the hash of the item id. The caller does not supply a partition key at all. `PackedContainerProxy` routes exclusively by item `id`; there is no `partition_key` parameter in the API.

### Why -- The Fundamental Tension

Cosmos DB partition keys serve two purposes:

1. **Data affinity**: Items with the same partition key live in the same logical partition, enabling efficient single-partition queries.
2. **Distribution**: Items spread across many logical partitions to avoid hotspots and the 20 GB logical partition limit.

Bin-packing **inherently conflicts** with data affinity. The library deliberately chooses distribution over affinity:

> **Rejected approach**: Expose a caller-supplied `partition_key` and use `sha256(pk + "|" + id)` for routing. This was the original design but was removed because (a) it created confusion -- the caller's pk did not function like a Cosmos partition key, and (b) it required extra parameters on `read_item` and `delete_item` that added no real partitioning value.

With hash-derived pks based solely on item id, items scatter uniformly across logical partitions. No hot partition, no 20 GB risk.

### What About Hierarchical Partition Keys?

Cosmos DB supports up to 3-level hierarchical partition keys (e.g. `/tenantId` → `/category` → `/bucketId`). This could allow partition-scoped queries while distributing within each scope. However:
- It does **not** solve the 20 GB limit for Level 1 if a single caller pk value is huge.
- Python SDK support for hierarchical PKs adds complexity.
- This is a potential future enhancement. If re-added, `partition_key` could be introduced as an optional parameter without breaking the current API.

### Trade-offs

- **All queries are cross-partition**: Since items are scattered across many bins by id hash, there is no way to query "all sensors items" without scanning every bin. This is accepted as a design trade-off -- the library is optimised for point reads, not queries.
- **Same id = same item**: With id-only routing, two items with the same `id` but different category/type fields cannot coexist. The id must be globally unique within a namespace.

---

## 4. Two API Layers: PackedContainerProxy and PackedKvClient

### Decision

Two public APIs are provided:

| API | Routing | Values | Error model |
|-----|---------|--------|-------------|
| `PackedContainerProxy` | `sha256(item_id)[:prefix]` | Full JSON dicts | Cosmos exceptions (409, 404, 412) |
| `PackedKvClient` | `sha256(key)[:prefix]` | Opaque bytes (base64-encoded) | `OperationResult` with status codes |

### Why

- **PackedContainerProxy** provides a familiar `ContainerProxy`-style interface. Code that uses `create_item`, `read_item`, `upsert_item`, `replace_item`, `delete_item` can adopt it with minimal changes. Note: it is not a strict drop-in because `read_item` and `delete_item` take only the item id (no `partition_key` parameter). This lowers the adoption barrier.
- **PackedKvClient** is simpler and more explicit. It exposes the reality that this is a key-value store. It returns machine-readable `OperationResult` objects with diagnostics (documents read, retries, etc.), making it suitable for automation agents and detailed monitoring.

### Trade-offs

- **Two APIs to maintain**: Changes to the bin document format or write path must be reflected in both.
- **PackedContainerProxy is a "leaky abstraction"**: It provides a ContainerProxy-style API but diverges in important ways (no server-side queries, no system properties on returned items, no `partition_key` parameter). Users must understand these differences.

---

## 5. Optimistic Concurrency with ETag-Based Retries

### Decision

Since multiple logical items can share the same physical bin document, concurrent writes to different entries in the same bin will conflict. The library uses **ETag-based optimistic concurrency control**:

1. Read the bin document (captures `_etag`).
2. Modify the `entries` map in memory.
3. Write back with `if_match=etag`.
4. If the ETag has changed (HTTP 412), re-read and retry.
5. Give up after `max_retries` attempts (default: 10).

### Why

- **Correctness**: Without ETags, concurrent upserts to the same bin document could silently overwrite each other's entries (last-writer-wins at the document level, but an entire entries map would be lost).
- **No distributed locks needed**: Optimistic concurrency avoids the need for external coordination. It works well when contention is low -- which it is when `prefix_length` is sized correctly.

### Trade-offs

- **Retries add latency under contention**: If many concurrent writers target the same bin, retries compound. This is mitigated by choosing a longer `prefix_length` (more bins = less contention per bin).
- **Retries exhaust → error**: After `max_retries` (default 10), the operation fails. This is a safety valve, not a normal operating condition. If retries are routinely exhausted, the prefix is too short.
- **This is where patch operations would help the most** (see [Decision 6](#6-read-modify-write-vs-patch-operations)).

---

## 6. Read-Modify-Write vs. Patch Operations

### Current State

All writes (create, upsert, replace, delete) use **Cosmos DB partial document update (patch)** to modify the specific entry within its bucket document, without reading or rewriting the entire document. The first write to a new bucket still uses `create_item` to create the bucket document, but subsequent writes to that bucket use patch operations exclusively.

### Why This Is Suboptimal

- **Write amplification**: A bin with 100 entries at 200 bytes each is ~20 KB. Updating a single entry rewrites all 20 KB.
- **Higher RU cost**: Replace operations charge RUs proportional to the full document size, not just the changed portion.
- **Wider contention window**: The read-modify-write cycle is not atomic. The longer the bin document, the higher the RU cost, and the longer the window during which another writer can invalidate the ETag.

### What Should Happen: Cosmos DB Partial Document Update (Patch)

Cosmos DB supports [partial document updates](https://learn.microsoft.com/en-us/azure/cosmos-db/partial-document-update) that modify specific paths within a document without reading or rewriting the whole thing:

```python
# Ideal: set a single entry without reading the full doc
container.patch_item(
    item=doc_id,
    partition_key=cosmos_pk,
    patch_operations=[
        {"op": "set", "path": f"/entries/{entry_id}", "value": item_body},
        {"op": "set", "path": "/updatedAt", "value": "2026-02-26T..."},
    ],
)
```

Advantages of patch:
- **No read required for upserts**: Set the entry directly. RU cost is proportional to the patch size, not the document size.
- **Reduced contention**: Patch operations on different paths within the same document do not conflict. Two concurrent writes to different entries in the same bin would both succeed.
- **Lower latency**: One operation instead of read + write.

### Known Limitation

Patch operations on a **non-existent document** return 404 -- they cannot create documents. The first write to a new bin must still use `upsert_item` to create the bin document. Subsequent writes to that bin can use patch.

### Status: Implemented

All single-item writes (`create_item`, `upsert_item`, `replace_item`, `delete_item`, `put`, `delete`) and bulk operations (`bulk_upsert_items`, `bulk_put`) now use Cosmos DB patch operations to add, update, or remove entries within bucket documents. This eliminates the read-before-write round-trip for updates and provides server-side conflict resolution -- two concurrent patches to different entries in the same bucket are serialised automatically.

The remaining read-modify-write use cases are limited to operations that require the full document (e.g. `read_all_items` scanning).

For bulk ingestion at scale, see [Decision 11: Adaptive Bulk Ingestion Strategy](#11-adaptive-bulk-ingestion-strategy) for how the library automatically selects the cheapest write strategy (atomic patch for ≤9 entries, read-merge-upsert for ≥10) and supports the `patch_first` hint for streaming wave performance.

---

## 7. Query Is Client-Side Only

### Decision

`query_items()` and `read_all_items()` scan **all** bin documents in the namespace, unpack every entry, and filter client-side. No predicates are pushed down to Cosmos DB.

### Why

Items with the same logical partition key are scattered across many bins (by design -- see [Decision 3](#3-partition-key-is-hash-derived-not-caller-controlled)). There is no partition-scoped index that could answer "give me all sensor items." The only way to find them is to read every bin and check.

Additionally, the packed entries are nested under the `entries` map with SHA-256 keys. Cosmos SQL queries cannot efficiently predicate on nested values inside a dynamically-keyed map.

### Trade-offs

- **Query cost scales with total bins, not result size**: A query that returns 5 items out of 10,000 bins still reads (or at minimum enumerates) all 10,000 bin documents.
- **No ORDER BY, DISTINCT, aggregates, or JOINs**: The client-side predicate parser handles only simple `c.field op value` comparisons with AND.
- **This is acceptable**: The library is designed for point-read-heavy workloads. If your application requires rich queries, use standard Cosmos documents (unpacked) for the queryable data and packed documents for the high-volume KV-style data.

---

## 8. Namespace Isolation

### Decision

Every bin document's `id` and `pk` are prefixed with a `namespace` string (e.g. `"myapp:a7f3"`). Different namespaces within the same Cosmos container are completely isolated from each other.

### Why

- **Multi-tenant or multi-purpose containers**: A single Cosmos container can serve multiple applications or logical datasets without interference.
- **No cross-contamination**: A query for namespace `"app-a"` never sees documents from `"app-b"`.
- **Cheap isolation**: No need to create separate containers (which would have separate throughput provisioning in provisioned-throughput mode).

### Trade-offs

- **Shared throughput and indexing cost**: All namespaces share the container's provisioned or serverless throughput. A noisy namespace can affect others.
- **No per-namespace access control**: Cosmos RBAC operates at the container level, not by namespace.

---

## 9. The 2 MB Document Size Constraint

### Decision

Bin documents are bounded by both `max_entries_per_doc` (count) and `max_doc_bytes_approx` (byte size, reserved for future enforcement). When a bin is full, the write fails with an actionable error message suggesting a longer prefix.

### Why

Cosmos DB items cannot exceed 2 MB. A bin with too many entries (or entries that are individually large) will breach this limit. Rather than allowing writes to fail with an opaque Cosmos error, the library enforces a configurable ceiling and returns a clear, descriptive error.

### Current Behaviour

- `max_entries_per_doc` is enforced at write time. Default: 200.
- `max_doc_bytes_approx` (default 1.8 MB) is **not yet enforced** -- it is reserved for a future byte-size check before writes.

### Overflow Pages -- Removed

An earlier version of the design included overflow pages: when a bin exceeded `max_entries_per_doc`, a new "page" document was created in the same logical partition. This was removed in PR #1 because:

- Overflow pages broke the single-point-read guarantee (reads might need to scan multiple pages).
- They added significant complexity to the write path (page creation, tail tracking, page scanning).
- The simpler approach -- reject the write and tell the user to increase `prefix_length` -- is operationally clearer and keeps reads fast.

### Trade-offs

- **Writes can fail**: If the prefix is too short for the data volume, some bins will hit `max_entries_per_doc` and writes will be rejected. This is a deliberate signal, not a silent failure.
- **No automatic rebalancing**: The library does not automatically move entries from a full bin to a less-full one. The user must increase `prefix_length` (which takes effect for new writes; existing bins are unaffected).

---

## 10. SHA-256 for Cross-Language Determinism

### Decision

All hashing uses SHA-256 with UTF-8 encoding. No salting, no random seeds, no language-specific hash functions.

### Why

- **Universally available**: Every language has a SHA-256 implementation that produces identical output for the same input.
- **Deterministic**: No per-process or per-machine variation. A key hashed in Python produces the same bucket ID in Java, .NET, Go, or JavaScript.
- **Uniform distribution**: SHA-256 is cryptographically strong, meaning the hash outputs are effectively uniformly distributed. This ensures even bin density without hot buckets.

### Trade-offs

- **Computational cost**: SHA-256 is more expensive than simpler hash functions (e.g. CRC32, FNV-1a). For this use case the cost is negligible -- it's dwarfed by the Cosmos DB network round-trip.
- **Not sortable**: SHA-256 hashes have no ordering relationship with the input keys. This reinforces that range queries are not supported by design.

---

## 11. Adaptive Bulk Ingestion Strategy

### Decision

Both `bulk_upsert_items` and `bulk_put` accept a `patch_first: bool = False` parameter that hints whether buckets are expected to exist. The library then **automatically** selects the cheapest write strategy per bucket based on how many entries target that bucket:

| Batch size per bucket | Strategy when bucket exists | Cosmos calls | Why |
|---|---|---|---|
| **≤ 9 entries** | Single atomic `patch` | **1** | Fits in one patch call (Cosmos 10-op limit, 1 reserved for `updatedAt`). Atomic, no read needed, no lost-update risk. |
| **≥ 10 entries** | `read` → merge in memory → `upsert` | **2** | Cheaper than `ceil(N/9)` sequential patches, each of which pays the full-document RU cost anyway. |

The `patch_first` flag controls the **first-attempt / fallback** order:

| Mode | First attempt | Fallback on error | Optimal when |
|------|--------------|-------------------|-------------|
| **Create-first** (default) | `create_item` (all entries) | Patch or read-merge-upsert on 409 | Loading into an empty container |
| **`patch_first=True`** | Patch or read-merge-upsert (auto-selected) | `create_item` on 404 (bucket missing) | Subsequent streaming waves when buckets already exist |

### Why This Was Needed -- Two Problems at Scale

#### Problem 1: Wasted 409s (create-first overhead)

When ingesting datasets too large to fit in memory as a single batch (e.g. 300M items at ~100 bytes each = ~30 GB), the caller must stream items in waves:

```python
WAVE_SIZE = 10_000_000
for wave_start in range(0, 300_000_000, WAVE_SIZE):
    wave = generate_items(wave_start, wave_start + WAVE_SIZE)
    await proxy.bulk_upsert_items(wave)
```

With the default **create-first** strategy, Wave 1 creates all buckets (e.g. 65,536 buckets with `prefix_length=4`). But Waves 2-29 all try `create_item` first on buckets that already exist → get a 409 → then fall back to patch. Every bucket in every subsequent wave wastes one Cosmos call on the failed create.

**The impact is severe.** For 30 waves of 10M items across 65,536 buckets:
- Wave 1: 65,536 Cosmos calls (create succeeds for all buckets)
- Waves 2-29: 65,536 × 2 = 131,072 Cosmos calls per wave (failed create + patch)
- Total: 65,536 + (29 × 131,072) = **3,866,624 calls** (vs. an optimal 30 × 65,536 = **1,966,080 calls**)

That's **~2× the necessary Cosmos calls** -- a 97% overhead.

#### Problem 2: Sequential Patch Bottleneck

The Cosmos DB patch API allows a maximum of 10 operations per patch call (with 1 reserved for the `updatedAt` timestamp → 9 usable for entries). When a wave delivers more than 9 entries to a single bucket, the library must issue `ceil(N/9)` sequential patch calls for that bucket.

With `prefix_length=4` (65,536 buckets) and 10M items per wave, each bucket receives ~153 entries per wave → `ceil(153/9) = 17` sequential patch calls per bucket per wave.

**This destroyed wave throughput:**

| Wave | Cosmos calls per wave | Items/sec |
|------|----------------------|-----------|
| Wave 1 (create) | 65,536 (1 per bucket) | ~13,245 |
| Wave 2+ (17 patches/bucket) | **1,114,112** (17 × 65,536) | ~4,556 |

Each patch call pays the full-document RU cost (Cosmos reads the entire document internally, applies the operations, writes it back). So 17 patch calls cost ~17× a single write -- plus 17 network round-trips.

### The Fix -- Hybrid Strategy

The library now automatically selects the optimal approach per bucket:

```
if entries_for_this_bucket <= 9:
    # 1 Cosmos call -- atomic patch, no read needed
    patch(entries)
else:
    # 2 Cosmos calls -- cheaper than ceil(N/9) patches
    doc = read(bucket)
    doc.entries.update(new_entries)
    upsert(doc)
```

**The inflection point is exactly 10 entries.** At 10 entries, 2 patch calls (each paying full-doc RU) is already more expensive than 1 read (~5-10% of a write in RUs) + 1 upsert. The savings grow linearly: at 153 entries/bucket the hybrid uses 2 calls instead of 17 -- an **8.5× reduction**.

Combined with the `patch_first` hint for wave ordering:

```python
for i, wave_start in enumerate(range(0, total, WAVE_SIZE)):
    wave = generate_items(wave_start, wave_start + WAVE_SIZE)
    await proxy.bulk_upsert_items(
        wave,
        patch_first=(i > 0),  # first wave creates, rest auto-select
    )
```

### Benchmark Evidence

| Strategy | 10M items | Waves | Time | Items/sec |
|----------|-----------|-------|------|-----------|
| Multi-wave create-first (50K/wave) | 10,000,000 | 200 | >2 hours | <1,400 |
| Single-batch create-first | 10,000,000 | 1 | 566s (9.4 min) | 17,661 |
| Multi-wave with sequential patches | 300,000,000 | 30 | Projected 17+ hrs | ~4,556 |
| **Multi-wave hybrid (target)** | 300,000,000 | 30 | -- | -- |

### Implementation

In `_bulk_upsert_bucket` / `_bulk_put_bucket`:

- **`len(entries) ≤ 9`**: Use a single `patch` call (atomic, 1 Cosmos call).
- **`len(entries) ≥ 10`**: Use `read_item` → merge entries dict → `upsert_item` (2 Cosmos calls).
- **`patch_first=False`** (default): Try `create_item` first → on 409, apply the appropriate strategy above.
- **`patch_first=True`**: Apply the strategy above first → on 404/None, fall back to `create_item`.

The existing `_bulk_patch_entries` / `_bulk_patch_kv_entries` methods remain available for the ≤9 path and for single-item CRUD operations.

### Concurrency Safety Note

The read-merge-upsert path (≥10 entries) uses an unconditional `upsert_item`. This is safe for **dedicated bulk ingestion** (sole writer to a container). For mixed workloads where bulk ingestion runs alongside live single-item CRUD, the `StorageInterface` already provides `replace_item(id, body, pk, if_match=etag)` with ETag-based optimistic concurrency. A future enhancement could switch the bulk path to use ETag-guarded replaces with retry for full concurrent safety.

### Trade-offs

- **≤9 entries**: Single atomic patch -- cheapest possible, no read needed, no lost-update risk.
- **≥10 entries**: Read-merge-upsert -- a magnitude cheaper than sequential patches, but not atomic (see Concurrency Safety Note above).
- **Automatic selection**: The library picks the strategy per bucket -- callers only need to set `patch_first` based on whether this is the first wave.
- **First-wave optimisation**: The recommended pattern is `patch_first=(wave_number > 0)` -- use create-first for wave 0, auto-selected strategy for all subsequent waves.

---

## Module Structure

```
src/packed_kv_cosmos/
├── __init__.py          # Public exports
├── client.py            # PackedKvClient -- lower-level KV API
├── config.py            # PackedKvConfig and PackingConfig dataclasses
├── hashing.py           # SHA-256 hashing utilities (cross-language contract)
├── model.py             # BucketRootDocument / BucketPageDocument models
├── packed_container.py  # PackedContainerProxy -- ContainerProxy drop-in
├── packing.py           # Value encoding (base64) and document ID generation
├── results.py           # OperationResult, Diagnostics, status constants
└── storage.py           # StorageInterface abstraction + CosmosStorage impl
```

### Dependency Flow

```
PackedContainerProxy ─┬─→ PackingConfig (config.py)
                      ├─→ hashing.py
                      ├─→ packing.py (ID generation)
                      └─→ ContainerProxy (azure-cosmos SDK, direct)

PackedKvClient ───────┬─→ PackedKvConfig (config.py)
                      ├─→ hashing.py
                      ├─→ packing.py (ID generation + base64 encoding)
                      ├─→ model.py (BucketRootDocument)
                      ├─→ results.py (OperationResult)
                      └─→ StorageInterface (storage.py, injectable)
```

`PackedContainerProxy` uses the Cosmos SDK `ContainerProxy` directly rather than going through `StorageInterface`. This was a pragmatic choice to keep the drop-in API as thin as possible. `PackedKvClient` uses the `StorageInterface` abstraction to allow unit testing with a `FakeStorage` implementation.

---

## Summary of Trade-offs

| Design choice | What you gain | What you give up |
|--------------|---------------|------------------|
| Bin-packing | Massive cost reduction for small items | Write amplification, no server-side indexing |
| Hash-based routing (id-only) | Stateless, deterministic, single point reads | No range queries, no ordered scans |
| Hash-derived partition key | Even distribution, no hot partitions | All queries are cross-partition, same id = same item |
| ETag-based retries | Correctness without distributed locks | Latency under contention |
| Patch operations (≤9 entries) | Atomic, 1 Cosmos call, no read needed | Patch on non-existent doc returns 404; limited to 9 entries per call |
| Read-merge-upsert (≥10 entries) | 2 Cosmos calls vs ceil(N/9) patches; 8.5× fewer calls at 153 entries/bucket | Not atomic; concurrent single-item writes between read and upsert could be overwritten (mitigatable via ETag-guarded replace) |
| Client-side query | Compatible API shape | Cost scales with bins, not results |
| No overflow pages | Single point read guarantee | Writes fail when a bin is full |
| SHA-256 | Cross-language determinism, uniform distribution | Slightly more CPU than simpler hashes |
| Adaptive bulk strategy | Auto-selects patch vs read-merge-upsert per bucket; eliminates 409 overhead with `patch_first` hint | Caller must set `patch_first` based on wave number |
