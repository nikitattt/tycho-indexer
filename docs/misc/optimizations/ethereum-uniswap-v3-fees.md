# Ethereum Uniswap V3 Fees Optimization Notes

This document summarizes the optimization work done on
`protocols/substreams/ethereum-uniswap-v3-fees` and captures the lessons that
should transfer to other Substreams packages.

The package was created by merging two older packages:

- `protocols/substreams/ethereum-uniswap-v3-logs-only`
- `protocols/substreams/ethereum-uniswap-v3-logs-with-fees`

The merged package had two separate problems:

- Protocol fee storage reading had been added on top of the old logs-only shape
  without fitting naturally into the module graph.
- The old logs-only event path was not optimized, especially around store reads
  in the event mapper.

The target workload is fast block processing, around 200 ms blocks, so the main
optimization rule was to avoid expensive work before proving that a block item
is relevant.

## Initial Findings

The dominant original bottleneck was `map_events`.

The old event mapper effectively did this for every log in every successful
transaction:

```rust
let key = format!("Pool:{}", log.address.to_hex());
if let Some(pool) = pools_store.get_last(key) {
    log_to_event(log, pool, &tx)
}
```

That caused a `store_pools` read for every receipt log, including ERC20
transfers, router logs, aggregator logs, and other unrelated logs.

Observed data from the old package showed:

```text
v3_logs:map_events - around 1100-1300 store reads/block
```

That was the first and most important problem. Store reads are not RAM-local
lookups in the way a normal in-process `HashMap` is, and they also force the
Substreams runtime to service many unnecessary accesses.

Other initial issues:

- Event signatures were decoded after the store lookup.
- There was no block-local cache for repeated pool address lookups.
- Intermediate event messages used strings for addresses and numeric values,
  then downstream modules parsed them back into bytes and big integers.
- One broad `Events` stream fed balances, ticks, liquidity, pool attributes,
  and protocol fee work.
- Protocol fee extraction scanned block traces separately from event extraction.
- `map_pools_created` scanned block logs even though factory pool creation is
  rare relative to normal blocks.

## Final Module Shape

The final shape keeps `map_pools_created` as the source of newly-created pools,
then builds one combined pool-data extraction module:

```text
map_pools_created
  -> store_pools
  -> map_pool_data
       -> map_events
       -> map_pool_protocol_fee_changes

map_events
  -> map_balance_changes
  -> map_pool_event_attribute_changes
  -> map_ticks_changes
  -> store_pool_current_tick
  -> map_liquidity_changes
```

`map_protocol_changes` consumes `map_pool_data` for block metadata, plus the
existing maps and stores for final Tycho output.

The important point is that `map_pool_data` is the real source-block hot path
now. `map_events` and `map_pool_protocol_fee_changes` are lightweight wrappers
over data already extracted by `map_pool_data`.

`map_pools_created` is still separate because `store_pools` depends on it, and
`map_pool_data` needs `store_pools`. Merging pool creation and pool data into a
single source-block module would break that dependency shape unless the whole
pool-discovery/store design changed.

## What Worked

### Event Topic Prefilter Before Store Reads

The biggest win was to classify logs by cheap event shape before touching
`store_pools`.

Current event classification checks:

- `topics.len()`
- `data.len()`
- `topic0`

Only supported Uniswap V3 pool events can reach the store lookup:

- `Initialize`
- `Swap`
- `Mint`
- `Burn`
- `Collect`
- `Flash`
- `SetFeeProtocol`
- `CollectProtocol`

This changed the store-read pattern from "one read per block log" to "one read
per V3-shaped pool-event address".

Observed impact:

```text
before: map_events around 860-1300 store reads/block
after:  map_pool_data/map_events around 14-28 store reads/block
```

This was the clearest optimization in the whole effort.

### Block-Local Pool Lookup Cache

`PoolLookupCache` caches both hits and misses by `[u8; 20]` address for the
current block.

That matters because a busy block can contain multiple logs for the same pool,
and unknown addresses can also repeat. With the cache, each pool address causes
at most one `store_pools` read per block.

This also avoids repeated string key construction except at the actual
store-read boundary.

### Bytes-Based Intermediate Proto

The intermediate `uniswap.v3.Events` proto was changed to use bytes for:

- pool addresses
- token addresses
- transaction hashes and addresses
- numeric values encoded as signed or unsigned big-endian bytes

This removed repeated hex string formatting and parsing between internal
modules.

This does affect consumers that directly consume the internal
`uniswap.v3.Events`, `uniswap.v3.TickDeltas`, `uniswap.v3.LiquidityChanges`, or
`uniswap.v3.BlockPoolData` outputs. The final Tycho `BlockChanges` output is
the intended external output and keeps the intended schema behavior, including
the corrected `protocol_fees/token0` and `protocol_fees/token1` attributes.

Observed performance varied by run, but the bytes-based shape was consistently
better than the earlier string-heavy shape in comparable runs.

### One Combined Pool Data Extractor

`map_pool_data` now extracts these from the source block in one place:

- block metadata
- pool events
- protocol fee storage changes

`map_events` simply returns `pool_data.events`.

`map_pool_protocol_fee_changes` simply converts
`pool_data.protocol_fee_changes` into Tycho entity attributes.

This is cleaner structurally because protocol fee extraction is no longer a
parallel ad-hoc block scan beside `map_events`. The source-block traversal that
knows which logs are V3 pool events also identifies which transactions and pool
addresses are candidates for protocol fee storage updates.

### Protocol Fee Extraction From Event-Known Candidate Pools

Protocol fee storage extraction is now narrowed to transactions that emitted
fee-affecting V3 pool events.

Only these event kinds can make a pool a protocol-fee candidate:

- `Swap`
- `Flash`
- `CollectProtocol`

For those transactions, storage changes are scanned for the Uniswap V3
`protocolFees` slot. The code then keeps only changes whose call/storage address
matches one of the candidate pools from that transaction.

This removes the need for `map_pool_protocol_fee_changes` to read `store_pools`.
Correctness comes from the same transaction context:

- the event proves the address is a known V3 pool,
- the storage change proves the protocol fee slot changed,
- ordinal comparison keeps the latest storage value per `(pool, token)` inside
  the block output.

For repeated same-pool events in one transaction, events are still emitted by
log ordinal. Protocol fee changes are tracked by storage-change ordinal and
stored as latest value per `(pool, token)`.

### Latest-By-Key Instead Of Raw Storage Change Collection

Protocol fee storage changes are not collected into a raw `Vec` and then
post-processed. The hot path streams through storage changes and keeps the
latest `PendingProtocolFeeChange` in an `FxHashMap` keyed by `(pool, token)`.

The comparison key is:

```text
(transaction_index, storage_change_ordinal)
```

That preserves correctness for multiple updates to the same pool/token while
avoiding unnecessary intermediate allocation.

The final `ProtocolFeeChange` output is sorted for deterministic downstream
behavior.

### Small Candidate Pool Set With ArrayVec

Most transactions touch a very small number of fee-candidate pools. The final
code uses:

```rust
ArrayVec<[u8; 20], 4>
```

for the common case and only promotes to `FxHashSet` when more than four pools
are seen in the same transaction.

This keeps the common path allocation-free while preserving correctness for
larger transactions.

### Delayed Candidate Lookup While Scanning Calls

The call traversal first checks cheap conditions:

- reverted calls are skipped,
- calls without storage changes are skipped,
- storage changes whose key is not the protocol fee slot are skipped.

Only after a protocol-fee slot change is found does the code check whether the
call address is one of the event-known candidate pools.

This matters because most calls and most storage changes are irrelevant. The
candidate-pool lookup is delayed until there is evidence that it may matter.

### Lazy Event Sort

Pool events are normally encountered in log ordinal order. The final code tracks
whether ordinals remain sorted while pushing events and only sorts when an
out-of-order ordinal is observed.

That keeps deterministic behavior without paying a sort cost on the common
path.

### Separate Event Attributes From Storage Attributes

Pool attributes now have a clearer split:

- `map_pool_event_attribute_changes` handles event-derived attributes such as
  `sqrt_price_x96`, `tick`, and `fee_protocol/token0|1`.
- `map_pool_protocol_fee_changes` handles storage-derived accrued protocol fee
  attributes, `protocol_fees/token0|1`.

This avoids mixing event decoding and state-diff decoding in one module and
makes the intended schema behavior explicit.

The old packages were incorrect around accrued protocol fee attributes. The
current `protocol_fees/token0` and `protocol_fees/token1` behavior is intended.

### PoolCreated Bloom Skip

`map_pools_created` now uses the block header `logs_bloom` to skip scanning the
entire block when the factory address is absent.

This is valid because Ethereum logs bloom includes emitting contract addresses.
For pool creation, the emitting address is the Uniswap V3 factory. If the
factory address cannot be present in the bloom, there cannot be a factory
`PoolCreated` log in that block.

The code is conservative:

- if the header is missing, it scans,
- if the bloom length is unexpected, it scans,
- only a definite bloom miss skips the block.

### Manual Decode For PoolCreated

`map_pools_created` manually checks the `PoolCreated` log shape and decodes only
the needed fields instead of using the generated ABI event decoder.

This worked well for `PoolCreated` because the event shape is small, fixed, and
rare:

- factory address must match,
- `topic0` must be `PoolCreated`,
- topic and data lengths are known,
- indexed token and fee values are in topics,
- tick spacing and pool address are in data.

The manual decoder also has a test for signed tick spacing.

### Store Delta String Parsing Cleanup

`map_protocol_changes` still consumes some store deltas whose values arrive as
strings from store internals. The parsing path was tightened to avoid extra
string allocation where possible, using direct UTF-8 parsing from bytes.

This is not the largest win, but it is a good example of cleaning repeated
downstream parsing once the bigger source-map issues are handled.

## What Did Not Work Or Was Reverted

### Generated ABI Decode Replacement For Pool Events

Manual decoding of hot pool event fields was considered and partially explored,
but it did not outperform the generated ABI decode path in practice.

For the main V3 pool events, generated decode stayed preferable because:

- the events are more complex than `PoolCreated`,
- there are several event shapes,
- the generated code is already reasonably optimized,
- manual decoding increased code risk and did not show a real benchmark win.

The final code keeps generated ABI decoding for pool events and manual decoding
only for `PoolCreated`.

### `topic0_eq(log, TOPIC)` Helper Dispatch

A helper-style classifier was tried/considered:

```rust
(1, 64) if topic0_eq(log, &INITIALIZE_TOPIC) => ...
```

The final code does not use that. It reads `topic0` once:

```rust
let topic0 = log.topics.first()?.as_slice();
```

and then compares directly inside the shape match.

The helper version adds repeated guard/function-boundary work in the hottest
matching path. Benchmark data did not justify keeping it.

### Length-First Topic Dispatch Micro-Rewrite

Another micro-optimization tried to classify by length before reading `topic0`.
It looked reasonable from first principles, because many logs can be rejected by
topic count and data length.

In real runs it did not improve the module and sometimes looked worse. The
current direct classifier is simpler and was kept.

### Inserting Fee Candidates From Raw `log.address`

An attempted change inserted fee candidate pools from `log.address` before using
the built event:

```rust
fee_candidate_pools.insert(address_key(&log.address))
```

That was rolled back. It added work before confirming a successful decode and
did not improve runs. The final code inserts after `log_to_event` succeeds and
uses the event's pool address.

### Explicit Vec Sort Replacing `itertools::sorted_unstable_by_key`

Replacing the final protocol fee sort with an explicit `Vec` plus
`sort_unstable_by_key` did not improve results. It was rolled back.

The final code uses the existing iterator sort for the protocol fee output.

### Over-Interpreting Single Provider Runs

Some runs showed large swings, especially with Pinax/provider load and
Substreams stage warm-up. For example, the same code could show `map_pool_data`
near the 30 ms range in one warm run and above 50 ms in another run.

The useful signal came from repeated comparisons over the same block ranges and
from structural counters such as store reads per block. Single "slowest module"
prints were not enough to decide a micro-optimization.

## Performance Summary

The exact timings varied by provider load and stage warm-up, but the durable
improvements were clear.

Before the main changes, the old package showed examples like:

```text
v3_logs:map_events             45-78 ms/block
store reads                    860-1300 reads/block
map_protocol_fee_changes       28-55 ms/block
map_pools_created              13-35 ms/block
```

After the main design changes, the hot path became:

```text
map_pool_data                  often around 28-45 ms/block, with noisy higher runs
store reads                    around 14-28 reads/block
map_pools_created              often around 12-35 ms/block depending on range/load
map_pool_protocol_fee_changes  lightweight wrapper when shown separately
```

The largest reliable improvement was the store-read collapse:

```text
around 1100+ reads/block -> around 14-28 reads/block
```

That is the main result to reproduce in other packages.

## Correctness Points Preserved

The optimization work preserved these behavioral requirements:

- Non-V3 logs must not trigger pool store reads.
- Repeated pool logs in the same block should reuse one pool lookup.
- Unknown pool addresses should also be cached as misses.
- Events must be emitted in log ordinal order.
- Protocol fee storage changes must only be emitted for known pool candidates.
- Multiple updates to the same `(pool, token)` should keep the latest ordinal.
- Storage-derived accrued protocol fees use `protocol_fees/token0|1`.
- Event-derived fee protocol settings use `fee_protocol/token0|1`.
- Pool creation still emits component creation, static attributes, initial pool
  attributes, and initial token balances.
- `PoolCreated` signed tick spacing must decode correctly.

## Tests Added Or Expanded

The optimization work added tests around the behavior most likely to break while
optimizing:

- Non-V3 logs are skipped before store lookup.
- Pool lookup hits and misses are cached per block.
- The event classifier recognizes only supported V3 pool event shapes.
- Events sharing topic/data lengths, especially `Burn` and `Collect`, are
  distinguished correctly.
- Event metadata and numeric fields are emitted as bytes.
- `map_pool_data` extracts block metadata, events, and protocol fee changes in
  one pass.
- Protocol fee storage changes without matching pool events are ignored.
- Non-fee-affecting events do not trigger protocol fee extraction.
- Storage changes outside event-known pool calls are ignored.
- Multiple fee-candidate pools in one transaction are handled.
- Out-of-order event ordinals are sorted only when needed.
- Protocol fee storage before the base tracking block is ignored.
- Extracted protocol fee changes are converted to Tycho attributes.
- Pool-created scan is skipped on definite factory bloom miss.
- Pool-created scan still runs on bloom hit.
- `PoolCreated` signed tick spacing is decoded correctly.
- Store delta bigint parsing avoids unnecessary string allocation.

The final verification used:

```text
cargo test -p ethereum-uniswap-v3-fees
cargo check -p ethereum-uniswap-v3-fees
cargo check --target wasm32-unknown-unknown -p ethereum-uniswap-v3-fees
cargo build --target wasm32-unknown-unknown --release -p ethereum-uniswap-v3-fees
```

## Guidelines For Future Package Optimization

Use this order of operations for similar Substreams packages.

1. Measure store reads first.

   If a map does store reads before cheap log/topic filtering, fix that before
   doing CPU micro-optimizations.

2. Reject by event shape before decoding.

   Check `topics.len()`, `data.len()`, and `topic0` before ABI decode. For
   source-block logs, this is usually much cheaper than decoding or store
   access.

3. Cache store lookups per block.

   Cache both hits and misses. Use fixed byte-array keys such as `[u8; 20]`
   where possible and format string store keys only at the store boundary.

4. Move source-block work into one extractor only when the graph permits it.

   Combining source block scans can help, but Substreams dependency order
   matters. If a later extractor depends on a store built from an earlier source
   map, those source maps cannot always be merged safely.

5. Use internal bytes messages for hot intermediate data.

   Avoid hex strings and decimal strings between internal modules when the next
   module will parse them back immediately. Keep final consumer schemas stable,
   but make internal protobufs cheap.

6. Narrow trace/storage scans using event evidence.

   If storage changes only matter for contracts that emitted known events in the
   same transaction, collect a small candidate set from logs first and use it to
   constrain call traversal.

7. Delay allocation and conversion until after cheap filters.

   Do not build transactions, attributes, strings, or big integers until the
   candidate has passed log shape, address, slot, and relevance checks.

8. Prefer small inline data structures for tiny per-transaction sets.

   `ArrayVec` is useful when most transactions have one to four candidates and
   only rare transactions need a hash set.

9. Sort lazily.

   Track whether data is already ordered and only sort when the input proves it
   is not.

10. Use block bloom for rare address-specific logs.

    Bloom checks are a strong fit for factory-created events or other rare logs
    emitted by one known contract. Always fall back to scanning if bloom data is
    missing or malformed.

11. Benchmark micro-optimizations and be ready to revert them.

    Several plausible changes made this package slower. Keep changes only when
    repeated runs on the same ranges show a benefit, or when the structural
    counter improvement is obvious.

12. Add correctness tests before changing hot paths.

    Performance changes around event order, duplicate events, storage ordinals,
    and same-transaction behavior are easy to get subtly wrong. Tests should
    cover those exact cases before optimizing.

## Current Remaining Limits

There are no obvious low-risk optimizations left in this package.

The remaining cost is mostly real work:

- scanning source-block logs for V3 event candidates,
- performing a small number of necessary pool store reads,
- decoding actual V3 pool events,
- scanning relevant transaction calls for protocol fee slot changes,
- emitting Tycho-compatible final changes.

Further improvement would require more invasive or workload-specific choices:

- restricting processing to a known target pool set,
- changing package semantics to ignore pools outside a configured universe,
- redesigning pool discovery and pool state so `map_pools_created` and
  `map_pool_data` can be collapsed,
- changing final output requirements,
- or tuning at the provider/runtime level rather than inside this Rust package.

For future work, the best next candidate is not another micro-optimization in
this package. It is applying the same methodology to other packages that still
read stores or decode logs before proving relevance.
