# Ethereum Uniswap V3 Logs With Fees

This package wraps `ethereum-uniswap-v3-logs-only` and adds storage-based Uniswap V3
protocol fee accrual tracking.

It exposes:

- `map_protocol_fee_changes`: emits only accrued protocol fee attributes.
- `map_protocol_changes_with_fees`: emits the parent logs-only output plus accrued protocol fee
  attributes.

Accrued fees are written on the normal pool component with new attributes:

- `protocol_fees_accrued/token0`
- `protocol_fees_accrued/token1`

The parent logs-only attributes are left unchanged, including the existing
`protocol_fees/token0` and `protocol_fees/token1` fee-protocol settings.

## Why the Parent Import Uses `.spkg`

The Base manifest imports the parent logs-only package as a pinned `.spkg`:

```yaml
imports:
    v3_logs: ./spkg/base-uniswap-v3-logs-only-v0.1.2.spkg
```

This is intentional. Substreams cache reuse is keyed by module hashes, not by module names or source
paths. Importing the exact `.spkg` used by the running logs-only extractor keeps parent module
hashes stable, especially `v3_logs:store_pools`, so providers can reuse warm cache for the expensive
parent stores.

Using a local YAML import can produce different hashes even when it points at the same source tree,
because the YAML reads the current local WASM artifact:

```yaml
imports:
    v3_logs: ../ethereum-uniswap-v3-logs-only/base-uniswap-v3.yaml
```

If that WASM was rebuilt with different dependency versions, Rust version, generated ABI output, or
build settings, the parent module hashes can change. When hashes change, the provider treats the
stores as cold and may need to prepare millions of historical blocks.

## Switching to the YAML Import

For local development against the current parent source, switch the import to:

```yaml
imports:
    v3_logs: ../ethereum-uniswap-v3-logs-only/base-uniswap-v3.yaml
```

Then build both WASMs:

```bash
cd /protocols/substreams
cargo build --release -p ethereum-uniswap-v3-logs-only --target wasm32-unknown-unknown
cargo build --release -p ethereum-uniswap-v3-logs-with-fees --target wasm32-unknown-unknown
```

Use this only when you are fine with cold-cache behavior or are intentionally creating a new parent
package/cache line.

## Verifying Cache Compatibility

Check the imported parent store hash:

```bash
cd /protocols/substreams/ethereum-uniswap-v3-logs-with-fees
substreams info base-uniswap-v3.yaml | rg -A2 "v3_logs:store_pools"
```

For cache reuse, `v3_logs:store_pools` must match the hash of the already-running/cached parent
package on the same Substreams provider.

If the hash matches but the provider still reports large historical processing, the cache is not
available for that endpoint/token/account. That is a provider/cache-visibility issue, not a module
logic issue.
