# Robinhood Uniswap V3 Logs With Fees

This package overlays accrued protocol-fee tracking on the existing Robinhood
Uniswap V3 package without rebuilding its historical stores.

It exposes:

- `map_protocol_fee_changes`: accrued-fee updates only.
- `map_protocol_changes_with_fees`: imported V3 output merged with fee updates.

Accrued values are decoded from pool storage slot `3` and emitted as:

- `protocol_fees/token0`
- `protocol_fees/token1`

## Parent package

Copy the exact `.spkg` used by the running Robinhood V3 extractor to:

```text
spkg/robinhood-uniswap-v3-fees-v0.1.0.spkg
```

The parent package is intentionally not committed. It must be byte-for-byte
identical to the deployed package so Substreams can reuse its cached stores.

Before deploying, verify the imported pool-store hash:

```bash
substreams info robinhood-uniswap-v3.yaml | rg -A8 "Name: v3:store_pools"
```

The expected hash for the package inspected during development is:

```text
3e47ecd0b62c7e46ccae10fb49684b67d334063e
```

If production reports a different hash, use the exact production package.

## Build and pack

```bash
make build
substreams pack robinhood-uniswap-v3.yaml
```

Configure Tycho with `map_protocol_changes_with_fees`. For a fee-only repair,
use `map_protocol_fee_changes`.

The current manifest starts the fee module at bootstrap continuation block
`20,702,036`. If the finalized bootstrap block changes, set this initial block
to `B + 1` before packing. The overlay emits accrued-fee changes through block
`43,005,491`. At block `43,005,492`, the imported parent package starts emitting
those attributes and the overlay stops its duplicate extraction work.

## Bootstrap cutover

At a finalized block `B`:

1. Pause the current extractor after `B`.
2. Seed slot-3 values as `protocol_fees/token0|1`.
3. Switch to this package while keeping the extractor identity and cursor.
4. Resume from `B + 1`.

Do not run the old and merged extractors concurrently for the same protocol
system.
