# tycho-tvl

`tycho-tvl` is a production sidecar for maintaining `token_price` and
`component_tvl` in a Tycho indexer database.

It is separate from `tycho-indexer`; it does not change indexing, state storage,
or swap simulation behavior. Prices and TVL are used as filtering metadata.

## Requirements

Before running `tycho-tvl`, the indexer database must be available:

- `DATABASE_URL` must point at the same Postgres database used by `tycho-indexer`.
- The indexer must already have protocol components, component balances, tokens,
  and latest block data for the selected chain.
- Database migrations must already be applied. `tycho-tvl` opens the existing
  database directly and does not run migrations or initialize enum rows.
- Supported simulation protocols are currently `uniswap_v2` and `uniswap_v3`.

There is no built-in default for `DATABASE_URL`. Pass it explicitly or set it
through the environment.

## Build

From the repository root:

```bash
cargo build --release -p tycho-tvl
```

The binary will be:

```bash
target/release/tycho-tvl
```

For local development, use:

```bash
cargo run -p tycho-tvl -- --help
```

## Configuration

Required connection settings:

```env
DATABASE_URL=postgresql://postgres:mypassword@localhost:5431/tycho_indexer_0
RUST_LOG=info
```

All CLI flags:

| Flag | Env | Default | Description |
| --- | --- | --- | --- |
| `--run-mode initial\|incremental` | | `incremental` | Selects broad discovery or cron-friendly update mode. |
| `--chain <chain>` | | `base` | Chain to process. Must match Tycho's `Chain` enum names. |
| `--database-url <url>` | `DATABASE_URL` | required | Postgres database used by the indexer. |
| `--protocol-systems <csv>` | | `uniswap_v2,uniswap_v3` | Comma-separated protocol systems to price and refresh TVL for. |
| `--cron-period-secs <secs>` | | `300` | Expected timer period for incremental mode. |
| `--recent-window-multiplier <n>` | | `2` | Incremental changed-component window is `cron_period_secs * recent_window_multiplier`. |
| `--max-rounds-initial <n>` | | `64` | Maximum solver relaxation rounds in initial mode. |
| `--max-rounds-incremental <n>` | | `4` | Maximum graph expansion and solver rounds in incremental mode. |
| `--write-batch-size <n>` | | `5000` | Batch size for token price writes and component TVL refreshes. |
| `--snapshot-batch-size <n>` | | `500` | Batch size for DB component/state loading and simulation batches. |
| `--max-deviation-bps <bps>` | | `300` | Rejects a pool edge when probe prices deviate too much. |
| `--max-incremental-intermediate-tokens <n>` | | `60` | Maximum unpriced frontier tokens expanded per incremental graph round. |
| `--max-incremental-components-per-token <n>` | | `25` | Maximum candidate pools loaded for each frontier token during incremental graph expansion. |
| `--max-incremental-graph-components <n>` | | `25000` | Maximum total component count used for incremental route-expansion context. Recently changed components are always included. |
| `--dry-run` | | `false` | Computes prices and TVL scope, logs stats, and writes nothing. |

## Run Modes

### Initial

Initial mode is the broad bootstrap/discovery job:

```bash
DATABASE_URL=postgresql://postgres:mypassword@localhost:5431/tycho_indexer_0 \
RUST_LOG=info \
cargo run --release -p tycho-tvl -- \
  --run-mode initial \
  --chain base
```

Initial mode:

- loads all tokens for the chain through the same storage path as `/v1/tokens`;
- loads all components for the configured protocol systems;
- starts from hard native-token anchors such as Base WETH;
- repeatedly prices the full loaded pool graph until no better prices appear or
  `--max-rounds-initial` is reached;
- writes hard anchors and prices discovered in the current run;
- refreshes `component_tvl` for all scoped components in batches.

Use a dry run first on large databases:

```bash
DATABASE_URL=postgresql://postgres:mypassword@localhost:5431/tycho_indexer_0 \
RUST_LOG=info \
cargo run --release -p tycho-tvl -- \
  --run-mode initial \
  --chain base \
  --dry-run
```

For very large datasets, reduce write pressure by lowering the batch size:

```bash
target/release/tycho-tvl \
  --run-mode initial \
  --chain base \
  --write-batch-size 1000
```

### Incremental

Incremental mode is intended for the recurring systemd timer:

```bash
DATABASE_URL=postgresql://postgres:mypassword@localhost:5431/tycho_indexer_0 \
RUST_LOG=info \
target/release/tycho-tvl \
  --run-mode incremental \
  --chain base \
  --cron-period-secs 300
```

Incremental mode:

- computes `recent_threshold = now() - cron_period_secs * recent_window_multiplier`;
- selects recently changed components directly from
  `component_balance_default.valid_from`;
- discovers target tokens from those changed components;
- loads old DB prices as seed context, but does not automatically rewrite them;
- expands a bounded token-pool-token candidate graph for
  `--max-rounds-incremental`, capped by
  `--max-incremental-intermediate-tokens`,
  `--max-incremental-components-per-token`, and
  `--max-incremental-graph-components`;
- can price a target through a newly discovered intermediate route, for example
  `WETH -> token2 -> target`;
- writes only prices refreshed or discovered by the current run inside the
  incremental graph;
- refreshes TVL only for components whose balances changed in the recent window.

If the timer runs every five minutes, the default settings inspect roughly the
last ten minutes of balance changes:

```text
300 seconds * 2 = 600 seconds
```

## Pricing Behavior

Prices are represented internally as:

```text
native token per whole token
```

The database `token_price.price` column is written as:

```text
raw token units per native token
```

This matches the existing Tycho TVL aggregation formula:

```sql
SUM(balance_float / token_price.price)
```

Each candidate pool edge is simulated with three native-value probe sizes:

```text
1.0 native
0.01 native
0.00001 native
```

For each successful forward probe, the raw implied price is:

```text
forward_native_per_token = probe_native_value / output_whole_token
```

The edge price is selected with a weighted median:

```text
1.0 native      weight 0.25
0.01 native     weight 0.60
0.00001 native  weight 0.15
```

The weighted median is used as the center price. Probes whose implied prices are
farther than `--max-deviation-bps` from that center are treated as outliers. An
edge is accepted only when at least two inlier probes remain and the remaining
probe weight is at least `0.60`.

This means a large `1.0 native` probe can show heavy price impact without
discarding the pool, as long as the smaller probes still agree. The slipped
large trade is liquidity information, not the reusable token price.

After a forward edge is accepted, `tycho-tvl` also checks sell-side recovery for
fee-on-transfer and asymmetric-tax tokens. For every accepted forward probe it
simulates selling the received output token amount back into the priced input
token:

```text
recovered_native = recovered_input_whole_token * input_native_per_token
sell_side_native_per_token = recovered_native / output_whole_token
```

If the sell-side probes are stable under the same inlier rules, the stored edge
price is:

```text
min(forward_native_per_token, sell_side_native_per_token)
```

So transfer tax lowers the estimated token value when it is consistently visible
in executable reverse quotes, but unstable large-trade slippage is not averaged
into the global token price.

The graph solver does not take the first available pool price. It evaluates all
candidate routes in the loaded graph and keeps the lowest cumulative path score.
Each edge contributes its probe deviation to the score. Lower score wins; ties
prefer fewer hops, then lower final-edge deviation.

Hard native-token anchors are protected from being repriced by pools.

## TVL Update

TVL refresh follows the same core semantics as the team testing script, but uses
production filters:

```sql
SUM(component_balance_default.balance_float / token_price.price)
```

It also:

- filters by chain;
- filters by configured protocol systems;
- skips invalid prices with `token_price.price > 0`;
- writes in batches;
- upserts `0.0` for refreshed components whose valid priced balances disappear,
  preventing stale high TVL rows from remaining in `component_tvl`.

## Verification

After an initial run:

```sql
SELECT count(*) FROM token_price;

SELECT count(*)
FROM component_tvl ct
JOIN protocol_component pc ON pc.id = ct.protocol_component_id
JOIN chain c ON c.id = pc.chain_id
WHERE c.name = 'base'
  AND ct.tvl > 0;
```

Then verify the API returns non-empty TVL-filtered components:

```bash
curl -s 'http://127.0.0.1:4242/v1/protocol_components' \
  -H 'content-type: application/json' \
  -H 'accept: application/json' \
  --data '{
    "chain": "base",
    "protocol_system": "uniswap_v3",
    "tvl_gt": 1.0
  }'
```

## systemd

Build and install the binary:

```bash
cargo build --release -p tycho-tvl
sudo install -m 0755 target/release/tycho-tvl /usr/local/bin/tycho-tvl
```

Create the environment file:

```bash
sudo install -d -m 0755 /etc/tycho
sudo tee /etc/tycho/tvl.env >/dev/null <<'EOF'
DATABASE_URL=postgresql://postgres:mypassword@localhost:5431/tycho_indexer_0
RUST_LOG=info
EOF
sudo chmod 0600 /etc/tycho/tvl.env
```

Install service and timer:

```bash
sudo cp crates/tycho-tvl/systemd/tycho-tvl.service /etc/systemd/system/
sudo cp crates/tycho-tvl/systemd/tycho-tvl.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

Run one incremental job manually through systemd:

```bash
sudo systemctl start tycho-tvl.service
sudo journalctl -u tycho-tvl.service -n 100 --no-pager
```

Enable the recurring five-minute timer:

```bash
sudo systemctl enable --now tycho-tvl.timer
systemctl list-timers tycho-tvl.timer
journalctl -u tycho-tvl.service -f
```

Initial mode is manual and intentionally not part of the timer:

```bash
sudo -u tycho \
  env $(sudo cat /etc/tycho/tvl.env | xargs) \
  /usr/local/bin/tycho-tvl --run-mode initial --chain base
```

## Operational Notes

- Run `initial` before relying on `tvl_gt` filters on a fresh database.
- Run `incremental` every five minutes after the initial bootstrap.
- Run database migrations with the normal indexer deployment path before starting
  `tycho-tvl`; the sidecar intentionally skips migrations on startup.
- If `--dry-run` reports zero prices, verify that the selected chain has WETH
  anchor token metadata, component balances, and supported protocol components.
- If `component_tvl` remains empty, verify `token_price` has rows and that token
  prices are positive.
- If incremental runs are too slow, reduce `--max-rounds-incremental`,
  `--snapshot-batch-size`, or the protocol system set.
- If initial runs create too much DB pressure, reduce `--write-batch-size`.

## Development

Run checks:

```bash
cargo check -p tycho-tvl
cargo test -p tycho-tvl
```
