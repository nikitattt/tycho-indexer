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
| `--run-mode initial\|incremental\|prune-stale-components` | | `incremental` | Selects broad discovery, cron-friendly update, or stale TVL pruning mode. |
| `--chain <chain>` | | `base` | Chain to process. Must match Tycho's `Chain` enum names. |
| `--database-url <url>` | `DATABASE_URL` | required | Postgres database used by the indexer. |
| `--protocol-systems <csv>` | | `uniswap_v2,uniswap_v3` | Comma-separated protocol systems to price and refresh TVL for. |
| `--cron-period-secs <secs>` | | `300` | Expected timer period for incremental mode. |
| `--recent-window-multiplier <n>` | | `2` | Incremental changed-component window is `cron_period_secs * recent_window_multiplier`. |
| `--active-window-days <days>` | | `42` | Prune mode treats components as active when any balance changed in this window. |
| `--max-rounds-initial <n>` | | `6` | Maximum solver relaxation rounds in initial mode. |
| `--max-rounds-incremental <n>` | | `4` | Maximum graph expansion and solver rounds in incremental mode. |
| `--min-initial-update-bps <bps>` | | `10` | Stops initial mode after applying a round when `updates / known_prices` at round start is below this value. Use `0` to disable this early stop. |
| `--min-price-improvement-bps <bps>` | | `10` | Existing token prices are replaced only when the native price changes by at least this amount. First-time prices are always accepted. Use `0` to disable this filter. |
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
- ignores tiny replacement prices below `--min-price-improvement-bps`, while
  still accepting first-time token prices;
- stops early after applying a low-yield round when
  `updates / known_prices < --min-initial-update-bps`;
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

For very large chains, the important runtime controls are:

```bash
target/release/tycho-tvl \
  --run-mode initial \
  --chain base \
  --max-rounds-initial 6 \
  --min-initial-update-bps 10 \
  --min-price-improvement-bps 10
```

`--max-rounds-initial` is the hard cap. `--min-initial-update-bps` is the
marginal-yield stop: after each round is applied, the process compares the
number of accepted updates with the number of known prices at the start of that
round. For example, `10` means stop once a round adds less than `0.10%` new
prices. This still writes the prices from the low-yield round before stopping.

Set `--min-initial-update-bps 0` if you want the job to run exactly until
`--max-rounds-initial` or until a round finds no updates.

`--min-price-improvement-bps` controls solver churn. A token that does not have
a price yet is always accepted. A token that already has a price is replaced
only when the new native-denominated price differs enough from the current
price. This makes later rounds converge faster when they are only finding tiny
route-level changes.

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

### Prune Stale Components

Prune mode is the daily cleanup job for keeping `tvl_gt` queries focused on
recently active pools:

```bash
DATABASE_URL=postgresql://postgres:mypassword@localhost:5431/tycho_indexer_0 \
RUST_LOG=info \
target/release/tycho-tvl \
  --run-mode prune-stale-components \
  --chain base \
  --active-window-days 42
```

It does not change `token_price`. It only sets `component_tvl.tvl = 0.0` for
scoped components whose existing TVL is nonzero and whose balances have not
changed recently:

```sql
NOT EXISTS (
  SELECT 1
  FROM component_balance_default cb
  WHERE cb.protocol_component_id = protocol_component.id
    AND cb.valid_from >= now() - interval '42 days'
)
```

This keeps old prices available as graph seeds/debug context while removing
dead pools from `/v1/protocol_components` queries that use `tvl_gt`.

Use `--dry-run` to count rows that would be zeroed:

```bash
target/release/tycho-tvl \
  --run-mode prune-stale-components \
  --chain base \
  --dry-run
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

## Logging

Use `RUST_LOG=info` for normal production runs. At this level, `tycho-tvl` logs:

- run configuration and database load milestones;
- graph size and target token counts;
- pricing round start, candidate counts, selected updates, and applied totals;
- one `TychoTvlPricingProgress` line roughly every minute during large pricing
  scans, including `progress_pct`, processed batches, candidates, rejected
  edges, and elapsed seconds;
- final write and TVL refresh summaries.

Per-batch component/state load details are `DEBUG` logs. Enable them only when
debugging a specific decode or simulation issue:

```bash
RUST_LOG=tycho_tvl=debug,info target/release/tycho-tvl ...
```

`TRACE` logs include individual skipped decode records and are too noisy for
normal initial runs.

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
sudo cp crates/tycho-tvl/systemd/tycho-tvl-prune.service /etc/systemd/system/
sudo cp crates/tycho-tvl/systemd/tycho-tvl-prune.timer /etc/systemd/system/
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

Enable the daily stale-component prune timer:

```bash
sudo systemctl enable --now tycho-tvl-prune.timer
systemctl list-timers tycho-tvl-prune.timer
journalctl -u tycho-tvl-prune.service -f
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
- Run `prune-stale-components` daily so pools without recent balance changes do
  not keep polluting `tvl_gt` query results.
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
