use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

mod pricing;

use anyhow::{bail, Context, Result};
use chrono::Duration as ChronoDuration;
use clap::{Parser, ValueEnum};
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;
use tycho_client::feed::{synchronizer::ComponentWithState, BlockHeader};
use tycho_common::{
    dto::{ProtocolComponent as ResponseProtocolComponent, ResponseProtocolState},
    models::{protocol::QualityRange, token::Token, Chain},
    simulation::protocol_sim::ProtocolSim,
    storage::{ProtocolGateway, Version},
    Bytes,
};
use tycho_simulation::{
    evm::protocol::{uniswap_v2::state::UniswapV2State, uniswap_v3::state::UniswapV3State},
    protocol::models::{DecoderContext, TryFromWithBlock},
};
use tycho_storage::postgres::{builder::GatewayBuilder, direct::DirectGateway, tvl::TvlGatewayExt};

use crate::pricing::{
    apply_updates, select_round_updates, should_write_price, PriceCandidate, PriceState,
    PriceWriteMode,
};

const DEFAULT_BATCH_SIZE: usize = 5_000;
const DEFAULT_SNAPSHOT_BATCH_SIZE: usize = 500;
const DEFAULT_DEVIATION_BPS: f64 = 300.0;

#[derive(Debug, Clone, ValueEnum)]
enum RunMode {
    Initial,
    Incremental,
}

#[derive(Debug, Parser)]
#[command(version, about = "Maintain Tycho token prices and component TVL")]
struct Cli {
    #[arg(long, value_enum, default_value = "incremental")]
    run_mode: RunMode,
    #[arg(long, default_value = "base")]
    chain: String,
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,
    #[arg(long, value_delimiter = ',', default_value = "uniswap_v2,uniswap_v3")]
    protocol_systems: Vec<String>,
    #[arg(long, default_value_t = 300)]
    cron_period_secs: i64,
    #[arg(long, default_value_t = 2)]
    recent_window_multiplier: i64,
    #[arg(long, default_value_t = 64)]
    max_rounds_initial: usize,
    #[arg(long, default_value_t = 4)]
    max_rounds_incremental: usize,
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    write_batch_size: usize,
    #[arg(long, default_value_t = DEFAULT_SNAPSHOT_BATCH_SIZE)]
    snapshot_batch_size: usize,
    #[arg(long, default_value_t = DEFAULT_DEVIATION_BPS)]
    max_deviation_bps: f64,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Default)]
#[allow(dead_code)]
struct RunStats {
    tokens_loaded: usize,
    target_tokens: usize,
    graph_tokens: usize,
    existing_prices: usize,
    components_scanned: usize,
    prices_computed: usize,
    prices_written: usize,
    tvl_updated: usize,
    rejected_edges: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    run(cli).await
}

async fn run(cli: Cli) -> Result<()> {
    let chain =
        Chain::from_str(&cli.chain).with_context(|| format!("Unsupported chain {}", cli.chain))?;
    let storage = GatewayBuilder::new(&cli.database_url)
        .set_chains(&[chain])
        .build_direct_gw()
        .await
        .context("failed to build direct storage gateway")?;

    let latest_block = storage
        .get_latest_block_number(&chain)
        .await
        .context("failed to load latest indexed block")?;

    let all_tokens =
        ProtocolGateway::get_tokens(&storage, chain, None, QualityRange::None(), None, None)
            .await
            .context("failed to load tokens")?
            .entity;
    let all_tokens_by_address = all_tokens
        .iter()
        .cloned()
        .map(|t| (t.address.clone(), t))
        .collect::<HashMap<_, _>>();

    let existing_db_prices = storage
        .get_existing_token_prices_for_chain(&chain)
        .await
        .context("failed to load existing token prices")?;

    let mut stats = RunStats {
        tokens_loaded: all_tokens.len(),
        existing_prices: existing_db_prices.len(),
        ..RunStats::default()
    };

    let hard_anchors = hard_anchors(&chain);
    let target_tokens = match cli.run_mode {
        RunMode::Initial => all_tokens_by_address
            .keys()
            .cloned()
            .collect::<HashSet<_>>(),
        RunMode::Incremental => {
            let now = storage
                .get_current_db_timestamp()
                .await
                .context("failed to get DB timestamp")?;
            let window_secs = cli.cron_period_secs * cli.recent_window_multiplier;
            let since = now - ChronoDuration::seconds(window_secs);
            ProtocolGateway::get_tokens(
                &storage,
                chain,
                None,
                QualityRange::None(),
                Some(since),
                None,
            )
            .await
            .context("failed to load recently traded tokens")?
            .entity
            .into_iter()
            .map(|t| t.address)
            .collect()
        }
    };
    stats.target_tokens = target_tokens.len();

    let protected_tokens = hard_anchors
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let mut price_book = hard_anchors
        .iter()
        .map(|(address, price)| (address.clone(), PriceState::seed(*price)))
        .collect::<HashMap<_, _>>();

    if matches!(cli.run_mode, RunMode::Incremental) {
        for (address, db_price) in existing_db_prices {
            if let Some(token) = all_tokens_by_address.get(&address) {
                if let Some(native_price) = db_price_to_native_per_token(db_price, token.decimals) {
                    price_book
                        .entry(address)
                        .or_insert_with(|| PriceState::seed(native_price));
                }
            }
        }
    }

    let max_rounds = match cli.run_mode {
        RunMode::Initial => cli.max_rounds_initial,
        RunMode::Incremental => cli.max_rounds_incremental,
    };

    // Build the pool graph that the pricing solver will repeatedly relax.
    //
    // Initial mode is a broad discovery job. It loads every component in the configured
    // chain/protocol-system scope and lets prices propagate outward from hard native-token anchors.
    // That is intentionally expensive, but it is the mode that should maximize coverage after a
    // fresh indexer boot or after importing stale seed prices.
    //
    // Incremental mode starts from tokens whose balances changed recently. It still expands the
    // token-pool-token graph outward for a few rounds, because a changed target token may only be
    // priceable through an intermediate token that also has to be discovered during this run:
    //
    //     WETH -> intermediate token -> recently traded target token
    //
    // The expanded graph is used for pricing context, but TVL refresh is kept scoped to components
    // that directly contain recently traded tokens. That avoids rewriting unrelated component_tvl
    // rows just because an intermediate token was needed to solve a route.
    let graph_scope = match cli.run_mode {
        RunMode::Initial => {
            let component_ids = storage
                .get_components_for_protocols(&chain, &cli.protocol_systems)
                .await
                .context("failed to load scoped components")?;
            ComponentGraphScope {
                component_ids: component_ids.iter().cloned().collect(),
                affected_component_ids: component_ids.into_iter().collect(),
                graph_tokens: target_tokens.clone(),
            }
        }
        RunMode::Incremental => build_incremental_graph_scope(
            &storage,
            chain,
            &cli.protocol_systems,
            &target_tokens,
            max_rounds,
            cli.snapshot_batch_size,
        )
        .await
        .context("failed to build incremental component graph")?,
    };
    stats.graph_tokens = graph_scope.graph_tokens.len();
    stats.components_scanned = graph_scope.component_ids.len();
    let component_ids = graph_scope
        .component_ids
        .iter()
        .cloned()
        .collect::<Vec<_>>();

    // Repeatedly price the loaded graph from the current price book.
    //
    // A single pass can only price edges whose input token already has a native price. After that
    // pass adds new token prices, the next pass can use those newly priced tokens as inputs and
    // price deeper paths. This is what allows multi-hop discovery such as:
    //
    //     WETH -> token2 -> token1
    //
    // The loop also deliberately revisits tokens that already have a derived price. Multiple pools
    // can connect the same token, and a later-discovered route can be more stable than the first
    // route. The solver ranks candidates by cumulative path score, where each pool edge contributes
    // the deviation observed across the three probe sizes. Lower score wins; ties prefer fewer hops
    // and then the lower single-edge deviation.
    for round in 0..max_rounds {
        if component_ids.is_empty() {
            break;
        }

        let round_prices = price_components(
            &storage,
            chain,
            latest_block,
            &cli.protocol_systems,
            &component_ids,
            &all_tokens_by_address,
            &price_book,
            cli.snapshot_batch_size,
            cli.max_deviation_bps,
            &mut stats,
        )
        .await
        .with_context(|| format!("failed to price component graph in round {round}"))?;

        let updates =
            select_round_updates(round_prices, &price_book, &protected_tokens, &target_tokens);
        if updates.is_empty() {
            break;
        }

        for update in &updates {
            debug!(
                token = %update.token,
                price = update.native_per_token,
                score_bps = update.score_bps,
                edge_deviation_bps = update.edge_deviation_bps,
                hops = update.hops,
                via_token = %update.via_token,
                component_id = %update.component_id,
                "TokenPriceCandidate"
            );
        }

        apply_updates(&mut price_book, updates);
        stats.prices_computed = price_book
            .values()
            .filter(|state| !state.is_seed)
            .count();
    }

    // Convert solver output into rows for token_price.
    //
    // Existing DB prices in incremental mode are loaded as seed context only. They are useful for
    // simulating from already-priced tokens, but simply reading an old DB price must not make it a
    // fresh write. Initial mode writes hard anchors and every price discovered by this run.
    // Incremental mode writes only derived prices inside the expanded incremental graph, which
    // includes target tokens plus any temporary intermediates reached while solving those targets.
    let db_prices = price_book
        .iter()
        .filter(|(address, state)| {
            let write_mode = match cli.run_mode {
                RunMode::Initial => PriceWriteMode::Initial,
                RunMode::Incremental => PriceWriteMode::Incremental,
            };
            should_write_price(
                write_mode,
                address,
                state,
                &protected_tokens,
                &graph_scope.graph_tokens,
            )
        })
        .filter_map(|(address, state)| {
            all_tokens_by_address
                .get(address)
                .and_then(|token| {
                    native_per_token_to_db_price(state.native_per_token, token.decimals)
                        .map(|db_price| (address.clone(), db_price))
                })
        })
        .collect::<HashMap<_, _>>();

    if cli.dry_run {
        info!(?stats, prices_ready = db_prices.len(), "TychoTvlDryRunComplete");
        return Ok(());
    }

    stats.prices_written = storage
        .upsert_token_prices_by_address(&chain, &db_prices, cli.write_batch_size)
        .await?;

    let affected = graph_scope
        .affected_component_ids
        .into_iter()
        .collect::<Vec<_>>();
    stats.tvl_updated = storage
        .refresh_component_tvl(&chain, &cli.protocol_systems, Some(&affected), cli.write_batch_size)
        .await
        .context("failed to refresh component TVL")?;

    info!(?stats, "TychoTvlRunComplete");
    Ok(())
}

struct ComponentGraphScope {
    // Components loaded and simulated while solving prices.
    component_ids: HashSet<String>,
    // Components whose TVL should be recomputed after price writes.
    affected_component_ids: HashSet<String>,
    // Tokens reached during graph construction. Incremental writes are limited to this set so old
    // seed prices outside the graph are not rewritten as if this run refreshed them.
    graph_tokens: HashSet<Bytes>,
}

async fn build_incremental_graph_scope(
    storage: &DirectGateway,
    chain: Chain,
    protocol_systems: &[String],
    target_tokens: &HashSet<Bytes>,
    max_expansion_rounds: usize,
    batch_size: usize,
) -> Result<ComponentGraphScope> {
    // Components directly containing recently traded tokens are the only rows whose TVL can become
    // stale because of the incremental trigger. Keep this set separate from the expanded pricing
    // graph: expansion may pull in unrelated pools only to discover intermediate token prices.
    let target_token_vec = target_tokens
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let affected_component_ids = storage
        .get_components_for_tokens(&chain, protocol_systems, &target_token_vec)
        .await
        .context("failed to load affected components for incremental TVL refresh")?
        .into_iter()
        .collect::<HashSet<_>>();

    let mut component_ids = HashSet::new();
    let mut seen_tokens = target_tokens.clone();
    let mut frontier = target_tokens.clone();

    // Expand token -> component -> token for a bounded number of rounds. This phase does not price
    // anything; it only decides which pool states are useful context for the solver. The bound is
    // important for the systemd timer path because otherwise one recently traded token connected to
    // a popular asset could expand into most of the chain.
    for round in 0..max_expansion_rounds {
        if frontier.is_empty() {
            break;
        }

        let frontier_vec = frontier
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let round_components = storage
            .get_components_for_tokens(&chain, protocol_systems, &frontier_vec)
            .await
            .with_context(|| {
                format!("failed to load graph components for expansion round {round}")
            })?;

        let new_component_ids = round_components
            .into_iter()
            .filter(|id| component_ids.insert(id.clone()))
            .collect::<Vec<_>>();
        if new_component_ids.is_empty() {
            break;
        }

        let discovered_tokens = discover_component_tokens(
            storage,
            chain,
            protocol_systems,
            &new_component_ids,
            batch_size,
        )
        .await
        .with_context(|| format!("failed to discover graph tokens in expansion round {round}"))?;
        frontier = discovered_tokens
            .into_iter()
            .filter(|token| seen_tokens.insert(token.clone()))
            .collect();
    }

    Ok(ComponentGraphScope { component_ids, affected_component_ids, graph_tokens: seen_tokens })
}

async fn discover_component_tokens(
    storage: &DirectGateway,
    chain: Chain,
    protocol_systems: &[String],
    component_ids: &[String],
    batch_size: usize,
) -> Result<HashSet<Bytes>> {
    // `get_components_for_tokens` returns ids only. The expansion step needs token addresses for
    // the next frontier, so load component metadata through the existing DB-backed
    // ProtocolGateway instead of calling the indexer RPC.
    let mut tokens = HashSet::new();
    for protocol_system in protocol_systems {
        for id_batch in component_ids.chunks(batch_size.max(1)) {
            let id_refs = id_batch
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let components = ProtocolGateway::get_protocol_components(
                storage,
                &chain,
                Some(protocol_system.clone()),
                Some(&id_refs),
                None,
                None,
            )
            .await
            .with_context(|| format!("failed to load protocol components for {protocol_system}"))?
            .entity;
            for component in components {
                tokens.extend(component.tokens);
            }
        }
    }
    Ok(tokens)
}

#[allow(clippy::too_many_arguments)]
async fn price_components(
    storage: &DirectGateway,
    chain: Chain,
    latest_block: u64,
    protocol_systems: &[String],
    component_ids: &[String],
    all_tokens: &HashMap<Bytes, Token>,
    known_prices: &HashMap<Bytes, PriceState>,
    snapshot_batch_size: usize,
    max_deviation_bps: f64,
    stats: &mut RunStats,
) -> Result<Vec<PriceCandidate>> {
    let mut candidates = Vec::new();

    for protocol_system in protocol_systems {
        for id_batch in component_ids.chunks(snapshot_batch_size.max(1)) {
            let id_refs = id_batch
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let components = ProtocolGateway::get_protocol_components(
                storage,
                &chain,
                Some(protocol_system.clone()),
                Some(&id_refs),
                None,
                None,
            )
            .await
            .with_context(|| format!("failed to load protocol components for {protocol_system}"))?
            .entity
            .into_iter()
            .map(ResponseProtocolComponent::from)
            .collect::<Vec<_>>();
            if components.is_empty() {
                continue;
            }

            let states = ProtocolGateway::get_protocol_states(
                storage,
                &chain,
                Some(Version::from_block_number(chain, latest_block as i64)),
                Some(protocol_system.clone()),
                Some(&id_refs),
                true,
                None,
            )
            .await
            .with_context(|| format!("failed to load protocol states for {protocol_system}"))?
            .entity
            .into_iter()
            .map(|state| (state.component_id.clone(), ResponseProtocolState::from(state)))
            .collect::<HashMap<_, _>>();

            // Decode current pool state and emit every executable priced-input -> output-token
            // quote as a candidate. This function deliberately does not decide "the" price for a
            // token. A token may have many candidate prices from different pools and different
            // multi-hop routes; `pricing::select_round_updates` compares them globally for the
            // current pass and only applies improvements to the price book.
            let snapshots = components
                .into_iter()
                .filter_map(|component| {
                    states
                        .get(&component.id)
                        .cloned()
                        .map(|state| ComponentWithState {
                            state,
                            component,
                            component_tvl: None,
                            entrypoints: Vec::new(),
                        })
                })
                .collect::<Vec<_>>();

            for component_state in snapshots {
                let sim = match decode_state(
                    protocol_system,
                    component_state.clone(),
                    latest_block,
                    all_tokens,
                )
                .await
                {
                    Ok(sim) => sim,
                    Err(err) => {
                        debug!(%protocol_system, error = %err, "StateDecodeSkipped");
                        continue;
                    }
                };

                let token_addresses = component_state.component.tokens.clone();
                for token_in_addr in &token_addresses {
                    let Some(token_in) = all_tokens.get(token_in_addr) else {
                        continue;
                    };
                    let Some(input_price_state) = known_prices.get(token_in_addr) else {
                        continue;
                    };

                    for token_out_addr in &token_addresses {
                        if token_out_addr == token_in_addr {
                            continue;
                        }

                        let Some(token_out) = all_tokens.get(token_out_addr) else {
                            continue;
                        };
                        match price_edge(
                            sim.as_ref(),
                            token_in,
                            token_out,
                            input_price_state.native_per_token,
                            max_deviation_bps,
                        ) {
                            Some(candidate) => {
                                candidates.push(PriceCandidate {
                                    token: token_out_addr.clone(),
                                    native_per_token: candidate.0,
                                    score_bps: input_price_state.score_bps + candidate.1,
                                    edge_deviation_bps: candidate.1,
                                    hops: input_price_state.hops + 1,
                                    via_token: token_in_addr.clone(),
                                    component_id: component_state.component.id.clone(),
                                });
                            }
                            None => stats.rejected_edges += 1,
                        }
                    }
                }
            }
        }
    }

    Ok(candidates)
}

async fn decode_state(
    protocol_system: &str,
    snapshot: ComponentWithState,
    latest_block: u64,
    all_tokens: &HashMap<Bytes, Token>,
) -> Result<Box<dyn ProtocolSim>> {
    let header = BlockHeader { number: latest_block, ..Default::default() };
    let account_balances = HashMap::new();
    let decoder_context = DecoderContext::new();

    match protocol_system {
        "uniswap_v2" => {
            let state = UniswapV2State::try_from_with_header(
                snapshot,
                header,
                &account_balances,
                all_tokens,
                &decoder_context,
            )
            .await?;
            Ok(Box::new(state) as Box<dyn ProtocolSim>)
        }
        "uniswap_v3" => {
            let state = UniswapV3State::try_from_with_header(
                snapshot,
                header,
                &account_balances,
                all_tokens,
                &decoder_context,
            )
            .await?;
            Ok(Box::new(state) as Box<dyn ProtocolSim>)
        }
        other => bail!("unsupported protocol system {other}"),
    }
}

fn price_edge(
    sim: &dyn ProtocolSim,
    token_in: &Token,
    token_out: &Token,
    token_in_native_price: f64,
    max_deviation_bps: f64,
) -> Option<(f64, f64)> {
    let probes = [(1.0_f64, 0.25_f64), (0.01_f64, 0.60_f64), (0.00001_f64, 0.15_f64)];
    let limits = sim
        .get_limits(token_in.address.clone(), token_out.address.clone())
        .ok();

    let mut quoted_prices = Vec::new();
    for (probe_native, weight) in probes {
        let input_whole = probe_native / token_in_native_price;
        let Some(amount_in) = amount_to_raw(input_whole, token_in.decimals) else {
            continue;
        };
        if amount_in.is_zero() {
            continue;
        }
        if limits
            .as_ref()
            .is_some_and(|(max_in, _)| &amount_in > max_in)
        {
            continue;
        }
        let Ok(quote) = sim.get_amount_out(amount_in, token_in, token_out) else {
            continue;
        };
        let Some(output_whole) = raw_to_amount(&quote.amount, token_out.decimals) else {
            continue;
        };
        if output_whole <= 0.0 || !output_whole.is_finite() {
            continue;
        }
        quoted_prices.push((probe_native / output_whole, weight));
    }

    if quoted_prices.len() < 2 {
        return None;
    }

    let median = weighted_median(&quoted_prices)?;
    let max_dev_bps = quoted_prices
        .iter()
        .map(|(price, _)| ((price - median).abs() / median) * 10_000.0)
        .fold(0.0, f64::max);
    if max_dev_bps > max_deviation_bps {
        return None;
    }

    Some((median, max_dev_bps))
}

fn amount_to_raw(amount: f64, decimals: u32) -> Option<BigUint> {
    if !amount.is_finite() || amount <= 0.0 {
        return None;
    }
    let raw = amount * 10_f64.powi(decimals as i32);
    if !raw.is_finite() || raw <= 0.0 || raw > u128::MAX as f64 {
        return None;
    }
    Some(BigUint::from(raw.floor() as u128))
}

fn raw_to_amount(raw: &BigUint, decimals: u32) -> Option<f64> {
    let value = raw.to_f64()?;
    Some(value / 10_f64.powi(decimals as i32))
}

fn native_per_token_to_db_price(native_per_token: f64, decimals: u32) -> Option<f64> {
    if !native_per_token.is_finite() || native_per_token <= 0.0 {
        return None;
    }
    Some(10_f64.powi(decimals as i32) / native_per_token)
}

fn db_price_to_native_per_token(db_price: f64, decimals: u32) -> Option<f64> {
    if !db_price.is_finite() || db_price <= 0.0 {
        return None;
    }
    Some(10_f64.powi(decimals as i32) / db_price)
}

fn weighted_median(values: &[(f64, f64)]) -> Option<f64> {
    let mut values = values
        .iter()
        .copied()
        .filter(|(value, weight)| value.is_finite() && *value > 0.0 && *weight > 0.0)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.0.total_cmp(&b.0));
    let total_weight = values
        .iter()
        .map(|(_, weight)| weight)
        .sum::<f64>();
    let mut cumulative = 0.0;
    for (value, weight) in values {
        cumulative += weight;
        if cumulative >= total_weight / 2.0 {
            return Some(value);
        }
    }
    None
}

fn hard_anchors(chain: &Chain) -> HashMap<Bytes, f64> {
    let mut anchors = HashMap::new();
    let address = match chain {
        Chain::Ethereum => Some("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
        Chain::Base | Chain::Unichain => Some("0x4200000000000000000000000000000000000006"),
        _ => None,
    };
    if let Some(address) = address {
        anchors.insert(Bytes::from_str(address).expect("hardcoded WETH address is valid"), 1.0);
    } else {
        warn!(?chain, "NoDefaultAnchorForChain");
    }
    anchors
}

fn init_tracing() {
    let format = tracing_subscriber::fmt::format()
        .with_level(true)
        .with_target(false)
        .compact();
    tracing_subscriber::fmt()
        .event_format(format)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_unit_conversion_round_trips() {
        let db_price = native_per_token_to_db_price(1.0 / 3_000.0, 6).unwrap();
        assert!((db_price - 3_000_000_000.0).abs() < 1.0);
        let native = db_price_to_native_per_token(db_price, 6).unwrap();
        assert!((native - 1.0 / 3_000.0).abs() < 1e-12);
    }

    #[test]
    fn weighted_median_prefers_middle_probe() {
        let values = [(100.0, 0.25), (101.0, 0.60), (150.0, 0.15)];
        assert_eq!(weighted_median(&values), Some(101.0));
    }

    #[test]
    fn weighted_median_ignores_invalid_values() {
        let values = [(0.0, 0.25), (42.0, 0.60), (50.0, 0.15)];
        assert_eq!(weighted_median(&values), Some(42.0));
    }
}
