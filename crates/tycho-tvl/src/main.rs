use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::{Duration as StdDuration, Instant},
};

mod db;
mod pricing;

use anyhow::{bail, Context, Result};
use chrono::Duration as ChronoDuration;
use clap::{Parser, ValueEnum};
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use tracing::{debug, info, trace, warn};
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
use tycho_storage::postgres::{builder::GatewayBuilder, direct::DirectGateway};

use crate::db::TvlDb;
use crate::pricing::{
    apply_updates, select_round_updates, should_write_price, PriceCandidate, PriceState,
    PriceWriteMode,
};

const DEFAULT_BATCH_SIZE: usize = 5_000;
const DEFAULT_SNAPSHOT_BATCH_SIZE: usize = 500;
const DEFAULT_DEVIATION_BPS: f64 = 300.0;
const DEFAULT_MAX_INCREMENTAL_INTERMEDIATE_TOKENS: usize = 60;
const DEFAULT_MAX_INCREMENTAL_COMPONENTS_PER_TOKEN: i64 = 25;
const DEFAULT_MAX_INCREMENTAL_GRAPH_COMPONENTS: usize = 25_000;
const MIN_EDGE_INLIER_WEIGHT: f64 = 0.60;

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
    #[arg(long, default_value_t = 6)]
    max_rounds_initial: usize,
    #[arg(long, default_value_t = 4)]
    max_rounds_incremental: usize,
    #[arg(long, default_value_t = 10.0)]
    min_initial_update_bps: f64,
    #[arg(long, default_value_t = 10.0)]
    min_price_improvement_bps: f64,
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    write_batch_size: usize,
    #[arg(long, default_value_t = DEFAULT_SNAPSHOT_BATCH_SIZE)]
    snapshot_batch_size: usize,
    #[arg(long, default_value_t = DEFAULT_DEVIATION_BPS)]
    max_deviation_bps: f64,
    #[arg(long, default_value_t = DEFAULT_MAX_INCREMENTAL_INTERMEDIATE_TOKENS)]
    max_incremental_intermediate_tokens: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_INCREMENTAL_COMPONENTS_PER_TOKEN)]
    max_incremental_components_per_token: i64,
    #[arg(long, default_value_t = DEFAULT_MAX_INCREMENTAL_GRAPH_COMPONENTS)]
    max_incremental_graph_components: usize,
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

#[derive(Debug, Default)]
struct DecodeSkipStats {
    by_reason: HashMap<String, usize>,
}

impl DecodeSkipStats {
    fn record(&mut self, err: &anyhow::Error) {
        *self
            .by_reason
            .entry(err.to_string())
            .or_default() += 1;
    }

    fn summary(&self) -> String {
        let mut reasons = self
            .by_reason
            .iter()
            .map(|(reason, count)| (reason.as_str(), *count))
            .collect::<Vec<_>>();
        reasons.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        reasons
            .into_iter()
            .map(|(reason, count)| format!("{reason}: {count}"))
            .collect::<Vec<_>>()
            .join("; ")
    }
}

#[derive(Debug, Clone)]
struct ProbeQuote {
    output_raw: BigUint,
    output_whole: f64,
    native_per_token: f64,
    weight: f64,
}

#[derive(Debug, Default)]
struct TargetCoverage {
    total: usize,
    priced: usize,
    seed_priced: usize,
    derived_priced: usize,
    writable: usize,
    unpriced: usize,
    unpriced_samples: Vec<String>,
}

fn target_coverage(
    target_tokens: &HashSet<Bytes>,
    price_book: &HashMap<Bytes, PriceState>,
    db_prices: &HashMap<Bytes, f64>,
) -> TargetCoverage {
    let mut coverage = TargetCoverage { total: target_tokens.len(), ..TargetCoverage::default() };

    let mut unpriced_samples = Vec::new();
    for token in target_tokens {
        match price_book.get(token) {
            Some(state) => {
                coverage.priced += 1;
                if state.is_seed {
                    coverage.seed_priced += 1;
                } else {
                    coverage.derived_priced += 1;
                }
            }
            None => {
                coverage.unpriced += 1;
                if unpriced_samples.len() < 10 {
                    unpriced_samples.push(token.to_string());
                }
            }
        }
        if db_prices.contains_key(token) {
            coverage.writable += 1;
        }
    }
    coverage.unpriced_samples = unpriced_samples;
    coverage
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
    info!(
        run_mode = ?cli.run_mode,
        chain = %chain,
        protocol_systems = ?cli.protocol_systems,
        cron_period_secs = cli.cron_period_secs,
        recent_window_multiplier = cli.recent_window_multiplier,
        max_rounds_initial = cli.max_rounds_initial,
        max_rounds_incremental = cli.max_rounds_incremental,
        min_initial_update_bps = cli.min_initial_update_bps,
        min_price_improvement_bps = cli.min_price_improvement_bps,
        write_batch_size = cli.write_batch_size,
        snapshot_batch_size = cli.snapshot_batch_size,
        max_deviation_bps = cli.max_deviation_bps,
        max_incremental_intermediate_tokens = cli.max_incremental_intermediate_tokens,
        max_incremental_components_per_token = cli.max_incremental_components_per_token,
        max_incremental_graph_components = cli.max_incremental_graph_components,
        dry_run = cli.dry_run,
        "TychoTvlRunStarting"
    );

    info!("TychoTvlOpeningDatabaseGateway");
    let storage = GatewayBuilder::new(&cli.database_url)
        .set_chains(&[chain])
        .build_direct_gw()
        .await
        .context("failed to build direct storage gateway")?;
    let tvl_db = TvlDb::connect(&cli.database_url)?;
    info!("TychoTvlDatabaseGatewayReady");

    info!("TychoTvlLoadingLatestBlock");
    let latest_block = tvl_db
        .get_latest_block_number(&chain)
        .await
        .context("failed to load latest indexed block")?;
    info!(latest_block, "TychoTvlLatestBlockLoaded");

    info!("TychoTvlLoadingTokens");
    let all_tokens =
        ProtocolGateway::get_tokens(&storage, chain, None, QualityRange::None(), None, None)
            .await
            .context("failed to load tokens")?
            .entity;
    info!(tokens = all_tokens.len(), "TychoTvlTokensLoaded");

    let all_tokens_by_address = all_tokens
        .iter()
        .cloned()
        .map(|t| (t.address.clone(), t))
        .collect::<HashMap<_, _>>();

    info!("TychoTvlLoadingExistingTokenPrices");
    let existing_db_prices = tvl_db
        .get_existing_token_prices_for_chain(&chain)
        .await
        .context("failed to load existing token prices")?;
    info!(prices = existing_db_prices.len(), "TychoTvlExistingTokenPricesLoaded");

    let mut stats = RunStats {
        tokens_loaded: all_tokens.len(),
        existing_prices: existing_db_prices.len(),
        ..RunStats::default()
    };

    let hard_anchors = hard_anchors(&chain);
    let mut incremental_affected_component_ids = HashSet::new();
    let target_tokens = match cli.run_mode {
        RunMode::Initial => all_tokens_by_address
            .keys()
            .cloned()
            .collect::<HashSet<_>>(),
        RunMode::Incremental => {
            info!("TychoTvlLoadingDbTimestamp");
            let now = tvl_db
                .get_current_db_timestamp()
                .await
                .context("failed to get DB timestamp")?;
            let window_secs = cli.cron_period_secs * cli.recent_window_multiplier;
            let since = now - ChronoDuration::seconds(window_secs);
            info!(
                now = %now,
                since = %since,
                window_secs,
                "TychoTvlLoadingRecentlyChangedComponents"
            );
            let changed_component_ids = tvl_db
                .get_recently_changed_components(&chain, &cli.protocol_systems, since)
                .await
                .context("failed to load recently changed components")?;
            info!(
                components = changed_component_ids.len(),
                "TychoTvlRecentlyChangedComponentsLoaded"
            );

            let changed_tokens = discover_component_tokens(
                &storage,
                chain,
                &cli.protocol_systems,
                &changed_component_ids,
                cli.snapshot_batch_size,
            )
            .await
            .context("failed to load tokens from recently changed components")?;
            incremental_affected_component_ids = changed_component_ids
                .into_iter()
                .collect();
            changed_tokens
        }
    };
    stats.target_tokens = target_tokens.len();
    info!(target_tokens = target_tokens.len(), "TychoTvlTargetTokensSelected");

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
    let seed_tokens = price_book
        .keys()
        .cloned()
        .collect::<HashSet<_>>();

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
    // Incremental mode starts from components whose balances changed recently, then discovers the
    // tokens inside those changed pools. This is important: starting from recently changed tokens
    // and asking for every pool containing those tokens makes hub assets such as WETH expand into
    // most of the chain. The route graph can still expand outward, but only with explicit per-token
    // and total graph caps.
    //
    // A changed target token may only be priceable through an intermediate token that also has to
    // be discovered during this run:
    //
    //     WETH -> intermediate token -> token in a recently changed component
    //
    // The expanded graph is used for pricing context, but TVL refresh is kept scoped to components
    // whose balances changed in the recent window. That avoids rewriting unrelated component_tvl
    // rows just because a token also appears in those unrelated components.
    let graph_scope = match cli.run_mode {
        RunMode::Initial => {
            info!("TychoTvlLoadingInitialComponentGraph");
            let component_ids = tvl_db
                .get_components_for_protocols(&chain, &cli.protocol_systems)
                .await
                .context("failed to load scoped components")?;
            info!(components = component_ids.len(), "TychoTvlInitialComponentGraphLoaded");
            ComponentGraphScope {
                component_ids: component_ids.iter().cloned().collect(),
                affected_component_ids: component_ids.into_iter().collect(),
                graph_tokens: target_tokens.clone(),
            }
        }
        RunMode::Incremental => build_incremental_graph_scope(
            &storage,
            &tvl_db,
            chain,
            &cli.protocol_systems,
            incremental_affected_component_ids,
            &target_tokens,
            &seed_tokens,
            max_rounds,
            cli.snapshot_batch_size,
            cli.max_incremental_intermediate_tokens,
            cli.max_incremental_components_per_token,
            cli.max_incremental_graph_components,
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
    info!(
        components = component_ids.len(),
        graph_tokens = graph_scope.graph_tokens.len(),
        affected_components = graph_scope.affected_component_ids.len(),
        "TychoTvlGraphScopeReady"
    );

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

        info!(
            round,
            components = component_ids.len(),
            known_prices = price_book.len(),
            rejected_edges = stats.rejected_edges,
            "TychoTvlPricingRoundStarting"
        );
        let known_prices_at_round_start = price_book.len();
        let round_prices = price_components(
            &storage,
            &tvl_db,
            round,
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
        info!(
            round,
            candidates = round_prices.len(),
            rejected_edges = stats.rejected_edges,
            "TychoTvlPricingRoundCandidatesBuilt"
        );

        let updates = select_round_updates(
            round_prices,
            &price_book,
            &protected_tokens,
            &target_tokens,
            cli.min_price_improvement_bps,
        );
        let update_rate_bps = update_rate_bps(updates.len(), known_prices_at_round_start);
        info!(
            round,
            updates = updates.len(),
            known_prices = known_prices_at_round_start,
            update_rate_bps,
            "TychoTvlPricingRoundUpdatesSelected"
        );
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
        info!(
            round,
            derived_prices = stats.prices_computed,
            total_price_book = price_book.len(),
            "TychoTvlPricingRoundApplied"
        );

        if matches!(cli.run_mode, RunMode::Initial)
            && cli.min_initial_update_bps > 0.0
            && update_rate_bps < cli.min_initial_update_bps
        {
            info!(
                round,
                update_rate_bps,
                min_initial_update_bps = cli.min_initial_update_bps,
                "TychoTvlInitialStopThresholdReached"
            );
            break;
        }
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
    info!(
        prices_ready = db_prices.len(),
        derived_prices = stats.prices_computed,
        "TychoTvlDbPricesPrepared"
    );
    let coverage = target_coverage(&target_tokens, &price_book, &db_prices);
    info!(
        total = coverage.total,
        priced = coverage.priced,
        seed_priced = coverage.seed_priced,
        derived_priced = coverage.derived_priced,
        writable = coverage.writable,
        unpriced = coverage.unpriced,
        unpriced_samples = ?coverage.unpriced_samples,
        "TychoTvlTargetPriceCoverage"
    );

    if cli.dry_run {
        info!(?stats, prices_ready = db_prices.len(), "TychoTvlDryRunComplete");
        return Ok(());
    }

    info!(prices = db_prices.len(), "TychoTvlWritingTokenPrices");
    stats.prices_written = tvl_db
        .upsert_token_prices_by_address(&chain, &db_prices, cli.write_batch_size)
        .await?;
    info!(prices_written = stats.prices_written, "TychoTvlTokenPricesWritten");

    let affected = graph_scope
        .affected_component_ids
        .into_iter()
        .collect::<Vec<_>>();
    info!(components = affected.len(), "TychoTvlRefreshingComponentTvl");
    stats.tvl_updated = tvl_db
        .refresh_component_tvl(&chain, &cli.protocol_systems, Some(&affected), cli.write_batch_size)
        .await
        .context("failed to refresh component TVL")?;
    info!(components_updated = stats.tvl_updated, "TychoTvlComponentTvlRefreshed");

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
    tvl_db: &TvlDb,
    chain: Chain,
    protocol_systems: &[String],
    affected_component_ids: HashSet<String>,
    target_tokens: &HashSet<Bytes>,
    seed_tokens: &HashSet<Bytes>,
    max_expansion_rounds: usize,
    batch_size: usize,
    max_intermediate_tokens: usize,
    max_components_per_token: i64,
    max_graph_components: usize,
) -> Result<ComponentGraphScope> {
    info!(
        target_tokens = target_tokens.len(),
        affected_components = affected_component_ids.len(),
        seed_tokens = seed_tokens.len(),
        max_expansion_rounds,
        batch_size,
        max_intermediate_tokens,
        max_components_per_token,
        max_graph_components,
        "TychoTvlIncrementalGraphBuildStarting"
    );

    // Components whose balances changed are the only rows whose TVL can become stale because of
    // the incremental trigger. Keep this set separate from route-expansion components: expansion
    // may pull in unrelated pools only to discover intermediate token prices.
    let mut component_ids = affected_component_ids.clone();
    let mut seen_tokens = target_tokens.clone();
    let (mut frontier, capped_initial_frontier_tokens) = limit_token_set(
        target_tokens
            .iter()
            .filter(|token| !seed_tokens.contains(*token))
            .cloned()
            .collect(),
        max_intermediate_tokens,
    );
    info!(
        initial_frontier_tokens = frontier.len(),
        capped_initial_frontier_tokens,
        initial_components = component_ids.len(),
        "TychoTvlIncrementalGraphSeeded"
    );

    // Expand token -> component -> token for a bounded number of rounds. This phase does not price
    // anything; it only decides which pool states are useful context for the solver. Unlike a
    // normal BFS, every frontier token is capped to a small number of candidate pools, ordered by
    // existing TVL where available. This mirrors the production route-search approach: find a
    // bounded candidate graph, then let simulation/ranking choose the best executable prices.
    for round in 0..max_expansion_rounds {
        if frontier.is_empty() {
            break;
        }
        if component_ids.len() >= max_graph_components {
            info!(
                round,
                components = component_ids.len(),
                max_graph_components,
                "TychoTvlGraphExpansionSkippedAtComponentCap"
            );
            break;
        }

        info!(
            round,
            frontier_tokens = frontier.len(),
            seen_tokens = seen_tokens.len(),
            known_components = component_ids.len(),
            "TychoTvlGraphExpansionRoundStarting"
        );
        let frontier_vec = frontier
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let round_components = tvl_db
            .get_components_for_tokens_limited(
                &chain,
                protocol_systems,
                &frontier_vec,
                max_components_per_token,
            )
            .await
            .with_context(|| {
                format!("failed to load graph components for expansion round {round}")
            })?;
        info!(round, components = round_components.len(), "TychoTvlGraphExpansionComponentsLoaded");

        let mut new_component_ids = round_components
            .into_iter()
            .filter(|id| !component_ids.contains(id))
            .collect::<Vec<_>>();
        let remaining_component_capacity = max_graph_components.saturating_sub(component_ids.len());
        let capped_components = new_component_ids
            .len()
            .saturating_sub(remaining_component_capacity);
        if new_component_ids.len() > remaining_component_capacity {
            new_component_ids.truncate(remaining_component_capacity);
        }
        for id in &new_component_ids {
            component_ids.insert(id.clone());
        }
        info!(
            round,
            new_components = new_component_ids.len(),
            capped_components,
            total_components = component_ids.len(),
            "TychoTvlGraphExpansionNewComponentsSelected"
        );
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
        let frontier_selection = select_next_frontier(
            discovered_tokens,
            &mut seen_tokens,
            seed_tokens,
            max_intermediate_tokens,
        );
        frontier = frontier_selection.frontier;
        info!(
            round,
            next_frontier_tokens = frontier.len(),
            seen_tokens = seen_tokens.len(),
            terminal_seed_tokens = frontier_selection.terminal_seed_tokens,
            repeated_tokens = frontier_selection.repeated_tokens,
            capped_frontier_tokens = frontier_selection.capped_frontier_tokens,
            "TychoTvlGraphExpansionRoundComplete"
        );
    }

    info!(
        components = component_ids.len(),
        affected_components = affected_component_ids.len(),
        graph_tokens = seen_tokens.len(),
        "TychoTvlIncrementalGraphBuildComplete"
    );
    Ok(ComponentGraphScope { component_ids, affected_component_ids, graph_tokens: seen_tokens })
}

#[derive(Debug, Default)]
struct FrontierSelection {
    frontier: HashSet<Bytes>,
    terminal_seed_tokens: usize,
    repeated_tokens: usize,
    capped_frontier_tokens: usize,
}

fn select_next_frontier(
    discovered_tokens: impl IntoIterator<Item = Bytes>,
    seen_tokens: &mut HashSet<Bytes>,
    seed_tokens: &HashSet<Bytes>,
    max_frontier_tokens: usize,
) -> FrontierSelection {
    let mut selection = FrontierSelection::default();
    let mut frontier = HashSet::new();

    for token in discovered_tokens {
        if !seen_tokens.insert(token.clone()) {
            selection.repeated_tokens += 1;
            continue;
        }

        // Existing DB prices and hard anchors are valid route endpoints. Expanding through them is
        // dangerous for incremental runs because hub assets such as WETH or USDC connect to
        // enormous portions of the graph. Keeping them out of the next frontier still lets their
        // current prices drive simulations in the components already loaded, while preventing one
        // recently changed component from becoming a near-full-chain scan.
        if seed_tokens.contains(&token) {
            selection.terminal_seed_tokens += 1;
            continue;
        }

        frontier.insert(token);
    }

    let (frontier, capped_frontier_tokens) = limit_token_set(frontier, max_frontier_tokens);
    selection.frontier = frontier;
    selection.capped_frontier_tokens = capped_frontier_tokens;
    selection
}

fn limit_token_set(tokens: HashSet<Bytes>, limit: usize) -> (HashSet<Bytes>, usize) {
    if limit == 0 {
        let capped = tokens.len();
        return (HashSet::new(), capped);
    }
    if tokens.len() <= limit {
        return (tokens, 0);
    }

    let original_len = tokens.len();
    let mut tokens = tokens.into_iter().collect::<Vec<_>>();
    tokens.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    tokens.truncate(limit);
    (tokens.into_iter().collect(), original_len - limit)
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
            debug!(
                protocol_system = %protocol_system,
                batch_components = id_batch.len(),
                "TychoTvlDiscoveringComponentTokensBatch"
            );
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
    info!(
        components = component_ids.len(),
        discovered_tokens = tokens.len(),
        "TychoTvlComponentTokensDiscovered"
    );
    Ok(tokens)
}

#[allow(clippy::too_many_arguments)]
async fn price_components(
    storage: &DirectGateway,
    tvl_db: &TvlDb,
    round: usize,
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
    let batch_size = snapshot_batch_size.max(1);
    let batches_per_protocol = component_ids.len().div_ceil(batch_size);
    let total_batches = protocol_systems
        .len()
        .saturating_mul(batches_per_protocol);
    let started_at = Instant::now();
    let mut last_progress_at = started_at;
    let mut processed_batches = 0usize;
    let rejected_edges_at_start = stats.rejected_edges;

    for protocol_system in protocol_systems {
        for (batch_index, id_batch) in component_ids
            .chunks(batch_size)
            .enumerate()
        {
            let load_ids = filter_component_ids_for_simulation(
                tvl_db,
                chain,
                protocol_system,
                id_batch,
                batch_index,
            )
            .await?;
            if load_ids.is_empty() {
                processed_batches += 1;
                maybe_log_pricing_progress(
                    round,
                    protocol_system,
                    batch_index,
                    processed_batches,
                    total_batches,
                    candidates.len(),
                    stats
                        .rejected_edges
                        .saturating_sub(rejected_edges_at_start),
                    started_at,
                    &mut last_progress_at,
                );
                continue;
            }

            let id_refs = load_ids
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();

            debug!(
                protocol_system = %protocol_system,
                batch_index,
                batch_components = id_batch.len(),
                eligible_components = load_ids.len(),
                "TychoTvlPricingBatchLoadingComponents"
            );
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
            debug!(
                protocol_system = %protocol_system,
                batch_index,
                components = components.len(),
                "TychoTvlPricingBatchComponentsLoaded"
            );
            if components.is_empty() {
                processed_batches += 1;
                maybe_log_pricing_progress(
                    round,
                    protocol_system,
                    batch_index,
                    processed_batches,
                    total_batches,
                    candidates.len(),
                    stats
                        .rejected_edges
                        .saturating_sub(rejected_edges_at_start),
                    started_at,
                    &mut last_progress_at,
                );
                continue;
            }

            debug!(
                protocol_system = %protocol_system,
                batch_index,
                components = components.len(),
                latest_block,
                "TychoTvlPricingBatchLoadingStates"
            );
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
            debug!(
                protocol_system = %protocol_system,
                batch_index,
                states = states.len(),
                "TychoTvlPricingBatchStatesLoaded"
            );

            // Decode current pool state and emit every executable priced-input -> output-token
            // quote as a candidate. This function deliberately does not decide "the" price for a
            // token. A token may have many candidate prices from different pools and different
            // multi-hop routes; `pricing::select_round_updates` compares them globally for the
            // current pass and only applies improvements to the price book.
            let components_len = components.len();
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
            debug!(
                protocol_system = %protocol_system,
                batch_index,
                snapshots = snapshots.len(),
                missing_states = components_len.saturating_sub(snapshots.len()),
                "TychoTvlPricingBatchSnapshotsBuilt"
            );

            let mut batch_candidates = 0usize;
            let mut decoded_components = 0usize;
            let mut skipped_components = 0usize;
            let mut decode_skip_stats = DecodeSkipStats::default();
            let rejected_before = stats.rejected_edges;
            for component_state in snapshots {
                let sim = match decode_state(
                    protocol_system,
                    component_state.clone(),
                    latest_block,
                    all_tokens,
                )
                .await
                {
                    Ok(sim) => {
                        decoded_components += 1;
                        sim
                    }
                    Err(err) => {
                        skipped_components += 1;
                        decode_skip_stats.record(&err);
                        trace!(
                            %protocol_system,
                            component_id = %component_state.component.id,
                            error = %err,
                            "StateDecodeSkipped"
                        );
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
                                batch_candidates += 1;
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
            debug!(
                protocol_system = %protocol_system,
                batch_index,
                decoded_components,
                skipped_components,
                candidates = batch_candidates,
                rejected_edges = stats.rejected_edges.saturating_sub(rejected_before),
                total_candidates = candidates.len(),
                decode_skip_reasons = %decode_skip_stats.summary(),
                "TychoTvlPricingBatchComplete"
            );
            processed_batches += 1;
            maybe_log_pricing_progress(
                round,
                protocol_system,
                batch_index,
                processed_batches,
                total_batches,
                candidates.len(),
                stats
                    .rejected_edges
                    .saturating_sub(rejected_edges_at_start),
                started_at,
                &mut last_progress_at,
            );
        }
    }

    Ok(candidates)
}

fn update_rate_bps(updates: usize, known_prices: usize) -> f64 {
    if known_prices == 0 {
        return 0.0;
    }
    (updates as f64 / known_prices as f64) * 10_000.0
}

#[allow(clippy::too_many_arguments)]
fn maybe_log_pricing_progress(
    round: usize,
    protocol_system: &str,
    batch_index: usize,
    processed_batches: usize,
    total_batches: usize,
    total_candidates: usize,
    rejected_edges: usize,
    started_at: Instant,
    last_progress_at: &mut Instant,
) {
    let now = Instant::now();
    if processed_batches < total_batches
        && now.duration_since(*last_progress_at) < StdDuration::from_secs(60)
    {
        return;
    }

    *last_progress_at = now;
    let progress_pct = if total_batches == 0 {
        100.0
    } else {
        processed_batches as f64 * 100.0 / total_batches as f64
    };
    info!(
        round,
        protocol_system,
        batch_index,
        processed_batches,
        total_batches,
        progress_pct,
        elapsed_secs = now.duration_since(started_at).as_secs(),
        total_candidates,
        rejected_edges,
        "TychoTvlPricingProgress"
    );
}

async fn filter_component_ids_for_simulation(
    tvl_db: &TvlDb,
    chain: Chain,
    protocol_system: &str,
    component_ids: &[String],
    batch_index: usize,
) -> Result<Vec<String>> {
    if protocol_system != "uniswap_v3" {
        return Ok(component_ids.to_vec());
    }

    debug!(
        protocol_system,
        batch_index,
        batch_components = component_ids.len(),
        "TychoTvlFilteringV3ComponentsWithTickState"
    );
    let filtered = tvl_db
        .filter_components_with_state_requirements(
            &chain,
            component_ids,
            &["liquidity", "sqrt_price_x96", "tick"],
            &["ticks/"],
        )
        .await
        .context("failed to filter uniswap_v3 components by required state attributes")?;
    debug!(
        protocol_system,
        batch_index,
        batch_components = component_ids.len(),
        eligible_components = filtered.len(),
        skipped_components = component_ids
            .len()
            .saturating_sub(filtered.len()),
        "TychoTvlV3ComponentsWithTickStateFiltered"
    );

    Ok(filtered)
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

    let mut probe_quotes = Vec::new();
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
        probe_quotes.push(ProbeQuote {
            output_raw: quote.amount,
            output_whole,
            native_per_token: probe_native / output_whole,
            weight,
        });
    }

    let quoted_prices = probe_quotes
        .iter()
        .map(|quote| (quote.native_per_token, quote.weight))
        .collect::<Vec<_>>();
    let selected = select_edge_price_from_probes(&quoted_prices, max_deviation_bps)?;
    let adjusted_price = sell_side_adjusted_price(
        sim,
        token_in,
        token_out,
        token_in_native_price,
        &probe_quotes,
        selected.0,
        max_deviation_bps,
    )
    .unwrap_or(selected.0);

    Some((adjusted_price, selected.1))
}

fn sell_side_adjusted_price(
    sim: &dyn ProtocolSim,
    token_in: &Token,
    token_out: &Token,
    token_in_native_price: f64,
    probe_quotes: &[ProbeQuote],
    selected_native_per_token: f64,
    max_deviation_bps: f64,
) -> Option<f64> {
    let mut reverse_prices = Vec::new();
    for quote in probe_quotes {
        let forward_dev_bps = ((quote.native_per_token - selected_native_per_token).abs()
            / selected_native_per_token)
            * 10_000.0;
        if forward_dev_bps > max_deviation_bps {
            continue;
        }

        let Ok(reverse_quote) = sim.get_amount_out(quote.output_raw.clone(), token_out, token_in)
        else {
            continue;
        };
        let Some(recovered_input_whole) = raw_to_amount(&reverse_quote.amount, token_in.decimals)
        else {
            continue;
        };
        if recovered_input_whole <= 0.0 || !recovered_input_whole.is_finite() {
            continue;
        }

        let recovered_native = recovered_input_whole * token_in_native_price;
        let reverse_native_per_token = recovered_native / quote.output_whole;
        if reverse_native_per_token.is_finite() && reverse_native_per_token > 0.0 {
            reverse_prices.push((reverse_native_per_token, quote.weight));
        }
    }

    let (sell_side_native_per_token, _) =
        select_edge_price_from_probes(&reverse_prices, max_deviation_bps)?;
    Some(sell_side_native_per_token.min(selected_native_per_token))
}

fn select_edge_price_from_probes(
    quoted_prices: &[(f64, f64)],
    max_deviation_bps: f64,
) -> Option<(f64, f64)> {
    if quoted_prices.len() < 2 {
        return None;
    }

    let median = weighted_median(quoted_prices)?;
    let mut inliers = quoted_prices
        .iter()
        .copied()
        .filter(|(price, _)| ((*price - median).abs() / median) * 10_000.0 <= max_deviation_bps)
        .collect::<Vec<_>>();
    let inlier_weight = inliers
        .iter()
        .map(|(_, weight)| *weight)
        .sum::<f64>();
    if inliers.len() < 2 || inlier_weight + f64::EPSILON < MIN_EDGE_INLIER_WEIGHT {
        return None;
    }

    let median = weighted_median(&inliers)?;
    inliers.retain(|(price, _)| ((*price - median).abs() / median) * 10_000.0 <= max_deviation_bps);
    let inlier_weight = inliers
        .iter()
        .map(|(_, weight)| *weight)
        .sum::<f64>();
    if inliers.len() < 2 || inlier_weight + f64::EPSILON < MIN_EDGE_INLIER_WEIGHT {
        return None;
    }

    let max_dev_bps = inliers
        .iter()
        .map(|(price, _)| ((price - median).abs() / median) * 10_000.0)
        .fold(0.0, f64::max);

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

    #[test]
    fn update_rate_is_reported_in_basis_points() {
        assert_eq!(update_rate_bps(100, 10_000), 100.0);
        assert_eq!(update_rate_bps(25, 50_000), 5.0);
        assert_eq!(update_rate_bps(1, 0), 0.0);
    }

    #[test]
    fn edge_price_accepts_large_probe_slippage_outlier() {
        let values = [(1.10, 0.25), (1.0, 0.60), (1.0, 0.15)];
        assert_eq!(select_edge_price_from_probes(&values, 300.0), Some((1.0, 0.0)));
    }

    #[test]
    fn edge_price_rejects_when_middle_probe_is_outlier() {
        let values = [(1.0, 0.25), (1.10, 0.60), (1.0, 0.15)];
        assert_eq!(select_edge_price_from_probes(&values, 300.0), None);
    }

    #[test]
    fn edge_price_accepts_tiny_probe_outlier() {
        let values = [(1.0, 0.25), (1.0, 0.60), (1.10, 0.15)];
        assert_eq!(select_edge_price_from_probes(&values, 300.0), Some((1.0, 0.0)));
    }

    #[test]
    fn incremental_frontier_does_not_expand_through_seed_tokens() {
        let seen = Bytes::from(vec![1; 20]);
        let seed = Bytes::from(vec![2; 20]);
        let new = Bytes::from(vec![3; 20]);
        let mut seen_tokens = HashSet::from([seen.clone()]);
        let seed_tokens = HashSet::from([seed.clone()]);

        let selection = select_next_frontier(
            vec![seen.clone(), seed.clone(), new.clone()],
            &mut seen_tokens,
            &seed_tokens,
            10,
        );

        assert_eq!(selection.frontier, HashSet::from([new]));
        assert_eq!(selection.terminal_seed_tokens, 1);
        assert_eq!(selection.repeated_tokens, 1);
        assert!(seen_tokens.contains(&seen));
        assert!(seen_tokens.contains(&seed));
    }

    #[test]
    fn incremental_frontier_is_capped() {
        let mut seen_tokens = HashSet::new();
        let seed_tokens = HashSet::new();
        let selection = select_next_frontier(
            vec![Bytes::from(vec![3; 20]), Bytes::from(vec![1; 20]), Bytes::from(vec![2; 20])],
            &mut seen_tokens,
            &seed_tokens,
            2,
        );

        assert_eq!(selection.frontier.len(), 2);
        assert_eq!(selection.capped_frontier_tokens, 1);
        assert!(selection
            .frontier
            .contains(&Bytes::from(vec![1; 20])));
        assert!(selection
            .frontier
            .contains(&Bytes::from(vec![2; 20])));
    }
}
