use std::collections::{HashMap, HashSet};

use tycho_common::Bytes;

const SCORE_EPSILON_BPS: f64 = 1e-9;

#[derive(Debug, Clone)]
pub struct PriceState {
    pub native_per_token: f64,
    pub score_bps: f64,
    pub hops: usize,
    pub is_seed: bool,
}

impl PriceState {
    pub fn seed(native_per_token: f64) -> Self {
        Self { native_per_token, score_bps: 0.0, hops: 0, is_seed: true }
    }

    pub fn derived(native_per_token: f64, score_bps: f64, hops: usize) -> Self {
        Self { native_per_token, score_bps, hops, is_seed: false }
    }
}

#[derive(Debug, Clone)]
pub struct PriceCandidate {
    pub token: Bytes,
    pub native_per_token: f64,
    pub score_bps: f64,
    pub edge_deviation_bps: f64,
    pub hops: usize,
    pub via_token: Bytes,
    pub component_id: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PriceWriteMode {
    Initial,
    Incremental,
}

pub fn should_write_price(
    mode: PriceWriteMode,
    token: &Bytes,
    state: &PriceState,
    protected_tokens: &HashSet<Bytes>,
    graph_tokens: &HashSet<Bytes>,
) -> bool {
    // Keep write behavior stricter than solver behavior.
    //
    // The solver may carry prices that were only used as route context. In incremental mode that
    // includes old DB prices loaded as seeds and intermediate prices discovered while solving a
    // target token. Only derived prices inside the current graph are eligible for persistence.
    // Initial mode has no old DB seed context, so anchors and newly discovered prices are written.
    match mode {
        PriceWriteMode::Initial => protected_tokens.contains(token) || !state.is_seed,
        PriceWriteMode::Incremental => !state.is_seed && graph_tokens.contains(token),
    }
}

pub fn select_round_updates(
    candidates: Vec<PriceCandidate>,
    prices: &HashMap<Bytes, PriceState>,
    protected_tokens: &HashSet<Bytes>,
    refreshable_seed_tokens: &HashSet<Bytes>,
) -> Vec<PriceCandidate> {
    // Reduce all quotes from this pass to at most one update per output token.
    //
    // Multiple pools can imply different prices for the same token. Instead of taking the first
    // executable quote, keep the candidate with the lowest cumulative path score. Each edge adds
    // the deviation observed across the three probe sizes, so routes through stable pools outrank
    // routes through thin or inconsistent pools. If two routes have the same score, prefer fewer
    // hops, then the route whose final edge had lower deviation.
    let mut best_by_token: HashMap<Bytes, PriceCandidate> = HashMap::new();
    for candidate in candidates {
        if !candidate.native_per_token.is_finite() || candidate.native_per_token <= 0.0 {
            continue;
        }
        if !candidate.score_bps.is_finite() || candidate.score_bps < 0.0 {
            continue;
        }
        best_by_token
            .entry(candidate.token.clone())
            .and_modify(|current| {
                if candidate_beats(
                    candidate_candidate_key(&candidate),
                    candidate_candidate_key(current),
                ) {
                    *current = candidate.clone();
                }
            })
            .or_insert(candidate);
    }

    best_by_token
        .into_values()
        .filter(|candidate| {
            should_update(candidate, prices, protected_tokens, refreshable_seed_tokens)
        })
        .collect()
}

pub fn apply_updates(
    prices: &mut HashMap<Bytes, PriceState>,
    updates: Vec<PriceCandidate>,
) -> Vec<Bytes> {
    let mut updated_tokens = Vec::with_capacity(updates.len());
    for update in updates {
        updated_tokens.push(update.token.clone());
        prices.insert(
            update.token,
            PriceState::derived(update.native_per_token, update.score_bps, update.hops),
        );
    }
    updated_tokens
}

fn should_update(
    candidate: &PriceCandidate,
    prices: &HashMap<Bytes, PriceState>,
    protected_tokens: &HashSet<Bytes>,
    refreshable_seed_tokens: &HashSet<Bytes>,
) -> bool {
    // Hard anchors define the native unit and must not drift because of pool quotes.
    if protected_tokens.contains(&candidate.token) {
        return false;
    }

    let Some(current) = prices.get(&candidate.token) else {
        return true;
    };

    // Incremental mode may seed target tokens from old DB prices so they can be used as input
    // liquidity. If a current quote exists for the same target, allow it to replace the seed even
    // though the seed has score 0. This is the "refresh existing prices first" behavior.
    if current.is_seed && refreshable_seed_tokens.contains(&candidate.token) {
        return true;
    }

    if candidate.score_bps + SCORE_EPSILON_BPS < current.score_bps {
        return true;
    }

    (candidate.score_bps - current.score_bps).abs() <= SCORE_EPSILON_BPS
        && candidate.hops < current.hops
}

fn candidate_candidate_key(candidate: &PriceCandidate) -> (f64, usize, f64) {
    (candidate.score_bps, candidate.hops, candidate.edge_deviation_bps)
}

fn candidate_beats(candidate: (f64, usize, f64), current: (f64, usize, f64)) -> bool {
    candidate.0 + SCORE_EPSILON_BPS < current.0
        || ((candidate.0 - current.0).abs() <= SCORE_EPSILON_BPS
            && (candidate.1 < current.1
                || (candidate.1 == current.1 && candidate.2 + SCORE_EPSILON_BPS < current.2)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(label: u8) -> Bytes {
        Bytes::from(vec![label; 20])
    }

    fn candidate(
        token_address: Bytes,
        native_per_token: f64,
        score_bps: f64,
        edge_deviation_bps: f64,
        hops: usize,
    ) -> PriceCandidate {
        PriceCandidate {
            token: token_address,
            native_per_token,
            score_bps,
            edge_deviation_bps,
            hops,
            via_token: token(9),
            component_id: "pool".to_string(),
        }
    }

    #[test]
    fn selects_best_pool_candidate_by_path_score() {
        let target = token(1);
        let updates = select_round_updates(
            vec![
                candidate(target.clone(), 2.0, 120.0, 120.0, 1),
                candidate(target.clone(), 1.8, 10.0, 10.0, 1),
            ],
            &HashMap::new(),
            &HashSet::new(),
            &HashSet::new(),
        );

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].native_per_token, 1.8);
        assert_eq!(updates[0].score_bps, 10.0);
    }

    #[test]
    fn revisits_existing_price_when_better_route_appears() {
        let target = token(1);
        let mut prices = HashMap::from([(target.clone(), PriceState::derived(2.0, 150.0, 1))]);

        let updates = select_round_updates(
            vec![candidate(target.clone(), 1.9, 20.0, 5.0, 2)],
            &prices,
            &HashSet::new(),
            &HashSet::new(),
        );
        let updated = apply_updates(&mut prices, updates);

        assert_eq!(updated, vec![target.clone()]);
        assert_eq!(
            prices
                .get(&target)
                .unwrap()
                .native_per_token,
            1.9
        );
        assert_eq!(prices.get(&target).unwrap().score_bps, 20.0);
    }

    #[test]
    fn protects_anchor_seed_from_being_repriced() {
        let anchor = token(0);
        let prices = HashMap::from([(anchor.clone(), PriceState::seed(1.0))]);
        let protected = HashSet::from([anchor.clone()]);

        let updates = select_round_updates(
            vec![candidate(anchor, 0.99, 1.0, 1.0, 1)],
            &prices,
            &protected,
            &HashSet::new(),
        );

        assert!(updates.is_empty());
    }

    #[test]
    fn can_price_incremental_target_through_new_intermediate() {
        let weth = token(0);
        let intermediate = token(1);
        let target = token(2);
        let mut prices = HashMap::from([(weth.clone(), PriceState::seed(1.0))]);
        let protected = HashSet::from([weth]);
        let refreshable = HashSet::from([target.clone()]);

        let first_round = select_round_updates(
            vec![PriceCandidate {
                token: intermediate.clone(),
                native_per_token: 0.01,
                score_bps: 5.0,
                edge_deviation_bps: 5.0,
                hops: 1,
                via_token: token(0),
                component_id: "weth-intermediate".to_string(),
            }],
            &prices,
            &protected,
            &refreshable,
        );
        apply_updates(&mut prices, first_round);

        let second_round = select_round_updates(
            vec![PriceCandidate {
                token: target.clone(),
                native_per_token: 0.02,
                score_bps: prices
                    .get(&intermediate)
                    .unwrap()
                    .score_bps
                    + 7.0,
                edge_deviation_bps: 7.0,
                hops: prices.get(&intermediate).unwrap().hops + 1,
                via_token: intermediate,
                component_id: "intermediate-target".to_string(),
            }],
            &prices,
            &protected,
            &refreshable,
        );
        apply_updates(&mut prices, second_round);

        assert_eq!(
            prices
                .get(&target)
                .unwrap()
                .native_per_token,
            0.02
        );
        assert_eq!(prices.get(&target).unwrap().hops, 2);
    }

    #[test]
    fn refreshes_seed_when_it_is_a_target_token() {
        let target = token(1);
        let prices = HashMap::from([(target.clone(), PriceState::seed(3.0))]);
        let refreshable = HashSet::from([target.clone()]);

        let updates = select_round_updates(
            vec![candidate(target.clone(), 2.9, 25.0, 25.0, 1)],
            &prices,
            &HashSet::new(),
            &refreshable,
        );

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].token, target);
    }

    #[test]
    fn write_filter_keeps_incremental_db_writes_to_derived_graph_prices() {
        let protected = HashSet::from([token(0)]);
        let graph_tokens = HashSet::from([token(1), token(2)]);

        assert!(should_write_price(
            PriceWriteMode::Incremental,
            &token(1),
            &PriceState::derived(2.0, 5.0, 1),
            &protected,
            &graph_tokens,
        ));
        assert!(!should_write_price(
            PriceWriteMode::Incremental,
            &token(2),
            &PriceState::seed(2.0),
            &protected,
            &graph_tokens,
        ));
        assert!(!should_write_price(
            PriceWriteMode::Incremental,
            &token(3),
            &PriceState::derived(2.0, 5.0, 1),
            &protected,
            &graph_tokens,
        ));
    }

    #[test]
    fn write_filter_initial_writes_anchors_and_discovered_prices() {
        let protected = HashSet::from([token(0)]);
        let graph_tokens = HashSet::new();

        assert!(should_write_price(
            PriceWriteMode::Initial,
            &token(0),
            &PriceState::seed(1.0),
            &protected,
            &graph_tokens,
        ));
        assert!(should_write_price(
            PriceWriteMode::Initial,
            &token(1),
            &PriceState::derived(2.0, 5.0, 1),
            &protected,
            &graph_tokens,
        ));
        assert!(!should_write_price(
            PriceWriteMode::Initial,
            &token(2),
            &PriceState::seed(2.0),
            &protected,
            &graph_tokens,
        ));
    }
}
