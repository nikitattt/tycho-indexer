use std::{
    collections::{HashMap, HashSet},
    env,
    num::NonZeroUsize,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel_async::{
    pooled_connection::deadpool::Pool, scoped_futures::ScopedFutureExt, AsyncConnection,
    AsyncPgConnection,
};
use lru::LruCache;
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinHandle,
};
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument};
use tycho_common::{
    models::{
        self,
        blockchain::{
            Block, EntryPoint, EntryPointWithTracingParams, TracedEntryPoint, TracingParams,
            TracingResult, Transaction,
        },
        contract::{Account, AccountBalance, AccountDelta},
        protocol::{
            ComponentBalance, ProtocolComponent, ProtocolComponentState,
            ProtocolComponentStateDelta, QualityRange,
        },
        token::Token,
        Address, Chain, ComponentId, ContractId, EntryPointId, ExtractionState, PaginationParams,
        ProtocolType, TxHash,
    },
    storage::{
        BlockIdentifier, BlockOrTimestamp, ChainGateway, ContractStateGateway, EntryPointFilter,
        EntryPointGateway, ExtractionStateGateway, Gateway, ProtocolGateway, StorageError, Version,
        WithTotal,
    },
    Bytes,
};

use super::{PostgresError, PostgresGateway};

/// Represents different types of database write operations.
#[derive(PartialEq, Clone, Debug)]
pub(crate) enum WriteOp {
    // Simply merge
    UpsertBlock(Vec<models::blockchain::Block>),
    // Simply merge
    UpsertTx(Vec<models::blockchain::Transaction>),
    // Simply keep last
    SaveExtractionState(ExtractionState),
    // Support saving a batch
    InsertContract(Vec<models::contract::Account>),
    // Simply merge
    UpdateContracts(Vec<(TxHash, models::contract::AccountDelta)>),
    // Simply merge
    InsertAccountBalances(Vec<models::contract::AccountBalance>),
    // Simply merge
    InsertProtocolComponents(Vec<models::protocol::ProtocolComponent>),
    // Simply merge
    InsertTokens(Vec<models::token::Token>),
    // Currently unused but supported, please see `CacheGateway.update_tokens` docs.
    #[allow(dead_code)]
    UpdateTokens(Vec<models::token::Token>),
    // Simply merge
    InsertComponentBalances(Vec<models::protocol::ComponentBalance>),
    // Simply merge
    UpsertProtocolState(Vec<(TxHash, models::protocol::ProtocolComponentStateDelta)>),
    // Simply merge
    InsertEntryPoints(HashMap<models::ComponentId, HashSet<models::blockchain::EntryPoint>>),
    // Simply merge
    InsertEntryPointTracingParams(
        HashMap<models::EntryPointId, HashSet<(TracingParams, ComponentId)>>,
    ),
    // Simply merge
    UpsertTracedEntryPoints(Vec<models::blockchain::TracedEntryPoint>),
}

impl WriteOp {
    fn variant_name(&self) -> &'static str {
        match self {
            WriteOp::UpsertBlock(_) => "UpsertBlock",
            WriteOp::UpsertTx(_) => "UpsertTx",
            WriteOp::SaveExtractionState(_) => "SaveExtractionState",
            WriteOp::InsertContract(_) => "InsertContract",
            WriteOp::UpdateContracts(_) => "UpdateContracts",
            WriteOp::InsertAccountBalances(_) => "InsertAccountBalances",
            WriteOp::InsertProtocolComponents(_) => "InsertProtocolComponents",
            WriteOp::InsertTokens(_) => "InsertTokens",
            WriteOp::UpdateTokens(_) => "UpdateTokens",
            WriteOp::InsertComponentBalances(_) => "InsertComponentBalances",
            WriteOp::UpsertProtocolState(_) => "UpsertProtocolState",
            WriteOp::InsertEntryPoints(_) => "InsertEntryPoints",
            WriteOp::InsertEntryPointTracingParams(_) => "InsertEntryPointTracingParams",
            WriteOp::UpsertTracedEntryPoints(_) => "UpsertTracedEntryPoints",
        }
    }

    fn order_key(&self) -> usize {
        match self {
            WriteOp::UpsertBlock(_) => 0,
            WriteOp::UpsertTx(_) => 1,
            WriteOp::InsertContract(_) => 2,
            WriteOp::UpdateContracts(_) => 3,
            WriteOp::InsertTokens(_) => 4,
            WriteOp::UpdateTokens(_) => 5,
            WriteOp::InsertAccountBalances(_) => 6,
            WriteOp::InsertProtocolComponents(_) => 7,
            WriteOp::InsertComponentBalances(_) => 8,
            WriteOp::UpsertProtocolState(_) => 9,
            WriteOp::InsertEntryPoints(_) => 10,
            WriteOp::InsertEntryPointTracingParams(_) => 11,
            WriteOp::UpsertTracedEntryPoints(_) => 12,
            WriteOp::SaveExtractionState(_) => 13,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BatchTxOptimizationConfig {
    write_only_referenced_txs: bool,
    retention_compact_protocol_writes: bool,
}

impl BatchTxOptimizationConfig {
    fn from_env() -> Self {
        Self {
            write_only_referenced_txs: env_flag("TYCHO_WRITE_ONLY_REFERENCED_TXS"),
            retention_compact_protocol_writes: env_flag(
                "TYCHO_EXPERIMENTAL_RETENTION_COMPACT_PROTOCOL_WRITES",
            ),
        }
    }

    fn mode_name(&self) -> &'static str {
        match (self.write_only_referenced_txs, self.retention_compact_protocol_writes) {
            (_, true) => "retention_compact_protocol_writes",
            (true, false) => "write_only_referenced_txs_observe_only",
            (false, false) => "observe",
        }
    }

    fn filters_transactions(&self) -> bool {
        false
    }

    fn logs_projection_stats(&self) -> bool {
        env_flag("TYCHO_LOG_BATCH_TX_PROJECTION_STATS")
            || (!self.retention_compact_protocol_writes && !self.write_only_referenced_txs)
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"))
        .unwrap_or(false)
}

#[derive(Clone, Copy, Debug)]
struct TxOrdering {
    block_ts: NaiveDateTime,
    block_number: u64,
    tx_index: u64,
}

impl TxOrdering {
    fn sort_key(&self) -> (NaiveDateTime, u64, u64) {
        (self.block_ts, self.block_number, self.tx_index)
    }
}

#[derive(Debug, Default)]
struct BatchTxOptimizationStats {
    tx_rows_total: usize,
    tx_rows_unique: usize,
    tx_rows_kept: usize,
    tx_rows_dropped: usize,
    tx_rows_droppable_raw: usize,
    tx_rows_droppable_after_compaction: usize,
    tx_required_raw: usize,
    tx_required_after_compaction: usize,
    tx_required_missing_from_batch_raw: usize,
    tx_required_missing_from_batch_after_compaction: usize,
    tx_missing_ordering_metadata: usize,
    component_balance_rows_raw: usize,
    component_balance_rows_compacted: usize,
    protocol_state_updated_attrs_raw: usize,
    protocol_state_updated_attrs_compacted: usize,
    protocol_state_deleted_attrs: usize,
    protocol_state_compaction_blocked_by_deletions: usize,
}

impl BatchTxOptimizationStats {
    fn log(
        &self,
        config: BatchTxOptimizationConfig,
        block_range: &BlockRange,
        retention_horizon: NaiveDateTime,
    ) {
        debug!(
            mode = config.mode_name(),
            write_only_referenced_txs = config.write_only_referenced_txs,
            retention_compact_protocol_writes = config.retention_compact_protocol_writes,
            block_range = %block_range,
            retention_horizon = %retention_horizon,
            tx_rows_total = self.tx_rows_total,
            tx_rows_unique = self.tx_rows_unique,
            tx_rows_kept = self.tx_rows_kept,
            tx_rows_dropped = self.tx_rows_dropped,
            tx_rows_droppable_raw = self.tx_rows_droppable_raw,
            tx_rows_droppable_after_compaction = self.tx_rows_droppable_after_compaction,
            tx_required_raw = self.tx_required_raw,
            tx_required_after_compaction = self.tx_required_after_compaction,
            tx_required_missing_from_batch_raw = self.tx_required_missing_from_batch_raw,
            tx_required_missing_from_batch_after_compaction =
                self.tx_required_missing_from_batch_after_compaction,
            tx_missing_ordering_metadata = self.tx_missing_ordering_metadata,
            component_balance_rows_raw = self.component_balance_rows_raw,
            component_balance_rows_compacted = self.component_balance_rows_compacted,
            protocol_state_updated_attrs_raw = self.protocol_state_updated_attrs_raw,
            protocol_state_updated_attrs_compacted = self.protocol_state_updated_attrs_compacted,
            protocol_state_deleted_attrs = self.protocol_state_deleted_attrs,
            protocol_state_compaction_blocked_by_deletions =
                self.protocol_state_compaction_blocked_by_deletions,
            "BatchTxOptimizationStats"
        );
    }
}

#[derive(Debug, Default)]
struct ProtocolCompactionStats {
    component_balance_rows_raw: usize,
    component_balance_rows_compacted: usize,
    protocol_state_updated_attrs_raw: usize,
    protocol_state_updated_attrs_compacted: usize,
    protocol_state_deleted_attrs: usize,
    protocol_state_compaction_blocked_by_deletions: usize,
    tx_missing_ordering_metadata: usize,
}

fn collect_tx_hashes_in_upsert_tx(ops: &[WriteOp]) -> HashSet<TxHash> {
    ops.iter()
        .filter_map(|op| match op {
            WriteOp::UpsertTx(txs) => Some(txs),
            _ => None,
        })
        .flat_map(|txs| txs.iter().map(|tx| tx.hash.clone()))
        .collect()
}

fn count_upsert_tx_rows(ops: &[WriteOp]) -> usize {
    ops.iter()
        .map(|op| match op {
            WriteOp::UpsertTx(txs) => txs.len(),
            _ => 0,
        })
        .sum()
}

fn count_droppable_upsert_tx_rows(ops: &[WriteOp], required: &HashSet<TxHash>) -> usize {
    ops.iter()
        .map(|op| match op {
            WriteOp::UpsertTx(txs) => txs
                .iter()
                .filter(|tx| !required.contains(&tx.hash))
                .count(),
            _ => 0,
        })
        .sum()
}

fn collect_tx_ordering(ops: &[WriteOp]) -> HashMap<TxHash, TxOrdering> {
    let blocks: HashMap<_, _> = ops
        .iter()
        .filter_map(|op| match op {
            WriteOp::UpsertBlock(blocks) => Some(blocks),
            _ => None,
        })
        .flat_map(|blocks| {
            blocks
                .iter()
                .map(|block| (block.hash.clone(), (block.ts, block.number)))
        })
        .collect();

    ops.iter()
        .filter_map(|op| match op {
            WriteOp::UpsertTx(txs) => Some(txs),
            _ => None,
        })
        .flat_map(|txs| {
            txs.iter().filter_map(|tx| {
                blocks.get(&tx.block_hash).map(|(block_ts, block_number)| {
                    (
                        tx.hash.clone(),
                        TxOrdering {
                            block_ts: *block_ts,
                            block_number: *block_number,
                            tx_index: tx.index,
                        },
                    )
                })
            })
        })
        .collect()
}

fn collect_protocol_tx_metadata(
    ops: &[WriteOp],
) -> HashMap<TxHash, super::protocol::ProtocolTxMetadata> {
    collect_tx_ordering(ops)
        .into_iter()
        .map(|(hash, ordering)| {
            (
                hash,
                super::protocol::ProtocolTxMetadata {
                    index: ordering.tx_index,
                    ts: ordering.block_ts,
                },
            )
        })
        .collect()
}

fn collect_upfront_required_tx_hashes(ops: &[WriteOp]) -> HashSet<TxHash> {
    let mut required = HashSet::new();
    for op in ops {
        match op {
            WriteOp::InsertContract(contracts) => {
                required.extend(
                    contracts
                        .iter()
                        .filter_map(|contract| contract.creation_tx.clone()),
                );
            }
            WriteOp::UpdateContracts(contracts) => {
                required.extend(contracts.iter().map(|(tx, _)| tx.clone()));
            }
            WriteOp::InsertAccountBalances(balances) => {
                required.extend(balances.iter().map(|balance| balance.modify_tx.clone()));
            }
            WriteOp::InsertProtocolComponents(components) => {
                required.extend(components.iter().map(|component| component.creation_tx.clone()));
            }
            _ => {}
        }
    }
    required
}

fn collect_protocol_write_tx_hashes(ops: &[WriteOp]) -> HashSet<TxHash> {
    let mut txs = HashSet::new();
    for op in ops {
        match op {
            WriteOp::InsertComponentBalances(balances) => {
                txs.extend(balances.iter().map(|balance| balance.modify_tx.clone()));
            }
            WriteOp::UpsertProtocolState(deltas) => {
                txs.extend(deltas.iter().map(|(tx, _)| tx.clone()));
            }
            _ => {}
        }
    }
    txs
}

fn has_protocol_versioned_writes(ops: &[WriteOp]) -> bool {
    ops.iter().any(|op| match op {
        WriteOp::InsertComponentBalances(balances) => !balances.is_empty(),
        WriteOp::UpsertProtocolState(deltas) => !deltas.is_empty(),
        _ => false,
    })
}

fn collect_required_tx_hashes(ops: &[WriteOp]) -> HashSet<TxHash> {
    let mut required = HashSet::new();
    for op in ops {
        match op {
            WriteOp::InsertContract(contracts) => {
                required.extend(
                    contracts
                        .iter()
                        .filter_map(|contract| contract.creation_tx.clone()),
                );
            }
            WriteOp::UpdateContracts(contracts) => {
                required.extend(contracts.iter().map(|(tx, _)| tx.clone()));
            }
            WriteOp::InsertAccountBalances(balances) => {
                required.extend(balances.iter().map(|balance| balance.modify_tx.clone()));
            }
            WriteOp::InsertProtocolComponents(components) => {
                required.extend(components.iter().map(|component| component.creation_tx.clone()));
            }
            WriteOp::InsertComponentBalances(balances) => {
                required.extend(balances.iter().map(|balance| balance.modify_tx.clone()));
            }
            WriteOp::UpsertProtocolState(deltas) => {
                required.extend(deltas.iter().map(|(tx, _)| tx.clone()));
            }
            WriteOp::UpsertBlock(_)
            | WriteOp::UpsertTx(_)
            | WriteOp::SaveExtractionState(_)
            | WriteOp::InsertTokens(_)
            | WriteOp::UpdateTokens(_)
            | WriteOp::InsertEntryPoints(_)
            | WriteOp::InsertEntryPointTracingParams(_)
            | WriteOp::UpsertTracedEntryPoints(_) => {}
        }
    }
    required
}

fn compact_protocol_writes_for_retention(
    ops: &mut [WriteOp],
    tx_ordering: &HashMap<TxHash, TxOrdering>,
    retention_horizon: NaiveDateTime,
    apply: bool,
) -> ProtocolCompactionStats {
    let mut stats = ProtocolCompactionStats::default();
    let mut missing_metadata = HashSet::new();

    for op in ops.iter_mut() {
        match op {
            WriteOp::InsertComponentBalances(balances) => {
                let (raw, compacted, missing) = compact_component_balances_for_retention(
                    balances,
                    tx_ordering,
                    retention_horizon,
                    apply,
                );
                stats.component_balance_rows_raw += raw;
                stats.component_balance_rows_compacted += compacted;
                missing_metadata.extend(missing);
            }
            WriteOp::UpsertProtocolState(deltas) => {
                let (raw, compacted, deleted, blocked, missing) =
                    compact_protocol_state_for_retention(
                        deltas,
                        tx_ordering,
                        retention_horizon,
                        apply,
                    );
                stats.protocol_state_updated_attrs_raw += raw;
                stats.protocol_state_updated_attrs_compacted += compacted;
                stats.protocol_state_deleted_attrs += deleted;
                stats.protocol_state_compaction_blocked_by_deletions += blocked;
                missing_metadata.extend(missing);
            }
            _ => {}
        }
    }

    stats.tx_missing_ordering_metadata = missing_metadata.len();
    stats
}

fn compact_component_balances_for_retention(
    balances: &mut Vec<ComponentBalance>,
    tx_ordering: &HashMap<TxHash, TxOrdering>,
    retention_horizon: NaiveDateTime,
    apply: bool,
) -> (usize, usize, HashSet<TxHash>) {
    let raw = balances.len();
    let mut missing_metadata = HashSet::new();
    let mut by_key: HashMap<(ComponentId, Address), Vec<usize>> = HashMap::new();

    for (idx, balance) in balances.iter().enumerate() {
        by_key
            .entry((balance.component_id.clone(), balance.token.clone()))
            .or_default()
            .push(idx);
        if !tx_ordering.contains_key(&balance.modify_tx) {
            missing_metadata.insert(balance.modify_tx.clone());
        }
    }

    let mut drop_indices = HashSet::new();
    for indices in by_key.values() {
        if indices.len() < 2 {
            continue;
        }
        if indices
            .iter()
            .any(|idx| !tx_ordering.contains_key(&balances[*idx].modify_tx))
        {
            continue;
        }

        let mut ordered = indices.clone();
        ordered.sort_by_key(|idx| {
            tx_ordering
                .get(&balances[*idx].modify_tx)
                .expect("metadata checked")
                .sort_key()
        });

        for pair in ordered.windows(2) {
            let current = pair[0];
            let next = pair[1];
            let next_ordering = tx_ordering
                .get(&balances[next].modify_tx)
                .expect("metadata checked");
            if next_ordering.block_ts <= retention_horizon {
                drop_indices.insert(current);
            }
        }
    }

    let compacted = drop_indices.len();
    if apply && compacted > 0 {
        let mut idx = 0usize;
        balances.retain(|_| {
            let keep = !drop_indices.contains(&idx);
            idx += 1;
            keep
        });
    }

    (raw, compacted, missing_metadata)
}

fn compact_protocol_state_for_retention(
    deltas: &mut Vec<(TxHash, ProtocolComponentStateDelta)>,
    tx_ordering: &HashMap<TxHash, TxOrdering>,
    retention_horizon: NaiveDateTime,
    apply: bool,
) -> (usize, usize, usize, usize, HashSet<TxHash>) {
    let mut raw = 0usize;
    let mut deleted = 0usize;
    let mut missing_metadata = HashSet::new();
    let mut by_key: HashMap<(ComponentId, String), Vec<(usize, String)>> = HashMap::new();
    let mut deletion_keys = HashSet::new();

    for (delta_idx, (tx, delta)) in deltas.iter().enumerate() {
        if !tx_ordering.contains_key(tx) {
            missing_metadata.insert(tx.clone());
        }
        for attr in delta.updated_attributes.keys() {
            raw += 1;
            by_key
                .entry((delta.component_id.clone(), attr.clone()))
                .or_default()
                .push((delta_idx, attr.clone()));
        }
        for attr in &delta.deleted_attributes {
            deleted += 1;
            deletion_keys.insert((delta.component_id.clone(), attr.clone()));
        }
    }

    let mut drop_attrs: HashSet<(usize, String)> = HashSet::new();
    let mut blocked_by_deletions = 0usize;
    for (key, occurrences) in by_key.iter() {
        if occurrences.len() < 2 {
            continue;
        }
        if deletion_keys.contains(key) {
            blocked_by_deletions += occurrences.len();
            continue;
        }
        if occurrences
            .iter()
            .any(|(idx, _)| !tx_ordering.contains_key(&deltas[*idx].0))
        {
            continue;
        }

        let mut ordered = occurrences.clone();
        ordered.sort_by_key(|(idx, _)| {
            tx_ordering
                .get(&deltas[*idx].0)
                .expect("metadata checked")
                .sort_key()
        });

        for pair in ordered.windows(2) {
            let current = &pair[0];
            let next = &pair[1];
            let next_ordering = tx_ordering
                .get(&deltas[next.0].0)
                .expect("metadata checked");
            if next_ordering.block_ts <= retention_horizon {
                drop_attrs.insert(current.clone());
            }
        }
    }

    let compacted = drop_attrs.len();
    if apply && compacted > 0 {
        for (delta_idx, (_, delta)) in deltas.iter_mut().enumerate() {
            delta
                .updated_attributes
                .retain(|attr, _| !drop_attrs.contains(&(delta_idx, attr.clone())));
        }
        deltas.retain(|(_, delta)| {
            !delta.updated_attributes.is_empty() || !delta.deleted_attributes.is_empty()
        });
    }

    (raw, compacted, deleted, blocked_by_deletions, missing_metadata)
}

fn filter_upsert_txs(ops: &mut [WriteOp], required: &HashSet<TxHash>) -> usize {
    let mut dropped = 0usize;
    for op in ops {
        if let WriteOp::UpsertTx(txs) = op {
            let before = txs.len();
            txs.retain(|tx| required.contains(&tx.hash));
            dropped += before - txs.len();
        }
    }
    dropped
}

fn apply_batch_tx_optimization(
    ops: &mut [WriteOp],
    retention_horizon: NaiveDateTime,
    config: BatchTxOptimizationConfig,
) -> BatchTxOptimizationStats {
    let tx_rows_total = count_upsert_tx_rows(ops);
    let upsert_tx_hashes = collect_tx_hashes_in_upsert_tx(ops);
    let tx_ordering = collect_tx_ordering(ops);
    let tx_required_raw = collect_required_tx_hashes(ops);
    let tx_required_missing_from_batch_raw = tx_required_raw
        .difference(&upsert_tx_hashes)
        .count();
    let tx_rows_droppable_raw = count_droppable_upsert_tx_rows(ops, &tx_required_raw);

    let mut compacted_ops = ops.to_vec();
    let compaction_stats = compact_protocol_writes_for_retention(
        &mut compacted_ops,
        &tx_ordering,
        retention_horizon,
        true,
    );
    let tx_required_after_compaction = collect_required_tx_hashes(&compacted_ops);
    let tx_rows_droppable_after_compaction =
        count_droppable_upsert_tx_rows(&compacted_ops, &tx_required_after_compaction);
    let tx_required_missing_from_batch_after_compaction = tx_required_after_compaction
        .difference(&upsert_tx_hashes)
        .count();

    let tx_rows_dropped = if config.filters_transactions() {
        let required = if config.retention_compact_protocol_writes {
            &tx_required_after_compaction
        } else {
            &tx_required_raw
        };
        filter_upsert_txs(ops, required)
    } else {
        0
    };

    BatchTxOptimizationStats {
        tx_rows_total,
        tx_rows_unique: upsert_tx_hashes.len(),
        tx_rows_kept: tx_rows_total.saturating_sub(tx_rows_dropped),
        tx_rows_dropped,
        tx_rows_droppable_raw,
        tx_rows_droppable_after_compaction,
        tx_required_raw: tx_required_raw.len(),
        tx_required_after_compaction: tx_required_after_compaction.len(),
        tx_required_missing_from_batch_raw,
        tx_required_missing_from_batch_after_compaction,
        tx_missing_ordering_metadata: compaction_stats.tx_missing_ordering_metadata,
        component_balance_rows_raw: compaction_stats.component_balance_rows_raw,
        component_balance_rows_compacted: compaction_stats.component_balance_rows_compacted,
        protocol_state_updated_attrs_raw: compaction_stats.protocol_state_updated_attrs_raw,
        protocol_state_updated_attrs_compacted: compaction_stats
            .protocol_state_updated_attrs_compacted,
        protocol_state_deleted_attrs: compaction_stats.protocol_state_deleted_attrs,
        protocol_state_compaction_blocked_by_deletions: compaction_stats
            .protocol_state_compaction_blocked_by_deletions,
    }
}

fn collect_light_batch_tx_stats(ops: &[WriteOp]) -> BatchTxOptimizationStats {
    let tx_rows_total = count_upsert_tx_rows(ops);
    let upsert_tx_hashes = collect_tx_hashes_in_upsert_tx(ops);
    let tx_required_raw = collect_required_tx_hashes(ops);
    let tx_required_missing_from_batch_raw = tx_required_raw
        .difference(&upsert_tx_hashes)
        .count();

    BatchTxOptimizationStats {
        tx_rows_total,
        tx_rows_unique: upsert_tx_hashes.len(),
        tx_rows_kept: tx_rows_total,
        tx_required_raw: tx_required_raw.len(),
        tx_required_after_compaction: tx_required_raw.len(),
        tx_required_missing_from_batch_raw,
        tx_required_missing_from_batch_after_compaction: tx_required_missing_from_batch_raw,
        ..Default::default()
    }
}

#[derive(Debug)]
struct BlockRange {
    start: models::blockchain::Block,
    end: models::blockchain::Block,
}

impl BlockRange {
    fn new(start: &models::blockchain::Block, end: &models::blockchain::Block) -> Self {
        Self { start: start.clone(), end: end.clone() }
    }
}

impl std::fmt::Display for BlockRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}, {}] - [{:#x}, {:#x}]",
            self.start.number, self.end.number, self.start.hash, self.end.hash
        )
    }
}

/// Represents a transaction in the database, including the block information,
/// a list of operations to be performed, and a channel to send the result.
pub struct DBTransaction {
    block_range: BlockRange,
    size: usize,
    operations: Vec<WriteOp>,
    tx: oneshot::Sender<Result<(), StorageError>>,
    /// Purely used to add an attribute to the span when the transaction is commited
    owner: Option<String>,
    /// Span of the caller that created this transaction, used to link the db_write span
    /// back to the originating trace across the channel boundary.
    caller_span: tracing::Span,
}

impl DBTransaction {
    /// Batch changes of the same kind.
    ///
    /// The final insertion order is determined via `WriteOp::order_key` and is fixed for all
    /// transaction.
    ///
    /// PERF: Use an array instead of a vec since the order is static.
    fn add_operation(&mut self, op: WriteOp) -> Result<(), StorageError> {
        for existing_op in self.operations.iter_mut() {
            match (existing_op, &op) {
                (WriteOp::UpsertBlock(l), WriteOp::UpsertBlock(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::UpsertTx(l), WriteOp::UpsertTx(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::SaveExtractionState(l), WriteOp::SaveExtractionState(r)) => {
                    l.clone_from(r);
                    return Ok(());
                }
                (WriteOp::InsertContract(l), WriteOp::InsertContract(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::UpdateContracts(l), WriteOp::UpdateContracts(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::InsertAccountBalances(l), WriteOp::InsertAccountBalances(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::InsertProtocolComponents(l), WriteOp::InsertProtocolComponents(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::InsertTokens(l), WriteOp::InsertTokens(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::UpdateTokens(l), WriteOp::InsertTokens(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::InsertComponentBalances(l), WriteOp::InsertComponentBalances(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::UpsertProtocolState(l), WriteOp::UpsertProtocolState(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                (WriteOp::InsertEntryPoints(l), WriteOp::InsertEntryPoints(r)) => {
                    for (component_id, entry_points) in r.iter() {
                        let entry = l
                            .entry(component_id.clone())
                            .or_insert_with(HashSet::new);
                        let len_before = entry.len();
                        entry.extend(entry_points.iter().cloned());
                        self.size += entry.len() - len_before;
                    }
                    return Ok(());
                }
                (
                    WriteOp::InsertEntryPointTracingParams(l),
                    WriteOp::InsertEntryPointTracingParams(r),
                ) => {
                    for (entry_point_id, params) in r.iter() {
                        let entry = l
                            .entry(entry_point_id.clone())
                            .or_insert_with(HashSet::new);
                        let len_before = entry.len();
                        entry.extend(params.iter().cloned());
                        self.size += entry.len() - len_before;
                    }
                    return Ok(());
                }
                (WriteOp::UpsertTracedEntryPoints(l), WriteOp::UpsertTracedEntryPoints(r)) => {
                    self.size += r.len();
                    l.extend(r.iter().cloned());
                    return Ok(());
                }
                _ => continue,
            }
        }
        // not quite accurate but currently all WriteOps are created with a single entry.
        self.size += 1;
        self.operations.push(op);
        Ok(())
    }
}

/// Represents different types of messages that can be sent to the DBCacheWriteExecutor.
pub enum DBCacheMessage {
    Write(DBTransaction),
}

/// Extractors can start transaction.
/// This will guarantee that a group of changes they provide is executed atomically.
///
/// The gateway keeps track of the blockchains progress.
/// A new transaction group finishes. This group has a block attached to it.
/// - If the block is old, we execute the transaction immediately.
/// - If the block is pending, we group the transaction with other transactions that finish before
///   we observe the next block.
///
/// # Write Cache
///
/// This struct handles writes in a centralised and sequential manner. It
/// provides a write-through cache through message passing. This means multiple
/// "writers" can send transactions of write operations simultaneously. Each of
/// those transactions is supposed to relate to a block. As soon as a new block
/// is observed, the currently pending changes are flushed to the database.
///
/// In case a new transaction with an older block comes in, the transaction is
/// immediately applied to the database.
///
/// In case the incoming transactions block is too far ahead / does not
/// connect with the last persisted block, an error is raised.
///
/// Transactions operations are deduplicated, but are executed as separate
/// database transactions therefore in case a transaction fails, it should not
/// affect any other pending transactions.
///
/// ## Deduplication
/// Block, transaction and revert operations are deduplicated. Meaning that if
/// they happen within a batch, they will only be sent once to the actual
/// database.
///
/// ## Design Decisions
/// The current design is bound to evm and diesel models. The bound is
/// purposefully kept somewhat decoupled but not entirely. The reason is to
/// ensure fast development but also have a path that shows how we could
/// decouple especially from evm bounds models, as most likely we will soon have
/// additional chains to deal with.
///
/// Read Operations
/// The class does provide read operations for completeness, but it will not consider any
/// cached changes while reading. Any reads are direct pass through to the database.
pub(crate) struct DBCacheWriteExecutor {
    name: String,
    chain: Chain,
    pool: Pool<AsyncPgConnection>,
    state_gateway: PostgresGateway,
    persisted_block: Option<models::blockchain::Block>,
    msg_receiver: mpsc::Receiver<DBCacheMessage>,
    batch_tx_optimization_config: BatchTxOptimizationConfig,
}

impl DBCacheWriteExecutor {
    pub(crate) async fn new(
        name: String,
        chain: Chain,
        pool: Pool<AsyncPgConnection>,
        state_gateway: PostgresGateway,
        msg_receiver: mpsc::Receiver<DBCacheMessage>,
    ) -> Self {
        let mut conn = pool
            .get()
            .await
            .expect("pool should be connected");

        let persisted_block = state_gateway
            .get_block(&BlockIdentifier::Latest(chain), &mut conn)
            .await
            .ok();

        debug!("Persisted block: {:?}", persisted_block);

        let batch_tx_optimization_config = BatchTxOptimizationConfig::from_env();
        info!(
            name = name.as_str(),
            mode = batch_tx_optimization_config.mode_name(),
            write_only_referenced_txs = batch_tx_optimization_config.write_only_referenced_txs,
            retention_compact_protocol_writes =
                batch_tx_optimization_config.retention_compact_protocol_writes,
            log_projection_stats = batch_tx_optimization_config.logs_projection_stats(),
            "BatchTxOptimizationConfig"
        );
        if batch_tx_optimization_config.retention_compact_protocol_writes {
            warn!(
                "TYCHO_EXPERIMENTAL_RETENTION_COMPACT_PROTOCOL_WRITES is enabled; protocol writes \
                 will be planned through partitioned versioning before transaction rows are filtered"
            );
        }
        if batch_tx_optimization_config.write_only_referenced_txs {
            warn!(
                "TYCHO_WRITE_ONLY_REFERENCED_TXS is observe-only in this build; same-batch-only \
                 transaction filtering is unsafe for cross-batch synthetic transaction references"
            );
        }

        Self {
            name,
            chain,
            pool,
            state_gateway,
            persisted_block,
            msg_receiver,
            batch_tx_optimization_config,
        }
    }

    /// Spawns a task to process incoming database messages (write requests or flush commands).
    pub fn run(mut self) -> JoinHandle<()> {
        info!(name = self.name, "DBCacheWriteExecutor started!");
        tokio::spawn(async move {
            while let Some(message) = self.msg_receiver.recv().await {
                match message {
                    DBCacheMessage::Write(db_tx) => {
                        // Process the write transaction
                        self.write(db_tx).await;
                    }
                }
            }
        })
    }

    #[instrument(name="db_write", skip_all, fields(block_range = %new_db_tx.block_range, extractor_id = tracing::field::Empty))]
    async fn write(&mut self, mut new_db_tx: DBTransaction) {
        tracing::Span::current().follows_from(new_db_tx.caller_span.id());
        debug!("NewDBTransactionStart");
        if let Some(extractor_id) = new_db_tx.owner.as_ref() {
            tracing::Span::current().record("extractor_id", extractor_id);
        }

        if self.batch_tx_optimization_config.logs_projection_stats() {
            let optimization_stats = apply_batch_tx_optimization(
                &mut new_db_tx.operations,
                self.state_gateway.retention_horizon(),
                self.batch_tx_optimization_config,
            );
            optimization_stats.log(
                self.batch_tx_optimization_config,
                &new_db_tx.block_range,
                self.state_gateway.retention_horizon(),
            );
        } else {
            let optimization_stats = collect_light_batch_tx_stats(&new_db_tx.operations);
            optimization_stats.log(
                self.batch_tx_optimization_config,
                &new_db_tx.block_range,
                self.state_gateway.retention_horizon(),
            );
        }

        let mut conn = self
            .pool
            .get()
            .await
            .expect("pool should be connected");

        let mut retry_count = 0;
        let max_retries = 3;
        let mut res =
            Err(PostgresError(StorageError::Unexpected("default response error".to_string())));

        while retry_count < max_retries {
            res = conn
                .build_transaction()
                .repeatable_read()
                .run(|conn| {
                    async {
                        self.execute_write_ops(&new_db_tx.operations, conn).await?;
                        Ok(())
                    }
                    .scope_boxed()
                })
                .await;

            match res {
                Ok(_) => break,
                Err(PostgresError(StorageError::Unexpected(ref e)))
                    if e.contains("deadlock detected") =>
                {
                    retry_count += 1;
                    if retry_count < max_retries {
                        let delay = std::time::Duration::from_secs(retry_count);
                        warn!(
                            "Deadlock detected, retrying in {:?} (attempt {}/{})",
                            delay,
                            retry_count + 1,
                            max_retries
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                }
                _ => break,
            }
        }

        if res.is_ok() {
            debug!("DBTransactionCommitted");
        }

        if let Err(e) = &res {
            error!(error = ?e, "DBTransactionFailed");
        }

        match self.persisted_block.as_ref() {
            None => {
                self.persisted_block = Some(new_db_tx.block_range.end);
            }
            Some(db_block) if db_block.number < new_db_tx.block_range.start.number => {
                self.persisted_block = Some(new_db_tx.block_range.end);
            }
            _ => {}
        }

        // Forward the result to the sender
        let _ = new_db_tx
            .tx
            .send(res.map_err(Into::into));
    }

    /// Executes an operation.
    ///
    /// This function handles different types of write operations such as
    /// upserts, updates, and reverts, ensuring data consistency in the database.
    async fn execute_write_ops(
        &mut self,
        operations: &[WriteOp],
        conn: &mut AsyncPgConnection,
    ) -> Result<(), PostgresError> {
        if self.batch_tx_optimization_config.retention_compact_protocol_writes
            && has_protocol_versioned_writes(operations)
        {
            self.execute_retention_aware_protocol_write_ops(operations, conn)
                .await
        } else {
            for op in operations {
                self.execute_write_op_allowing_duplicates(op, conn).await?;
            }
            Ok(())
        }
    }

    async fn execute_write_op_allowing_duplicates(
        &mut self,
        operation: &WriteOp,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), PostgresError> {
        match self.execute_write_op(operation, conn).await {
            Err(PostgresError(StorageError::DuplicateEntry(entity, id))) => {
                // As this db transaction is old. It can contain already stored txs, we log the
                // duplicate entry error and continue.
                debug!("Ignoring duplicate entry for {} with id {}", entity, id);
                Ok(())
            }
            other => other,
        }
    }

    async fn upsert_required_txs(
        &mut self,
        operations: &[WriteOp],
        required: &HashSet<TxHash>,
        conn: &mut AsyncPgConnection,
    ) -> Result<usize, PostgresError> {
        if required.is_empty() {
            return Ok(0);
        }

        let txs = operations
            .iter()
            .filter_map(|op| match op {
                WriteOp::UpsertTx(txs) => Some(txs.as_slice()),
                _ => None,
            })
            .flatten()
            .filter(|tx| required.contains(&tx.hash))
            .cloned()
            .collect::<Vec<_>>();

        if txs.is_empty() {
            return Ok(0);
        }

        self.state_gateway
            .upsert_tx(&txs, conn)
            .await?;
        Ok(txs.len())
    }

    async fn execute_retention_aware_protocol_write_ops(
        &mut self,
        operations: &[WriteOp],
        conn: &mut AsyncPgConnection,
    ) -> Result<(), PostgresError> {
        let tx_metadata = collect_protocol_tx_metadata(operations);
        let upfront_required_txs = collect_upfront_required_tx_hashes(operations);

        let mut upfront_tx_rows = 0usize;
        for op in operations {
            match op {
                WriteOp::UpsertTx(_) => {
                    upfront_tx_rows += self
                        .upsert_required_txs(operations, &upfront_required_txs, conn)
                        .await?;
                }
                WriteOp::InsertComponentBalances(_) | WriteOp::UpsertProtocolState(_) => {}
                _ if op.order_key() < WriteOp::InsertComponentBalances(Vec::new()).order_key() => {
                    self.execute_write_op_allowing_duplicates(op, conn).await?;
                }
                _ => {}
            }
        }

        let raw_protocol_txs = collect_protocol_write_tx_hashes(operations);
        let all_upsert_txs = collect_tx_hashes_in_upsert_tx(operations);
        let mut retained_required_txs = HashSet::new();
        let inserted_tx_hashes = upfront_required_txs.clone();
        let mut component_balance_plan = None;
        let mut protocol_state_plan = None;
        let mut raw_component_balance_rows = 0usize;
        let mut retained_component_balance_rows = 0usize;
        let mut raw_protocol_state_attrs = 0usize;
        let mut raw_protocol_state_deleted_attrs = 0usize;
        let mut retained_protocol_state_rows = 0usize;

        for op in operations {
            match op {
                WriteOp::InsertComponentBalances(balances) if !balances.is_empty() => {
                    let plan = self
                        .state_gateway
                        .plan_retained_component_balances(
                            balances.as_slice(),
                            &self.chain,
                            &tx_metadata,
                            conn,
                        )
                        .await?;
                    retained_required_txs.extend(plan.required_tx_hashes());
                    raw_component_balance_rows += plan.raw_rows;
                    retained_component_balance_rows += plan.retained_rows();
                    component_balance_plan = Some(plan);
                }
                WriteOp::UpsertProtocolState(deltas) if !deltas.is_empty() => {
                    let collected_changes = deltas
                        .iter()
                        .map(|(tx, update)| (tx.clone(), update))
                        .collect::<Vec<_>>();
                    let plan = self
                        .state_gateway
                        .plan_retained_protocol_states(
                            &self.chain,
                            collected_changes.as_slice(),
                            &tx_metadata,
                            conn,
                        )
                        .await?;
                    retained_required_txs.extend(plan.required_tx_hashes());
                    raw_protocol_state_attrs += plan.raw_updated_attrs;
                    raw_protocol_state_deleted_attrs += plan.raw_deleted_attrs;
                    retained_protocol_state_rows += plan.retained_rows();
                    protocol_state_plan = Some(plan);
                }
                _ => {}
            }
        }

        let discarded_protocol_txs = raw_protocol_txs
            .difference(&retained_required_txs)
            .cloned()
            .collect::<HashSet<_>>();
        let mut txs_to_keep = all_upsert_txs
            .difference(&discarded_protocol_txs)
            .cloned()
            .collect::<HashSet<_>>();
        txs_to_keep.extend(upfront_required_txs.iter().cloned());
        let post_plan_required = txs_to_keep
            .difference(&inserted_tx_hashes)
            .cloned()
            .collect::<HashSet<_>>();
        let post_plan_tx_rows_upserted = self
            .upsert_required_txs(operations, &post_plan_required, conn)
            .await?;

        if let Some(plan) = component_balance_plan {
            self.state_gateway
                .insert_retained_component_balances(&plan, conn)
                .await?;
        }

        if let Some(plan) = protocol_state_plan {
            self.state_gateway
                .insert_retained_protocol_states(&plan, conn)
                .await?;
        }

        for op in operations
            .iter()
            .filter(|op| op.order_key() > WriteOp::UpsertProtocolState(Vec::new()).order_key())
        {
            self.execute_write_op_allowing_duplicates(op, conn).await?;
        }

        let retained_tx_rows = upfront_required_txs
            .union(&retained_required_txs)
            .count();
        debug!(
            mode = "retention_aware_protocol_writes",
            upfront_tx_rows,
            post_plan_tx_rows_upserted,
            retained_tx_rows,
            tx_rows_total = count_upsert_tx_rows(operations),
            tx_rows_kept = txs_to_keep.len(),
            tx_rows_dropped = count_upsert_tx_rows(operations).saturating_sub(txs_to_keep.len()),
            discarded_protocol_tx_count = discarded_protocol_txs.len(),
            raw_component_balance_rows,
            retained_component_balance_rows,
            raw_protocol_state_attrs,
            raw_protocol_state_deleted_attrs,
            retained_protocol_state_rows,
            "RetainedProtocolWriteStats"
        );

        Ok(())
    }

    #[instrument(skip_all, fields(op=operation.variant_name()))]
    async fn execute_write_op(
        &mut self,
        operation: &WriteOp,
        conn: &mut AsyncPgConnection,
    ) -> Result<(), PostgresError> {
        trace!(op=?operation, name="ExecuteWriteOp");
        match operation {
            WriteOp::UpsertBlock(block) => {
                self.state_gateway
                    .upsert_block(block, conn)
                    .await?
            }
            WriteOp::UpsertTx(transaction) => {
                self.state_gateway
                    .upsert_tx(transaction, conn)
                    .await?
            }
            WriteOp::SaveExtractionState(state) => {
                self.state_gateway
                    .save_state(state, conn)
                    .await?
            }
            WriteOp::InsertContract(contracts) => {
                for contract in contracts.iter() {
                    self.state_gateway
                        .insert_contract(contract, conn)
                        .await?
                }
            }
            WriteOp::UpdateContracts(contracts) => {
                let collected_changes: Vec<(TxHash, &models::contract::AccountDelta)> = contracts
                    .iter()
                    .map(|(tx, update)| (tx.clone(), update))
                    .collect();
                let changes_slice = collected_changes.as_slice();
                self.state_gateway
                    .update_contracts(&self.chain, changes_slice, conn)
                    .await?
            }
            WriteOp::InsertAccountBalances(balances) => {
                self.state_gateway
                    .add_account_balances(balances.as_slice(), &self.chain, conn)
                    .await?
            }
            WriteOp::InsertProtocolComponents(components) => {
                self.state_gateway
                    .add_protocol_components(components.as_slice(), conn)
                    .await?
            }
            WriteOp::InsertTokens(tokens) => {
                self.state_gateway
                    .add_tokens(tokens.as_slice(), conn)
                    .await?
            }
            WriteOp::UpdateTokens(tokens) => {
                self.state_gateway
                    .update_tokens(tokens.as_slice(), conn)
                    .await?
            }
            WriteOp::InsertComponentBalances(balances) => {
                self.state_gateway
                    .add_component_balances(balances.as_slice(), &self.chain, conn)
                    .await?
            }
            WriteOp::UpsertProtocolState(deltas) => {
                let collected_changes: Vec<(
                    TxHash,
                    &models::protocol::ProtocolComponentStateDelta,
                )> = deltas
                    .iter()
                    .map(|(tx, update)| (tx.clone(), update))
                    .collect();
                let changes_slice = collected_changes.as_slice();
                self.state_gateway
                    .update_protocol_states(&self.chain, changes_slice, conn)
                    .await?
            }
            WriteOp::UpsertTracedEntryPoints(traced_entry_points) => {
                self.state_gateway
                    .upsert_traced_entry_points(traced_entry_points.as_slice(), conn)
                    .await?
            }
            WriteOp::InsertEntryPoints(new_entry_points) => {
                self.state_gateway
                    .insert_entry_points(new_entry_points, &self.chain, conn)
                    .await?
            }
            WriteOp::InsertEntryPointTracingParams(new_entry_point_tracing_params) => {
                self.state_gateway
                    .insert_entry_point_tracing_params(
                        new_entry_point_tracing_params,
                        &self.chain,
                        conn,
                    )
                    .await?
            }
        };
        Ok(())
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
struct RevertParameters {
    start_version: Option<BlockOrTimestamp>,
    end_version: BlockOrTimestamp,
}

type DeltasCache = LruCache<
    RevertParameters,
    (
        Vec<models::contract::AccountDelta>,
        Vec<models::protocol::ProtocolComponentStateDelta>,
        Vec<models::protocol::ComponentBalance>,
    ),
>;

type OpenTx = (DBTransaction, oneshot::Receiver<Result<(), StorageError>>);

pub struct CachedGateway {
    // Can we batch multiple block in here without breaking things?
    // Assuming we are still syncing?

    // TODO: Remove Mutex. It is not needed but avoids changing the Extractor trait.
    open_tx: Arc<Mutex<Option<OpenTx>>>,
    tx: mpsc::Sender<DBCacheMessage>,
    pool: Pool<AsyncPgConnection>,
    state_gateway: PostgresGateway,
    lru_cache: Arc<Mutex<DeltasCache>>,
}

impl Clone for CachedGateway {
    fn clone(&self) -> Self {
        Self {
            // create a separate open tx state for new instances
            open_tx: Arc::new(Mutex::new(None)),
            tx: self.tx.clone(),
            pool: self.pool.clone(),
            state_gateway: self.state_gateway.clone(),
            lru_cache: self.lru_cache.clone(),
        }
    }
}

impl CachedGateway {
    // Accumulating transactions does not drop previous data nor are transactions nested.
    pub async fn start_transaction(&self, block: &models::blockchain::Block, owner: Option<&str>) {
        let mut open_tx = self.open_tx.lock().await;

        if let Some(tx) = open_tx.as_mut() {
            tx.0.block_range.end = block.clone();
        } else {
            let (tx, rx) = oneshot::channel();
            *open_tx = Some((
                DBTransaction {
                    block_range: BlockRange::new(block, block),
                    size: 0,
                    operations: vec![],
                    tx,
                    owner: owner.map(String::from),
                    caller_span: tracing::Span::none(),
                },
                rx,
            ));
        }
    }

    async fn add_op(&self, op: WriteOp) -> Result<(), StorageError> {
        let mut open_tx = self.open_tx.lock().await;
        match open_tx.as_mut() {
            None => {
                Err(StorageError::Unexpected("Usage error: No transaction started".to_string()))
            }
            Some((tx, _)) => {
                tx.add_operation(op)?;
                Ok(())
            }
        }
    }

    pub async fn commit_transaction(&self, min_ops_batch_size: usize) -> Result<(), StorageError> {
        let mut open_tx = self.open_tx.lock().await;
        match open_tx.take() {
            None => {
                Err(StorageError::Unexpected("Usage error: Commit without transaction".to_string()))
            }
            Some((mut db_txn, rx)) => {
                if db_txn.size > min_ops_batch_size {
                    let span = info_span!("DatabaseCommit", size = db_txn.size);
                    db_txn.caller_span = tracing::Span::current();
                    async move {
                        db_txn
                            .operations
                            .sort_by_key(|e| e.order_key());
                        debug!(
                            size = db_txn.size,
                            ops = ?db_txn
                                .operations
                                .iter()
                                .map(WriteOp::variant_name)
                                .collect::<Vec<_>>(),
                            "Submitting db operation batch!"
                        );
                        self.tx
                            .send(DBCacheMessage::Write(db_txn))
                            .await
                            .expect("Send message to receiver ok");
                        rx.await
                            .map_err(|_| StorageError::WriteCacheGoneAway())??;

                        Ok::<(), StorageError>(())
                    }
                    .instrument(span)
                    .await?;
                } else {
                    // if we are not ready to commit, give the OpenTx struct back.
                    *open_tx = Some((db_txn, rx));
                }
                Ok(())
            }
        }
    }

    #[allow(private_interfaces)]
    pub fn new(
        tx: mpsc::Sender<DBCacheMessage>,
        pool: Pool<AsyncPgConnection>,
        state_gateway: PostgresGateway,
    ) -> Self {
        CachedGateway {
            tx,
            open_tx: Arc::new(Mutex::new(None)),
            pool,
            state_gateway,
            lru_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(5).unwrap()))),
        }
    }

    pub async fn get_delta(
        &self,
        chain: &Chain,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
    ) -> Result<
        (
            Vec<models::contract::AccountDelta>,
            Vec<models::protocol::ProtocolComponentStateDelta>,
            Vec<models::protocol::ComponentBalance>,
        ),
        StorageError,
    > {
        let mut lru_cache = self.lru_cache.lock().await;

        if start_version.is_none() {
            tracing::warn!("Get delta called with start_version = None, this might be a bug in one of the extractors")
        }

        // Construct a key for the LRU cache
        let key = RevertParameters {
            start_version: start_version.cloned(),
            end_version: end_version.clone(),
        };

        // Check if the delta is already in the LRU cache
        if let Some(delta) = lru_cache.get(&key) {
            tracing::debug!("Cached delta hit for {:?}", key);
            return Ok(delta.clone());
        }

        tracing::debug!("Cache didn't hit delta. Getting delta for {:?}", key);

        // Fetch the delta from the database
        let mut db = self.pool.get().await.unwrap();
        let accounts_delta = self
            .state_gateway
            .get_accounts_delta(chain, start_version, end_version, &mut db)
            .await?;
        let protocol_delta = self
            .state_gateway
            .get_protocol_states_delta(chain, start_version, end_version, &mut db)
            .await?;
        let balance_deltas = self
            .state_gateway
            .get_balance_deltas(chain, start_version, end_version, &mut db)
            .await?;

        // Insert the new delta into the LRU cache
        lru_cache
            .put(key, (accounts_delta.clone(), protocol_delta.clone(), balance_deltas.clone()));

        Ok((accounts_delta, protocol_delta, balance_deltas))
    }
}

#[async_trait]
impl ExtractionStateGateway for CachedGateway {
    #[instrument(skip_all)]
    async fn get_state(&self, name: &str, chain: &Chain) -> Result<ExtractionState, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_state(name, chain, &mut conn)
            .await
    }
    #[instrument(skip_all)]
    async fn save_state(&self, new: &ExtractionState) -> Result<(), StorageError> {
        self.add_op(WriteOp::SaveExtractionState(new.clone()))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ChainGateway for CachedGateway {
    #[instrument(skip_all)]
    async fn upsert_block(&self, new: &[Block]) -> Result<(), StorageError> {
        self.add_op(WriteOp::UpsertBlock(new.to_vec()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_block(&self, id: &BlockIdentifier) -> Result<Block, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_block(id, &mut conn)
            .await
    }

    async fn upsert_tx(&self, new: &[Transaction]) -> Result<(), StorageError> {
        self.add_op(WriteOp::UpsertTx(new.to_vec()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_tx(&self, hash: &TxHash) -> Result<Transaction, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_tx(hash, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn revert_state(&self, to: &BlockIdentifier) -> Result<(), StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .revert_state(to, &mut conn)
            .await
    }
}

#[async_trait]
impl ContractStateGateway for CachedGateway {
    #[instrument(skip_all)]
    async fn get_contract(
        &self,
        id: &ContractId,
        version: Option<&Version>,
        include_slots: bool,
    ) -> Result<Account, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_contract(id, version, include_slots, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn get_contracts(
        &self,
        chain: &Chain,
        addresses: Option<&[Address]>,
        version: Option<&Version>,
        include_slots: bool,
        pagination_params: Option<&PaginationParams>,
    ) -> Result<WithTotal<Vec<Account>>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_contracts(chain, addresses, version, include_slots, pagination_params, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn insert_contract(&self, new: &Account) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertContract(vec![new.clone()]))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn update_contracts(&self, new: &[(TxHash, AccountDelta)]) -> Result<(), StorageError> {
        self.add_op(WriteOp::UpdateContracts(new.to_vec()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn delete_contract(&self, id: &ContractId, at_tx: &TxHash) -> Result<(), StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .delete_contract(id, at_tx, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn get_accounts_delta(
        &self,
        chain: &Chain,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
    ) -> Result<Vec<AccountDelta>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_accounts_delta(chain, start_version, end_version, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn add_account_balances(
        &self,
        account_balances: &[AccountBalance],
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertAccountBalances(account_balances.to_vec()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_account_balances(
        &self,
        chain: &Chain,
        addresses: Option<&[Address]>,
        version: Option<&Version>,
    ) -> Result<HashMap<Address, HashMap<Address, AccountBalance>>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_account_balances(chain, addresses, version, false, &mut conn)
            .await
    }
}

#[async_trait]
impl ProtocolGateway for CachedGateway {
    #[instrument(skip_all)]
    async fn get_protocol_components(
        &self,
        chain: &Chain,
        system: Option<String>,
        ids: Option<&[&str]>,
        min_tvl: Option<f64>,
        pagination_params: Option<&PaginationParams>,
    ) -> Result<WithTotal<Vec<ProtocolComponent>>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_protocol_components(chain, system, ids, min_tvl, pagination_params, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn get_token_owners(
        &self,
        chain: &Chain,
        tokens: &[Address],
        min_balance: Option<f64>,
    ) -> Result<HashMap<Address, (ComponentId, Bytes)>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_token_owners(chain, tokens, min_balance, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn add_protocol_components(&self, new: &[ProtocolComponent]) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertProtocolComponents(new.to_vec()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn delete_protocol_components(
        &self,
        to_delete: &[ProtocolComponent],
        block_ts: NaiveDateTime,
    ) -> Result<(), StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .delete_protocol_components(to_delete, block_ts, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn add_protocol_types(
        &self,
        new_protocol_types: &[ProtocolType],
    ) -> Result<(), StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .add_protocol_types(new_protocol_types, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn get_protocol_states(
        &self,
        chain: &Chain,
        at: Option<Version>,
        system: Option<String>,
        ids: Option<&[&str]>,
        retrieve_balances: bool,
        pagination_params: Option<&PaginationParams>,
    ) -> Result<WithTotal<Vec<ProtocolComponentState>>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_protocol_states(
                chain,
                at,
                system,
                ids,
                retrieve_balances,
                pagination_params,
                &mut conn,
            )
            .await
    }

    #[instrument(skip_all)]
    async fn update_protocol_states(
        &self,
        new: &[(TxHash, ProtocolComponentStateDelta)],
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::UpsertProtocolState(new.to_vec()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_tokens(
        &self,
        chain: Chain,
        address: Option<&[&Address]>,
        quality: QualityRange,
        traded_n_days_ago: Option<NaiveDateTime>,
        pagination_params: Option<&PaginationParams>,
    ) -> Result<WithTotal<Vec<Token>>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_tokens(chain, address, quality, traded_n_days_ago, pagination_params, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn add_component_balances(
        &self,
        component_balances: &[ComponentBalance],
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertComponentBalances(component_balances.to_vec()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn add_tokens(&self, tokens: &[Token]) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertTokens(tokens.to_vec()))
            .await?;
        Ok(())
    }

    /// Updates tokens without using the write cache.
    ///
    /// This method is currently only used by the tycho-ethereum job and therefore does
    /// not use the write cache. It creates a single transaction and executes all
    /// updates immediately.
    ///
    /// ## Note
    /// This is a short term solution. Ideally we should have a simple gateway version
    /// for these use cases that creates a single transactions and emits them immediately.
    #[instrument(skip_all)]
    async fn update_tokens(&self, tokens: &[Token]) -> Result<(), StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;

        conn.transaction(|conn| {
            async {
                self.state_gateway
                    .update_tokens(tokens, conn)
                    .await?;
                Result::<(), PostgresError>::Ok(())
            }
            .scope_boxed()
        })
        .await
        .map_err(|e| StorageError::Unexpected(format!("Failed to update tokens: {}", e.0)))
    }

    #[instrument(skip_all)]
    async fn get_protocol_states_delta(
        &self,
        chain: &Chain,
        start_version: Option<&BlockOrTimestamp>,
        end_version: &BlockOrTimestamp,
    ) -> Result<Vec<ProtocolComponentStateDelta>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_protocol_states_delta(chain, start_version, end_version, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn get_balance_deltas(
        &self,
        chain: &Chain,
        start_version: Option<&BlockOrTimestamp>,
        target_version: &BlockOrTimestamp,
    ) -> Result<Vec<ComponentBalance>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_balance_deltas(chain, start_version, target_version, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn get_component_balances(
        &self,
        chain: &Chain,
        ids: Option<&[&str]>,
        version: Option<&Version>,
    ) -> Result<HashMap<String, HashMap<Bytes, ComponentBalance>>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_component_balances(chain, ids, version, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn get_token_prices(&self, chain: &Chain) -> Result<HashMap<Bytes, f64>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_token_prices(chain, &mut conn)
            .await
    }

    /// TODO: add to transaction instead
    #[instrument(skip_all)]
    async fn upsert_component_tvl(
        &self,
        chain: &Chain,
        tvl_values: &HashMap<String, f64>,
    ) -> Result<(), StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .upsert_component_tvl(chain, tvl_values, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn get_protocol_systems(
        &self,
        chain: &Chain,
        pagination_params: Option<&PaginationParams>,
    ) -> Result<WithTotal<Vec<String>>, StorageError> {
        self.state_gateway
            .get_protocol_systems(chain, pagination_params)
            .await
    }

    #[instrument(skip_all)]
    async fn get_component_tvls(
        &self,
        chain: &Chain,
        system: Option<String>,
        ids: Option<&[&str]>,
        pagination_params: Option<&PaginationParams>,
    ) -> Result<WithTotal<HashMap<String, f64>>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_component_tvls(chain, system, ids, pagination_params, &mut conn)
            .await
    }
}

#[async_trait]
impl EntryPointGateway for CachedGateway {
    #[instrument(skip_all)]
    async fn insert_entry_points(
        &self,
        entry_points: &HashMap<models::ComponentId, HashSet<models::blockchain::EntryPoint>>,
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertEntryPoints(entry_points.clone()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn insert_entry_point_tracing_params(
        &self,
        entry_points_params: &HashMap<EntryPointId, HashSet<(TracingParams, ComponentId)>>,
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::InsertEntryPointTracingParams(entry_points_params.clone()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_entry_points(
        &self,
        filter: EntryPointFilter,
        pagination_params: Option<&PaginationParams>,
    ) -> Result<WithTotal<HashMap<ComponentId, HashSet<EntryPoint>>>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_entry_points(filter, pagination_params, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn get_entry_points_tracing_params(
        &self,
        filter: EntryPointFilter,
        pagination_params: Option<&PaginationParams>,
    ) -> Result<WithTotal<HashMap<ComponentId, HashSet<EntryPointWithTracingParams>>>, StorageError>
    {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_entry_points_tracing_params(filter, pagination_params, &mut conn)
            .await
    }

    #[instrument(skip_all)]
    async fn upsert_traced_entry_points(
        &self,
        traced_entry_points: &[TracedEntryPoint],
    ) -> Result<(), StorageError> {
        self.add_op(WriteOp::UpsertTracedEntryPoints(traced_entry_points.to_vec()))
            .await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn get_traced_entry_points(
        &self,
        entry_points: &HashSet<EntryPointId>,
    ) -> Result<HashMap<EntryPointId, HashMap<TracingParams, TracingResult>>, StorageError> {
        let mut conn =
            self.pool.get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        self.state_gateway
            .get_tracing_results(entry_points, &mut conn)
            .await
    }
}

impl Gateway for CachedGateway {}

#[cfg(test)]
mod test_serial_db {
    use std::{collections::HashSet, slice, str::FromStr, time::Duration};

    use tycho_common::models::ChangeType;

    use super::*;
    use crate::postgres::{db_fixtures, db_fixtures::yesterday_one_am, testing::run_against_db};

    fn fixed_ts(day: u32) -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2023, 1, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }

    fn test_block(number: u64, hash: &str, ts: NaiveDateTime) -> models::blockchain::Block {
        models::blockchain::Block::new(
            number,
            Chain::Ethereum,
            Bytes::from(hash),
            Bytes::default(),
            ts,
        )
    }

    fn test_tx(
        hash: &str,
        block: &models::blockchain::Block,
        index: u64,
    ) -> models::blockchain::Transaction {
        models::blockchain::Transaction {
            hash: Bytes::from(hash),
            block_hash: block.hash.clone(),
            from: Bytes::zero(20),
            to: Some(Bytes::zero(20)),
            index,
        }
    }

    fn retention_compaction_config() -> BatchTxOptimizationConfig {
        BatchTxOptimizationConfig {
            write_only_referenced_txs: false,
            retention_compact_protocol_writes: true,
        }
    }

    fn observe_config() -> BatchTxOptimizationConfig {
        BatchTxOptimizationConfig {
            write_only_referenced_txs: false,
            retention_compact_protocol_writes: false,
        }
    }

    #[test]
    fn observe_mode_reports_retention_candidates_without_mutating_ops() {
        let block_1 = test_block(
            1,
            "0x0800000000000000000000000000000000000000000000000000000000000000",
            fixed_ts(1),
        );
        let block_2 = test_block(
            2,
            "0x0900000000000000000000000000000000000000000000000000000000000000",
            fixed_ts(2),
        );
        let tx_1 = test_tx(
            "0x8800000000000000000000000000000000000000000000000000000000000000",
            &block_1,
            1,
        );
        let tx_2 = test_tx(
            "0x9900000000000000000000000000000000000000000000000000000000000000",
            &block_2,
            1,
        );
        let component_id = "pool-observe".to_string();
        let mut ops = vec![
            WriteOp::UpsertBlock(vec![block_1, block_2]),
            WriteOp::UpsertTx(vec![tx_1.clone(), tx_2.clone()]),
            WriteOp::UpsertProtocolState(vec![
                (
                    tx_1.hash.clone(),
                    ProtocolComponentStateDelta::new(
                        &component_id,
                        HashMap::from([("reserve0".to_string(), Bytes::from(1_u64).lpad(32, 0))]),
                        HashSet::new(),
                    ),
                ),
                (
                    tx_2.hash.clone(),
                    ProtocolComponentStateDelta::new(
                        &component_id,
                        HashMap::from([("reserve0".to_string(), Bytes::from(2_u64).lpad(32, 0))]),
                        HashSet::new(),
                    ),
                ),
            ]),
        ];
        let original_ops = ops.clone();

        let stats = apply_batch_tx_optimization(&mut ops, fixed_ts(10), observe_config());

        assert_eq!(ops, original_ops);
        assert_eq!(stats.tx_rows_dropped, 0);
        assert_eq!(stats.tx_rows_droppable_raw, 0);
        assert_eq!(stats.tx_rows_droppable_after_compaction, 1);
        assert_eq!(stats.protocol_state_updated_attrs_compacted, 1);
        assert_eq!(stats.tx_required_raw, 2);
        assert_eq!(stats.tx_required_after_compaction, 1);
    }

    #[test]
    fn retention_compaction_stats_do_not_mutate_ops() {
        let block_1 = test_block(
            1,
            "0x0100000000000000000000000000000000000000000000000000000000000000",
            fixed_ts(1),
        );
        let block_2 = test_block(
            2,
            "0x0200000000000000000000000000000000000000000000000000000000000000",
            fixed_ts(2),
        );
        let tx_1 = test_tx(
            "0x1100000000000000000000000000000000000000000000000000000000000000",
            &block_1,
            1,
        );
        let tx_2 = test_tx(
            "0x2200000000000000000000000000000000000000000000000000000000000000",
            &block_2,
            1,
        );
        let token = Bytes::from("0x0000000000000000000000000000000000000001");
        let component_id = "pool-1".to_string();

        let mut ops = vec![
            WriteOp::UpsertBlock(vec![block_1, block_2]),
            WriteOp::UpsertTx(vec![tx_1.clone(), tx_2.clone()]),
            WriteOp::InsertComponentBalances(vec![
                ComponentBalance::new(
                    token.clone(),
                    Bytes::from(1_u64).lpad(32, 0),
                    1.0,
                    tx_1.hash.clone(),
                    &component_id,
                ),
                ComponentBalance::new(
                    token,
                    Bytes::from(2_u64).lpad(32, 0),
                    2.0,
                    tx_2.hash.clone(),
                    &component_id,
                ),
            ]),
            WriteOp::UpsertProtocolState(vec![
                (
                    tx_1.hash.clone(),
                    ProtocolComponentStateDelta::new(
                        &component_id,
                        HashMap::from([("reserve0".to_string(), Bytes::from(1_u64).lpad(32, 0))]),
                        HashSet::new(),
                    ),
                ),
                (
                    tx_2.hash.clone(),
                    ProtocolComponentStateDelta::new(
                        &component_id,
                        HashMap::from([("reserve0".to_string(), Bytes::from(2_u64).lpad(32, 0))]),
                        HashSet::new(),
                    ),
                ),
            ]),
        ];

        let original_ops = ops.clone();
        let stats =
            apply_batch_tx_optimization(&mut ops, fixed_ts(10), retention_compaction_config());

        assert_eq!(stats.component_balance_rows_compacted, 1);
        assert_eq!(stats.protocol_state_updated_attrs_compacted, 1);
        assert_eq!(stats.tx_rows_dropped, 0);
        assert_eq!(stats.tx_rows_droppable_raw, 0);
        assert_eq!(stats.tx_rows_droppable_after_compaction, 1);
        assert_eq!(stats.tx_required_after_compaction, 1);
        assert_eq!(ops, original_ops);
    }

    #[test]
    fn retention_compaction_preserves_protocol_component_creation_tx() {
        let block_1 = test_block(
            1,
            "0x0300000000000000000000000000000000000000000000000000000000000000",
            fixed_ts(1),
        );
        let block_2 = test_block(
            2,
            "0x0400000000000000000000000000000000000000000000000000000000000000",
            fixed_ts(2),
        );
        let tx_1 = test_tx(
            "0x3300000000000000000000000000000000000000000000000000000000000000",
            &block_1,
            1,
        );
        let tx_2 = test_tx(
            "0x4400000000000000000000000000000000000000000000000000000000000000",
            &block_2,
            1,
        );
        let component_id = "pool-creation".to_string();

        let mut ops = vec![
            WriteOp::UpsertBlock(vec![block_1, block_2]),
            WriteOp::UpsertTx(vec![tx_1.clone(), tx_2.clone()]),
            WriteOp::InsertProtocolComponents(vec![ProtocolComponent {
                id: component_id.clone(),
                protocol_system: "ambient".to_string(),
                protocol_type_name: "ambient_pool".to_string(),
                chain: Chain::Ethereum,
                tokens: vec![],
                contract_addresses: vec![],
                static_attributes: HashMap::new(),
                change: ChangeType::Creation,
                creation_tx: tx_1.hash.clone(),
                created_at: fixed_ts(1),
            }]),
            WriteOp::UpsertProtocolState(vec![
                (
                    tx_1.hash.clone(),
                    ProtocolComponentStateDelta::new(
                        &component_id,
                        HashMap::from([("reserve0".to_string(), Bytes::from(1_u64).lpad(32, 0))]),
                        HashSet::new(),
                    ),
                ),
                (
                    tx_2.hash.clone(),
                    ProtocolComponentStateDelta::new(
                        &component_id,
                        HashMap::from([("reserve0".to_string(), Bytes::from(2_u64).lpad(32, 0))]),
                        HashSet::new(),
                    ),
                ),
            ]),
        ];

        let stats =
            apply_batch_tx_optimization(&mut ops, fixed_ts(10), retention_compaction_config());

        assert_eq!(stats.protocol_state_updated_attrs_compacted, 1);
        assert_eq!(stats.tx_rows_dropped, 0);
        assert_eq!(stats.tx_rows_droppable_after_compaction, 0);
        assert_eq!(stats.tx_required_after_compaction, 2);

        let WriteOp::UpsertTx(txs) = &ops[1] else {
            panic!("expected UpsertTx");
        };
        assert_eq!(txs, &vec![tx_1, tx_2]);
    }

    #[test]
    fn retention_compaction_does_not_cross_protocol_state_deletions() {
        let block_1 = test_block(
            1,
            "0x0500000000000000000000000000000000000000000000000000000000000000",
            fixed_ts(1),
        );
        let block_2 = test_block(
            2,
            "0x0600000000000000000000000000000000000000000000000000000000000000",
            fixed_ts(2),
        );
        let block_3 = test_block(
            3,
            "0x0700000000000000000000000000000000000000000000000000000000000000",
            fixed_ts(3),
        );
        let tx_1 = test_tx(
            "0x5500000000000000000000000000000000000000000000000000000000000000",
            &block_1,
            1,
        );
        let tx_2 = test_tx(
            "0x6600000000000000000000000000000000000000000000000000000000000000",
            &block_2,
            1,
        );
        let tx_3 = test_tx(
            "0x7700000000000000000000000000000000000000000000000000000000000000",
            &block_3,
            1,
        );
        let component_id = "pool-deletion".to_string();

        let mut ops = vec![
            WriteOp::UpsertBlock(vec![block_1, block_2, block_3]),
            WriteOp::UpsertTx(vec![tx_1.clone(), tx_2.clone(), tx_3.clone()]),
            WriteOp::UpsertProtocolState(vec![
                (
                    tx_1.hash.clone(),
                    ProtocolComponentStateDelta::new(
                        &component_id,
                        HashMap::from([("reserve0".to_string(), Bytes::from(1_u64).lpad(32, 0))]),
                        HashSet::new(),
                    ),
                ),
                (
                    tx_2.hash.clone(),
                    ProtocolComponentStateDelta::new(
                        &component_id,
                        HashMap::new(),
                        HashSet::from(["reserve0".to_string()]),
                    ),
                ),
                (
                    tx_3.hash.clone(),
                    ProtocolComponentStateDelta::new(
                        &component_id,
                        HashMap::from([("reserve0".to_string(), Bytes::from(3_u64).lpad(32, 0))]),
                        HashSet::new(),
                    ),
                ),
            ]),
        ];

        let stats =
            apply_batch_tx_optimization(&mut ops, fixed_ts(10), retention_compaction_config());

        assert_eq!(stats.protocol_state_updated_attrs_compacted, 0);
        assert_eq!(stats.protocol_state_deleted_attrs, 1);
        assert_eq!(stats.protocol_state_compaction_blocked_by_deletions, 2);
        assert_eq!(stats.tx_rows_dropped, 0);

        let WriteOp::UpsertProtocolState(deltas) = &ops[2] else {
            panic!("expected UpsertProtocolState");
        };
        assert_eq!(deltas.len(), 3);
    }

    #[tokio::test]
    async fn test_write_and_flush() {
        run_against_db(|connection_pool| async move {
            let mut connection = connection_pool
                .get()
                .await
                .expect("Failed to get a connection from the pool");
            let chain_id = db_fixtures::insert_chain(&mut connection, "ethereum").await;
            db_fixtures::insert_token(
                &mut connection,
                chain_id,
                "0000000000000000000000000000000000000000",
                "ETH",
                18,
                Some(100),
            )
            .await;
            let gateway: PostgresGateway = PostgresGateway::from_connection(&mut connection).await;
            let (tx, rx) = mpsc::channel(10);
            let write_executor = DBCacheWriteExecutor::new(
                "ethereum".to_owned(),
                Chain::Ethereum,
                connection_pool.clone(),
                gateway.clone(),
                rx,
            )
            .await;

            let handle = write_executor.run();

            // Send write block message
            let block = get_sample_block(1);
            let os_rx = send_write_message(
                &tx,
                block.clone(),
                vec![WriteOp::UpsertBlock(vec![block.clone()])],
            )
            .await;
            os_rx
                .await
                .expect("Response from channel ok")
                .expect("Transaction cached");

            handle.abort();

            let block_id = BlockIdentifier::Number((Chain::Ethereum, 1));
            let fetched_block = gateway
                .get_block(&block_id, &mut connection)
                .await
                .expect("Failed to fetch extraction state");

            assert_eq!(fetched_block, block);
        })
        .await;
    }

    #[tokio::test]
    async fn test_writes_and_new_blocks() {
        run_against_db(|connection_pool| async move {
            let mut connection = connection_pool
                .get()
                .await
                .expect("Failed to get a connection from the pool");
            let chain_id = db_fixtures::insert_chain(&mut connection, "ethereum").await;
            db_fixtures::insert_token(
                &mut connection,
                chain_id,
                "0000000000000000000000000000000000000000",
                "ETH",
                18,
                Some(100),
            )
            .await;
            db_fixtures::insert_protocol_system(&mut connection, "ambient".to_owned()).await;
            db_fixtures::insert_protocol_type(&mut connection, "ambient_pool", None, None, None)
                .await;
            let gateway: PostgresGateway = PostgresGateway::from_connection(&mut connection).await;
            let (tx, rx) = mpsc::channel(10);

            let write_executor = DBCacheWriteExecutor::new(
                "ethereum".to_owned(),
                Chain::Ethereum,
                connection_pool.clone(),
                gateway.clone(),
                rx,
            )
            .await;

            let handle = write_executor.run();

            // Send first block messages
            let block_1 = get_sample_block(1);
            let tx_1 = get_sample_transaction(1);
            let extraction_state_1 = get_sample_extraction(1);
            let usdc_address = Bytes::from("0xdAC17F958D2ee523a2206206994597C13D831ec7");
            let token = models::token::Token::new(
                &usdc_address,
                "USDT",
                6,
                0,
                &[Some(64), None],
                Chain::Ethereum,
                100,
            );
            let protocol_component_id = "ambient_USDT-USDC".to_owned();
            let protocol_component = models::protocol::ProtocolComponent {
                id: protocol_component_id.clone(),
                protocol_system: "ambient".to_string(),
                protocol_type_name: "ambient_pool".to_string(),
                chain: Default::default(),
                tokens: vec![usdc_address.clone()],
                contract_addresses: vec![],
                change: ChangeType::Creation,
                creation_tx: tx_1.hash.clone(),
                static_attributes: Default::default(),
                created_at: Default::default(),
            };
            let component_balance = models::protocol::ComponentBalance {
                token: usdc_address.clone(),
                balance_float: 0.0,
                balance: Bytes::from(&[0u8]),
                modify_tx: tx_1.hash.clone(),
                component_id: protocol_component_id.clone(),
            };
            let os_rx_1 = send_write_message(
                &tx,
                block_1.clone(),
                vec![
                    WriteOp::UpsertBlock(vec![block_1.clone()]),
                    WriteOp::UpsertTx(vec![tx_1.clone()]),
                    WriteOp::SaveExtractionState(extraction_state_1.clone()),
                    WriteOp::InsertTokens(vec![token]),
                    WriteOp::InsertProtocolComponents(vec![protocol_component]),
                    WriteOp::InsertComponentBalances(vec![component_balance]),
                ],
            )
            .await;
            os_rx_1
                .await
                .expect("Response from channel ok")
                .expect("Transaction cached");

            // Send second block messages
            let block_2 = get_sample_block(2);
            let attributes: HashMap<String, Bytes> =
                vec![("reserve1".to_owned(), Bytes::from(1000u64).lpad(32, 0))]
                    .into_iter()
                    .collect();
            let protocol_state_delta = models::protocol::ProtocolComponentStateDelta::new(
                protocol_component_id.as_str(),
                attributes,
                HashSet::new(),
            );
            let os_rx_2 = send_write_message(
                &tx,
                block_2.clone(),
                vec![
                    WriteOp::UpsertBlock(vec![block_2.clone()]),
                    WriteOp::UpsertProtocolState(vec![(tx_1.hash.clone(), protocol_state_delta)]),
                ],
            )
            .await;
            os_rx_2
                .await
                .expect("Response from channel ok")
                .expect("Transaction cached");

            // Send third block messages
            let block_3 = get_sample_block(3);
            let os_rx_3 =
                send_write_message(&tx, block_3.clone(), vec![WriteOp::UpsertBlock(vec![block_3])])
                    .await;
            os_rx_3
                .await
                .expect("Response from channel ok")
                .expect("Transaction cached");

            handle.abort();

            // Assert that transactions have been flushed
            let block_id_1 = BlockIdentifier::Number((Chain::Ethereum, 1));
            let fetched_block_1 = gateway
                .get_block(&block_id_1, &mut connection)
                .await
                .expect("Failed to fetch block");

            let fetched_tx = gateway
                .get_tx(&tx_1.hash.clone(), &mut connection)
                .await
                .expect("Failed to fetch tx");

            let fetched_extraction_state = gateway
                .get_state("vm:test", &Chain::Ethereum, &mut connection)
                .await
                .expect("Failed to fetch extraction state");

            let block_id_2 = BlockIdentifier::Number((Chain::Ethereum, 2));
            let fetched_block_2 = gateway
                .get_block(&block_id_2, &mut connection)
                .await
                .expect("Failed to fetch block");

            let block_id_3 = BlockIdentifier::Number((Chain::Ethereum, 3));
            let block_3 = get_sample_block(3);
            let fetched_block_3 = gateway
                .get_block(&block_id_3, &mut connection)
                .await
                .expect("Failed to fetch block");

            // Assert block 1 messages have been flushed
            assert_eq!(fetched_block_1, block_1);
            assert_eq!(fetched_tx, tx_1);
            assert_eq!(fetched_extraction_state, extraction_state_1);
            // Assert block 2 messages have been flushed
            assert_eq!(fetched_block_2, block_2);
            // Assert block 3 messages have been flushed
            assert_eq!(fetched_block_3, block_3);
        })
        .await
    }

    #[test_log::test(tokio::test)]
    async fn test_cached_gateway() {
        // Setup
        run_against_db(|connection_pool| async move {
            let mut connection = connection_pool
                .get()
                .await
                .expect("Failed to get a connection from the pool");
            let chain_id = db_fixtures::insert_chain(&mut connection, "ethereum").await;
            db_fixtures::insert_token(
                &mut connection,
                chain_id,
                "0000000000000000000000000000000000000000",
                "ETH",
                18,
                Some(100),
            )
            .await;
            let gateway: PostgresGateway = PostgresGateway::from_connection(&mut connection).await;
            let (tx, rx) = mpsc::channel(10);

            let write_executor = DBCacheWriteExecutor::new(
                "ethereum".to_owned(),
                Chain::Ethereum,
                connection_pool.clone(),
                gateway.clone(),
                rx,
            )
            .await;

            let handle = write_executor.run();
            let cached_gw = CachedGateway::new(tx, connection_pool.clone(), gateway);

            // Send first block messages
            let block_1 = get_sample_block(1);
            let tx_1 = get_sample_transaction(1);
            cached_gw
                .start_transaction(&block_1, None)
                .await;
            cached_gw
                .upsert_block(slice::from_ref(&block_1))
                .await
                .expect("Upsert block 1 ok");
            cached_gw
                .upsert_tx(slice::from_ref(&tx_1))
                .await
                .expect("Upsert tx 1 ok");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("committing tx failed");

            // Send second block messages
            let block_2 = get_sample_block(2);
            cached_gw
                .start_transaction(&block_2, None)
                .await;
            cached_gw
                .upsert_block(slice::from_ref(&block_2))
                .await
                .expect("Upsert block 2 ok");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("committing tx failed");

            // Send third block messages
            let block_3 = get_sample_block(3);
            cached_gw
                .start_transaction(&block_3, None)
                .await;
            cached_gw
                .upsert_block(slice::from_ref(&block_3))
                .await
                .expect("Upsert block 3 ok");
            cached_gw
                .commit_transaction(0)
                .await
                .expect("committing tx failed");

            handle.abort();

            // Assert that messages from block 1,2 and 3 have been commited to the db.
            let block_id_1 = BlockIdentifier::Number((Chain::Ethereum, 1));
            let fetched_block_1 = cached_gw
                .get_block(&block_id_1)
                .await
                .expect("Failed to fetch block");

            let fetched_tx = cached_gw
                .get_tx(&tx_1.hash.clone())
                .await
                .expect("Failed to fetch tx");

            let block_id_2 = BlockIdentifier::Number((Chain::Ethereum, 2));
            let fetched_block_2 = cached_gw
                .get_block(&block_id_2)
                .await
                .expect("Failed to fetch block");

            let block_id_3 = BlockIdentifier::Number((Chain::Ethereum, 3));
            let fetched_block_3 = cached_gw
                .get_block(&block_id_3)
                .await
                .expect("Failed to fetch block");

            // Assert block 1 messages have been flushed
            assert_eq!(fetched_block_1, block_1);
            assert_eq!(fetched_tx, tx_1);
            // Assert block 2 messages have been flushed
            assert_eq!(fetched_block_2, block_2);
            // Assert block 3 is still pending in cache
            assert_eq!(fetched_block_3, block_3);
        })
        .await;
    }

    fn get_sample_block(version: usize) -> models::blockchain::Block {
        let ts1 = yesterday_one_am();
        let ts2 = ts1 + Duration::from_secs(3600);
        let ts3 = ts2 + Duration::from_secs(3600);
        match version {
            1 => models::blockchain::Block::new(
                1,
                Chain::Ethereum,
                "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6"
                    .parse()
                    .expect("Invalid hash"),
                Bytes::default(),
                ts1,
            ),
            2 => models::blockchain::Block::new(
                2,
                Chain::Ethereum,
                "0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9"
                    .parse()
                    .expect("Invalid hash"),
                "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6"
                    .parse()
                    .expect("Invalid hash"),
                ts2,
            ),
            3 => models::blockchain::Block::new(
                3,
                Chain::Ethereum,
                "0x3d6122660cc824376f11ee842f83addc3525e2dd6756b9bcf0affa6aa88cf741"
                    .parse()
                    .expect("Invalid hash"),
                "0xb495a1d7e6663152ae92708da4843337b958146015a2802f4193a410044698c9"
                    .parse()
                    .expect("Invalid hash"),
                ts3,
            ),
            _ => panic!("Block version not found"),
        }
    }

    fn get_sample_transaction(version: usize) -> models::blockchain::Transaction {
        match version {
            1 => models::blockchain::Transaction {
                hash: Bytes::from(
                    "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945",
                ),
                block_hash: Bytes::from(
                    "0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6",
                ),
                from: Bytes::from("0x4648451b5F87FF8F0F7D622bD40574bb97E25980"),
                to: Some(Bytes::from("0x6B175474E89094C44Da98b954EedeAC495271d0F")),
                index: 1,
            },
            _ => panic!("Block version not found"),
        }
    }

    fn get_sample_extraction(version: usize) -> ExtractionState {
        match version {
            1 => ExtractionState::new(
                "vm:test".to_string(),
                Chain::Ethereum,
                None,
                "cursor@420".as_bytes(),
                Bytes::from_str("88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6")
                    .unwrap(),
            ),
            _ => panic!("Block version not found"),
        }
    }

    async fn send_write_message(
        tx: &mpsc::Sender<DBCacheMessage>,
        block: models::blockchain::Block,
        operations: Vec<WriteOp>,
    ) -> oneshot::Receiver<Result<(), StorageError>> {
        let (os_tx, os_rx) = oneshot::channel();
        let db_transaction = DBTransaction {
            block_range: BlockRange::new(&block, &block),
            size: operations.len(),
            operations,
            tx: os_tx,
            owner: None,
            caller_span: tracing::Span::none(),
        };

        tx.send(DBCacheMessage::Write(db_transaction))
            .await
            .expect("Failed to send write message through mpsc channel");
        os_rx
    }

    //noinspection SpellCheckingInspection
    #[allow(dead_code)]
    async fn setup_data(conn: &mut AsyncPgConnection) {
        // set up blocks and txns
        let chain_id = db_fixtures::insert_chain(conn, "ethereum").await;
        let blk = db_fixtures::insert_blocks(conn, chain_id).await;
        let ts = chrono::Local::now().naive_utc() - Duration::from_secs(3600);
        let tx_hashes = [
            "0xbb7e16d797a9e2fbc537e30f91ed3d27a254dd9578aa4c3af3e5f0d3e8130945".to_string(),
            "0x794f7df7a3fe973f1583fbb92536f9a8def3a89902439289315326c04068de54".to_string(),
            "0x3108322284d0a89a7accb288d1a94384d499504fe7e04441b0706c7628dee7b7".to_string(),
            "0x50449de1973d86f21bfafa7c72011854a7e33a226709dc3e2e4edcca34188388".to_string(),
        ];

        let txn = db_fixtures::insert_txns(
            conn,
            &[
                (blk[0], 1i64, &tx_hashes[0]),
                (blk[0], 2i64, &tx_hashes[1]),
                // ----- Block 01 LAST
                (blk[1], 1i64, &tx_hashes[2]),
                (blk[1], 2i64, &tx_hashes[3]),
                // ----- Block 02 LAST
            ],
        )
        .await;
        let (_, native_token) = db_fixtures::insert_token(
            conn,
            chain_id,
            "0000000000000000000000000000000000000000",
            "ETH",
            18,
            Some(100),
        )
        .await;

        // set up contract data
        let c0 = db_fixtures::insert_account(
            conn,
            "6B175474E89094C44Da98b954EedeAC495271d0F",
            "account0",
            chain_id,
            Some(txn[0]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 0, native_token, txn[0], Some(&ts), c0).await;
        db_fixtures::insert_contract_code(conn, c0, txn[0], Bytes::from_str("C0C0C0").unwrap())
            .await;
        db_fixtures::insert_account_balance(
            conn,
            100,
            native_token,
            txn[1],
            Some(&(ts + Duration::from_secs(3600))),
            c0,
        )
        .await;
        db_fixtures::insert_slots(conn, c0, txn[1], &ts, None, &[(2, 1, None)]).await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[1],
            &ts,
            Some(&(ts + Duration::from_secs(3600))),
            &[(0, 1, None), (1, 5, None)],
        )
        .await;
        db_fixtures::insert_account_balance(conn, 101, native_token, txn[3], None, c0).await;
        db_fixtures::insert_slots(
            conn,
            c0,
            txn[3],
            &(ts + Duration::from_secs(3600)),
            None,
            &[(0, 2, Some(1)), (1, 3, Some(5)), (5, 25, None), (6, 30, None)],
        )
        .await;

        let c1 = db_fixtures::insert_account(
            conn,
            "73BcE791c239c8010Cd3C857d96580037CCdd0EE",
            "c1",
            chain_id,
            Some(txn[2]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 50, native_token, txn[2], None, c1).await;
        db_fixtures::insert_contract_code(conn, c1, txn[2], Bytes::from_str("C1C1C1").unwrap())
            .await;
        db_fixtures::insert_slots(
            conn,
            c1,
            txn[3],
            &(ts + Duration::from_secs(3600)),
            None,
            &[(0, 128, None), (1, 255, None)],
        )
        .await;

        let c2 = db_fixtures::insert_account(
            conn,
            "94a3F312366b8D0a32A00986194053C0ed0CdDb1",
            "c2",
            chain_id,
            Some(txn[1]),
        )
        .await;
        db_fixtures::insert_account_balance(conn, 25, native_token, txn[1], None, c2).await;
        db_fixtures::insert_contract_code(conn, c2, txn[1], Bytes::from_str("C2C2C2").unwrap())
            .await;
        db_fixtures::insert_slots(
            conn,
            c2,
            txn[1],
            &(ts + Duration::from_secs(3600)),
            None,
            &[(1, 2, None), (2, 4, None)],
        )
        .await;
        db_fixtures::delete_account(conn, c2, &(ts + Duration::from_secs(3600))).await;

        // set up protocol state data
        let protocol_system_id =
            db_fixtures::insert_protocol_system(conn, "ambient".to_owned()).await;
        let protocol_type_id = db_fixtures::insert_protocol_type(
            conn,
            "Pool",
            Some(models::FinancialType::Swap),
            None,
            Some(models::ImplementationType::Custom),
        )
        .await;
        let protocol_component_id = db_fixtures::insert_protocol_component(
            conn,
            "state1",
            chain_id,
            protocol_system_id,
            protocol_type_id,
            txn[0],
            None,
            None,
        )
        .await;
        // protocol state for state1-reserve1
        db_fixtures::insert_protocol_state(
            conn,
            protocol_component_id,
            txn[0],
            "reserve1".to_owned(),
            Bytes::from(1100u64).lpad(32, 0),
            None,
            Some(txn[2]),
        )
        .await;
    }
}
