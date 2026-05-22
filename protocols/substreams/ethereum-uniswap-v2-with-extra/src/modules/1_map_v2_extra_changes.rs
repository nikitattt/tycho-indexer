use std::collections::HashMap;

use itertools::Itertools;
use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2 as eth;
use tycho_substreams::prelude::{
    Attribute, Block as TychoBlock, BlockChanges, EntityChanges, Transaction,
    TransactionChangesBuilder,
};

use crate::{modules::pool_key, pb::uniswap::v2::Pool, storage::v2_extra_attribute};

#[substreams::handlers::map]
pub fn map_v2_extra_changes(
    block: eth::Block,
    pools_store: StoreGetProto<Pool>,
) -> Result<BlockChanges, substreams::errors::Error> {
    Ok(collect_v2_extra_changes(block, |pool| {
        pools_store
            .get_last(pool_key(pool))
            .is_some()
    }))
}

fn collect_v2_extra_changes<F>(block: eth::Block, mut is_known_pool: F) -> BlockChanges
where
    F: FnMut(&[u8]) -> bool,
{
    let mut latest_attributes: HashMap<(Vec<u8>, String), PendingAttribute> = HashMap::new();
    let mut known_pools: HashMap<Vec<u8>, bool> = HashMap::new();

    for tx in block.transaction_traces.iter() {
        if tx.status != i32::from(eth::TransactionTraceStatus::Succeeded) {
            continue;
        }

        let mut extra_storage_changes = tx
            .calls
            .iter()
            .filter(|call| !call.state_reverted)
            .flat_map(|call| call.storage_changes.iter())
            .filter(|change| v2_extra_attribute(change).is_some())
            .collect::<Vec<_>>();

        if extra_storage_changes.is_empty() {
            continue;
        }

        extra_storage_changes.sort_unstable_by_key(|change| change.ordinal);

        let tycho_tx = Transaction {
            hash: tx.hash.clone(),
            from: tx.from.clone(),
            to: tx.to.clone(),
            index: tx.index.into(),
        };

        for storage_change in extra_storage_changes {
            if !is_known_pool_cached(&storage_change.address, &mut known_pools, &mut is_known_pool)
            {
                continue;
            }

            let Some(attribute) = v2_extra_attribute(storage_change) else {
                continue;
            };
            let component_id = format!("0x{}", hex::encode(&storage_change.address));

            latest_attributes.insert(
                (storage_change.address.clone(), attribute.name.clone()),
                PendingAttribute {
                    tx: tycho_tx.clone(),
                    component_id,
                    attribute,
                    order: (tycho_tx.index, storage_change.ordinal),
                },
            );
        }
    }

    let mut transaction_changes: HashMap<u64, TransactionChangesBuilder> = HashMap::new();
    for pending in latest_attributes
        .into_values()
        .sorted_unstable_by_key(|pending| pending.order)
    {
        let builder = transaction_changes
            .entry(pending.tx.index)
            .or_insert_with(|| TransactionChangesBuilder::new(&pending.tx));

        builder.add_entity_change(&EntityChanges {
            component_id: pending.component_id,
            attributes: vec![pending.attribute],
        });
    }

    BlockChanges {
        block: Some(block_metadata(&block)),
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .collect::<Vec<_>>(),
        ..Default::default()
    }
}

fn block_metadata(block: &eth::Block) -> TychoBlock {
    TychoBlock {
        number: block.number,
        hash: block.hash.clone(),
        parent_hash: block
            .header
            .as_ref()
            .map(|header| header.parent_hash.clone())
            .unwrap_or_default(),
        ts: block
            .header
            .as_ref()
            .and_then(|header| header.timestamp.as_ref())
            .map(|timestamp| timestamp.seconds as u64)
            .unwrap_or_default(),
    }
}

fn is_known_pool_cached<F>(
    address: &[u8],
    known_pools: &mut HashMap<Vec<u8>, bool>,
    is_known_pool: &mut F,
) -> bool
where
    F: FnMut(&[u8]) -> bool,
{
    if let Some(is_known) = known_pools.get(address) {
        return *is_known;
    }

    let is_known = is_known_pool(address);
    known_pools.insert(address.to_vec(), is_known);
    is_known
}

struct PendingAttribute {
    tx: Transaction,
    component_id: String,
    attribute: Attribute,
    order: (u64, u64),
}

#[cfg(test)]
mod tests {
    use hex_literal::hex;
    use substreams::scalar::BigInt;
    use substreams_ethereum::pb::eth::v2::{
        Block, BlockHeader, Call, StorageChange, TransactionTrace, TransactionTraceStatus,
    };

    use crate::storage::{K_LAST_ATTRIBUTE, K_LAST_SLOT, TOTAL_SUPPLY_ATTRIBUTE};

    use super::*;

    #[test]
    fn repeated_updates_in_one_transaction_keep_final_value() {
        let pool = pool_address();
        let block = block_with_transactions(vec![transaction(
            0,
            vec![call(vec![
                storage_change(&pool, K_LAST_SLOT, 1, 2, 10),
                storage_change(&pool, K_LAST_SLOT, 2, 3, 20),
            ])],
        )]);

        let changes = collect_v2_extra_changes(block, |address| address == pool.as_slice());

        let attributes = &changes.changes[0].entity_changes[0].attributes;
        assert_eq!(attributes.len(), 1);
        assert_eq!(attributes[0].name, K_LAST_ATTRIBUTE);
        assert_eq!(BigInt::from_signed_bytes_be(&attributes[0].value), BigInt::from(3));
    }

    #[test]
    fn emits_raw_pool_component_ids() {
        let pool = pool_address();
        let block = block_with_transactions(vec![transaction(
            0,
            vec![call(vec![storage_change(&pool, K_LAST_SLOT, 1, 2, 10)])],
        )]);

        let changes = collect_v2_extra_changes(block, |address| address == pool.as_slice());

        assert_eq!(
            changes.changes[0]
                .component_changes
                .len(),
            0
        );
        assert_eq!(
            changes.changes[0].entity_changes[0].component_id,
            format!("0x{}", hex::encode(pool))
        );
    }

    #[test]
    fn skips_failed_transactions_and_reverted_calls() {
        let pool = pool_address();
        let reverted_call = Call {
            state_reverted: true,
            storage_changes: vec![storage_change(&pool, K_LAST_SLOT, 1, 2, 10)],
            ..Default::default()
        };
        let block = block_with_transactions(vec![
            TransactionTrace {
                status: i32::from(TransactionTraceStatus::Failed),
                calls: vec![call(vec![storage_change(&pool, K_LAST_SLOT, 1, 2, 10)])],
                ..transaction(0, vec![])
            },
            transaction(1, vec![reverted_call]),
        ]);

        let changes = collect_v2_extra_changes(block, |address| address == pool.as_slice());

        assert!(changes.changes.is_empty());
    }

    #[test]
    fn emits_both_extra_attributes_for_known_pool() {
        let pool = pool_address();
        let block = block_with_transactions(vec![transaction(
            0,
            vec![call(vec![
                storage_change(&pool, K_LAST_SLOT, 1, 2, 10),
                storage_change(&pool, crate::storage::TOTAL_SUPPLY_SLOT, 5, 6, 20),
            ])],
        )]);

        let changes = collect_v2_extra_changes(block, |address| address == pool.as_slice());
        let attributes = &changes.changes[0].entity_changes[0].attributes;

        assert!(attributes
            .iter()
            .any(|attr| attr.name == K_LAST_ATTRIBUTE));
        assert!(attributes
            .iter()
            .any(|attr| attr.name == TOTAL_SUPPLY_ATTRIBUTE));
    }

    fn block_with_transactions(transaction_traces: Vec<TransactionTrace>) -> Block {
        Block { header: Some(BlockHeader::default()), transaction_traces, ..Default::default() }
    }

    fn transaction(index: u32, calls: Vec<Call>) -> TransactionTrace {
        TransactionTrace {
            index,
            status: i32::from(TransactionTraceStatus::Succeeded),
            hash: vec![index as u8; 32],
            from: vec![0x01; 20],
            to: vec![0x02; 20],
            calls,
            ..Default::default()
        }
    }

    fn call(storage_changes: Vec<StorageChange>) -> Call {
        Call { storage_changes, ..Default::default() }
    }

    fn storage_change(
        address: &[u8],
        key: [u8; 32],
        old_value: u64,
        new_value: u64,
        ordinal: u64,
    ) -> StorageChange {
        StorageChange {
            address: address.to_vec(),
            key: key.to_vec(),
            old_value: BigInt::from(old_value).to_signed_bytes_be(),
            new_value: BigInt::from(new_value).to_signed_bytes_be(),
            ordinal,
        }
    }

    fn pool_address() -> Vec<u8> {
        hex!("1111111111111111111111111111111111111111").to_vec()
    }
}
