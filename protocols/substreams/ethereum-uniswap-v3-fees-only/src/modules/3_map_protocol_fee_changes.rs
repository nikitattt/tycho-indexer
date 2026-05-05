use std::collections::HashMap;

use itertools::Itertools;
use substreams::store::{StoreGet, StoreGetRaw};
use substreams_ethereum::{pb::eth::v2 as eth, Event};
use tycho_substreams::prelude::{
    Attribute, BlockChanges, ChangeType, EntityChanges, FinancialType, ImplementationType,
    ProtocolComponent, ProtocolType, Transaction, TransactionChangesBuilder,
};

use crate::{
    abi::pool::events::{
        Burn, Collect, CollectProtocol, Flash, Initialize, Mint, SetFeeProtocol, Swap,
    },
    modules::{
        fee_component_id, map_protocol_fee_pool_candidates::find_set_fee_protocol_pool_ordinals,
        pool_key,
    },
    storage::protocol_fee_attributes,
};

#[substreams::handlers::map]
pub fn map_protocol_fee_changes(
    block: eth::Block,
    store_protocol_fee_pools: StoreGetRaw,
) -> Result<BlockChanges, substreams::errors::Error> {
    let current_block_candidates = find_set_fee_protocol_pool_ordinals(&block);
    let mut created_this_block = HashMap::<Vec<u8>, bool>::new();
    let mut transaction_changes: HashMap<u64, TransactionChangesBuilder> = HashMap::new();

    for tx in block.transactions() {
        if tx.status != 1 {
            continue;
        }

        let tycho_tx = Transaction {
            hash: tx.hash.clone(),
            from: tx.from.clone(),
            to: tx.to.clone(),
            index: tx.index.into(),
        };

        for (log, call_view) in tx.logs_with_calls() {
            if call_view.call.state_reverted {
                continue;
            }
            if !is_v3_pool_activity_log(log) {
                continue;
            }

            let pool = &log.address;
            let key = pool_key(pool);
            let was_tracked = store_protocol_fee_pools
                .get_last(&key)
                .is_some();
            let is_current_candidate = current_block_candidates
                .get(pool)
                .is_some_and(|candidate_ordinal| log.ordinal >= *candidate_ordinal);

            if !was_tracked && !is_current_candidate {
                continue;
            }

            let builder = transaction_changes
                .entry(tycho_tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tycho_tx));

            if !was_tracked && is_current_candidate && !created_this_block.contains_key(pool) {
                builder.add_protocol_component(&fee_component(pool));
                created_this_block.insert(pool.clone(), true);
            }

            let attributes = protocol_fee_attributes(&call_view.call.storage_changes, pool);
            if !attributes.is_empty() {
                builder.add_entity_change(&EntityChanges {
                    component_id: fee_component_id(pool),
                    attributes,
                });
            }
        }
    }

    Ok(BlockChanges {
        block: Some((&block).into()),
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .collect::<Vec<_>>(),
        ..Default::default()
    })
}

fn is_v3_pool_activity_log(log: &eth::Log) -> bool {
    Initialize::match_and_decode(log).is_some()
        || Swap::match_and_decode(log).is_some()
        || Flash::match_and_decode(log).is_some()
        || Mint::match_and_decode(log).is_some()
        || Burn::match_and_decode(log).is_some()
        || Collect::match_and_decode(log).is_some()
        || SetFeeProtocol::match_and_decode(log).is_some()
        || CollectProtocol::match_and_decode(log).is_some()
}

fn fee_component(pool: &[u8]) -> ProtocolComponent {
    ProtocolComponent {
        id: fee_component_id(pool),
        tokens: vec![],
        contracts: vec![pool.to_vec()],
        static_att: vec![
            Attribute {
                name: "pool_address".to_string(),
                value: pool.to_vec(),
                change: ChangeType::Creation.into(),
            },
            Attribute {
                name: "source_component_id".to_string(),
                value: pool.to_vec(),
                change: ChangeType::Creation.into(),
            },
        ],
        change: ChangeType::Creation.into(),
        protocol_type: Some(ProtocolType {
            name: "uniswap_v3_fees".to_string(),
            financial_type: FinancialType::Swap.into(),
            attribute_schema: vec![],
            implementation_type: ImplementationType::Custom.into(),
        }),
    }
}
