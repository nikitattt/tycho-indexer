use std::collections::HashMap;

use itertools::Itertools;
use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::{pb::eth::v2 as eth, Event};
use tycho_substreams::prelude::{
    BlockChanges, EntityChanges, Transaction, TransactionChangesBuilder,
};

use crate::{
    abi::pool::events::{
        Burn, Collect, CollectProtocol, Flash, Initialize, Mint, SetFeeProtocol, Swap,
    },
    modules::pool_key,
    pb::uniswap::v3::Pool,
    storage::protocol_fee_attributes,
};

#[substreams::handlers::map]
pub fn map_protocol_fee_changes(
    block: eth::Block,
    pools_store: StoreGetProto<Pool>,
) -> Result<BlockChanges, substreams::errors::Error> {
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
            if pools_store
                .get_last(pool_key(pool))
                .is_none()
            {
                continue;
            }

            let builder = transaction_changes
                .entry(tycho_tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tycho_tx));

            let attributes = protocol_fee_attributes(&call_view.call.storage_changes, pool);
            if !attributes.is_empty() {
                builder.add_entity_change(&EntityChanges {
                    component_id: format!("0x{}", hex::encode(pool)),
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
