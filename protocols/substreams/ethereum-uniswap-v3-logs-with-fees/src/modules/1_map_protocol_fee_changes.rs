use std::collections::HashMap;

use itertools::Itertools;
use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2 as eth;
use tycho_substreams::prelude::{
    Attribute, BlockChanges, EntityChanges, Transaction, TransactionChangesBuilder,
};

use crate::{
    modules::pool_key,
    pb::uniswap::v3::Pool,
    storage::{protocol_fee_attributes, PROTOCOL_FEES_SLOT},
};

#[substreams::handlers::map]
pub fn map_protocol_fee_changes(
    block: eth::Block,
    pools_store: StoreGetProto<Pool>,
) -> Result<BlockChanges, substreams::errors::Error> {
    let mut latest_attributes: HashMap<(Vec<u8>, String), PendingAttribute> = HashMap::new();
    let mut known_pools: HashMap<Vec<u8>, bool> = HashMap::new();

    for tx in block.transactions() {
        if tx.status != 1 {
            continue;
        }

        let mut protocol_fee_storage_changes = tx
            .calls()
            .filter(|call_view| !call_view.call.state_reverted)
            .flat_map(|call_view| call_view.call.storage_changes.iter())
            .filter(|change| change.key == PROTOCOL_FEES_SLOT)
            .collect::<Vec<_>>();

        if protocol_fee_storage_changes.is_empty() {
            continue;
        }

        protocol_fee_storage_changes.sort_unstable_by_key(|change| change.ordinal);

        let tycho_tx = Transaction {
            hash: tx.hash.clone(),
            from: tx.from.clone(),
            to: tx.to.clone(),
            index: tx.index.into(),
        };

        for storage_change in protocol_fee_storage_changes {
            let pool = &storage_change.address;
            let attributes = protocol_fee_attributes(storage_change);

            if attributes.is_empty() || !is_known_pool(pool, &pools_store, &mut known_pools) {
                continue;
            }

            for attribute in attributes {
                let key = (pool.clone(), attribute.name.clone());
                latest_attributes.insert(
                    key,
                    PendingAttribute {
                        tx: tycho_tx.clone(),
                        pool: pool.clone(),
                        attribute,
                        order: (tycho_tx.index, storage_change.ordinal),
                    },
                );
            }
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
            component_id: format!("0x{}", hex::encode(pending.pool)),
            attributes: vec![pending.attribute],
        });
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

fn is_known_pool(
    address: &[u8],
    pools_store: &StoreGetProto<Pool>,
    known_pools: &mut HashMap<Vec<u8>, bool>,
) -> bool {
    if let Some(is_known) = known_pools.get(address) {
        return *is_known;
    }

    let is_known = pools_store
        .get_last(pool_key(address))
        .is_some();
    known_pools.insert(address.to_vec(), is_known);
    is_known
}

struct PendingAttribute {
    tx: Transaction,
    pool: Vec<u8>,
    attribute: Attribute,
    order: (u64, u64),
}
