use itertools::Itertools;
use std::collections::HashMap;
use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2::{self as eth};

use substreams_helper::{event_handler::EventHandler, hex::Hexable};

use crate::{
    abi::pool::events::Sync, storage::v2_extra_attribute, store_key::StoreKey,
    traits::PoolAddresser,
};
use tycho_substreams::prelude::*;

// Auxiliary struct to serve as a key for the HashMaps.
#[derive(Clone, Hash, Eq, PartialEq)]
struct ComponentKey<T> {
    component_id: String,
    name: T,
}

impl<T> ComponentKey<T> {
    fn new(component_id: String, name: T) -> Self {
        ComponentKey { component_id, name }
    }
}

#[derive(Clone)]
struct PartialChanges {
    transaction: Transaction,
    entity_changes: HashMap<ComponentKey<String>, Attribute>,
    balance_changes: HashMap<ComponentKey<Vec<u8>>, BalanceChange>,
}

impl PartialChanges {
    // Consolidate the entity changes into a vector of EntityChanges. Initially, the entity changes
    // are in a map to prevent duplicates. For each transaction, we need to have only one final
    // state change, per state. Example:
    // If we have two sync events for the same pool (in the same tx), we need to have only one final
    // state change for the reserves. This will be the last sync event, as it is the final state
    // of the pool after the transaction.
    fn consolidate_entity_changes(self) -> Vec<EntityChanges> {
        self.entity_changes
            .into_iter()
            .map(|(key, attribute)| (key.component_id, attribute))
            .into_group_map()
            .into_iter()
            .map(|(component_id, attributes)| EntityChanges { component_id, attributes })
            .collect()
    }
}

#[substreams::handlers::map]
pub fn map_protocol_changes(
    block: eth::Block,
    block_entity_changes: BlockChanges,
    pools_store: StoreGetProto<ProtocolComponent>,
) -> Result<BlockChanges, substreams::errors::Error> {
    // Sync event is sufficient for our use-case. Since it's emitted on every reserve-altering
    // function call, we can use it as the only event to update the reserves of a pool.
    let mut block_entity_changes = block_entity_changes;
    let mut tx_changes: HashMap<Vec<u8>, PartialChanges> = HashMap::new();

    handle_sync(&block, &mut tx_changes, &pools_store);
    merge_block(&mut tx_changes, &mut block_entity_changes);

    let extra_changes = collect_v2_extra_changes(&block, &pools_store);
    merge_extra_changes(&mut block_entity_changes, extra_changes);

    block_entity_changes
        .changes
        .sort_unstable_by_key(|change| {
            change
                .tx
                .as_ref()
                .map(|tx| tx.index)
                .unwrap_or_default()
        });

    Ok(block_entity_changes)
}

/// Handle the sync events and update the reserves of the pools.
///
/// This function is called for each block, and it will handle the sync events for each transaction.
/// On UniswapV2, Sync events are emitted on every reserve-altering function call, so we can use
/// only this event to keep track of the pool state.
///
/// This function also relies on an intermediate HashMap to store the changes for each transaction.
/// This is necessary because we need to consolidate the changes for each transaction before adding
/// them to the block_entity_changes. This HashMap prevents us from having duplicate changes for the
/// same pool and token. See the PartialChanges struct for more details.
fn handle_sync(
    block: &eth::Block,
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    store: &StoreGetProto<ProtocolComponent>,
) {
    let mut on_sync = |event: Sync, _tx: &eth::TransactionTrace, _log: &eth::Log| {
        let pool_address_hex = _log.address.to_hex();

        let pool =
            store.must_get_last(StoreKey::Pool.get_unique_pool_key(pool_address_hex.as_str()));
        // Convert reserves to bytes
        let reserves_bytes = [event.reserve0, event.reserve1];

        let tx_change = tx_changes
            .entry(_tx.hash.clone())
            .or_insert_with(|| PartialChanges {
                transaction: _tx.into(),
                entity_changes: HashMap::new(),
                balance_changes: HashMap::new(),
            });

        for (i, reserve_bytes) in reserves_bytes.iter().enumerate() {
            let attribute_name = format!("reserve{}", i);
            // By using a HashMap, we can overwrite the previous value of the reserve attribute if
            // it is for the same pool and the same attribute name (reserves).
            tx_change.entity_changes.insert(
                ComponentKey::new(pool_address_hex.clone(), attribute_name.clone()),
                Attribute {
                    name: attribute_name,
                    value: reserve_bytes
                        .clone()
                        .to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            );
        }

        // Update balance changes for each token
        for (index, token) in pool.tokens.iter().enumerate() {
            let balance = &reserves_bytes[index];
            // HashMap also prevents having duplicate balance changes for the same pool and token.
            tx_change.balance_changes.insert(
                ComponentKey::new(pool_address_hex.clone(), token.clone()),
                BalanceChange {
                    token: token.clone(),
                    balance: balance.clone().to_signed_bytes_be(),
                    component_id: pool_address_hex.as_bytes().to_vec(),
                },
            );
        }
    };

    let mut eh = EventHandler::new(block);
    // Filter the sync events by the pool address, to make sure we don't process events for other
    // Protocols that use the same event signature.
    eh.filter_by_address(PoolAddresser { store });
    eh.on::<Sync, _>(&mut on_sync);
    eh.handle_events();
}

/// Merge the changes from the sync events with the create_pool events previously mapped on
/// block_entity_changes.
///
/// Parameters:
/// - tx_changes: HashMap with the changes for each transaction. This is the same HashMap used in
///   handle_sync
/// - block_entity_changes: The BlockChanges struct that will be updated with the changes from the
///   sync events.
///
/// This HashMap comes pre-filled with the changes for the create_pool events, mapped in
///   1_map_pool_created.
///
/// This function is called after the handle_sync function, and it is expected that
/// block_entity_changes will be complete after this function ends.
fn merge_block(
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    block_entity_changes: &mut BlockChanges,
) {
    let mut tx_entity_changes_map = HashMap::new();

    // Add created pools to the tx_changes_map
    for change in block_entity_changes
        .changes
        .clone()
        .into_iter()
    {
        let transaction = change.tx.as_ref().unwrap();
        tx_entity_changes_map
            .entry(transaction.hash.clone())
            .and_modify(|c: &mut TransactionChanges| {
                c.component_changes
                    .extend(change.component_changes.clone());
                c.entity_changes
                    .extend(change.entity_changes.clone());
            })
            .or_insert(change);
    }

    // First, iterate through the previously created transactions, extracted from the
    // map_pool_created step. If there are sync events for this transaction, add them to the
    // block_entity_changes and the corresponding balance changes.
    for change in tx_entity_changes_map.values_mut() {
        let tx = change
            .clone()
            .tx
            .expect("Transaction not found")
            .clone();

        // If there are sync events for this transaction, add them to the block_entity_changes
        if let Some(partial_changes) = tx_changes.remove(&tx.hash) {
            change.entity_changes = partial_changes
                .clone()
                .consolidate_entity_changes();
            change.balance_changes = partial_changes
                .balance_changes
                .into_values()
                .collect();
        }
    }

    // If there are any transactions left in the tx_changes, it means that they are transactions
    // that changed the state of the pools, but were not included in the block_entity_changes.
    // This happens for every regular transaction that does not actually create a pool. By the
    // end of this function, we expect block_entity_changes to be up-to-date with the changes
    // for all sync and new_pools in the block.
    for partial_changes in tx_changes.values() {
        tx_entity_changes_map.insert(
            partial_changes.transaction.hash.clone(),
            TransactionChanges {
                tx: Some(partial_changes.transaction.clone()),
                contract_changes: vec![],
                entity_changes: partial_changes
                    .clone()
                    .consolidate_entity_changes(),
                balance_changes: partial_changes
                    .balance_changes
                    .clone()
                    .into_values()
                    .collect(),
                component_changes: vec![],
                ..Default::default()
            },
        );
    }

    block_entity_changes.changes = tx_entity_changes_map
        .into_values()
        .collect();
}

fn collect_v2_extra_changes(
    block: &eth::Block,
    pools_store: &StoreGetProto<ProtocolComponent>,
) -> Vec<TransactionChanges> {
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
            if !is_known_pool_cached(&storage_change.address, &mut known_pools, pools_store) {
                continue;
            }

            let Some(attribute) = v2_extra_attribute(storage_change) else {
                continue;
            };
            let component_id = storage_change.address.to_hex();

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

    transaction_changes
        .drain()
        .sorted_unstable_by_key(|(index, _)| *index)
        .filter_map(|(_, builder)| builder.build())
        .collect::<Vec<_>>()
}

fn is_known_pool_cached(
    address: &[u8],
    known_pools: &mut HashMap<Vec<u8>, bool>,
    pools_store: &StoreGetProto<ProtocolComponent>,
) -> bool {
    if let Some(is_known) = known_pools.get(address) {
        return *is_known;
    }

    let pool_address = format!("0x{}", hex::encode(address));
    let is_known = pools_store
        .get_last(StoreKey::Pool.get_unique_pool_key(pool_address.as_str()))
        .is_some();
    known_pools.insert(address.to_vec(), is_known);
    is_known
}

fn merge_extra_changes(block_changes: &mut BlockChanges, extra_changes: Vec<TransactionChanges>) {
    let mut transaction_changes: HashMap<u64, TransactionChanges> = HashMap::new();
    merge_block_changes(std::mem::take(&mut block_changes.changes), &mut transaction_changes);
    merge_block_changes(extra_changes, &mut transaction_changes);

    block_changes.changes = transaction_changes
        .drain()
        .sorted_unstable_by_key(|(index, _)| *index)
        .map(|(_, changes)| changes)
        .collect();
}

fn merge_block_changes(
    changes: Vec<TransactionChanges>,
    transaction_changes: &mut HashMap<u64, TransactionChanges>,
) {
    for change in changes {
        let Some(tx) = change.tx.clone() else {
            continue;
        };

        transaction_changes
            .entry(tx.index)
            .and_modify(|existing| merge_transaction_change(existing, change.clone()))
            .or_insert(change);
    }
}

fn merge_transaction_change(existing: &mut TransactionChanges, incoming: TransactionChanges) {
    existing
        .contract_changes
        .extend(incoming.contract_changes);

    for entity_change in incoming.entity_changes {
        merge_entity_change(&mut existing.entity_changes, entity_change);
    }

    for component in incoming.component_changes {
        upsert_component(&mut existing.component_changes, component);
    }

    for balance_change in incoming.balance_changes {
        upsert_balance_change(&mut existing.balance_changes, balance_change);
    }
}

fn merge_entity_change(existing: &mut Vec<EntityChanges>, incoming: EntityChanges) {
    let Some(entity_change) = existing
        .iter_mut()
        .find(|change| change.component_id == incoming.component_id)
    else {
        existing.push(incoming);
        return;
    };

    for attribute in incoming.attributes {
        match entity_change
            .attributes
            .iter_mut()
            .find(|existing_attribute| existing_attribute.name == attribute.name)
        {
            Some(existing_attribute) => *existing_attribute = attribute,
            None => entity_change.attributes.push(attribute),
        }
    }
}

fn upsert_component(existing: &mut Vec<ProtocolComponent>, incoming: ProtocolComponent) {
    if existing
        .iter()
        .any(|component| component.id == incoming.id)
    {
        return;
    }

    existing.push(incoming);
}

fn upsert_balance_change(existing: &mut Vec<BalanceChange>, incoming: BalanceChange) {
    match existing
        .iter_mut()
        .find(|balance_change| {
            balance_change.component_id == incoming.component_id
                && balance_change.token == incoming.token
        }) {
        Some(balance_change) => *balance_change = incoming,
        None => existing.push(incoming),
    }
}

struct PendingAttribute {
    tx: Transaction,
    component_id: String,
    attribute: Attribute,
    order: (u64, u64),
}
