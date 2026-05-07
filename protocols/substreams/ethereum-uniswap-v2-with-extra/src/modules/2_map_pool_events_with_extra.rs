use std::collections::HashMap;

use itertools::Itertools;
use tycho_substreams::prelude::{
    BalanceChange, BlockChanges, EntityChanges, EntryPoint, EntryPointParams, ProtocolComponent,
    TransactionChanges,
};

#[substreams::handlers::map]
pub fn map_pool_events_with_extra(
    parent_changes: BlockChanges,
    extra_changes: BlockChanges,
) -> Result<BlockChanges, substreams::errors::Error> {
    Ok(merge_pool_events_with_extra(parent_changes, extra_changes))
}

fn merge_pool_events_with_extra(
    parent_changes: BlockChanges,
    extra_changes: BlockChanges,
) -> BlockChanges {
    let block = parent_changes
        .block
        .clone()
        .or_else(|| extra_changes.block.clone());
    let mut storage_changes = parent_changes.storage_changes;
    storage_changes.extend(extra_changes.storage_changes);

    let mut transaction_changes: HashMap<u64, TransactionChanges> = HashMap::new();
    merge_block_changes(parent_changes.changes, &mut transaction_changes);
    merge_block_changes(extra_changes.changes, &mut transaction_changes);

    BlockChanges {
        block,
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .map(|(_, changes)| changes)
            .collect(),
        storage_changes,
    }
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

    for entrypoint in incoming.entrypoints {
        upsert_entrypoint(&mut existing.entrypoints, entrypoint);
    }

    for entrypoint_params in incoming.entrypoint_params {
        upsert_entrypoint_params(&mut existing.entrypoint_params, entrypoint_params);
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

fn upsert_entrypoint(existing: &mut Vec<EntryPoint>, incoming: EntryPoint) {
    if !existing.contains(&incoming) {
        existing.push(incoming);
    }
}

fn upsert_entrypoint_params(existing: &mut Vec<EntryPointParams>, incoming: EntryPointParams) {
    if !existing.contains(&incoming) {
        existing.push(incoming);
    }
}

#[cfg(test)]
mod tests {
    use substreams::scalar::BigInt;
    use tycho_substreams::prelude::{Attribute, ChangeType, EntityChanges, Transaction};

    use super::*;

    #[test]
    fn parent_and_extra_outputs_merge_into_one_transaction_pool_update() {
        let parent_changes = BlockChanges {
            changes: vec![transaction_changes(0, "0xpool0", "reserve0", 100)],
            ..Default::default()
        };
        let extra_changes = BlockChanges {
            changes: vec![transaction_changes(0, "0xpool0", "k_last", 20)],
            ..Default::default()
        };

        let merged = merge_pool_events_with_extra(parent_changes, extra_changes);

        assert_eq!(merged.changes.len(), 1);
        let entity_changes = &merged.changes[0].entity_changes;
        assert_eq!(entity_changes.len(), 1);
        assert_eq!(entity_changes[0].attributes.len(), 2);
        assert!(entity_changes[0]
            .attributes
            .iter()
            .any(|attr| attr.name == "reserve0"));
        assert!(entity_changes[0]
            .attributes
            .iter()
            .any(|attr| attr.name == "k_last"));
    }

    #[test]
    fn extra_attr_wins_when_parent_has_same_attr() {
        let parent_changes = BlockChanges {
            changes: vec![transaction_changes(0, "0xpool0", "k_last", 1)],
            ..Default::default()
        };
        let extra_changes = BlockChanges {
            changes: vec![transaction_changes(0, "0xpool0", "k_last", 2)],
            ..Default::default()
        };

        let merged = merge_pool_events_with_extra(parent_changes, extra_changes);
        let attribute = &merged.changes[0].entity_changes[0].attributes[0];

        assert_eq!(attribute.name, "k_last");
        assert_eq!(BigInt::from_signed_bytes_be(&attribute.value), BigInt::from(2));
    }

    #[test]
    fn sorts_final_transaction_changes_by_tx_index() {
        let parent_changes = BlockChanges {
            changes: vec![transaction_changes(2, "0xpool2", "reserve0", 100)],
            ..Default::default()
        };
        let extra_changes = BlockChanges {
            changes: vec![transaction_changes(1, "0xpool1", "k_last", 20)],
            ..Default::default()
        };

        let merged = merge_pool_events_with_extra(parent_changes, extra_changes);

        assert_eq!(
            merged.changes[0]
                .tx
                .as_ref()
                .unwrap()
                .index,
            1
        );
        assert_eq!(
            merged.changes[1]
                .tx
                .as_ref()
                .unwrap()
                .index,
            2
        );
    }

    fn transaction_changes(
        tx_index: u64,
        component_id: &str,
        attribute_name: &str,
        value: i64,
    ) -> TransactionChanges {
        TransactionChanges {
            tx: Some(Transaction { index: tx_index, ..Default::default() }),
            entity_changes: vec![EntityChanges {
                component_id: component_id.to_string(),
                attributes: vec![Attribute {
                    name: attribute_name.to_string(),
                    value: BigInt::from(value).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                }],
            }],
            ..Default::default()
        }
    }
}
