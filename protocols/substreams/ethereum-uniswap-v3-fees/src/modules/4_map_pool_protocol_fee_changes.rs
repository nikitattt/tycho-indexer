use crate::pb::uniswap::v3::{BlockPoolData, ProtocolFeeChange, ProtocolFeeToken};
use itertools::Itertools;
use rustc_hash::FxHashMap;
use substreams_helper::hex::Hexable;
use tycho_substreams::prelude::*;

#[substreams::handlers::map]
pub fn map_pool_protocol_fee_changes(
    pool_data: BlockPoolData,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    Ok(collect_pool_protocol_fee_changes(pool_data.protocol_fee_changes))
}

fn collect_pool_protocol_fee_changes(
    protocol_fee_changes: Vec<ProtocolFeeChange>,
) -> BlockEntityChanges {
    let mut transaction_changes: FxHashMap<u64, TransactionChangesBuilder> = FxHashMap::default();

    for change in protocol_fee_changes {
        let Some(tx) = change
            .transaction
            .as_ref()
            .map(Into::into)
        else {
            continue;
        };

        add_pool_attribute(
            &mut transaction_changes,
            &tx,
            &change.pool_address,
            Attribute {
                name: protocol_fee_attribute_name(change.token()).to_string(),
                value: change.value,
                change: ChangeType::Update.into(),
            },
        );
    }

    BlockEntityChanges {
        block: None,
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .map(|changes| TransactionEntityChanges {
                tx: changes.tx,
                entity_changes: changes.entity_changes,
                component_changes: changes.component_changes,
                balance_changes: changes.balance_changes,
            })
            .collect(),
    }
}

fn protocol_fee_attribute_name(token: ProtocolFeeToken) -> &'static str {
    match token {
        ProtocolFeeToken::Token0 => "protocol_fees/token0",
        ProtocolFeeToken::Token1 => "protocol_fees/token1",
    }
}

fn add_pool_attribute(
    transaction_changes: &mut FxHashMap<u64, TransactionChangesBuilder>,
    tx: &Transaction,
    pool_address: &[u8],
    attribute: Attribute,
) {
    let builder = transaction_changes
        .entry(tx.index)
        .or_insert_with(|| TransactionChangesBuilder::new(tx));

    builder.add_entity_change(&EntityChanges {
        component_id: pool_address.to_hex(),
        attributes: vec![attribute],
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::uniswap::v3::Transaction;
    use substreams::scalar::BigInt;

    #[test]
    fn emits_protocol_fee_storage_attributes_from_extracted_changes() {
        let pool = pool_bytes(1);
        let changes = collect_pool_protocol_fee_changes(vec![
            protocol_fee_change(pool.clone(), ProtocolFeeToken::Token0, 10, 7, 3),
            protocol_fee_change(pool.clone(), ProtocolFeeToken::Token1, 11, 7, 4),
        ]);
        let attrs = attributes_for_pool(&changes, &pool);

        assert_eq!(attr_value(&attrs, "protocol_fees/token0"), BigInt::from(3));
        assert_eq!(attr_value(&attrs, "protocol_fees/token1"), BigInt::from(4));
    }

    #[test]
    fn skips_extracted_protocol_fee_changes_without_transaction() {
        let pool = pool_bytes(1);
        let changes = collect_pool_protocol_fee_changes(vec![ProtocolFeeChange {
            pool_address: pool,
            token: ProtocolFeeToken::Token0.into(),
            value: amount(3),
            ordinal: 10,
            transaction: None,
        }]);

        assert!(changes.changes.is_empty());
    }

    fn attributes_for_pool(
        changes: &tycho_substreams::prelude::BlockEntityChanges,
        pool: &[u8],
    ) -> Vec<tycho_substreams::prelude::Attribute> {
        let component_id = pool.to_hex();
        changes
            .changes
            .iter()
            .flat_map(|tx| tx.entity_changes.iter())
            .filter(|entity| entity.component_id == component_id)
            .flat_map(|entity| entity.attributes.iter().cloned())
            .collect()
    }

    fn attr_value(attrs: &[tycho_substreams::prelude::Attribute], name: &str) -> BigInt {
        let attr = attrs
            .iter()
            .find(|attr| attr.name == name)
            .unwrap_or_else(|| panic!("missing attribute {name}"));
        BigInt::from_signed_bytes_be(&attr.value)
    }

    fn protocol_fee_change(
        pool_address: Vec<u8>,
        token: ProtocolFeeToken,
        ordinal: u64,
        tx_index: u64,
        value: i64,
    ) -> ProtocolFeeChange {
        ProtocolFeeChange {
            pool_address,
            token: token.into(),
            value: amount(value),
            ordinal,
            transaction: Some(tx(tx_index)),
        }
    }

    fn tx(index: u64) -> Transaction {
        Transaction { index, hash: vec![index as u8; 32], ..Default::default() }
    }

    fn amount(value: i64) -> Vec<u8> {
        BigInt::from(value).to_signed_bytes_be()
    }

    fn pool_bytes(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }
}
