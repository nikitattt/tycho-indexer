use anyhow::{anyhow, Result};
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2 as eth;
use tiny_keccak::{Hasher, Keccak};
use tycho_substreams::prelude::{
    Attribute, BlockChanges, ChangeType, EntityChanges, ImplementationType, ProtocolComponent,
    Transaction, TransactionChanges,
};

const SWAP_SELECTOR: [u8; 4] = [0xf3, 0xcd, 0x91, 0x4c];
const COLLECT_PROTOCOL_FEES_SELECTOR: [u8; 4] = [0x81, 0x61, 0xb8, 0x74];
// Owned.owner occupies slot 0; ProtocolFees.protocolFeesAccrued is the next declaration.
const PROTOCOL_FEES_ACCRUED_SLOT: u8 = 1;
// Calldata offsets include the 4-byte selector. The swap tuple is ABI-encoded in place.
const SWAP_ZERO_FOR_ONE_LAST_BYTE: usize = 195;
const CURRENCY0_START: usize = 16;
const CURRENCY1_START: usize = 48;
const ADDRESS_LENGTH: usize = 20;

#[substreams::handlers::map]
pub fn map_protocol_fee_changes(params: String, block: eth::Block) -> Result<BlockChanges> {
    if block.detail_level != i32::from(eth::block::DetailLevel::DetaillevelExtended) {
        return Err(anyhow!("map_protocol_fee_changes requires the extended Ethereum block model"));
    }

    let pool_manager = parse_address(&params)?;
    let changes = block
        .transaction_traces
        .iter()
        .filter_map(|tx| transaction_changes(tx, &pool_manager))
        .collect();

    Ok(BlockChanges { block: Some((&block).into()), changes, ..Default::default() })
}

fn transaction_changes(
    tx: &eth::TransactionTrace,
    pool_manager: &[u8; ADDRESS_LENGTH],
) -> Option<TransactionChanges> {
    if tx.status != i32::from(eth::TransactionTraceStatus::Succeeded) {
        return None;
    }

    let mut pending = Vec::new();
    let mut creates_component = false;

    for call in &tx.calls {
        if call.state_reverted {
            continue;
        }

        if call.call_type() == eth::CallType::Create && call.address == pool_manager.as_slice() {
            creates_component = true;
        }

        if call.address != pool_manager.as_slice() || call.storage_changes.is_empty() {
            continue;
        }

        let Some(currency) = fee_currency(&call.input) else {
            continue;
        };
        let mapping_key = protocol_fees_accrued_key(&currency);

        for change in &call.storage_changes {
            if change.address != pool_manager.as_slice()
                || change.key != mapping_key
                || change.old_value == change.new_value
            {
                continue;
            }
            record_latest(&mut pending, currency, change);
        }
    }

    if !creates_component && pending.is_empty() {
        return None;
    }

    let tycho_tx = Transaction::from(tx);
    let mut changes = TransactionChanges::new(&tycho_tx);
    let component_id = format!("0x{}", hex::encode(pool_manager));

    if creates_component {
        changes.component_changes.push(
            ProtocolComponent::at_contract(pool_manager)
                .as_swap_type("uniswap_v4_protocol_fees", ImplementationType::Custom),
        );
    }

    if !pending.is_empty() {
        pending.sort_unstable_by_key(|fee| fee.ordinal);
        changes
            .entity_changes
            .push(EntityChanges {
                component_id,
                attributes: pending
                    .into_iter()
                    .map(|fee| Attribute {
                        name: protocol_fee_attribute_name(&fee.currency),
                        // Tycho attributes use signed big-endian integers. Converting from the
                        // unsigned storage word preserves values above 2^255 - 1 as positive.
                        value: BigInt::from_unsigned_bytes_be(&fee.new_value).to_signed_bytes_be(),
                        change: ChangeType::Update.into(),
                    })
                    .collect(),
            });
    }

    Some(changes)
}

fn fee_currency(input: &[u8]) -> Option<[u8; ADDRESS_LENGTH]> {
    let selector = input.get(..4)?;
    if selector == COLLECT_PROTOCOL_FEES_SELECTOR {
        return read_address(input, CURRENCY1_START);
    }
    if selector != SWAP_SELECTOR || input.len() <= SWAP_ZERO_FOR_ONE_LAST_BYTE {
        return None;
    }

    match input[SWAP_ZERO_FOR_ONE_LAST_BYTE] {
        0 => read_address(input, CURRENCY1_START),
        1 => read_address(input, CURRENCY0_START),
        _ => None,
    }
}

fn read_address(input: &[u8], start: usize) -> Option<[u8; ADDRESS_LENGTH]> {
    let mut address = [0u8; ADDRESS_LENGTH];
    address.copy_from_slice(input.get(start..start + ADDRESS_LENGTH)?);
    Some(address)
}

fn protocol_fees_accrued_key(currency: &[u8; ADDRESS_LENGTH]) -> [u8; 32] {
    let mut encoded = [0u8; 64];
    encoded[12..32].copy_from_slice(currency);
    encoded[63] = PROTOCOL_FEES_ACCRUED_SLOT;

    let mut key = [0u8; 32];
    let mut hasher = Keccak::v256();
    hasher.update(&encoded);
    hasher.finalize(&mut key);
    key
}

fn protocol_fee_attribute_name(currency: &[u8; ADDRESS_LENGTH]) -> String {
    format!("protocol_fees_accrued/0x{}", hex::encode(currency))
}

fn record_latest(
    pending: &mut Vec<PendingFee>,
    currency: [u8; ADDRESS_LENGTH],
    change: &eth::StorageChange,
) {
    if let Some(current) = pending
        .iter_mut()
        .find(|current| current.currency == currency)
    {
        if change.ordinal > current.ordinal {
            current.ordinal = change.ordinal;
            current
                .new_value
                .clone_from(&change.new_value);
        }
        return;
    }

    pending.push(PendingFee {
        currency,
        ordinal: change.ordinal,
        new_value: change.new_value.clone(),
    });
}

struct PendingFee {
    currency: [u8; ADDRESS_LENGTH],
    ordinal: u64,
    new_value: Vec<u8>,
}

fn parse_address(raw: &str) -> Result<[u8; ADDRESS_LENGTH]> {
    let encoded = raw.trim().trim_start_matches("0x");
    if encoded.len() != ADDRESS_LENGTH * 2 {
        return Err(anyhow!(
            "PoolManager address must contain {} hex characters, got {}",
            ADDRESS_LENGTH * 2,
            encoded.len()
        ));
    }

    let mut address = [0u8; ADDRESS_LENGTH];
    hex::decode_to_slice(encoded, &mut address)
        .map_err(|error| anyhow!("invalid PoolManager address: {error}"))?;
    Ok(address)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANAGER: [u8; ADDRESS_LENGTH] = [0x44; ADDRESS_LENGTH];
    const TOKEN0: [u8; ADDRESS_LENGTH] = [0x11; ADDRESS_LENGTH];
    const TOKEN1: [u8; ADDRESS_LENGTH] = [0x22; ADDRESS_LENGTH];

    #[test]
    fn computes_protocol_fees_accrued_mapping_key_at_slot_one() {
        assert_eq!(
            hex::encode(protocol_fees_accrued_key(&TOKEN0)),
            "8eec1c9afb183a84aac7003cf8e730bfb6385f6e43761d6425fba4265de3a9eb"
        );
        assert_eq!(
            hex::encode(protocol_fees_accrued_key(&[0; ADDRESS_LENGTH])),
            "a6eef7e35abe7026729641147f7915573c7e97b47efa546f5f6e3230263bcb49"
        );
    }

    #[test]
    fn reads_swap_input_currency_without_abi_decoding() {
        assert_eq!(fee_currency(&swap_input(false)), Some(TOKEN1));
        assert_eq!(fee_currency(&swap_input(true)), Some(TOKEN0));
    }

    #[test]
    fn reads_collect_protocol_fees_currency_without_abi_decoding() {
        assert_eq!(fee_currency(&collect_input(TOKEN1)), Some(TOKEN1));
    }

    #[test]
    fn emits_absolute_swap_value_with_full_uint256_precision() {
        let value = vec![0xff; 32];
        let tx = succeeded_tx(vec![fee_call(swap_input(true), TOKEN0, 10, vec![0], value.clone())]);

        let changes = transaction_changes(&tx, &MANAGER).expect("fee change");
        let attribute = &changes.entity_changes[0].attributes[0];

        assert_eq!(
            attribute.name,
            "protocol_fees_accrued/0x1111111111111111111111111111111111111111"
        );
        assert_eq!(
            BigInt::from_signed_bytes_be(&attribute.value),
            BigInt::from_unsigned_bytes_be(&value)
        );
    }

    #[test]
    fn emits_lower_value_after_collection_including_zero() {
        let tx =
            succeeded_tx(vec![fee_call(collect_input(TOKEN1), TOKEN1, 10, vec![10], Vec::new())]);

        let changes = transaction_changes(&tx, &MANAGER).expect("fee collection");
        let attribute = &changes.entity_changes[0].attributes[0];

        assert_eq!(
            attribute.name,
            "protocol_fees_accrued/0x2222222222222222222222222222222222222222"
        );
        assert_eq!(BigInt::from_signed_bytes_be(&attribute.value), BigInt::from(0));
    }

    #[test]
    fn keeps_last_absolute_value_for_currency_in_transaction() {
        let tx = succeeded_tx(vec![
            fee_call(swap_input(true), TOKEN0, 10, vec![0], vec![10]),
            fee_call(swap_input(true), TOKEN0, 20, vec![10], vec![25]),
        ]);

        let changes = transaction_changes(&tx, &MANAGER).expect("fee changes");
        let attributes = &changes.entity_changes[0].attributes;

        assert_eq!(attributes.len(), 1);
        assert_eq!(BigInt::from_signed_bytes_be(&attributes[0].value), BigInt::from(25));
    }

    #[test]
    fn skips_reverted_calls_and_failed_transactions() {
        let mut call = fee_call(swap_input(true), TOKEN0, 10, vec![0], vec![10]);
        call.state_reverted = true;
        assert!(transaction_changes(&succeeded_tx(vec![call]), &MANAGER).is_none());

        let mut tx = succeeded_tx(vec![fee_call(swap_input(true), TOKEN0, 10, vec![0], vec![10])]);
        tx.status = i32::from(eth::TransactionTraceStatus::Failed);
        assert!(transaction_changes(&tx, &MANAGER).is_none());
    }

    #[test]
    fn creates_singleton_component_on_pool_manager_deployment() {
        let call = eth::Call {
            address: MANAGER.to_vec(),
            call_type: eth::CallType::Create.into(),
            ..Default::default()
        };

        let changes =
            transaction_changes(&succeeded_tx(vec![call]), &MANAGER).expect("component creation");
        let component = &changes.component_changes[0];

        assert_eq!(component.id, format!("0x{}", hex::encode(MANAGER)));
        assert_eq!(component.contracts, vec![MANAGER.to_vec()]);
        assert!(component.tokens.is_empty());
        assert_eq!(
            component
                .protocol_type
                .as_ref()
                .expect("protocol type")
                .name,
            "uniswap_v4_protocol_fees"
        );
    }

    fn swap_input(zero_for_one: bool) -> Vec<u8> {
        let mut input = vec![0; SWAP_ZERO_FOR_ONE_LAST_BYTE + 1];
        input[..4].copy_from_slice(&SWAP_SELECTOR);
        input[CURRENCY0_START..CURRENCY0_START + ADDRESS_LENGTH].copy_from_slice(&TOKEN0);
        input[CURRENCY1_START..CURRENCY1_START + ADDRESS_LENGTH].copy_from_slice(&TOKEN1);
        input[SWAP_ZERO_FOR_ONE_LAST_BYTE] = u8::from(zero_for_one);
        input
    }

    fn collect_input(currency: [u8; ADDRESS_LENGTH]) -> Vec<u8> {
        let mut input = vec![0; CURRENCY1_START + ADDRESS_LENGTH];
        input[..4].copy_from_slice(&COLLECT_PROTOCOL_FEES_SELECTOR);
        input[CURRENCY1_START..CURRENCY1_START + ADDRESS_LENGTH].copy_from_slice(&currency);
        input
    }

    fn fee_call(
        input: Vec<u8>,
        currency: [u8; ADDRESS_LENGTH],
        ordinal: u64,
        old_value: Vec<u8>,
        new_value: Vec<u8>,
    ) -> eth::Call {
        eth::Call {
            address: MANAGER.to_vec(),
            input,
            storage_changes: vec![eth::StorageChange {
                address: MANAGER.to_vec(),
                key: protocol_fees_accrued_key(&currency).to_vec(),
                old_value,
                new_value,
                ordinal,
            }],
            ..Default::default()
        }
    }

    fn succeeded_tx(calls: Vec<eth::Call>) -> eth::TransactionTrace {
        eth::TransactionTrace {
            status: eth::TransactionTraceStatus::Succeeded.into(),
            calls,
            ..Default::default()
        }
    }
}
