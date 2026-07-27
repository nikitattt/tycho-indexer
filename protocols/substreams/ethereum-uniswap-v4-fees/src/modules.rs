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
const BOOTSTRAP_TX_DOMAIN: &[u8] = b"tycho/uniswap-v4-fees/component/v1";

#[substreams::handlers::map]
pub fn map_protocol_fee_changes(params: String, block: eth::Block) -> Result<BlockChanges> {
    map_protocol_fee_changes_impl(&params, block)
}

fn map_protocol_fee_changes_impl(params: &str, block: eth::Block) -> Result<BlockChanges> {
    if block.detail_level != i32::from(eth::block::DetailLevel::DetaillevelExtended) {
        return Err(anyhow!("map_protocol_fee_changes requires the extended Ethereum block model"));
    }

    let config = parse_params(params)?;
    let mut changes = Vec::new();

    if block.number == config.component_creation_block {
        let tx = bootstrap_transaction(&block, &config.pool_manager);
        let mut bootstrap_changes = TransactionChanges::new(&tx);
        let component_id = format!("0x{}", hex::encode(config.pool_manager));
        bootstrap_changes
            .component_changes
            .push(
                ProtocolComponent::new(&component_id)
                    .as_swap_type("uniswap_v4_protocol_fees", ImplementationType::Custom),
            );
        changes.push(bootstrap_changes);
    }

    changes.extend(
        block
            .transaction_traces
            .iter()
            .filter_map(|tx| transaction_changes(tx, &config.pool_manager)),
    );

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

    for call in &tx.calls {
        if call.state_reverted {
            continue;
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

    if pending.is_empty() {
        return None;
    }

    let tycho_tx = Transaction::from(tx);
    let mut changes = TransactionChanges::new(&tycho_tx);
    let component_id = format!("0x{}", hex::encode(pool_manager));

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

struct Config {
    pool_manager: [u8; ADDRESS_LENGTH],
    component_creation_block: u64,
}

fn parse_params(raw: &str) -> Result<Config> {
    let mut pool_manager = None;
    let mut component_creation_block = None;

    for param in raw.trim().split('&') {
        let (key, value) = param
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid parameter `{param}`; expected key=value"))?;

        match key {
            "pool_manager" => pool_manager = Some(parse_address(value)?),
            "component_creation_block" => {
                component_creation_block = Some(
                    value
                        .parse::<u64>()
                        .map_err(|error| anyhow!("invalid component_creation_block: {error}"))?,
                )
            }
            _ => return Err(anyhow!("unknown parameter `{key}`")),
        }
    }

    Ok(Config {
        pool_manager: pool_manager.ok_or_else(|| anyhow!("missing pool_manager parameter"))?,
        component_creation_block: component_creation_block
            .ok_or_else(|| anyhow!("missing component_creation_block parameter"))?,
    })
}

fn bootstrap_transaction(block: &eth::Block, pool_manager: &[u8; ADDRESS_LENGTH]) -> Transaction {
    let mut hash = [0u8; 32];
    let mut hasher = Keccak::v256();
    hasher.update(BOOTSTRAP_TX_DOMAIN);
    hasher.update(&block.hash);
    hasher.update(pool_manager);
    hasher.finalize(&mut hash);

    let index = block
        .transaction_traces
        .iter()
        .map(|tx| u64::from(tx.index))
        .max()
        .map_or(0, |index| index + 1);

    Transaction {
        hash: hash.to_vec(),
        from: vec![0; ADDRESS_LENGTH],
        to: pool_manager.to_vec(),
        index,
    }
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
    fn creates_unlinked_singleton_component_at_configured_block() {
        let block_number = 123;
        let block = eth::Block {
            number: block_number,
            hash: vec![0xaa; 32],
            header: Some(eth::BlockHeader {
                timestamp: Some(Default::default()),
                ..Default::default()
            }),
            detail_level: eth::block::DetailLevel::DetaillevelExtended.into(),
            ..Default::default()
        };
        let params = format!(
            "pool_manager=0x{}&component_creation_block={block_number}",
            hex::encode(MANAGER)
        );

        let changes = map_protocol_fee_changes_impl(&params, block).expect("component creation");
        let component = &changes.changes[0].component_changes[0];

        assert_eq!(component.id, format!("0x{}", hex::encode(MANAGER)));
        assert!(component.contracts.is_empty());
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

    #[test]
    fn does_not_create_component_outside_configured_block() {
        let block = eth::Block {
            number: 124,
            hash: vec![0xaa; 32],
            header: Some(eth::BlockHeader {
                timestamp: Some(Default::default()),
                ..Default::default()
            }),
            detail_level: eth::block::DetailLevel::DetaillevelExtended.into(),
            ..Default::default()
        };
        let params =
            format!("pool_manager=0x{}&component_creation_block=123", hex::encode(MANAGER));

        let changes = map_protocol_fee_changes_impl(&params, block).expect("block changes");

        assert!(changes.changes.is_empty());
    }

    #[test]
    fn parses_named_params_in_any_order() {
        let config = parse_params(&format!(
            "component_creation_block=123&pool_manager=0x{}",
            hex::encode(MANAGER)
        ))
        .expect("params");

        assert_eq!(config.pool_manager, MANAGER);
        assert_eq!(config.component_creation_block, 123);
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
