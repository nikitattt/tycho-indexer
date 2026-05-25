use serde::Deserialize;
use substreams::prelude::BigInt;
use substreams_ethereum::pb::eth::v2::{self as eth};
use substreams_helper::hex::Hexable;
use tiny_keccak::{Hasher, Keccak};
use tycho_substreams::prelude::*;

use hex_literal::hex;

const BLOOM_BYTES: usize = 256;
const BLOOM_BITS: usize = 2048;
const PAIR_CREATED_TOPIC: [u8; 32] =
    hex!("0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9");

#[derive(Debug, Deserialize)]
struct Params {
    factory_address: String,
    protocol_type_name: String,
}

#[derive(Debug, PartialEq, Eq)]
struct PairCreatedData {
    token0: Vec<u8>,
    token1: Vec<u8>,
    pair: Vec<u8>,
}

#[substreams::handlers::map]
pub fn map_pools_created(
    params: String,
    block: eth::Block,
) -> Result<BlockChanges, substreams::errors::Error> {
    let params: Params = serde_qs::from_str(params.as_str()).expect("Unable to deserialize params");
    let factory_address = decode_factory_address(&params.factory_address);

    Ok(BlockChanges {
        block: Some((&block).into()),
        changes: get_pools(&block, &factory_address, &params.protocol_type_name),
        ..Default::default()
    })
}

fn decode_factory_address(factory_address: &str) -> [u8; 20] {
    let address =
        hex::decode(factory_address.trim_start_matches("0x")).expect("invalid factory address");
    assert_eq!(address.len(), 20, "factory address must be 20 bytes");

    let mut decoded = [0u8; 20];
    decoded.copy_from_slice(&address);
    decoded
}

fn get_pools(
    block: &eth::Block,
    factory_address: &[u8; 20],
    protocol_type_name: &str,
) -> Vec<TransactionChanges> {
    if !block_bloom_may_contain_address(block, factory_address) {
        return Vec::new();
    }

    let mut new_pools = Vec::new();

    for tx in block
        .transaction_traces
        .iter()
        .filter(|tx| tx.status == i32::from(eth::TransactionTraceStatus::Succeeded))
    {
        let Some(receipt) = tx.receipt.as_ref() else {
            continue;
        };

        for log in &receipt.logs {
            if log.address.as_slice() != factory_address || !is_pair_created_log(log) {
                continue;
            }

            if let Some(event) = decode_pair_created(log) {
                new_pools.push(pool_created_changes(event, tx, protocol_type_name));
            }
        }
    }

    new_pools
}

fn block_bloom_may_contain_address(block: &eth::Block, address: &[u8; 20]) -> bool {
    let Some(header) = block.header.as_ref() else {
        return true;
    };

    if header.logs_bloom.len() != BLOOM_BYTES {
        return true;
    }

    bloom_contains(&header.logs_bloom, address)
}

fn bloom_contains(bloom: &[u8], value: &[u8]) -> bool {
    bloom_bit_indexes(value)
        .iter()
        .all(|bit| bloom[BLOOM_BYTES - 1 - (bit / 8)] & (1 << (bit % 8)) != 0)
}

fn bloom_bit_indexes(value: &[u8]) -> [usize; 3] {
    let mut hash = [0u8; 32];
    let mut hasher = Keccak::v256();
    hasher.update(value);
    hasher.finalize(&mut hash);

    [
        (((hash[0] as usize) << 8) | hash[1] as usize) & (BLOOM_BITS - 1),
        (((hash[2] as usize) << 8) | hash[3] as usize) & (BLOOM_BITS - 1),
        (((hash[4] as usize) << 8) | hash[5] as usize) & (BLOOM_BITS - 1),
    ]
}

fn is_pair_created_log(log: &eth::Log) -> bool {
    log.topics.len() == 3
        && log.data.len() == 64
        && log.topics[0].as_slice() == PAIR_CREATED_TOPIC.as_slice()
}

fn decode_pair_created(log: &eth::Log) -> Option<PairCreatedData> {
    Some(PairCreatedData {
        token0: address_from_topic(&log.topics[1])?,
        token1: address_from_topic(&log.topics[2])?,
        pair: address_from_topic(log.data.get(0..32)?)?,
    })
}

fn address_from_topic(topic: &[u8]) -> Option<Vec<u8>> {
    if topic.len() != 32 {
        return None;
    }

    Some(topic[12..32].to_vec())
}

fn pool_created_changes(
    event: PairCreatedData,
    tx: &eth::TransactionTrace,
    protocol_type_name: &str,
) -> TransactionChanges {
    let tycho_tx: Transaction = tx.into();
    let pair_hex = event.pair.to_hex();
    let pair_component_id = pair_hex.as_bytes().to_vec();
    let zero = BigInt::from(0).to_signed_bytes_be();

    TransactionChanges {
        tx: Some(tycho_tx.clone()),
        contract_changes: vec![],
        entity_changes: vec![EntityChanges {
            component_id: pair_hex.clone(),
            attributes: vec![
                Attribute {
                    name: "reserve0".to_string(),
                    value: zero.clone(),
                    change: ChangeType::Creation.into(),
                },
                Attribute {
                    name: "reserve1".to_string(),
                    value: zero.clone(),
                    change: ChangeType::Creation.into(),
                },
            ],
        }],
        component_changes: vec![ProtocolComponent {
            id: pair_hex,
            tokens: vec![event.token0.clone(), event.token1.clone()],
            contracts: vec![],
            static_att: vec![
                // Trading fee is hardcoded to 0.3%, saved as int in bps.
                Attribute {
                    name: "fee".to_string(),
                    value: BigInt::from(30).to_signed_bytes_be(),
                    change: ChangeType::Creation.into(),
                },
                Attribute {
                    name: "pool_address".to_string(),
                    value: event.pair.clone(),
                    change: ChangeType::Creation.into(),
                },
            ],
            change: i32::from(ChangeType::Creation),
            protocol_type: Some(ProtocolType {
                name: protocol_type_name.to_string(),
                financial_type: FinancialType::Swap.into(),
                attribute_schema: vec![],
                implementation_type: ImplementationType::Custom.into(),
            }),
            tx: Some(tycho_tx),
        }],
        balance_changes: vec![
            BalanceChange {
                token: event.token0,
                balance: zero.clone(),
                component_id: pair_component_id.clone(),
            },
            BalanceChange { token: event.token1, balance: zero, component_id: pair_component_id },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_factory_pair_created_logs() {
        let factory = address_array(1);
        let token0 = address(2);
        let token1 = address(3);
        let pair = address(4);

        let changes = get_pools(
            &block(vec![pair_created_log(
                factory.to_vec(),
                token0.clone(),
                token1.clone(),
                pair.clone(),
            )]),
            &factory,
            "uniswap_v2_pool",
        );

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].component_changes[0].id, pair.to_hex());
        assert_eq!(changes[0].component_changes[0].tokens, vec![token0, token1]);
        assert_eq!(changes[0].balance_changes.len(), 2);
        assert_eq!(static_attr(&changes[0].component_changes[0], "fee"), BigInt::from(30));
    }

    #[test]
    fn skips_pool_scan_when_factory_is_absent_from_block_bloom() {
        let factory = address_array(1);
        let block = block_with_bloom(
            vec![pair_created_log(factory.to_vec(), address(2), address(3), address(4))],
            vec![0; BLOOM_BYTES],
        );

        let changes = get_pools(&block, &factory, "uniswap_v2_pool");

        assert!(changes.is_empty());
    }

    #[test]
    fn scans_pair_created_logs_when_factory_is_present_in_block_bloom() {
        let factory = address_array(1);
        let token0 = address(2);
        let token1 = address(3);
        let pair = address(4);
        let block = block_with_bloom(
            vec![pair_created_log(factory.to_vec(), token0.clone(), token1.clone(), pair.clone())],
            bloom_for(&factory),
        );

        let changes = get_pools(&block, &factory, "uniswap_v2_pool");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].component_changes[0].id, pair.to_hex());
        assert_eq!(changes[0].component_changes[0].tokens, vec![token0, token1]);
    }

    #[test]
    fn manually_decodes_pair_created_log() {
        let token0 = address(2);
        let token1 = address(3);
        let pair = address(4);
        let log = pair_created_log(address(1), token0.clone(), token1.clone(), pair.clone());

        let event = decode_pair_created(&log).unwrap();

        assert_eq!(event, PairCreatedData { token0, token1, pair });
    }

    #[test]
    fn ignores_pair_created_shaped_logs_from_non_factory_addresses() {
        let factory = address_array(1);
        let other = address(9);

        let changes = get_pools(
            &block(vec![pair_created_log(other, address(2), address(3), address(4))]),
            &factory,
            "uniswap_v2_pool",
        );

        assert!(changes.is_empty());
    }

    fn static_attr(component: &ProtocolComponent, name: &str) -> BigInt {
        let attr = component
            .static_att
            .iter()
            .find(|attr| attr.name == name)
            .unwrap_or_else(|| panic!("missing static attr {name}"));
        BigInt::from_signed_bytes_be(&attr.value)
    }

    fn block(logs: Vec<eth::Log>) -> eth::Block {
        eth::Block {
            transaction_traces: vec![eth::TransactionTrace {
                status: i32::from(eth::TransactionTraceStatus::Succeeded),
                index: 7,
                hash: vec![7; 32],
                from: address(7),
                to: address(8),
                receipt: Some(eth::TransactionReceipt { logs, ..Default::default() }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn block_with_bloom(logs: Vec<eth::Log>, logs_bloom: Vec<u8>) -> eth::Block {
        eth::Block {
            header: Some(eth::BlockHeader { logs_bloom, ..Default::default() }),
            ..block(logs)
        }
    }

    fn pair_created_log(
        factory: Vec<u8>,
        token0: Vec<u8>,
        token1: Vec<u8>,
        pair: Vec<u8>,
    ) -> eth::Log {
        let mut data = address_word(&pair);
        data.extend(word(1));

        eth::Log {
            address: factory,
            topics: vec![PAIR_CREATED_TOPIC.to_vec(), address_word(&token0), address_word(&token1)],
            data,
            ordinal: 10,
            ..Default::default()
        }
    }

    fn address(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }

    fn address_array(seed: u8) -> [u8; 20] {
        [seed; 20]
    }

    fn address_word(address: &[u8]) -> Vec<u8> {
        let mut topic = vec![0; 12];
        topic.extend_from_slice(address);
        topic
    }

    fn word(value: u64) -> Vec<u8> {
        let mut word = vec![0; 32];
        word[24..32].copy_from_slice(&value.to_be_bytes());
        word
    }

    fn bloom_for(value: &[u8]) -> Vec<u8> {
        let mut bloom = vec![0; BLOOM_BYTES];
        for bit in bloom_bit_indexes(value) {
            bloom[BLOOM_BYTES - 1 - (bit / 8)] |= 1 << (bit % 8);
        }
        bloom
    }
}
