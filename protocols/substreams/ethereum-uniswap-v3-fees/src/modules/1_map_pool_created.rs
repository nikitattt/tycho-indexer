use hex_literal::hex;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2::{self as eth};
use substreams_helper::hex::Hexable;
use tiny_keccak::{Hasher, Keccak};

use crate::abi::factory::events::PoolCreated;

use tycho_substreams::prelude::*;

const BLOOM_BYTES: usize = 256;
const BLOOM_BITS: usize = 2048;
const POOL_CREATED_TOPIC: [u8; 32] =
    hex!("783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118");

#[substreams::handlers::map]
pub fn map_pools_created(
    params: String,
    block: eth::Block,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    let factory_address = decode_factory_address(&params)?;

    Ok(BlockEntityChanges { block: None, changes: get_new_pools(&block, &factory_address) })
}

fn decode_factory_address(factory_address: &str) -> Result<[u8; 20], substreams::errors::Error> {
    let address = hex::decode(factory_address.trim_start_matches("0x"))
        .map_err(|err| anyhow::anyhow!("invalid factory address `{factory_address}`: {err}"))?;

    if address.len() != 20 {
        return Err(anyhow::anyhow!(
            "invalid factory address `{factory_address}`: expected 20 bytes, got {}",
            address.len()
        ));
    }

    let mut decoded = [0u8; 20];
    decoded.copy_from_slice(&address);
    Ok(decoded)
}

fn get_new_pools(block: &eth::Block, factory_address: &[u8; 20]) -> Vec<TransactionEntityChanges> {
    if !block_bloom_may_contain_address(block, factory_address) {
        return Vec::new();
    }

    let mut new_pools = Vec::new();

    for tx in block
        .transaction_traces
        .iter()
        .filter(|tx| tx.status == 1)
    {
        let Some(receipt) = &tx.receipt else {
            continue;
        };

        for log in &receipt.logs {
            if log.address.as_slice() != factory_address || !is_pool_created_log(log) {
                continue;
            }

            if let Some(event) = decode_pool_created(log) {
                new_pools.push(pool_created_changes(event, tx));
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
    let bit_indexes = bloom_bit_indexes(value);

    bit_indexes
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

fn is_pool_created_log(log: &eth::Log) -> bool {
    log.topics.len() == 4
        && log.data.len() == 64
        && log.topics[0].as_slice() == POOL_CREATED_TOPIC.as_slice()
}

fn decode_pool_created(log: &eth::Log) -> Option<PoolCreated> {
    let fee = log.topics[3].as_slice();
    if fee.len() != 32 {
        return None;
    }

    Some(PoolCreated {
        token0: address_from_topic(&log.topics[1])?,
        token1: address_from_topic(&log.topics[2])?,
        fee: BigInt::from_unsigned_bytes_be(fee),
        tick_spacing: BigInt::from_signed_bytes_be(log.data.get(0..32)?),
        pool: address_from_topic(log.data.get(32..64)?)?,
    })
}

fn address_from_topic(topic: &[u8]) -> Option<Vec<u8>> {
    if topic.len() != 32 {
        return None;
    }

    Some(topic[12..32].to_vec())
}

fn pool_created_changes(
    event: PoolCreated,
    tx: &eth::TransactionTrace,
) -> TransactionEntityChanges {
    let tycho_tx: Transaction = tx.into();
    let pool_id = event.pool.to_hex();

    TransactionEntityChanges {
        tx: Some(tycho_tx),
        entity_changes: vec![EntityChanges {
            component_id: pool_id.clone(),
            attributes: vec![
                Attribute {
                    name: "liquidity".to_string(),
                    value: BigInt::from(0).to_signed_bytes_be(),
                    change: ChangeType::Creation.into(),
                },
                Attribute {
                    name: "tick".to_string(),
                    value: BigInt::from(0).to_signed_bytes_be(),
                    change: ChangeType::Creation.into(),
                },
                Attribute {
                    name: "sqrt_price_x96".to_string(),
                    value: BigInt::from(0).to_signed_bytes_be(),
                    change: ChangeType::Creation.into(),
                },
            ],
        }],
        component_changes: vec![ProtocolComponent {
            id: pool_id.clone(),
            tokens: vec![event.token0.clone(), event.token1.clone()],
            contracts: vec![],
            static_att: vec![
                Attribute {
                    name: "fee".to_string(),
                    value: event.fee.to_signed_bytes_be(),
                    change: ChangeType::Creation.into(),
                },
                Attribute {
                    name: "tick_spacing".to_string(),
                    value: event.tick_spacing.to_signed_bytes_be(),
                    change: ChangeType::Creation.into(),
                },
                Attribute {
                    name: "pool_address".to_string(),
                    value: event.pool.clone(),
                    change: ChangeType::Creation.into(),
                },
            ],
            change: i32::from(ChangeType::Creation),
            protocol_type: Option::from(ProtocolType {
                name: "uniswap_v3_pool".to_string(),
                financial_type: FinancialType::Swap.into(),
                attribute_schema: vec![],
                implementation_type: ImplementationType::Custom.into(),
            }),
        }],
        balance_changes: vec![
            BalanceChange {
                token: event.token0,
                balance: BigInt::from(0).to_signed_bytes_be(),
                component_id: pool_id.as_bytes().to_vec(),
            },
            BalanceChange {
                token: event.token1,
                balance: BigInt::from(0).to_signed_bytes_be(),
                component_id: pool_id.as_bytes().to_vec(),
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethabi::{
        ethereum_types::{Address, U256},
        Token,
    };
    use hex_literal::hex;
    use substreams_ethereum::pb::eth::v2::TransactionReceipt;

    const POOL_CREATED_TOPIC: [u8; 32] =
        hex!("783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118");

    #[test]
    fn emits_factory_pool_created_logs() {
        let factory = address_array(1);
        let token0 = address(2);
        let token1 = address(3);
        let pool = address(4);

        let changes = get_new_pools(
            &block(vec![pool_created_log(
                factory.to_vec(),
                token0.clone(),
                token1.clone(),
                3000,
                60,
                pool.clone(),
            )]),
            &factory,
        );

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].component_changes[0].id, pool.to_hex());
        assert_eq!(changes[0].component_changes[0].tokens, vec![token0, token1]);
        assert_eq!(changes[0].balance_changes.len(), 2);
        assert_eq!(static_attr(&changes[0].component_changes[0], "fee"), BigInt::from(3000));
        assert_eq!(static_attr(&changes[0].component_changes[0], "tick_spacing"), BigInt::from(60));
    }

    #[test]
    fn ignores_pool_created_shaped_logs_from_non_factory_addresses() {
        let factory = address_array(1);
        let other_address = address(9);

        let changes = get_new_pools(
            &block(vec![pool_created_log(
                other_address,
                address(2),
                address(3),
                3000,
                60,
                address(4),
            )]),
            &factory,
        );

        assert!(changes.is_empty());
    }

    #[test]
    fn skips_pool_scan_when_factory_is_absent_from_block_bloom() {
        let factory = address_array(1);
        let token0 = address(2);
        let token1 = address(3);
        let pool = address(4);
        let block = block_with_bloom(
            vec![pool_created_log(factory.to_vec(), token0, token1, 3000, 60, pool)],
            vec![0; BLOOM_BYTES],
        );

        let changes = get_new_pools(&block, &factory);

        assert!(changes.is_empty());
    }

    #[test]
    fn scans_pool_created_logs_when_factory_is_present_in_block_bloom() {
        let factory = address_array(1);
        let token0 = address(2);
        let token1 = address(3);
        let pool = address(4);
        let block = block_with_bloom(
            vec![pool_created_log(
                factory.to_vec(),
                token0.clone(),
                token1.clone(),
                3000,
                60,
                pool.clone(),
            )],
            bloom_for(&factory),
        );

        let changes = get_new_pools(&block, &factory);

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].component_changes[0].id, pool.to_hex());
        assert_eq!(changes[0].component_changes[0].tokens, vec![token0, token1]);
    }

    #[test]
    fn manually_decodes_signed_tick_spacing() {
        let factory = address_array(1);
        let log = pool_created_log(factory.to_vec(), address(2), address(3), 500, -10, address(4));

        let event = decode_pool_created(&log).unwrap();

        assert_eq!(event.fee, BigInt::from(500));
        assert_eq!(event.tick_spacing, BigInt::from(-10));
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
                status: 1,
                index: 7,
                hash: vec![7; 32],
                from: address(7),
                to: address(8),
                receipt: Some(TransactionReceipt { logs, ..Default::default() }),
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

    fn pool_created_log(
        factory: Vec<u8>,
        token0: Vec<u8>,
        token1: Vec<u8>,
        fee: u64,
        tick_spacing: i64,
        pool: Vec<u8>,
    ) -> eth::Log {
        eth::Log {
            address: factory,
            topics: vec![
                POOL_CREATED_TOPIC.to_vec(),
                address_topic(&token0),
                address_topic(&token1),
                uint_topic(fee),
            ],
            data: ethabi::encode(&[
                Token::Int(int24_token(tick_spacing)),
                Token::Address(Address::from_slice(&pool)),
            ]),
            ordinal: 10,
            ..Default::default()
        }
    }

    fn int24_token(value: i64) -> U256 {
        if value >= 0 {
            U256::from(value as u64)
        } else {
            let mut word = [0xff; 32];
            let signed = (value as i32).to_be_bytes();
            word[28..32].copy_from_slice(&signed);
            U256::from_big_endian(&word)
        }
    }

    fn address_topic(address: &[u8]) -> Vec<u8> {
        let mut topic = vec![0; 12];
        topic.extend_from_slice(address);
        topic
    }

    fn uint_topic(value: u64) -> Vec<u8> {
        let mut topic = [0; 32];
        U256::from(value).to_big_endian(&mut topic);
        topic.to_vec()
    }

    fn address(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }

    fn address_array(seed: u8) -> [u8; 20] {
        [seed; 20]
    }

    fn bloom_for(value: &[u8]) -> Vec<u8> {
        let mut bloom = vec![0; BLOOM_BYTES];
        for bit in bloom_bit_indexes(value) {
            bloom[BLOOM_BYTES - 1 - (bit / 8)] |= 1 << (bit % 8);
        }
        bloom
    }
}
