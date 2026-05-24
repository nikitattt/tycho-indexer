use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2::{self as eth};
use substreams_helper::hex::Hexable;

use crate::abi::factory::events::PoolCreated;

use tycho_substreams::prelude::*;

#[substreams::handlers::map]
pub fn map_pools_created(
    params: String,
    block: eth::Block,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    let factory_address = decode_factory_address(&params)?;

    Ok(BlockEntityChanges { block: None, changes: get_new_pools(&block, &factory_address) })
}

fn decode_factory_address(factory_address: &str) -> Result<Vec<u8>, substreams::errors::Error> {
    let address = hex::decode(factory_address.trim_start_matches("0x"))
        .map_err(|err| anyhow::anyhow!("invalid factory address `{factory_address}`: {err}"))?;

    if address.len() != 20 {
        return Err(anyhow::anyhow!(
            "invalid factory address `{factory_address}`: expected 20 bytes, got {}",
            address.len()
        ));
    }

    Ok(address)
}

fn get_new_pools(block: &eth::Block, factory_address: &[u8]) -> Vec<TransactionEntityChanges> {
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
            if log.address.as_slice() != factory_address || !PoolCreated::match_log(log) {
                continue;
            }

            if let Ok(event) = PoolCreated::decode(log) {
                new_pools.push(pool_created_changes(event, tx));
            }
        }
    }

    new_pools
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
        let factory = address(1);
        let token0 = address(2);
        let token1 = address(3);
        let pool = address(4);

        let changes = get_new_pools(
            &block(vec![pool_created_log(
                factory.clone(),
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
        let factory = address(1);
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

    fn pool_created_log(
        factory: Vec<u8>,
        token0: Vec<u8>,
        token1: Vec<u8>,
        fee: u64,
        tick_spacing: u64,
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
                Token::Int(U256::from(tick_spacing)),
                Token::Address(Address::from_slice(&pool)),
            ]),
            ordinal: 10,
            ..Default::default()
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
}
