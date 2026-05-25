pub use map_events::map_events;
pub use map_pool_data::map_pool_data;
pub use map_pool_event_attribute_changes::map_pool_event_attribute_changes;
pub use map_pool_protocol_fee_changes::map_pool_protocol_fee_changes;
pub use map_protocol_changes::map_protocol_changes;
use substreams_ethereum::pb::eth::v2::TransactionTrace;

use crate::pb::uniswap::v3::Transaction;

#[path = "3_map_events.rs"]
mod map_events;

mod fixed_pools;

#[path = "3_map_pool_data.rs"]
mod map_pool_data;

#[path = "4_map_and_store_balance_changes.rs"]
mod map_store_balance_changes;

#[path = "4_map_and_store_ticks.rs"]
mod map_store_ticks;

#[path = "4_map_and_store_liquidity.rs"]
mod map_store_liquidity;

#[path = "4_map_pool_event_attribute_changes.rs"]
mod map_pool_event_attribute_changes;

#[path = "4_map_pool_protocol_fee_changes.rs"]
mod map_pool_protocol_fee_changes;

#[path = "5_map_protocol_changes.rs"]
mod map_protocol_changes;

impl From<TransactionTrace> for Transaction {
    fn from(value: TransactionTrace) -> Self {
        Self { hash: value.hash, from: value.from, to: value.to, index: value.index.into() }
    }
}

impl From<&TransactionTrace> for Transaction {
    fn from(value: &TransactionTrace) -> Self {
        Self {
            hash: value.hash.clone(),
            from: value.from.clone(),
            to: value.to.clone(),
            index: value.index.into(),
        }
    }
}

impl From<&Transaction> for tycho_substreams::prelude::Transaction {
    fn from(value: &Transaction) -> Self {
        Self {
            hash: value.hash.clone(),
            from: value.from.clone(),
            to: value.to.clone(),
            index: value.index,
        }
    }
}

impl From<Transaction> for tycho_substreams::prelude::Transaction {
    fn from(value: Transaction) -> Self {
        Self { hash: value.hash, from: value.from, to: value.to, index: value.index }
    }
}
