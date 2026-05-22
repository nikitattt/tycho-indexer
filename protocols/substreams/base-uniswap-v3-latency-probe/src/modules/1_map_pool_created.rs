use substreams_ethereum::pb::eth::v2::{self as eth};
use tycho_substreams::prelude::*;

#[substreams::handlers::map]
pub fn map_pools_created(
    _block: eth::Block,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    Ok(BlockEntityChanges { block: None, changes: vec![] })
}
