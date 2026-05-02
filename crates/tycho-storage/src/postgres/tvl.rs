use std::collections::HashMap;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use diesel::{
    prelude::*,
    sql_query,
    sql_types::{BigInt, Text, Timestamp},
    upsert::excluded,
    QueryableByName,
};
use diesel_async::RunQueryDsl;
use tycho_common::{models::Chain, storage::StorageError, Bytes};

use super::{direct::DirectGateway, PostgresError};
use crate::postgres::schema;

#[derive(Debug, QueryableByName)]
struct DbTimestamp {
    #[diesel(sql_type = Timestamp)]
    ts: NaiveDateTime,
}

#[derive(Debug, QueryableByName)]
struct TvlRefreshCount {
    #[diesel(sql_type = BigInt)]
    updated_count: i64,
}

#[async_trait]
pub trait TvlGatewayExt {
    async fn get_current_db_timestamp(&self) -> Result<NaiveDateTime, StorageError>;

    async fn get_latest_block_number(&self, chain: &Chain) -> Result<u64, StorageError>;

    async fn get_existing_token_prices_for_chain(
        &self,
        chain: &Chain,
    ) -> Result<HashMap<Bytes, f64>, StorageError>;

    async fn get_components_for_tokens(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
        token_addresses: &[Bytes],
    ) -> Result<Vec<String>, StorageError>;

    async fn get_components_for_protocols(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
    ) -> Result<Vec<String>, StorageError>;

    async fn upsert_token_prices_by_address(
        &self,
        chain: &Chain,
        prices_by_address: &HashMap<Bytes, f64>,
        batch_size: usize,
    ) -> Result<usize, StorageError>;

    async fn refresh_component_tvl(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
        component_ids: Option<&[String]>,
        batch_size: usize,
    ) -> Result<usize, StorageError>;
}

#[async_trait]
impl TvlGatewayExt for DirectGateway {
    async fn get_current_db_timestamp(&self) -> Result<NaiveDateTime, StorageError> {
        let mut conn =
            self.pool().get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;

        let row = sql_query("SELECT now()::timestamp AS ts")
            .get_result::<DbTimestamp>(&mut conn)
            .await
            .map_err(PostgresError::from)?;
        Ok(row.ts)
    }

    async fn get_latest_block_number(&self, chain: &Chain) -> Result<u64, StorageError> {
        let mut conn =
            self.pool().get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        let chain_id = self
            .state_gateway()
            .get_chain_id(chain)?;

        let number = schema::block::table
            .filter(schema::block::chain_id.eq(chain_id))
            .filter(schema::block::main.eq(true))
            .select(schema::block::number)
            .order(schema::block::number.desc())
            .first::<i64>(&mut conn)
            .await
            .optional()
            .map_err(PostgresError::from)?
            .ok_or_else(|| StorageError::NotFound("Block".to_string(), chain.to_string()))?;

        Ok(number as u64)
    }

    async fn get_existing_token_prices_for_chain(
        &self,
        chain: &Chain,
    ) -> Result<HashMap<Bytes, f64>, StorageError> {
        let mut conn =
            self.pool().get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;

        self.state_gateway()
            .get_token_prices(chain, &mut conn)
            .await
    }

    async fn get_components_for_tokens(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
        token_addresses: &[Bytes],
    ) -> Result<Vec<String>, StorageError> {
        if protocol_systems.is_empty() || token_addresses.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn =
            self.pool().get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        let chain_id = self
            .state_gateway()
            .get_chain_id(chain)?;
        let addresses: Vec<Vec<u8>> = token_addresses
            .iter()
            .map(Bytes::to_vec)
            .collect();

        let ids =
            schema::protocol_component::table
                .inner_join(
                    schema::protocol_component_holds_token::table
                        .on(schema::protocol_component_holds_token::protocol_component_id
                            .eq(schema::protocol_component::id)),
                )
                .inner_join(
                    schema::token::table
                        .on(schema::token::id.eq(schema::protocol_component_holds_token::token_id)),
                )
                .inner_join(
                    schema::account::table.on(schema::account::id.eq(schema::token::account_id)),
                )
                .inner_join(schema::protocol_system::table.on(
                    schema::protocol_system::id.eq(schema::protocol_component::protocol_system_id),
                ))
                .filter(schema::protocol_component::chain_id.eq(chain_id))
                .filter(schema::protocol_component::deleted_at.is_null())
                .filter(schema::protocol_system::name.eq_any(protocol_systems))
                .filter(schema::account::address.eq_any(addresses))
                .select(schema::protocol_component::external_id)
                .distinct()
                .load::<String>(&mut conn)
                .await
                .map_err(PostgresError::from)?;

        Ok(ids)
    }

    async fn get_components_for_protocols(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
    ) -> Result<Vec<String>, StorageError> {
        if protocol_systems.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn =
            self.pool().get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        let chain_id = self
            .state_gateway()
            .get_chain_id(chain)?;

        let ids =
            schema::protocol_component::table
                .inner_join(schema::protocol_system::table.on(
                    schema::protocol_system::id.eq(schema::protocol_component::protocol_system_id),
                ))
                .filter(schema::protocol_component::chain_id.eq(chain_id))
                .filter(schema::protocol_component::deleted_at.is_null())
                .filter(schema::protocol_system::name.eq_any(protocol_systems))
                .select(schema::protocol_component::external_id)
                .load::<String>(&mut conn)
                .await
                .map_err(PostgresError::from)?;

        Ok(ids)
    }

    async fn upsert_token_prices_by_address(
        &self,
        chain: &Chain,
        prices_by_address: &HashMap<Bytes, f64>,
        batch_size: usize,
    ) -> Result<usize, StorageError> {
        if prices_by_address.is_empty() {
            return Ok(0);
        }

        let mut conn =
            self.pool().get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;
        let chain_id = self
            .state_gateway()
            .get_chain_id(chain)?;
        let addresses: Vec<Vec<u8>> = prices_by_address
            .keys()
            .map(Bytes::to_vec)
            .collect();

        let token_ids = schema::token::table
            .inner_join(
                schema::account::table.on(schema::account::id.eq(schema::token::account_id)),
            )
            .filter(schema::account::chain_id.eq(chain_id))
            .filter(schema::account::address.eq_any(addresses))
            .select((schema::account::address, schema::token::id))
            .load::<(Vec<u8>, i64)>(&mut conn)
            .await
            .map_err(PostgresError::from)?;

        let rows = token_ids
            .into_iter()
            .filter_map(|(address, token_id)| {
                prices_by_address
                    .get(&Bytes::from(address))
                    .copied()
                    .filter(|price| price.is_finite() && *price > 0.0)
                    .map(|price| (token_id, price))
            })
            .collect::<Vec<_>>();

        let mut affected = 0;
        for chunk in rows.chunks(batch_size.max(1)) {
            affected += diesel::insert_into(schema::token_price::table)
                .values(
                    chunk
                        .iter()
                        .map(|(token_id, price)| {
                            (
                                schema::token_price::token_id.eq(*token_id),
                                schema::token_price::price.eq(*price),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
                .on_conflict(schema::token_price::token_id)
                .do_update()
                .set(schema::token_price::price.eq(excluded(schema::token_price::price)))
                .execute(&mut conn)
                .await
                .map_err(PostgresError::from)?;
        }

        Ok(affected)
    }

    async fn refresh_component_tvl(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
        component_ids: Option<&[String]>,
        batch_size: usize,
    ) -> Result<usize, StorageError> {
        if protocol_systems.is_empty() {
            return Ok(0);
        }

        let component_batches: Vec<Option<&[String]>> = match component_ids {
            Some(ids) if ids.is_empty() => return Ok(0),
            Some(ids) => ids
                .chunks(batch_size.max(1))
                .map(Some)
                .collect(),
            None => vec![None],
        };

        let mut conn =
            self.pool().get().await.map_err(|e| {
                StorageError::Unexpected(format!("Failed to retrieve connection: {e}"))
            })?;

        let mut affected = 0usize;
        for batch in component_batches {
            let mut query = String::from(
                r#"
WITH scoped_components AS (
    SELECT pc.id, pc.external_id
    FROM protocol_component pc
    JOIN chain c ON c.id = pc.chain_id
    JOIN protocol_system ps ON ps.id = pc.protocol_system_id
    WHERE c.name = $1
      AND ps.name = ANY($2)
      AND pc.deleted_at IS NULL
"#,
            );
            if batch.is_some() {
                query.push_str("      AND pc.external_id = ANY($3)\n");
            }
            query.push_str(
                r#"),
aggregated AS (
    SELECT
        bal.protocol_component_id,
        SUM(bal.balance_float / token_price.price) AS tvl
    FROM component_balance_default AS bal
    INNER JOIN token_price ON bal.token_id = token_price.token_id
    INNER JOIN scoped_components sc ON sc.id = bal.protocol_component_id
    WHERE token_price.price > 0
    GROUP BY bal.protocol_component_id
),
upserted AS (
    INSERT INTO component_tvl (protocol_component_id, tvl)
    SELECT
        sc.id,
        COALESCE(aggregated.tvl, 0.0) AS tvl
    FROM scoped_components sc
    LEFT JOIN aggregated ON aggregated.protocol_component_id = sc.id
    ON CONFLICT (protocol_component_id)
    DO UPDATE SET tvl = EXCLUDED.tvl
    RETURNING protocol_component_id
)
SELECT COUNT(*)::bigint AS updated_count FROM upserted
"#,
            );

            let row = if let Some(ids) = batch {
                sql_query(query)
                    .bind::<Text, _>(chain.to_string())
                    .bind::<diesel::sql_types::Array<Text>, _>(protocol_systems.to_vec())
                    .bind::<diesel::sql_types::Array<Text>, _>(ids.to_vec())
                    .get_result::<TvlRefreshCount>(&mut conn)
                    .await
                    .map_err(PostgresError::from)?
            } else {
                sql_query(query)
                    .bind::<Text, _>(chain.to_string())
                    .bind::<diesel::sql_types::Array<Text>, _>(protocol_systems.to_vec())
                    .get_result::<TvlRefreshCount>(&mut conn)
                    .await
                    .map_err(PostgresError::from)?
            };
            affected += row.updated_count as usize;
        }

        Ok(affected)
    }
}
