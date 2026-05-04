use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use diesel::{
    sql_query,
    sql_types::{Array, BigInt, Binary, Double, Text, Timestamp},
    QueryableByName,
};
use diesel_async::{
    pooled_connection::{deadpool::Pool, AsyncDieselConnectionManager},
    AsyncPgConnection, RunQueryDsl,
};
use tracing::info;
use tycho_common::{models::Chain, Bytes};

#[derive(Clone)]
pub struct TvlDb {
    pool: Pool<AsyncPgConnection>,
}

#[derive(Debug, QueryableByName)]
struct DbTimestamp {
    #[diesel(sql_type = Timestamp)]
    ts: NaiveDateTime,
}

#[derive(Debug, QueryableByName)]
struct LatestBlock {
    #[diesel(sql_type = BigInt)]
    number: i64,
}

#[derive(Debug, QueryableByName)]
struct TokenPriceRow {
    #[diesel(sql_type = Binary)]
    address: Vec<u8>,
    #[diesel(sql_type = Double)]
    price: f64,
}

#[derive(Debug, QueryableByName)]
struct ComponentExternalId {
    #[diesel(sql_type = Text)]
    external_id: String,
}

#[derive(Debug, QueryableByName)]
struct TvlRefreshCount {
    #[diesel(sql_type = BigInt)]
    updated_count: i64,
}

impl TvlDb {
    pub fn connect(database_url: &str) -> Result<Self> {
        let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
        let pool = Pool::builder(config)
            .build()
            .context("failed to build tycho-tvl DB pool")?;
        Ok(Self { pool })
    }

    pub async fn get_current_db_timestamp(&self) -> Result<NaiveDateTime> {
        let mut conn = self.connection().await?;
        let row = sql_query("SELECT now()::timestamp AS ts")
            .get_result::<DbTimestamp>(&mut conn)
            .await
            .context("failed to read current DB timestamp")?;
        Ok(row.ts)
    }

    pub async fn get_latest_block_number(&self, chain: &Chain) -> Result<u64> {
        let mut conn = self.connection().await?;
        let row = sql_query(
            r#"
SELECT b.number AS number
FROM block b
JOIN chain c ON c.id = b.chain_id
WHERE c.name = $1
  AND b.main = true
ORDER BY b.number DESC
LIMIT 1
"#,
        )
        .bind::<Text, _>(chain.to_string())
        .get_result::<LatestBlock>(&mut conn)
        .await
        .with_context(|| format!("failed to load latest block for {chain}"))?;
        Ok(row.number as u64)
    }

    pub async fn get_existing_token_prices_for_chain(
        &self,
        chain: &Chain,
    ) -> Result<HashMap<Bytes, f64>> {
        let mut conn = self.connection().await?;
        let rows = sql_query(
            r#"
SELECT a.address AS address, tp.price AS price
FROM token_price tp
JOIN token t ON t.id = tp.token_id
JOIN account a ON a.id = t.account_id
JOIN chain c ON c.id = a.chain_id
WHERE c.name = $1
"#,
        )
        .bind::<Text, _>(chain.to_string())
        .load::<TokenPriceRow>(&mut conn)
        .await
        .with_context(|| format!("failed to load token prices for {chain}"))?;

        Ok(rows
            .into_iter()
            .map(|row| (Bytes::from(row.address), row.price))
            .collect())
    }

    pub async fn get_recently_changed_components(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
        since: NaiveDateTime,
    ) -> Result<Vec<String>> {
        if protocol_systems.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.connection().await?;
        let rows = sql_query(
            r#"
SELECT DISTINCT pc.external_id AS external_id
FROM component_balance_default cb
JOIN protocol_component pc ON pc.id = cb.protocol_component_id
JOIN chain c ON c.id = pc.chain_id
JOIN protocol_system ps ON ps.id = pc.protocol_system_id
WHERE c.name = $1
  AND ps.name = ANY($2)
  AND pc.deleted_at IS NULL
  AND cb.valid_from > $3
"#,
        )
        .bind::<Text, _>(chain.to_string())
        .bind::<Array<Text>, _>(protocol_systems.to_vec())
        .bind::<Timestamp, _>(since)
        .load::<ComponentExternalId>(&mut conn)
        .await
        .with_context(|| format!("failed to load recently changed components for {chain}"))?;

        Ok(rows
            .into_iter()
            .map(|row| row.external_id)
            .collect())
    }

    pub async fn get_components_for_tokens_limited(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
        token_addresses: &[Bytes],
        limit_per_token: i64,
    ) -> Result<Vec<String>> {
        if protocol_systems.is_empty() || token_addresses.is_empty() || limit_per_token <= 0 {
            return Ok(Vec::new());
        }

        let addresses = token_addresses
            .iter()
            .map(Bytes::to_vec)
            .collect::<Vec<_>>();
        let mut conn = self.connection().await?;
        let rows = sql_query(
            r#"
WITH requested_token AS (
    SELECT unnest($3::bytea[]) AS address
),
ranked_components AS (
    SELECT
        requested_token.address,
        pc.external_id,
        ROW_NUMBER() OVER (
            PARTITION BY requested_token.address
            ORDER BY COALESCE(component_tvl.tvl, 0.0) DESC, pc.id ASC
        ) AS component_rank
    FROM requested_token
    JOIN chain c
      ON c.name = $1
    JOIN account a
      ON a.address = requested_token.address
     AND a.chain_id = c.id
    JOIN token t
      ON t.account_id = a.id
    JOIN protocol_component_holds_token pcht
      ON pcht.token_id = t.id
    JOIN protocol_component pc
      ON pc.id = pcht.protocol_component_id
    JOIN protocol_system ps
      ON ps.id = pc.protocol_system_id
    LEFT JOIN component_tvl
      ON component_tvl.protocol_component_id = pc.id
    WHERE pc.chain_id = c.id
      AND pc.deleted_at IS NULL
      AND ps.name = ANY($2)
)
SELECT DISTINCT external_id
FROM ranked_components
WHERE component_rank <= $4
"#,
        )
        .bind::<Text, _>(chain.to_string())
        .bind::<Array<Text>, _>(protocol_systems.to_vec())
        .bind::<Array<Binary>, _>(addresses)
        .bind::<BigInt, _>(limit_per_token)
        .load::<ComponentExternalId>(&mut conn)
        .await
        .with_context(|| format!("failed to load limited token components for {chain}"))?;

        Ok(rows
            .into_iter()
            .map(|row| row.external_id)
            .collect())
    }

    pub async fn filter_components_with_state_requirements(
        &self,
        chain: &Chain,
        component_ids: &[String],
        required_attributes: &[&str],
        required_attribute_prefixes: &[&str],
    ) -> Result<Vec<String>> {
        if component_ids.is_empty() {
            return Ok(Vec::new());
        }

        let required_attributes = required_attributes
            .iter()
            .map(|attr| (*attr).to_string())
            .collect::<Vec<_>>();
        let required_attribute_prefixes = required_attribute_prefixes
            .iter()
            .map(|prefix| (*prefix).to_string())
            .collect::<Vec<_>>();

        let mut conn = self.connection().await?;
        let rows = sql_query(
            r#"
WITH requested_component AS (
    SELECT unnest($2::text[]) AS external_id
),
scoped_component AS (
    SELECT pc.id, pc.external_id
    FROM requested_component requested
    JOIN chain c
      ON c.name = $1
    JOIN protocol_component pc
      ON pc.external_id = requested.external_id
     AND pc.chain_id = c.id
    WHERE pc.deleted_at IS NULL
)
SELECT scoped.external_id
FROM scoped_component scoped
WHERE NOT EXISTS (
    SELECT 1
    FROM unnest($3::text[]) required_attr(attribute_name)
    WHERE NOT EXISTS (
        SELECT 1
        FROM protocol_state_default ps
        WHERE ps.protocol_component_id = scoped.id
          AND ps.attribute_name = required_attr.attribute_name
    )
)
AND NOT EXISTS (
    SELECT 1
    FROM unnest($4::text[]) required_prefix(attribute_prefix)
    WHERE NOT EXISTS (
        SELECT 1
        FROM protocol_state_default ps
        WHERE ps.protocol_component_id = scoped.id
          AND ps.attribute_name LIKE required_prefix.attribute_prefix || '%'
    )
)
"#,
        )
        .bind::<Text, _>(chain.to_string())
        .bind::<Array<Text>, _>(component_ids.to_vec())
        .bind::<Array<Text>, _>(required_attributes)
        .bind::<Array<Text>, _>(required_attribute_prefixes)
        .load::<ComponentExternalId>(&mut conn)
        .await
        .with_context(|| format!("failed to filter component state requirements for {chain}"))?;

        Ok(rows
            .into_iter()
            .map(|row| row.external_id)
            .collect())
    }

    pub async fn get_components_for_protocols(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
    ) -> Result<Vec<String>> {
        if protocol_systems.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.connection().await?;
        let rows = sql_query(
            r#"
SELECT pc.external_id AS external_id
FROM protocol_component pc
JOIN chain c ON c.id = pc.chain_id
JOIN protocol_system ps ON ps.id = pc.protocol_system_id
WHERE c.name = $1
  AND ps.name = ANY($2)
  AND pc.deleted_at IS NULL
"#,
        )
        .bind::<Text, _>(chain.to_string())
        .bind::<Array<Text>, _>(protocol_systems.to_vec())
        .load::<ComponentExternalId>(&mut conn)
        .await
        .with_context(|| format!("failed to load scoped components for {chain}"))?;

        Ok(rows
            .into_iter()
            .map(|row| row.external_id)
            .collect())
    }

    pub async fn upsert_token_prices_by_address(
        &self,
        chain: &Chain,
        prices_by_address: &HashMap<Bytes, f64>,
        batch_size: usize,
    ) -> Result<usize> {
        if prices_by_address.is_empty() {
            return Ok(0);
        }

        let rows = prices_by_address
            .iter()
            .filter(|(_, price)| price.is_finite() && **price > 0.0)
            .map(|(address, price)| (address.to_vec(), *price))
            .collect::<Vec<_>>();

        let mut conn = self.connection().await?;
        let mut affected = 0usize;
        for chunk in rows.chunks(batch_size.max(1)) {
            let addresses = chunk
                .iter()
                .map(|(address, _)| address.clone())
                .collect::<Vec<_>>();
            let prices = chunk
                .iter()
                .map(|(_, price)| *price)
                .collect::<Vec<_>>();
            let count = sql_query(
                r#"
WITH input_price AS (
    SELECT *
    FROM unnest($2::bytea[], $3::float8[]) AS input(address, price)
),
token_rows AS (
    SELECT t.id AS token_id, input_price.price
    FROM input_price
    JOIN chain c ON c.name = $1
    JOIN account a
      ON a.address = input_price.address
     AND a.chain_id = c.id
    JOIN token t
      ON t.account_id = a.id
)
INSERT INTO token_price (token_id, price)
SELECT token_id, price
FROM token_rows
ON CONFLICT (token_id)
DO UPDATE SET price = EXCLUDED.price
"#,
            )
            .bind::<Text, _>(chain.to_string())
            .bind::<Array<Binary>, _>(addresses)
            .bind::<Array<Double>, _>(prices)
            .execute(&mut conn)
            .await
            .with_context(|| format!("failed to upsert token prices for {chain}"))?;
            affected += count;
        }

        Ok(affected)
    }

    pub async fn refresh_component_tvl(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
        component_ids: Option<&[String]>,
        batch_size: usize,
    ) -> Result<usize> {
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

        let mut conn = self.connection().await?;
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
                    .bind::<Array<Text>, _>(protocol_systems.to_vec())
                    .bind::<Array<Text>, _>(ids.to_vec())
                    .get_result::<TvlRefreshCount>(&mut conn)
                    .await
                    .with_context(|| format!("failed to refresh component TVL for {chain}"))?
            } else {
                sql_query(query)
                    .bind::<Text, _>(chain.to_string())
                    .bind::<Array<Text>, _>(protocol_systems.to_vec())
                    .get_result::<TvlRefreshCount>(&mut conn)
                    .await
                    .with_context(|| format!("failed to refresh component TVL for {chain}"))?
            };
            affected += row.updated_count as usize;
        }

        Ok(affected)
    }

    pub async fn count_inactive_nonzero_component_tvl(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
        active_since: NaiveDateTime,
    ) -> Result<usize> {
        if protocol_systems.is_empty() {
            return Ok(0);
        }

        let mut conn = self.connection().await?;
        let row = sql_query(
            r#"
SELECT COUNT(*)::bigint AS updated_count
FROM component_tvl ct
JOIN protocol_component pc ON pc.id = ct.protocol_component_id
JOIN chain c ON c.id = pc.chain_id
JOIN protocol_system ps ON ps.id = pc.protocol_system_id
WHERE c.name = $1
  AND ps.name = ANY($2)
  AND pc.deleted_at IS NULL
  AND ct.tvl <> 0.0
  AND NOT EXISTS (
      SELECT 1
      FROM component_balance_default cb
      WHERE cb.protocol_component_id = pc.id
        AND cb.valid_from >= $3
  )
"#,
        )
        .bind::<Text, _>(chain.to_string())
        .bind::<Array<Text>, _>(protocol_systems.to_vec())
        .bind::<Timestamp, _>(active_since)
        .get_result::<TvlRefreshCount>(&mut conn)
        .await
        .with_context(|| format!("failed to count inactive component TVL rows for {chain}"))?;

        Ok(row.updated_count as usize)
    }

    pub async fn zero_inactive_component_tvl(
        &self,
        chain: &Chain,
        protocol_systems: &[String],
        active_since: NaiveDateTime,
        batch_size: usize,
    ) -> Result<usize> {
        if protocol_systems.is_empty() {
            return Ok(0);
        }

        let mut conn = self.connection().await?;
        let mut affected = 0usize;
        let mut batch_index = 0usize;
        loop {
            let row = sql_query(
                r#"
WITH target_component AS (
    SELECT pc.id
    FROM component_tvl ct
    JOIN protocol_component pc ON pc.id = ct.protocol_component_id
    JOIN chain c ON c.id = pc.chain_id
    JOIN protocol_system ps ON ps.id = pc.protocol_system_id
    WHERE c.name = $1
      AND ps.name = ANY($2)
      AND pc.deleted_at IS NULL
      AND ct.tvl <> 0.0
      AND NOT EXISTS (
          SELECT 1
          FROM component_balance_default cb
          WHERE cb.protocol_component_id = pc.id
            AND cb.valid_from >= $3
      )
    ORDER BY pc.id
    LIMIT $4
),
updated AS (
    UPDATE component_tvl ct
    SET tvl = 0.0
    FROM target_component target
    WHERE ct.protocol_component_id = target.id
    RETURNING ct.protocol_component_id
)
SELECT COUNT(*)::bigint AS updated_count
FROM updated
"#,
            )
            .bind::<Text, _>(chain.to_string())
            .bind::<Array<Text>, _>(protocol_systems.to_vec())
            .bind::<Timestamp, _>(active_since)
            .bind::<BigInt, _>(batch_size.max(1) as i64)
            .get_result::<TvlRefreshCount>(&mut conn)
            .await
            .with_context(|| format!("failed to zero inactive component TVL rows for {chain}"))?;

            let updated = row.updated_count as usize;
            if updated == 0 {
                break;
            }
            affected += updated;
            info!(
                batch_index,
                batch_updated = updated,
                total_updated = affected,
                "TychoTvlPruneStaleComponentsBatchUpdated"
            );
            batch_index += 1;
        }

        Ok(affected)
    }

    async fn connection(
        &self,
    ) -> Result<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
        self.pool
            .get()
            .await
            .context("failed to retrieve tycho-tvl DB connection")
    }
}
