use std::str::FromStr;
use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr};
use sea_query_binder::SqlxBinder;

use crate::schema::value::{ConfigType, ConfigValue};
use crate::schema::log::{SystemLog, LogSchema, LogStatus};

enum LogSelector {
    Time(DateTime<Utc>),
    Last(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>)
}

async fn select_log(pool: &Pool<Postgres>,
    selector: LogSelector,
    device_id: Option<i64>,
    status: Option<&str>
) -> Result<Vec<LogSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            SystemLog::DeviceId,
            SystemLog::Timestamp,
            SystemLog::Status,
            SystemLog::Value,
            SystemLog::Type
        ])
        .from(SystemLog::Table)
        .to_owned();
    match selector {
        LogSelector::Time(timestamp) => {
            stmt = stmt.and_where(Expr::col(SystemLog::Timestamp).eq(timestamp)).to_owned();
        },
        LogSelector::Last(timestamp) => {
            stmt = stmt.and_where(Expr::col(SystemLog::Timestamp).gt(timestamp)).to_owned();
        },
        LogSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col(SystemLog::Timestamp).gte(begin))
                .and_where(Expr::col(SystemLog::Timestamp).lte(end))
                .to_owned();
        }
    }
    if let Some(id) = device_id {
        stmt = stmt.and_where(Expr::col(SystemLog::DeviceId).eq(id)).to_owned();
    }
    if let Some(status) = status {
        stmt = stmt.and_where(Expr::col(SystemLog::Status).eq(status)).to_owned();
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(3);
            let type_ = ConfigType::from(row.get::<i16,_>(4));
            LogSchema {
                device_id: row.get(0),
                timestamp: row.get(1),
                status: LogStatus::from(row.get::<i16,_>(2)).to_string(),
                value: ConfigValue::from_bytes(&bytes, type_)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_log_by_id(pool: &Pool<Postgres>,
    timestamp: DateTime<Utc>,
    device_id: i64
) -> Result<LogSchema, Error>
{
    select_log(pool, LogSelector::Time(timestamp), Some(device_id), None).await?.into_iter().next()
        .ok_or(Error::RowNotFound)
}

pub(crate) async fn select_log_by_time(pool: &Pool<Postgres>,
    timestamp: DateTime<Utc>,
    device_id: Option<i64>,
    status: Option<&str>
) -> Result<Vec<LogSchema>, Error>
{
    select_log(pool, LogSelector::Time(timestamp), device_id, status).await
}

pub(crate) async fn select_log_by_last_time(pool: &Pool<Postgres>,
    last: DateTime<Utc>,
    device_id: Option<i64>,
    status: Option<&str>
) -> Result<Vec<LogSchema>, Error>
{
    select_log(pool, LogSelector::Last(last), device_id, status).await
}

pub(crate) async fn select_log_by_range_time(pool: &Pool<Postgres>,
    begin: DateTime<Utc>,
    end: DateTime<Utc>,
    device_id: Option<i64>,
    status: Option<&str>
) -> Result<Vec<LogSchema>, Error>
{
    select_log(pool, LogSelector::Range(begin, end), device_id, status).await
}

pub(crate) async fn insert_log(pool: &Pool<Postgres>,
    timestamp: DateTime<Utc>,
    device_id: i64,
    status: &str,
    value: ConfigValue
) -> Result<(), Error>
{
    let bytes = value.to_bytes();
    let type_ = i16::from(value.get_type());

    let (sql, values) = Query::insert()
        .into_table(SystemLog::Table)
        .columns([
            SystemLog::DeviceId,
            SystemLog::Timestamp,
            SystemLog::Status,
            SystemLog::Value,
            SystemLog::Type
        ])
        .values([
            device_id.into(),
            timestamp.into(),
            i16::from(LogStatus::from_str(status).unwrap()).into(),
            bytes.into(),
            type_.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn update_log(pool: &Pool<Postgres>,
    timestamp: DateTime<Utc>,
    device_id: i64,
    status: Option<&str>,
    value: Option<ConfigValue>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(SystemLog::Table)
        .to_owned();

    if let Some(value) = status {
        stmt = stmt.value(SystemLog::Status, i16::from(LogStatus::from_str(value).unwrap())).to_owned();
    }
    if let Some(value) = value {
        let bytes = value.to_bytes();
        let type_ = i16::from(value.get_type());
        stmt = stmt
            .value(SystemLog::Value, bytes)
            .value(SystemLog::Type, type_)
            .to_owned();
    }
    let (sql, values) = stmt
        .and_where(Expr::col(SystemLog::DeviceId).eq(device_id))
        .and_where(Expr::col(SystemLog::Timestamp).eq(timestamp))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_log(pool: &Pool<Postgres>,
    timestamp: DateTime<Utc>,
    device_id: i64
) -> Result<(), Error>
{
    let (sql, values) = Query::delete()
        .from_table(SystemLog::Table)
        .and_where(Expr::col(SystemLog::DeviceId).eq(device_id))
        .and_where(Expr::col(SystemLog::Timestamp).eq(timestamp))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
