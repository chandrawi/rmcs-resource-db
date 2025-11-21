use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataType, DataValue};
use crate::schema::log::{SystemLog, LogSchema};
use crate::utility::tag as Tag;

pub(crate) enum LogSelector {
    Time(DateTime<Utc>),
    Latest(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    First(usize, usize),
    Last(usize, usize),
    None
}

pub(crate) async fn select_log(pool: &Pool<Postgres>,
    selector: LogSelector,
    ids: Option<&[i32]>,
    device_ids: Option<&[Uuid]>,
    model_ids: Option<&[Uuid]>,
    tag: Option<i16>
) -> Result<Vec<LogSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            SystemLog::Id,
            SystemLog::Timestamp,
            SystemLog::DeviceId,
            SystemLog::ModelId,
            SystemLog::Tag,
            SystemLog::Value,
            SystemLog::Type
        ])
        .from(SystemLog::Table)
        .to_owned();

    if let Some(ids) = ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col(SystemLog::Id).eq(ids[0])).to_owned();
        } else {
            stmt = stmt.and_where(Expr::col(SystemLog::Id).is_in(ids.to_vec())).to_owned();
        }
    }
    if let Some(ids) = device_ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((SystemLog::Table, SystemLog::DeviceId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((SystemLog::Table, SystemLog::DeviceId)).is_in(ids.to_vec())).to_owned();
        }
    }
    if let Some(ids) = model_ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((SystemLog::Table, SystemLog::ModelId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((SystemLog::Table, SystemLog::ModelId)).is_in(ids.to_vec())).to_owned();
        }
    }
    if let Some(t) = tag {
        stmt = stmt.and_where(Expr::col(SystemLog::Tag).eq(t)).to_owned();
    }

    match selector {
        LogSelector::Time(timestamp) => {
            stmt = stmt.and_where(Expr::col(SystemLog::Timestamp).eq(timestamp)).to_owned();
        },
        LogSelector::Latest(timestamp) => {
            stmt = stmt.and_where(Expr::col(SystemLog::Timestamp).gt(timestamp))
                .order_by(SystemLog::Timestamp, Order::Asc)
                .to_owned();
        },
        LogSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col(SystemLog::Timestamp).gte(begin))
                .and_where(Expr::col(SystemLog::Timestamp).lte(end))
                .order_by(SystemLog::Timestamp, Order::Asc)
                .to_owned();
        },
        LogSelector::First(number, offset) => {
            stmt = stmt
                .order_by((SystemLog::Table, SystemLog::Id), Order::Asc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        LogSelector::Last(number, offset) => {
            stmt = stmt
                .order_by((SystemLog::Table, SystemLog::Id), Order::Desc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        LogSelector::None => {}
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(5);
            let type_ = DataType::from(row.get::<i16,_>(6));
            LogSchema {
                id: row.get(0),
                timestamp: row.get(1),
                device_id: row.get(2),
                model_id: row.get(3),
                tag: row.get(4),
                value: DataValue::from_bytes(&bytes, type_)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn insert_log(pool: &Pool<Postgres>,
    timestamp: DateTime<Utc>,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    value: DataValue,
    tag: Option<i16>
) -> Result<i32, Error>
{
    let bytes = value.to_bytes();
    let type_ = i16::from(value.get_type());
    let tag = tag.unwrap_or(Tag::DEFAULT);

    let (sql, values) = Query::insert()
        .into_table(SystemLog::Table)
        .columns([
            SystemLog::Timestamp,
            SystemLog::DeviceId,
            SystemLog::ModelId,
            SystemLog::Tag,
            SystemLog::Value,
            SystemLog::Type
        ])
        .values([
            timestamp.into(),
            device_id.into(),
            model_id.into(),
            tag.into(),
            bytes.into(),
            type_.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(SystemLog::Id)))
        .from(SystemLog::Table)
        .to_string(PostgresQueryBuilder);
    let id: i32 = sqlx::query(&sql)
        .map(|row: PgRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_log(pool: &Pool<Postgres>,
    id: Option<i32>,
    timestamp: Option<DateTime<Utc>>,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    value: Option<DataValue>,
    tag: Option<i16>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(SystemLog::Table)
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(SystemLog::Id).eq(id)).to_owned();
    }
    if let Some(timestamp) = timestamp {
        stmt = stmt.and_where(Expr::col(SystemLog::Timestamp).eq(timestamp)).to_owned();
        if let Some(device_id) = device_id {
            stmt = stmt.and_where(Expr::col(SystemLog::DeviceId).eq(device_id)).to_owned();
        }
        if let Some(model_id) = model_id {
            stmt = stmt.and_where(Expr::col(SystemLog::ModelId).eq(model_id)).to_owned();
        }
        if let Some(tag) = tag {
            stmt = stmt.and_where(Expr::col(SystemLog::Tag).eq(tag)).to_owned();
        }
    }

    if let (Some(tag), None) = (tag, timestamp) {
        stmt = stmt.value(SystemLog::Tag, tag).to_owned();
    }
    if let Some(value) = value {
        let bytes = value.to_bytes();
        let type_ = i16::from(value.get_type());
        stmt = stmt
            .value(SystemLog::Value, bytes)
            .value(SystemLog::Type, type_)
            .to_owned();
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_log(pool: &Pool<Postgres>,
    id: Option<i32>,
    timestamp: Option<DateTime<Utc>>,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    tag: Option<i16>
) -> Result<(), Error>
{
    let mut stmt = Query::delete()
        .from_table(SystemLog::Table)
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(SystemLog::Id).eq(id)).to_owned();
    }
    if let Some(timestamp) = timestamp {
        stmt = stmt.and_where(Expr::col(SystemLog::Timestamp).eq(timestamp)).to_owned();
        if let Some(device_id) = device_id {
            stmt = stmt.and_where(Expr::col(SystemLog::DeviceId).eq(device_id)).to_owned();
        }
        if let Some(model_id) = model_id {
            stmt = stmt.and_where(Expr::col(SystemLog::ModelId).eq(model_id)).to_owned();
        }
        if let Some(tag) = tag {
            stmt = stmt.and_where(Expr::col(SystemLog::Tag).eq(tag)).to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
