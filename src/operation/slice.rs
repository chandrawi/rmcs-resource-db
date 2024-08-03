use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Func, Order};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::slice::{SliceData, SliceDataSet, SliceSchema, SliceSetSchema};

enum SliceSelector {
    Id(i32),
    Time(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    None
}

async fn select_slice(pool: &Pool<Postgres>,
    selector: SliceSelector,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    name: Option<String>
) -> Result<Vec<SliceSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            SliceData::Id,
            SliceData::DeviceId,
            SliceData::ModelId,
            SliceData::TimestampBegin,
            SliceData::TimestampEnd,
            SliceData::Name,
            SliceData::Description
        ])
        .from(SliceData::Table)
        .to_owned();
    if let Some(id) = device_id {
        stmt = stmt.and_where(Expr::col(SliceData::DeviceId).eq(id)).to_owned();
    }
    if let Some(id) = model_id {
        stmt = stmt.and_where(Expr::col(SliceData::ModelId).eq(id)).to_owned();
    }
    if let Some(name) = name {
        stmt = stmt.and_where(Expr::col(SliceData::Name).like(name)).to_owned();
    }
    match selector {
        SliceSelector::Id(id) => {
            stmt = stmt.and_where(Expr::col(SliceData::Id).eq(id)).to_owned();
        },
        SliceSelector::Time(time) => {
            stmt = stmt
                .and_where(Expr::col(SliceData::TimestampBegin).lte(time))
                .and_where(Expr::col(SliceData::TimestampEnd).gte(time))
                .to_owned();
        },
        SliceSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col(SliceData::TimestampBegin).gte(begin))
                .and_where(Expr::col(SliceData::TimestampEnd).lte(end))
                .to_owned();
        }
        SliceSelector::None => {}
    }
    let (sql, values) = stmt
        .order_by(SliceData::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            SliceSchema {
                id: row.get(0),
                device_id: row.get(1),
                model_id: row.get(2),
                timestamp_begin: row.get(3),
                timestamp_end: row.get(4),
                name: row.get(5),
                description: row.get(6)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_slice_by_id(pool: &Pool<Postgres>,
    id: i32
) -> Result<SliceSchema, Error>
{
    select_slice(pool, SliceSelector::Id(id), None, None, None).await?.into_iter().next()
        .ok_or(Error::RowNotFound)
}

pub(crate) async fn select_slice_by_time(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>
) -> Result<Vec<SliceSchema>, Error>
{
    select_slice(pool, SliceSelector::Time(timestamp), Some(device_id), Some(model_id), None).await
}

pub(crate) async fn select_slice_by_range_time(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid,
    begin: DateTime<Utc>,
    end: DateTime<Utc>
) -> Result<Vec<SliceSchema>, Error>
{
    select_slice(pool, SliceSelector::Range(begin, end), Some(device_id), Some(model_id), None).await
}

pub(crate) async fn select_slice_by_name_time(pool: &Pool<Postgres>,
    name: &str,
    timestamp: DateTime<Utc>
) -> Result<Vec<SliceSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_slice(pool, SliceSelector::Time(timestamp), None, None, Some(name_like)).await
}

pub(crate) async fn select_slice_by_name_range_time(pool: &Pool<Postgres>,
    name: &str,
    begin: DateTime<Utc>,
    end: DateTime<Utc>
) -> Result<Vec<SliceSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_slice(pool, SliceSelector::Range(begin, end), None, None, Some(name_like)).await
}

pub(crate) async fn select_slice_by_option(pool: &Pool<Postgres>,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    name: Option<&str>,
    begin_or_timestamp: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>
) -> Result<Vec<SliceSchema>, Error>
{
    let name_like = name.map(|s| String::from("%") + s + "%");
    let selector = match (begin_or_timestamp, end) {
        (Some(begin), Some(end)) => SliceSelector::Range(begin, end),
        (Some(timestamp), None) => SliceSelector::Time(timestamp),
        _ => SliceSelector::None
    };
    select_slice(pool, selector, device_id, model_id, name_like).await
}

pub(crate) async fn insert_slice(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid,
    timestamp_begin: DateTime<Utc>,
    timestamp_end: DateTime<Utc>,
    name: &str,
    description: Option<&str>
) -> Result<i32, Error>
{
    let (sql, values) = Query::insert()
        .into_table(SliceData::Table)
        .columns([
            SliceData::DeviceId,
            SliceData::ModelId,
            SliceData::TimestampBegin,
            SliceData::TimestampEnd,
            SliceData::Name,
            SliceData::Description
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp_begin.into(),
            timestamp_end.into(),
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(SliceData::Id)))
        .from(SliceData::Table)
        .to_string(PostgresQueryBuilder);
    let id: i32 = sqlx::query(&sql)
        .map(|row: PgRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_slice(pool: &Pool<Postgres>,
    id: i32,
    timestamp_begin: Option<DateTime<Utc>>,
    timestamp_end: Option<DateTime<Utc>>,
    name: Option<&str>,
    description: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(SliceData::Table)
        .to_owned();

    if let Some(timestamp) = timestamp_begin {
        stmt = stmt.value(SliceData::TimestampBegin, timestamp).to_owned();
    }
    if let Some(timestamp) = timestamp_end {
        stmt = stmt.value(SliceData::TimestampEnd, timestamp).to_owned();
    }
    if let Some(name) = name {
        stmt = stmt.value(SliceData::Name, name).to_owned();
    }
    if let Some(description) = description {
        stmt = stmt.value(SliceData::Description, description).to_owned();
    }
    let (sql, values) = stmt
        .and_where(Expr::col(SliceData::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_slice(pool: &Pool<Postgres>,
    id: i32
) -> Result<(), Error>
{
    let (sql, values) = Query::delete()
        .from_table(SliceData::Table)
        .and_where(Expr::col(SliceData::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

async fn select_slice_set(pool: &Pool<Postgres>,
    selector: SliceSelector,
    set_id: Option<Uuid>,
    name: Option<String>
) -> Result<Vec<SliceSetSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            SliceDataSet::Id,
            SliceDataSet::SetId,
            SliceDataSet::TimestampBegin,
            SliceDataSet::TimestampEnd,
            SliceDataSet::Name,
            SliceDataSet::Description
        ])
        .from(SliceDataSet::Table)
        .to_owned();
    if let Some(id) = set_id {
        stmt = stmt.and_where(Expr::col(SliceDataSet::SetId).eq(id)).to_owned();
    }
    if let Some(name) = name {
        stmt = stmt.and_where(Expr::col(SliceDataSet::Name).like(name)).to_owned();
    }
    match selector {
        SliceSelector::Id(id) => {
            stmt = stmt.and_where(Expr::col(SliceDataSet::Id).eq(id)).to_owned();
        },
        SliceSelector::Time(time) => {
            stmt = stmt
                .and_where(Expr::col(SliceDataSet::TimestampBegin).lte(time))
                .and_where(Expr::col(SliceDataSet::TimestampEnd).gte(time))
                .to_owned();
        },
        SliceSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col(SliceDataSet::TimestampBegin).gte(begin))
                .and_where(Expr::col(SliceDataSet::TimestampEnd).lte(end))
                .to_owned();
        }
        SliceSelector::None => {}
    }
    let (sql, values) = stmt
        .order_by(SliceData::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            SliceSetSchema {
                id: row.get(0),
                set_id: row.get(1),
                timestamp_begin: row.get(2),
                timestamp_end: row.get(3),
                name: row.get(4),
                description: row.get(5)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_slice_set_by_id(pool: &Pool<Postgres>,
    id: i32
) -> Result<SliceSetSchema, Error>
{
    select_slice_set(pool, SliceSelector::Id(id), None, None).await?.into_iter().next()
        .ok_or(Error::RowNotFound)
}

pub(crate) async fn select_slice_set_by_time(pool: &Pool<Postgres>,
    set_id: Uuid,
    timestamp: DateTime<Utc>
) -> Result<Vec<SliceSetSchema>, Error>
{
    select_slice_set(pool, SliceSelector::Time(timestamp), Some(set_id), None).await
}

pub(crate) async fn select_slice_set_by_range_time(pool: &Pool<Postgres>,
    set_id: Uuid,
    begin: DateTime<Utc>,
    end: DateTime<Utc>
) -> Result<Vec<SliceSetSchema>, Error>
{
    select_slice_set(pool, SliceSelector::Range(begin, end), Some(set_id), None).await
}

pub(crate) async fn select_slice_set_by_name_time(pool: &Pool<Postgres>,
    name: &str,
    timestamp: DateTime<Utc>
) -> Result<Vec<SliceSetSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_slice_set(pool, SliceSelector::Time(timestamp), None, Some(name_like)).await
}

pub(crate) async fn select_slice_set_by_name_range_time(pool: &Pool<Postgres>,
    name: &str,
    begin: DateTime<Utc>,
    end: DateTime<Utc>
) -> Result<Vec<SliceSetSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_slice_set(pool, SliceSelector::Range(begin, end), None, Some(name_like)).await
}

pub(crate) async fn select_slice_set_by_option(pool: &Pool<Postgres>,
    set_id: Option<Uuid>,
    name: Option<&str>,
    begin_or_timestamp: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>
) -> Result<Vec<SliceSetSchema>, Error>
{
    let name_like = name.map(|s| String::from("%") + s + "%");
    let selector = match (begin_or_timestamp, end) {
        (Some(begin), Some(end)) => SliceSelector::Range(begin, end),
        (Some(timestamp), None) => SliceSelector::Time(timestamp),
        _ => SliceSelector::None
    };
    select_slice_set(pool, selector, set_id, name_like).await
}

pub(crate) async fn insert_slice_set(pool: &Pool<Postgres>,
    set_id: Uuid,
    timestamp_begin: DateTime<Utc>,
    timestamp_end: DateTime<Utc>,
    name: &str,
    description: Option<&str>
) -> Result<i32, Error>
{
    let (sql, values) = Query::insert()
        .into_table(SliceDataSet::Table)
        .columns([
            SliceDataSet::SetId,
            SliceDataSet::TimestampBegin,
            SliceDataSet::TimestampEnd,
            SliceDataSet::Name,
            SliceDataSet::Description
        ])
        .values([
            set_id.into(),
            timestamp_begin.into(),
            timestamp_end.into(),
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(SliceData::Id)))
        .from(SliceData::Table)
        .to_string(PostgresQueryBuilder);
    let id: i32 = sqlx::query(&sql)
        .map(|row: PgRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_slice_set(pool: &Pool<Postgres>,
    id: i32,
    timestamp_begin: Option<DateTime<Utc>>,
    timestamp_end: Option<DateTime<Utc>>,
    name: Option<&str>,
    description: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(SliceDataSet::Table)
        .to_owned();

    if let Some(timestamp) = timestamp_begin {
        stmt = stmt.value(SliceDataSet::TimestampBegin, timestamp).to_owned();
    }
    if let Some(timestamp) = timestamp_end {
        stmt = stmt.value(SliceDataSet::TimestampEnd, timestamp).to_owned();
    }
    if let Some(name) = name {
        stmt = stmt.value(SliceDataSet::Name, name).to_owned();
    }
    if let Some(description) = description {
        stmt = stmt.value(SliceDataSet::Description, description).to_owned();
    }
    let (sql, values) = stmt
        .and_where(Expr::col(SliceDataSet::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_slice_set(pool: &Pool<Postgres>,
    id: i32
) -> Result<(), Error>
{
    let (sql, values) = Query::delete()
        .from_table(SliceDataSet::Table)
        .and_where(Expr::col(SliceDataSet::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
