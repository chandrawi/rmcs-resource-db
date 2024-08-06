use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Func, Order};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::slice::{SliceData, SliceDataSet, SliceSchema, SliceSetSchema};

pub(crate) enum SliceSelector {
    Time(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    None
}

pub(crate) async fn select_slice(pool: &Pool<Postgres>,
    selector: SliceSelector,
    id: Option<i32>,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    name: Option<&str>
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

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(SliceData::Id).eq(id)).to_owned();
    }
    else {
        if let Some(id) = device_id {
            stmt = stmt.and_where(Expr::col(SliceData::DeviceId).eq(id)).to_owned();
        }
        if let Some(id) = model_id {
            stmt = stmt.and_where(Expr::col(SliceData::ModelId).eq(id)).to_owned();
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col(SliceData::Name).like(name_like)).to_owned();
        }
        match selector {
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
        stmt = stmt.order_by(SliceData::Id, Order::Asc).to_owned();
    }

    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

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

pub(crate) async fn select_slice_set(pool: &Pool<Postgres>,
    selector: SliceSelector,
    id: Option<i32>,
    set_id: Option<Uuid>,
    name: Option<&str>
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

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(SliceDataSet::Id).eq(id)).to_owned();
    }
    else {
        if let Some(id) = set_id {
            stmt = stmt.and_where(Expr::col(SliceDataSet::SetId).eq(id)).to_owned();
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col(SliceDataSet::Name).like(name_like)).to_owned();
        }
        match selector {
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
        stmt = stmt.order_by(SliceDataSet::Id, Order::Asc).to_owned();
    }

    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

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
        .expr(Func::max(Expr::col(SliceDataSet::Id)))
        .from(SliceDataSet::Table)
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
