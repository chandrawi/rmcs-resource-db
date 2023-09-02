use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Func, Order};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::slice::{DataSlice, SliceSchema};

enum SliceSelector {
    Id(i32),
    Name(String),
    Device(Uuid),
    Model(Uuid),
    DeviceModel(Uuid, Uuid)
}

async fn select_slice(pool: &Pool<Postgres>,
    selector: SliceSelector
) -> Result<Vec<SliceSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            DataSlice::Id,
            DataSlice::DeviceId,
            DataSlice::ModelId,
            DataSlice::TimestampBegin,
            DataSlice::TimestampEnd,
            DataSlice::IndexBegin,
            DataSlice::IndexEnd,
            DataSlice::Name,
            DataSlice::Description
        ])
        .from(DataSlice::Table)
        .to_owned();
    match selector {
        SliceSelector::Id(id) => {
            stmt = stmt.and_where(Expr::col(DataSlice::Id).eq(id)).to_owned();
        },
        SliceSelector::Name(name) => {
            stmt = stmt.and_where(Expr::col(DataSlice::Name).like(name)).to_owned();
        },
        SliceSelector::Device(id) => {
            stmt = stmt.and_where(Expr::col(DataSlice::DeviceId).eq(id)).to_owned();
        },
        SliceSelector::Model(id) => {
            stmt = stmt.and_where(Expr::col(DataSlice::ModelId).eq(id)).to_owned();
        },
        SliceSelector::DeviceModel(device, model) => {
            stmt = stmt
                .and_where(Expr::col(DataSlice::DeviceId).eq(device))
                .and_where(Expr::col(DataSlice::ModelId).eq(model))
                .to_owned();
        }
    }
    let (sql, values) = stmt
        .order_by(DataSlice::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            SliceSchema {
                id: row.get(0),
                device_id: row.get(1),
                model_id: row.get(2),
                timestamp_begin: row.get(3),
                timestamp_end: row.get(4),
                index_begin: row.get(5),
                index_end: row.get(6),
                name: row.get(7),
                description: row.get(8)
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
    select_slice(pool, SliceSelector::Id(id)).await?.into_iter().next()
        .ok_or(Error::RowNotFound)
}

pub(crate) async fn select_slice_by_name(pool: &Pool<Postgres>,
    name: &str
) -> Result<Vec<SliceSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_slice(pool, SliceSelector::Name(String::from(name_like))).await
}

pub(crate) async fn select_slice_by_device(pool: &Pool<Postgres>,
    device_id: Uuid
) -> Result<Vec<SliceSchema>, Error>
{
    select_slice(pool, SliceSelector::Device(device_id)).await
}

pub(crate) async fn select_slice_by_model(pool: &Pool<Postgres>,
    model_id: Uuid
) -> Result<Vec<SliceSchema>, Error>
{
    select_slice(pool, SliceSelector::Model(model_id)).await
}

pub(crate) async fn select_slice_by_device_model(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid
) -> Result<Vec<SliceSchema>, Error>
{
    select_slice(pool, SliceSelector::DeviceModel(device_id, model_id)).await
}

pub(crate) async fn insert_slice(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid,
    timestamp_begin: DateTime<Utc>,
    timestamp_end: DateTime<Utc>,
    index_begin: Option<i32>,
    index_end: Option<i32>,
    name: &str,
    description: Option<&str>
) -> Result<i32, Error>
{
    let (sql, values) = Query::insert()
        .into_table(DataSlice::Table)
        .columns([
            DataSlice::DeviceId,
            DataSlice::ModelId,
            DataSlice::TimestampBegin,
            DataSlice::TimestampEnd,
            DataSlice::IndexBegin,
            DataSlice::IndexEnd,
            DataSlice::Name,
            DataSlice::Description
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp_begin.into(),
            timestamp_end.into(),
            index_begin.into(),
            index_end.into(),
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(DataSlice::Id)))
        .from(DataSlice::Table)
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
    index_begin: Option<i32>,
    index_end: Option<i32>,
    name: Option<&str>,
    description: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(DataSlice::Table)
        .to_owned();

    if let Some(timestamp) = timestamp_begin {
        stmt = stmt.value(DataSlice::TimestampBegin, timestamp).to_owned();
    }
    if let Some(timestamp) = timestamp_end {
        stmt = stmt.value(DataSlice::TimestampEnd, timestamp).to_owned();
    }
    if let Some(index) = index_begin {
        stmt = stmt.value(DataSlice::IndexBegin, index).to_owned();
    }
    if let Some(index) = index_end {
        stmt = stmt.value(DataSlice::IndexEnd, index).to_owned();
    }
    if let Some(name) = name {
        stmt = stmt.value(DataSlice::Name, name).to_owned();
    }
    if let Some(description) = description {
        stmt = stmt.value(DataSlice::Description, description).to_owned();
    }
    let (sql, values) = stmt
        .and_where(Expr::col(DataSlice::Id).eq(id))
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
        .from_table(DataSlice::Table)
        .and_where(Expr::col(DataSlice::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
