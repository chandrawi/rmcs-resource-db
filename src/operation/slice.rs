use sqlx::{Pool, Row, Error};
use sqlx::mysql::{MySql, MySqlRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{MysqlQueryBuilder, Query, Expr, Func};
use sea_query_binder::SqlxBinder;

use crate::schema::slice::{SliceData, SliceSchema};

enum SliceSelector {
    Id(u32),
    Name(String),
    Device(u64),
    Model(u32),
    DeviceModel(u64, u32)
}

async fn select_slice(pool: &Pool<MySql>,
    selector: SliceSelector
) -> Result<Vec<SliceSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            SliceData::Id,
            SliceData::DeviceId,
            SliceData::ModelId,
            SliceData::TimestampBegin,
            SliceData::TimestampEnd,
            SliceData::IndexBegin,
            SliceData::IndexEnd,
            SliceData::Name,
            SliceData::Description
        ])
        .from(SliceData::Table)
        .to_owned();
    match selector {
        SliceSelector::Id(id) => {
            stmt = stmt.and_where(Expr::col(SliceData::Id).eq(id)).to_owned();
        },
        SliceSelector::Name(name) => {
            stmt = stmt.and_where(Expr::col(SliceData::Name).like(name)).to_owned();
        },
        SliceSelector::Device(id) => {
            stmt = stmt.and_where(Expr::col(SliceData::DeviceId).eq(id)).to_owned();
        },
        SliceSelector::Model(id) => {
            stmt = stmt.and_where(Expr::col(SliceData::ModelId).eq(id)).to_owned();
        },
        SliceSelector::DeviceModel(device, model) => {
            stmt = stmt
                .and_where(Expr::col(SliceData::DeviceId).eq(device))
                .and_where(Expr::col(SliceData::ModelId).eq(model))
                .to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: MySqlRow| {
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

pub(crate) async fn select_slice_by_id(pool: &Pool<MySql>,
    id: u32
) -> Result<SliceSchema, Error>
{
    select_slice(pool, SliceSelector::Id(id)).await?.into_iter().next()
        .ok_or(Error::RowNotFound)
}

pub(crate) async fn select_slice_by_name(pool: &Pool<MySql>,
    name: &str
) -> Result<Vec<SliceSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_slice(pool, SliceSelector::Name(String::from(name_like))).await
}

pub(crate) async fn select_slice_by_device(pool: &Pool<MySql>,
    device_id: u64
) -> Result<Vec<SliceSchema>, Error>
{
    select_slice(pool, SliceSelector::Device(device_id)).await
}

pub(crate) async fn select_slice_by_model(pool: &Pool<MySql>,
    model_id: u32
) -> Result<Vec<SliceSchema>, Error>
{
    select_slice(pool, SliceSelector::Model(model_id)).await
}

pub(crate) async fn select_slice_by_device_model(pool: &Pool<MySql>,
    device_id: u64,
    model_id: u32
) -> Result<Vec<SliceSchema>, Error>
{
    select_slice(pool, SliceSelector::DeviceModel(device_id, model_id)).await
}

pub(crate) async fn insert_slice(pool: &Pool<MySql>,
    device_id: u64,
    model_id: u32,
    timestamp_begin: DateTime<Utc>,
    timestamp_end: DateTime<Utc>,
    index_begin: Option<u16>,
    index_end: Option<u16>,
    name: &str,
    description: Option<&str>
) -> Result<u32, Error>
{
    let (sql, values) = Query::insert()
        .into_table(SliceData::Table)
        .columns([
            SliceData::DeviceId,
            SliceData::ModelId,
            SliceData::TimestampBegin,
            SliceData::TimestampEnd,
            SliceData::IndexBegin,
            SliceData::IndexEnd,
            SliceData::Name,
            SliceData::Description
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
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(SliceData::Id)))
        .from(SliceData::Table)
        .to_string(MysqlQueryBuilder);
    let id: u32 = sqlx::query(&sql)
        .map(|row: MySqlRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_slice(pool: &Pool<MySql>,
    id: u32,
    timestamp_begin: Option<DateTime<Utc>>,
    timestamp_end: Option<DateTime<Utc>>,
    index_begin: Option<u16>,
    index_end: Option<u16>,
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
    if let Some(index) = index_begin {
        stmt = stmt.value(SliceData::IndexBegin, index).to_owned();
    }
    if let Some(index) = index_end {
        stmt = stmt.value(SliceData::IndexEnd, index).to_owned();
    }
    if let Some(name) = name {
        stmt = stmt.value(SliceData::Name, name).to_owned();
    }
    if let Some(description) = description {
        stmt = stmt.value(SliceData::Description, description).to_owned();
    }
    let (sql, values) = stmt
        .and_where(Expr::col(SliceData::Id).eq(id))
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_slice(pool: &Pool<MySql>,
    id: u32
) -> Result<(), Error>
{
    let (sql, values) = Query::delete()
        .from_table(SliceData::Table)
        .and_where(Expr::col(SliceData::Id).eq(id))
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
