use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataType, DataValue, ArrayDataValue};
use crate::schema::model::Model;
use crate::schema::buffer::{DataBuffer, BufferSchema, BufferStatus};
use crate::operation::data::select_data_types;

pub(crate) enum BufferSelector {
    Time(DateTime<Utc>),
    First(usize, usize),
    Last(usize, usize),
    None
}

pub(crate) async fn select_buffer(pool: &Pool<Postgres>, 
    selector: BufferSelector,
    id: Option<i32>,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    status: Option<BufferStatus>
) -> Result<Vec<BufferSchema>, Error>
{
    let (timestamp, order, number, offset) = match selector {
        BufferSelector::First(number, offset) => (None, Order::Asc, number, offset),
        BufferSelector::Last(number, offset) => (None, Order::Desc, number, offset),
        BufferSelector::Time(ts) => (Some(ts), Order::Asc, 1, 0),
        _ => (None, Order::Asc, 1, 0)
    };

    let mut stmt = Query::select().to_owned();
    stmt = stmt
        .columns([
            (DataBuffer::Table, DataBuffer::Id),
            (DataBuffer::Table, DataBuffer::DeviceId),
            (DataBuffer::Table, DataBuffer::ModelId),
            (DataBuffer::Table, DataBuffer::Timestamp),
            (DataBuffer::Table, DataBuffer::Data),
            (DataBuffer::Table, DataBuffer::Status)
        ])
        .column((Model::Table, Model::DataType))
        .from(DataBuffer::Table)
        .inner_join(Model::Table, 
            Expr::col((DataBuffer::Table, DataBuffer::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(DataBuffer::Id).eq(id)).to_owned();
    } else {
        if let Some(id) = device_id {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).eq(id)).to_owned();
        }
        if let Some(id) = model_id {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).eq(id)).to_owned();
        }
        if let Some(ts) = timestamp {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).eq(ts)).to_owned();
        }
        if let Some(stat) = status {
            let status = i16::from(stat);
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Status)).eq(status)).to_owned();
        }
        stmt = stmt
            .order_by((DataBuffer::Table, DataBuffer::Id), order)
            .limit(number as u64)
            .offset(offset as u64)
            .to_owned();
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(4);
            let types: Vec<DataType> = row.get::<Vec<u8>,_>(6).into_iter().map(|ty| ty.into()).collect();
            BufferSchema {
                id: row.get(0),
                device_id: row.get(1),
                model_id: row.get(2),
                timestamp: row.get(3),
                data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
                status: BufferStatus::from(row.get::<i16,_>(5))
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_buffer_types(pool: &Pool<Postgres>,
    buffer_id: i32
) -> Result<Vec<DataType>, Error>
{
    let (sql, values) = Query::select()
        .columns([
            (Model::Table, Model::DataType)
        ])
        .from(DataBuffer::Table)
        .inner_join(Model::Table,
            Expr::col((DataBuffer::Table, DataBuffer::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .and_where(Expr::col((DataBuffer::Table, DataBuffer::Id)).eq(buffer_id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            row.get::<Vec<u8>,_>(0).into_iter().map(|ty| ty.into()).collect()
        })
        .fetch_one(pool)
        .await
}

pub(crate) async fn insert_buffer(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    data: Vec<DataValue>,
    status: BufferStatus
) -> Result<i32, Error>
{
    let types = select_data_types(pool, model_id).await?;
    let converted_values = ArrayDataValue::from_vec(&data).convert(&types);
    let bytes = match converted_values {
        Some(value) => value.to_bytes(),
        None => return Err(Error::RowNotFound)
    };

    let (sql, values) = Query::insert()
        .into_table(DataBuffer::Table)
        .columns([
            DataBuffer::DeviceId,
            DataBuffer::ModelId,
            DataBuffer::Timestamp,
            DataBuffer::Data,
            DataBuffer::Status
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp.into(),
            bytes.into(),
            i16::from(status).into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(DataBuffer::Id)))
        .from(DataBuffer::Table)
        .to_string(PostgresQueryBuilder);
    let id: i32 = sqlx::query(&sql)
        .map(|row: PgRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_buffer(pool: &Pool<Postgres>,
    id: i32,
    data: Option<Vec<DataValue>>,
    status: Option<BufferStatus>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(DataBuffer::Table)
        .to_owned();

    if let Some(value) = data {
        let types = select_buffer_types(pool, id).await?;
        let converted_values = ArrayDataValue::from_vec(&value).convert(&types);
        let bytes = match converted_values {
            Some(value) => value.to_bytes(),
            None => return Err(Error::RowNotFound)
        };
        stmt = stmt.value(DataBuffer::Data, bytes).to_owned();
    }
    if let Some(value) = status {
        stmt = stmt.value(DataBuffer::Status, i16::from(value)).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(DataBuffer::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_buffer(pool: &Pool<Postgres>,
    id: i32
) -> Result<(), Error>
{
    let (sql, values) = Query::delete()
        .from_table(DataBuffer::Table)
        .and_where(Expr::col(DataBuffer::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn count_buffer(pool: &Pool<Postgres>,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    status: Option<BufferStatus>
) -> Result<usize, Error>
{
    let mut stmt = Query::select()
        .expr(Expr::col(DataBuffer::Id).count())
        .from(DataBuffer::Table)
        .to_owned();

    if let Some(id) = device_id {
        stmt = stmt.and_where(Expr::col(DataBuffer::DeviceId).eq(id)).to_owned();
    }
    if let Some(id) = model_id {
        stmt = stmt.and_where(Expr::col(DataBuffer::ModelId).eq(id)).to_owned();
    }
    if let Some(stat) = status {
        let status = i16::from(stat);
        stmt = stmt.and_where(Expr::col(DataBuffer::Status).eq(status)).to_owned();
    }

    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let count: i64 = sqlx::query_with(&sql, values)
        .map(|row| {
            row.get(0)
        })
        .fetch_one(pool)
        .await?;

    Ok(count as usize)
}
