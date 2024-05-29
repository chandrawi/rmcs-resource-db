use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataType, DataValue, ArrayDataValue};
use crate::schema::model::Model;
use crate::schema::data::DataModel;
use crate::schema::buffer::{DataBuffer, BufferSchema, BufferStatus};
use crate::operation::data::select_data_model;

enum BufferSelector {
    Id(i32),
    Time,
    First(u32),
    Last(u32)
}

async fn select_buffer(pool: &Pool<Postgres>, 
    selector: BufferSelector,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    timestamp: Option<DateTime<Utc>>,
    status: Option<BufferStatus>
) -> Result<Vec<BufferSchema>, Error>
{
    let (order, number) = match selector {
        BufferSelector::Id(_) => (Order::Asc, 1),
        BufferSelector::First(number) => (Order::Asc, number),
        BufferSelector::Last(number) => (Order::Desc, number),
        BufferSelector::Time => (Order::Asc, 1)
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
    if let BufferSelector::Id(id) = selector {
        stmt = stmt.and_where(Expr::col(DataBuffer::Id).eq(id)).to_owned();
    } else {
        stmt = stmt
            .order_by((DataBuffer::Table, DataBuffer::Id), order)
            .limit(number.into())
            .to_owned();
    }
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

pub(crate) async fn select_buffer_model(pool: &Pool<Postgres>,
    buffer_id: i32
) -> Result<DataModel, Error>
{
    let (sql, values) = Query::select()
        .columns([
            (Model::Table, Model::ModelId),
            (Model::Table, Model::DataType)
        ])
        .from(DataBuffer::Table)
        .inner_join(Model::Table,
            Expr::col((DataBuffer::Table, DataBuffer::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .and_where(Expr::col((DataBuffer::Table, DataBuffer::Id)).eq(buffer_id))
        .build_sqlx(PostgresQueryBuilder);

    let (model_id, data_type) = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {(
            row.get(0),
            row.get::<Vec<u8>,_>(1).into_iter().map(|ty| ty.into()).collect()
        )})
        .fetch_one(pool)
        .await?;

    Ok(DataModel { id: model_id, data_type })
}

pub(crate) async fn select_buffer_by_id(pool: &Pool<Postgres>, 
    id: i32
) -> Result<BufferSchema, Error>
{
    let selector = BufferSelector::Id(id);
    let buffers = select_buffer(pool, selector, None, None, None, None).await?;
    Ok(buffers.into_iter().next().ok_or(Error::RowNotFound)?)
}

pub(crate) async fn select_buffer_by_time(pool: &Pool<Postgres>, 
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    status: Option<BufferStatus>
) -> Result<BufferSchema, Error>
{
    let selector = BufferSelector::Time;
    let buffers = select_buffer(pool, selector, Some(device_id), Some(model_id), Some(timestamp), status).await?;
    Ok(buffers.into_iter().next().ok_or(Error::RowNotFound)?)
}

pub(crate) async fn select_buffer_first(pool: &Pool<Postgres>, 
    number: u32,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    status: Option<BufferStatus>
) -> Result<Vec<BufferSchema>, Error>
{
    let selector = BufferSelector::First(number);
    let buffers = select_buffer(pool, selector, device_id, model_id, None, status).await?;
    Ok(buffers)
}

pub(crate) async fn select_buffer_last(pool: &Pool<Postgres>, 
    number: u32,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    status: Option<BufferStatus>
) -> Result<Vec<BufferSchema>, Error>
{
    let selector = BufferSelector::Last(number);
    let buffers = select_buffer(pool, selector, device_id, model_id, None, status).await?;
    Ok(buffers)
}

pub(crate) async fn insert_buffer(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    data: Vec<DataValue>,
    status: BufferStatus
) -> Result<i32, Error>
{
    let model = select_data_model(pool, model_id).await?;
    let converted_values = ArrayDataValue::from_vec(&data).convert(&model.data_type);
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
            model.id.into(),
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
        let model = select_buffer_model(pool, id).await?;
        let converted_values = ArrayDataValue::from_vec(&value).convert(&model.data_type);
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
