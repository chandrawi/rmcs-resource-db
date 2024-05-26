use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataType, DataValue, ArrayDataValue};
use crate::schema::model::Model;
use crate::schema::data::DataModel;
use crate::schema::buffer::{DataBuffer, BufferBytesSchema, BufferSchema, BufferStatus};
use crate::operation::data;

enum BufferSelector {
    Id(i32),
    Time,
    First(u32),
    Last(u32)
}

async fn select_buffer_bytes(pool: &Pool<Postgres>, 
    selector: BufferSelector,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    timestamp: Option<DateTime<Utc>>,
    status: Option<BufferStatus>
) -> Result<Vec<BufferBytesSchema>, Error>
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
            DataBuffer::Id,
            DataBuffer::DeviceId,
            DataBuffer::ModelId,
            DataBuffer::Timestamp,
            DataBuffer::Data,
            DataBuffer::Status
        ])
        .from(DataBuffer::Table)
        .to_owned();
    if let BufferSelector::Id(id) = selector {
        stmt = stmt.and_where(Expr::col(DataBuffer::Id).eq(id)).to_owned();
    } else {
        stmt = stmt
            .order_by(DataBuffer::Id, order)
            .limit(number.into())
            .to_owned();
    }
    if let Some(id) = device_id {
        stmt = stmt.and_where(Expr::col(DataBuffer::DeviceId).eq(id)).to_owned();
    }
    if let Some(id) = model_id {
        stmt = stmt.and_where(Expr::col(DataBuffer::ModelId).eq(id)).to_owned();
    }
    if let Some(ts) = timestamp {
        stmt = stmt.and_where(Expr::col(DataBuffer::Timestamp).eq(ts)).to_owned();
    }
    if let Some(stat) = status {
        let status = i16::from(stat);
        stmt = stmt.and_where(Expr::col(DataBuffer::Status).eq(status)).to_owned();
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            BufferBytesSchema {
                id: row.get(0),
                device_id: row.get(1),
                model_id: row.get(2),
                timestamp: row.get(3),
                bytes: row.get(4),
                status: BufferStatus::from(row.get::<i16,_>(5))
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_model_buffer(pool: &Pool<Postgres>,
    model_id_vec: Vec<Uuid>
) -> Result<Vec<DataModel>, Error>
{
    // get unique_id_vec from input model_id_vec
    let mut unique_id_vec = model_id_vec.clone();
    unique_id_vec.sort();
    unique_id_vec.dedup();

    let (sql, values) = Query::select()
        .columns([
            (Model::Table, Model::ModelId),
            (Model::Table, Model::DataType)
        ])
        .from(Model::Table)
        .and_where(Expr::col((Model::Table, Model::ModelId)).is_in(unique_id_vec))
        .order_by((Model::Table, Model::ModelId), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    unique_id_vec = Vec::new();
    let mut data_model = DataModel::default();
    let mut data_model_vec = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let model_id: Uuid = row.get(0);
            // on every new id found add unique_id_vec
            if unique_id_vec.iter().filter(|&&el| el == model_id).count() == 0 {
                unique_id_vec.push(model_id);
                data_model.id = model_id;
                data_model.types = Vec::new();
                // insert new data_model to data_model_vec
                data_model_vec.push(data_model.clone());
            }
            // add a type to data_model types
            data_model.types.push(DataType::from(row.get::<i16,_>(1)));
            // update data_model_vec with updated data_model
            data_model_vec.pop();
            data_model_vec.push(data_model.clone());

        })
        .fetch_all(pool)
        .await?;

    let index_map = model_id_vec.iter()
        .map(|&id| {
            unique_id_vec.iter().position(|&unique| unique == id)
        });
    // map data_model in data_model_vec coming from database using index_map
    let data_model_map = index_map
        .map(|index| {
            match index {
                Some(i) => data_model_vec[i].clone(),
                None => DataModel::default()
            }
        })
        .collect();

    Ok(data_model_map)
}

pub(crate) async fn select_buffer_by_id(pool: &Pool<Postgres>, 
    id: i32
) -> Result<BufferSchema, Error>
{
    let selector = BufferSelector::Id(id);
    let bytes = select_buffer_bytes(pool, selector, None, None, None, None).await?;
    let bytes = bytes.into_iter().next().ok_or(Error::RowNotFound)?;
    let model = data::select_data_model(pool, bytes.model_id).await?;
    Ok(bytes.to_buffer_schema(&model.types))
}

pub(crate) async fn select_buffer_by_time(pool: &Pool<Postgres>, 
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    status: Option<BufferStatus>
) -> Result<BufferSchema, Error>
{
    let selector = BufferSelector::Time;
    let bytes = select_buffer_bytes(pool, selector, Some(device_id), Some(model_id), Some(timestamp), status).await?;
    let bytes = bytes.into_iter().next().ok_or(Error::RowNotFound)?;
    let model = data::select_data_model(pool, bytes.model_id).await?;
    Ok(bytes.to_buffer_schema(&model.types))
}

pub(crate) async fn select_buffer_first(pool: &Pool<Postgres>, 
    number: u32,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    status: Option<BufferStatus>
) -> Result<Vec<BufferSchema>, Error>
{
    let selector = BufferSelector::First(number);
    let bytes = select_buffer_bytes(pool, selector, device_id, model_id, None, status).await?;
    let model_id_vec: Vec<Uuid> = bytes.iter().map(|el| el.model_id).collect();
    let models = select_model_buffer(pool, model_id_vec).await?;
    Ok(
        bytes.into_iter().enumerate().map(|(i, buf)| {
            buf.to_buffer_schema(&models[i].types)
        }).collect()
    )
}

pub(crate) async fn select_buffer_last(pool: &Pool<Postgres>, 
    number: u32,
    device_id: Option<Uuid>,
    model_id: Option<Uuid>,
    status: Option<BufferStatus>
) -> Result<Vec<BufferSchema>, Error>
{
    let selector = BufferSelector::Last(number);
    let bytes = select_buffer_bytes(pool, selector, device_id, model_id, None, status).await?;
    let model_id_vec: Vec<Uuid> = bytes.iter().map(|el| el.model_id).collect();
    let models = select_model_buffer(pool, model_id_vec).await?;
    Ok(
        bytes.into_iter().enumerate().map(|(i, buf)| {
            buf.to_buffer_schema(&models[i].types)
        }).collect()
    )
}

pub(crate) async fn insert_buffer(pool: &Pool<Postgres>,
    device_id: Uuid,
    model: DataModel,
    timestamp: DateTime<Utc>,
    data: Vec<DataValue>,
    status: BufferStatus
) -> Result<i32, Error>
{
    let converted_values = ArrayDataValue::from_vec(&data).convert(&model.types);
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
        let bytes = ArrayDataValue::from_vec(&value).to_bytes();
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
