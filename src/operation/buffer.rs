use sqlx::{Pool, Row, Error};
use sqlx::mysql::{MySql, MySqlRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{MysqlQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;

use crate::schema::value::{DataIndexing, DataType, DataValue, ArrayDataValue};
use crate::schema::model::{Model, ModelType};
use crate::schema::data::DataModel;
use crate::schema::buffer::{Buffer, BufferBytesSchema, BufferSchema};
use crate::operation::data;

enum BufferSelector {
    Id(u32),
    First(u32),
    Last(u32)
}

async fn select_buffer_bytes(pool: &Pool<MySql>, 
    selector: BufferSelector,
    device_id: Option<u64>,
    model_id: Option<u32>,
    status: Option<&str>
) -> Result<Vec<BufferBytesSchema>, Error>
{
    let (order, number) = match selector {
        BufferSelector::Id(_) => (Order::Asc, 1),
        BufferSelector::First(number) => (Order::Asc, number),
        BufferSelector::Last(number) => (Order::Desc, number)
    };

    let mut stmt = Query::select().to_owned();
    stmt = stmt
        .columns([
            Buffer::Id,
            Buffer::DeviceId,
            Buffer::ModelId,
            Buffer::Timestamp,
            Buffer::Data,
            Buffer::Status,
            Buffer::Index
        ])
        .from(Buffer::Table)
        .to_owned();
    if let BufferSelector::Id(id) = selector {
        stmt = stmt.and_where(Expr::col(Buffer::Id).eq(id)).to_owned();
    } else {
        stmt = stmt
            .order_by(Buffer::Id, order)
            .limit(number.into())
            .to_owned();
    }
    if let Some(id) = device_id {
        stmt = stmt.and_where(Expr::col(Buffer::DeviceId).eq(id)).to_owned();
    }
    if let Some(id) = model_id {
        stmt = stmt.and_where(Expr::col(Buffer::ModelId).eq(id)).to_owned();
    }
    if let Some(stat) = status {
        stmt = stmt.and_where(Expr::col(Buffer::Status).eq(stat)).to_owned();
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: MySqlRow| {
            BufferBytesSchema {
                id: row.get(0),
                device_id: row.get(1),
                model_id: row.get(2),
                timestamp: row.get(3),
                index: row.try_get(6).ok(),
                bytes: row.get(4),
                status: row.get(5)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_model_buffer(pool: &Pool<MySql>,
    model_id_vec: Vec<u32>
) -> Result<Vec<DataModel>, Error>
{
    // get unique_id_vec from input model_id_vec
    let mut unique_id_vec = model_id_vec.clone();
    unique_id_vec.sort();
    unique_id_vec.dedup();

    let (sql, values) = Query::select()
        .columns([
            (Model::Table, Model::ModelId),
            (Model::Table, Model::Indexing)
        ])
        .column((ModelType::Table, ModelType::Type))
        .from(Model::Table)
        .inner_join(ModelType::Table,
            Expr::col((Model::Table, Model::ModelId))
            .equals((ModelType::Table, ModelType::ModelId))
        )
        .and_where(Expr::col((Model::Table, Model::ModelId)).is_in(unique_id_vec))
        .order_by((ModelType::Table, ModelType::ModelId), Order::Asc)
        .order_by((ModelType::Table, ModelType::Index), Order::Asc)
        .build_sqlx(MysqlQueryBuilder);

    unique_id_vec = Vec::new();
    let mut data_model = DataModel::default();
    let mut data_model_vec = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: MySqlRow| {
            let model_id: u32 = row.get(0);
            // on every new id found add unique_id_vec and update data_model indexing
            if unique_id_vec.iter().filter(|&&el| el == model_id).count() == 0 {
                unique_id_vec.push(model_id);
                data_model.id = model_id;
                data_model.indexing = DataIndexing::from_str(row.get(1));
                data_model.types = Vec::new();
                // insert new data_model to data_model_vec
                data_model_vec.push(data_model.clone());
            }
            // add a type to data_model types
            data_model.types.push(DataType::from_str(row.get(2)));
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

pub(crate) async fn select_buffer_by_id(pool: &Pool<MySql>, 
    id: u32
) -> Result<BufferSchema, Error>
{
    let selector = BufferSelector::Id(id);
    let bytes = select_buffer_bytes(pool, selector, None, None, None).await?;
    let bytes = bytes.into_iter().next().ok_or(Error::RowNotFound)?;
    let model = data::select_data_model(pool, bytes.model_id).await?;
    Ok(bytes.to_buffer_schema(&model.types))
}

pub(crate) async fn select_buffer_first(pool: &Pool<MySql>, 
    number: u32,
    device_id: Option<u64>,
    model_id: Option<u32>,
    status: Option<&str>
) -> Result<Vec<BufferSchema>, Error>
{
    let selector = BufferSelector::First(number);
    let bytes = select_buffer_bytes(pool, selector, device_id, model_id, status).await?;
    let model_id_vec: Vec<u32> = bytes.iter().map(|el| el.model_id).collect();
    let models = select_model_buffer(pool, model_id_vec).await?;
    Ok(
        bytes.into_iter().enumerate().map(|(i, buf)| {
            buf.to_buffer_schema(&models[i].types)
        }).collect()
    )
}

pub(crate) async fn select_buffer_last(pool: &Pool<MySql>, 
    number: u32,
    device_id: Option<u64>,
    model_id: Option<u32>,
    status: Option<&str>
) -> Result<Vec<BufferSchema>, Error>
{
    let selector = BufferSelector::Last(number);
    let bytes = select_buffer_bytes(pool, selector, device_id, model_id, status).await?;
    let model_id_vec: Vec<u32> = bytes.iter().map(|el| el.model_id).collect();
    let models = select_model_buffer(pool, model_id_vec).await?;
    Ok(
        bytes.into_iter().enumerate().map(|(i, buf)| {
            buf.to_buffer_schema(&models[i].types)
        }).collect()
    )
}

pub(crate) async fn insert_buffer(pool: &Pool<MySql>,
    device_id: u64,
    model_id: u32,
    timestamp: DateTime<Utc>,
    index: Option<u16>,
    data: Vec<DataValue>,
    status: &str
) -> Result<u32, Error>
{
    let bytes = ArrayDataValue::from_vec(&data).to_bytes();

    let (sql, values) = Query::insert()
        .into_table(Buffer::Table)
        .columns([
            Buffer::DeviceId,
            Buffer::ModelId,
            Buffer::Timestamp,
            Buffer::Index,
            Buffer::Data,
            Buffer::Status
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp.into(),
            index.unwrap_or_default().into(),
            bytes.into(),
            status.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(Buffer::Id)))
        .from(Buffer::Table)
        .to_string(MysqlQueryBuilder);
    let id: u32 = sqlx::query(&sql)
        .map(|row: MySqlRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_buffer(pool: &Pool<MySql>,
    id: u32,
    data: Option<Vec<DataValue>>,
    status: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(Buffer::Table)
        .to_owned();

    if let Some(value) = data {
        let bytes = ArrayDataValue::from_vec(&value).to_bytes();
        stmt = stmt.value(Buffer::Data, bytes).to_owned();
    }
    if let Some(value) = status {
        stmt = stmt.value(Buffer::Status, value).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(Buffer::Id).eq(id))
        .build_sqlx(MysqlQueryBuilder);

    println!("{}", &sql);
    println!("{:?}", values);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_buffer(pool: &Pool<MySql>,
    id: u32
) -> Result<(), Error>
{
    let (sql, values) = Query::delete()
        .from_table(Buffer::Table)
        .and_where(Expr::col(Buffer::Id).eq(id))
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
