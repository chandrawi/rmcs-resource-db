use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order, Condition};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataType, DataValue, ArrayDataValue};
use crate::schema::model::Model;
use crate::schema::data::{Data, DataModel, DataSchema, DatasetSchema};
use crate::schema::set::SetMap;

enum DataSelector {
    Time(DateTime<Utc>),
    Last(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    NumberBefore(DateTime<Utc>, u32),
    NumberAfter(DateTime<Utc>, u32)
}

async fn select_data(pool: &Pool<Postgres>, 
    selector: DataSelector,
    device_id: Uuid,
    model_id: Uuid
) -> Result<Vec<DataSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (Data::Table, Data::Timestamp),
            (Data::Table, Data::Data)
        ])
        .column((Model::Table, Model::DataType))
        .from(Data::Table)
        .inner_join(Model::Table, 
            Expr::col((Data::Table, Data::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .and_where(Expr::col((Data::Table, Data::DeviceId)).eq(device_id))
        .and_where(Expr::col((Data::Table, Data::ModelId)).eq(model_id))
        .to_owned();
    match selector {
        DataSelector::Time(time) => {
            stmt = stmt.and_where(Expr::col((Data::Table, Data::Timestamp)).eq(time)).to_owned();
        },
        DataSelector::Last(last) => {
            stmt = stmt.and_where(Expr::col((Data::Table, Data::Timestamp)).gt(last))
            .order_by((Data::Table, Data::Timestamp), Order::Asc)
            .to_owned();
        },
        DataSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(begin))
                .and_where(Expr::col((Data::Table, Data::Timestamp)).lte(end))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .to_owned();
        },
        DataSelector::NumberBefore(time, limit) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).lte(time))
                .order_by((Data::Table, Data::Timestamp), Order::Desc)
                .limit(limit.into())
                .to_owned();
        },
        DataSelector::NumberAfter(time, limit) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(time))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .limit(limit.into())
                .to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(1);
            let types: Vec<DataType> = row.get::<Vec<u8>,_>(2).into_iter().map(|ty| ty.into()).collect();
            DataSchema {
                device_id: device_id.clone(),
                model_id: model_id.clone(),
                timestamp: row.get(0),
                data: ArrayDataValue::from_bytes(&bytes, &types).to_vec()
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_data_model(pool: &Pool<Postgres>,
    model_id: Uuid
) -> Result<DataModel, Error>
{
    let (sql, values) = Query::select()
        .column((Model::Table, Model::DataType))
        .from(Model::Table)
        .and_where(Expr::col((Model::Table, Model::ModelId)).eq(model_id))
        .build_sqlx(PostgresQueryBuilder);

    let data_type = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            row.get::<Vec<u8>,_>(0).into_iter().map(|ty| ty.into()).collect()
        })
        .fetch_one(pool)
        .await?;

    Ok(DataModel { id: model_id, data_type })
}

pub(crate) async fn select_data_by_time(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    timestamp: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    select_data(pool, DataSelector::Time(timestamp), device_id, model_id).await
}

pub(crate) async fn select_data_by_last_time(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    last: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    select_data(pool, DataSelector::Last(last), device_id, model_id).await
}

pub(crate) async fn select_data_by_range_time(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    begin: DateTime<Utc>,
    end: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    select_data(pool, DataSelector::Range(begin, end), device_id, model_id).await
}

pub(crate) async fn select_data_by_number_before(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    before: DateTime<Utc>,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    select_data(pool, DataSelector::NumberBefore(before, number), device_id, model_id).await
}

pub(crate) async fn select_data_by_number_after(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    after: DateTime<Utc>,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    select_data(pool, DataSelector::NumberAfter(after, number), device_id, model_id).await
}

pub(crate) async fn insert_data(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    timestamp: DateTime<Utc>,
    data: Vec<DataValue>
) -> Result<(), Error>
{
    let model = select_data_model(pool, model_id).await?;
    let converted_values = ArrayDataValue::from_vec(&data).convert(&model.data_type);
    let bytes = match converted_values {
        Some(value) => value.to_bytes(),
        None => return Err(Error::RowNotFound)
    };

    let stmt = Query::insert()
        .into_table(Data::Table)
        .columns([
            Data::DeviceId,
            Data::ModelId,
            Data::Timestamp,
            Data::Data
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp.into(),
            bytes.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_data(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    timestamp: DateTime<Utc>
) -> Result<(), Error>
{
    let stmt = Query::delete()
        .from_table(Data::Table)
        .and_where(Expr::col(Data::DeviceId).eq(device_id))
        .and_where(Expr::col(Data::ModelId).eq(model_id))
        .and_where(Expr::col(Data::Timestamp).eq(timestamp))
        .to_owned();
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

async fn select_dataset(pool: &Pool<Postgres>, 
    selector: DataSelector,
    set_id: Uuid
) -> Result<(Vec<DataSchema>, Vec<DatasetSchema>), Error>
{
    let mut stmt = Query::select()
        .columns([
            (Data::Table, Data::DeviceId),
            (Data::Table, Data::ModelId),
            (Data::Table, Data::Timestamp),
            (Data::Table, Data::Data)
        ])
        .column((Model::Table, Model::DataType))
        .columns([
            (SetMap::Table, SetMap::DataIndex),
            (SetMap::Table, SetMap::SetPosition),
            (SetMap::Table, SetMap::SetNumber)
        ])
        .from(Data::Table)
        .inner_join(Model::Table, 
            Expr::col((Data::Table, Data::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .inner_join(SetMap::Table, 
            Condition::all()
            .add(Expr::col((Data::Table, Data::DeviceId)).equals((SetMap::Table, SetMap::DeviceId)))
            .add(Expr::col((Data::Table, Data::ModelId)).equals((SetMap::Table, SetMap::ModelId)))
        )
        .and_where(Expr::col((SetMap::Table, SetMap::SetId)).eq(set_id))
        .to_owned();
    match selector {
        DataSelector::Time(time) => {
            stmt = stmt.and_where(Expr::col((Data::Table, Data::Timestamp)).eq(time)).to_owned();
        },
        DataSelector::Last(last) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gt(last))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .to_owned();
        },
        DataSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(begin))
                .and_where(Expr::col((Data::Table, Data::Timestamp)).lte(end))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .to_owned();
        },
        DataSelector::NumberBefore(time, limit) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).lte(time))
                .order_by((Data::Table, Data::Timestamp), Order::Desc)
                .limit(limit.into())
                .to_owned();
        },
        DataSelector::NumberAfter(time, limit) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(time))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .limit(limit.into())
                .to_owned();
        }
    }
    let (sql, values) = stmt
        .order_by((SetMap::Table, SetMap::SetPosition), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut data_schema_vec: Vec<DataSchema> = Vec::new();
    let mut dataset_schema_vec: Vec<DatasetSchema> = Vec::new();
    let mut last_timestamp: Option<DateTime<Utc>> = None;

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            // construct data_schema and insert it to data_schema_vec
            let bytes: Vec<u8> = row.get(3);
            let types: Vec<DataType> = row.get::<Vec<u8>,_>(4).into_iter().map(|ty| ty.into()).collect();
            let data_schema = DataSchema {
                device_id: row.get(0),
                model_id: row.get(1),
                timestamp: row.get(2),
                data: ArrayDataValue::from_bytes(&bytes, &types).to_vec()
            };
            data_schema_vec.push(data_schema.clone());
            // get last dataset_schema in dataset_schema_vec
            let mut dataset_schema = dataset_schema_vec.pop().unwrap_or_default();
            // on every new timestamp found insert new dataset_schema to dataset_schema_vec
            if last_timestamp != Some(data_schema.timestamp) {
                if last_timestamp != None {
                    dataset_schema_vec.push(dataset_schema.clone());
                }
                // initialize dataset_schema data vector with Null
                let number: i16 = row.get(7);
                dataset_schema = DatasetSchema::default();
                for _i in 0..number {
                    dataset_schema.data.push(DataValue::Null);
                }
            }
            dataset_schema.set_id = set_id;
            dataset_schema.timestamp = data_schema.timestamp;
            let indexes: Vec<u8> = row.get(5);
            let position: i16 = row.get(6);
            // filter data vector by dataset data indexes of particular model
            // and replace dataset_schema data vector on the set position with filtered data vector
            for (position_offset, index) in indexes.into_iter().enumerate() {
                dataset_schema.data[position as usize + position_offset] = 
                    data_schema.data.get(index as usize).map(|value| value.to_owned()).unwrap_or_default()
            }
            last_timestamp = Some(data_schema.timestamp);
            // update dataset_schema_vec with updated dataset_schema
            dataset_schema_vec.push(dataset_schema);
        })
        .fetch_all(pool)
        .await?;

    Ok((data_schema_vec, dataset_schema_vec))
}

pub(crate) async fn select_data_by_set_time(pool: &Pool<Postgres>,
    set_id: Uuid,
    timestamp: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::Time(timestamp), set_id).await;
    result.map(|schemas| schemas.0)
}

pub(crate) async fn select_data_by_set_last_time(pool: &Pool<Postgres>,
    set_id: Uuid,
    last: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::Last(last), set_id).await;
    result.map(|schemas| schemas.0)
}

pub(crate) async fn select_data_by_set_range_time(pool: &Pool<Postgres>,
    set_id: Uuid,
    begin: DateTime<Utc>,
    end: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::Range(begin, end), set_id).await;
    result.map(|schemas| schemas.0)
}

pub(crate) async fn select_data_by_set_number_before(pool: &Pool<Postgres>,
    set_id: Uuid,
    before: DateTime<Utc>,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::NumberBefore(before, number), set_id).await;
    result.map(|schemas| schemas.0)
}

pub(crate) async fn select_data_by_set_number_after(pool: &Pool<Postgres>,
    set_id: Uuid,
    after: DateTime<Utc>,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::NumberAfter(after, number), set_id).await;
    result.map(|schemas| schemas.0)
}

pub(crate) async fn select_dataset_by_time(pool: &Pool<Postgres>,
    set_id: Uuid,
    timestamp: DateTime<Utc>
) -> Result<Vec<DatasetSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::Time(timestamp), set_id).await;
    result.map(|schemas| schemas.1)
}

pub(crate) async fn select_dataset_by_last_time(pool: &Pool<Postgres>,
    set_id: Uuid,
    last: DateTime<Utc>
) -> Result<Vec<DatasetSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::Last(last), set_id).await;
    result.map(|schemas| schemas.1)
}

pub(crate) async fn select_dataset_by_range_time(pool: &Pool<Postgres>,
    set_id: Uuid,
    begin: DateTime<Utc>,
    end: DateTime<Utc>
) -> Result<Vec<DatasetSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::Range(begin, end), set_id).await;
    result.map(|schemas| schemas.1)
}

pub(crate) async fn select_dataset_by_number_before(pool: &Pool<Postgres>,
    set_id: Uuid,
    before: DateTime<Utc>,
    number: u32
) -> Result<Vec<DatasetSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::NumberBefore(before, number), set_id).await;
    result.map(|schemas| schemas.1)
}

pub(crate) async fn select_dataset_by_number_after(pool: &Pool<Postgres>,
    set_id: Uuid,
    after: DateTime<Utc>,
    number: u32
) -> Result<Vec<DatasetSchema>, Error>
{
    let result = select_dataset(pool, DataSelector::NumberAfter(after, number), set_id).await;
    result.map(|schemas| schemas.1)
}
