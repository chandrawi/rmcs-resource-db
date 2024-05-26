use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataType, DataValue, ArrayDataValue};
use crate::schema::model::Model;
use crate::schema::data::{Data, DataModel, DataSchema};

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
            (Data::Table, Data::DeviceId),
            (Data::Table, Data::ModelId),
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
            stmt = stmt.and_where(Expr::col((Data::Table, Data::Timestamp)).gt(last)).to_owned();
        },
        DataSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(begin))
                .and_where(Expr::col((Data::Table, Data::Timestamp)).lte(end))
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
            let bytes: Vec<u8> = row.get(3);
            let types: Vec<DataType> = row.get::<Vec<u8>,_>(4).into_iter().map(|ty| ty.into()).collect();
            DataSchema {
                device_id: row.get(0),
                model_id: row.get(1),
                timestamp: row.get(2),
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
    let selector = DataSelector::Time(timestamp);
    Ok(
        select_data(pool, selector, device_id, model_id).await?
    )
}

pub(crate) async fn select_data_by_last_time(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    last: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::Last(last);
    Ok(
        select_data(pool, selector, device_id, model_id).await?
    )
}

pub(crate) async fn select_data_by_range_time(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    begin: DateTime<Utc>,
    end: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::Range(begin, end);
    Ok(
        select_data(pool, selector, device_id, model_id).await?
    )
}

pub(crate) async fn select_data_by_number_before(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    before: DateTime<Utc>,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::NumberBefore(before, number);
    Ok(
        select_data(pool, selector, device_id, model_id).await?
    )
}

pub(crate) async fn select_data_by_number_after(pool: &Pool<Postgres>,
    model_id: Uuid,
    device_id: Uuid,
    after: DateTime<Utc>,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::NumberAfter(after, number);
    Ok(
        select_data(pool, selector, device_id, model_id).await?
    )
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
