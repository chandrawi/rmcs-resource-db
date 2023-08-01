use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::NaiveDateTime;
use sea_query::{PostgresQueryBuilder, Query, Expr, Order};
use sea_query_binder::SqlxBinder;

use crate::schema::value::{DataIndexing, DataType, DataValue, ArrayDataValue};
use crate::schema::model::{Model, ModelType};
use crate::schema::data::{
    Data, DataModel, DataBytesSchema, DataSchema
};

enum DataSelector {
    Time(NaiveDateTime),
    Last(NaiveDateTime),
    Range(NaiveDateTime, NaiveDateTime),
    NumberBefore(NaiveDateTime, u32),
    NumberAfter(NaiveDateTime, u32)
}

async fn select_data_bytes(pool: &Pool<Postgres>, 
    selector: DataSelector,
    device_id: i64,
    model_id: i32,
    index: Option<i32>
) -> Result<Vec<DataBytesSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            Data::DeviceId,
            Data::ModelId,
            Data::Timestamp,
            Data::Data,
            Data::Index
        ])
        .from(Data::Table)
        .and_where(Expr::col(Data::DeviceId).eq(device_id))
        .and_where(Expr::col(Data::ModelId).eq(model_id))
        .to_owned();
    match selector {
        DataSelector::Time(time) => {
            stmt = stmt.and_where(Expr::col(Data::Timestamp).eq(time)).to_owned();
            if let Some(i) = index {
                stmt = stmt.and_where(Expr::col(Data::Index).eq(i)).to_owned();
            }
        },
        DataSelector::Last(last) => {
            stmt = stmt.and_where(Expr::col(Data::Timestamp).gt(last)).to_owned();
        },
        DataSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col(Data::Timestamp).gte(begin))
                .and_where(Expr::col(Data::Timestamp).lte(end))
                .to_owned();
        },
        DataSelector::NumberBefore(time, limit) => {
            stmt = stmt
                .and_where(Expr::col(Data::Timestamp).lte(time))
                .order_by(Data::Timestamp, Order::Desc)
                .limit(limit.into())
                .to_owned();
        },
        DataSelector::NumberAfter(time, limit) => {
            stmt = stmt
                .and_where(Expr::col(Data::Timestamp).gte(time))
                .order_by(Data::Timestamp, Order::Asc)
                .limit(limit.into())
                .to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            DataBytesSchema {
                device_id: row.get(0),
                model_id: row.get(1),
                timestamp: row.get(2),
                index: row.try_get(4).unwrap_or_default(),
                bytes: row.get(3)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_data_model(pool: &Pool<Postgres>,
    model_id: i32
) -> Result<DataModel, Error>
{
    let (sql, values) = Query::select()
        .column((Model::Table, Model::Indexing))
        .column((ModelType::Table, ModelType::Type))
        .from(Model::Table)
        .inner_join(ModelType::Table,
            Expr::col((Model::Table, Model::ModelId))
            .equals((ModelType::Table, ModelType::ModelId))
        )
        .and_where(Expr::col((Model::Table, Model::ModelId)).eq(model_id))
        .order_by((ModelType::Table, ModelType::Index), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut data_type = DataModel { id: model_id, indexing: DataIndexing::Timestamp, types: Vec::new() };

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            data_type.indexing = DataIndexing::from(row.get::<i16,_>(0));
            data_type.types.push(DataType::from(row.get::<i16,_>(1)))
        })
        .fetch_all(pool)
        .await?;

    Ok(data_type)
}

pub(crate) async fn select_data_by_time(pool: &Pool<Postgres>,
    model: DataModel,
    device_id: i64,
    timestamp: NaiveDateTime,
    index: Option<i32>
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::Time(timestamp);
    let bytes = select_data_bytes(pool, selector, device_id, model.id, index).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn select_data_by_last_time(pool: &Pool<Postgres>,
    model: DataModel,
    device_id: i64,
    last: NaiveDateTime
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::Last(last);
    let bytes = select_data_bytes(pool, selector, device_id, model.id, None).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn select_data_by_range_time(pool: &Pool<Postgres>,
    model: DataModel,
    device_id: i64,
    begin: NaiveDateTime,
    end: NaiveDateTime
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::Range(begin, end);
    let bytes = select_data_bytes(pool, selector, device_id, model.id, None).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn select_data_by_number_before(pool: &Pool<Postgres>,
    model: DataModel,
    device_id: i64,
    before: NaiveDateTime,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::NumberBefore(before, number);
    let bytes = select_data_bytes(pool, selector, device_id, model.id, None).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn select_data_by_number_after(pool: &Pool<Postgres>,
    model: DataModel,
    device_id: i64,
    after: NaiveDateTime,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::NumberAfter(after, number);
    let bytes = select_data_bytes(pool, selector, device_id, model.id, None).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn insert_data(pool: &Pool<Postgres>,
    model: DataModel,
    device_id: i64,
    timestamp: NaiveDateTime,
    index: Option<i32>,
    data: Vec<DataValue>
) -> Result<(), Error>
{
    let bytes = ArrayDataValue::from_vec(&data).to_bytes();

    let stmt = Query::insert()
        .into_table(Data::Table)
        .columns([
            Data::DeviceId,
            Data::ModelId,
            Data::Timestamp,
            Data::Index,
            Data::Data
        ])
        .values([
            device_id.into(),
            model.id.into(),
            timestamp.into(),
            index.unwrap_or_default().into(),
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
    model: DataModel,
    device_id: i64,
    timestamp: NaiveDateTime,
    index: Option<i32>,
) -> Result<(), Error>
{
    let stmt = Query::delete()
        .from_table(Data::Table)
        .and_where(Expr::col(Data::DeviceId).eq(device_id))
        .and_where(Expr::col(Data::ModelId).eq(model.id))
        .and_where(Expr::col(Data::Timestamp).eq(timestamp))
        .and_where(Expr::col(Data::Index).eq(index.unwrap_or_default()))
        .to_owned();
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
