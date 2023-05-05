use sqlx::{Pool, Row, Error};
use sqlx::mysql::{MySql, MySqlRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{MysqlQueryBuilder, Query, Expr, Order};
use sea_query_binder::SqlxBinder;

use crate::schema::value::{DataIndexing, DataType, DataValue, ArrayDataValue};
use crate::schema::model::{Model, ModelType};
use crate::schema::data::{
    DataTimestamp, DataTimestampIndex, DataTimestampMicros, 
    DataModel, DataBytesSchema, DataSchema
};

enum DataSelector {
    Time(DateTime<Utc>),
    Last(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    NumberBefore(DateTime<Utc>, u32),
    NumberAfter(DateTime<Utc>, u32)
}

async fn select_data_bytes(pool: &Pool<MySql>, 
    indexing: DataIndexing,
    selector: DataSelector,
    device_id: u64,
    model_id: u32,
    index: Option<u16>
) -> Result<Vec<DataBytesSchema>, Error>
{
    let mut stmt = Query::select().to_owned();
    match indexing {
        DataIndexing::Timestamp => {
            stmt = stmt
                .columns([
                    DataTimestamp::DeviceId,
                    DataTimestamp::ModelId,
                    DataTimestamp::Timestamp,
                    DataTimestamp::Data
                ])
                .from(DataTimestamp::Table)
                .and_where(Expr::col(DataTimestamp::DeviceId).eq(device_id))
                .and_where(Expr::col(DataTimestamp::ModelId).eq(model_id))
                .to_owned();
        },
        DataIndexing::TimestampIndex => {
            stmt = stmt
                .columns([
                    DataTimestampIndex::DeviceId,
                    DataTimestampIndex::ModelId,
                    DataTimestampIndex::Timestamp,
                    DataTimestampIndex::Data,
                    DataTimestampIndex::Index
                ])
                .from(DataTimestampIndex::Table)
                .and_where(Expr::col(DataTimestampIndex::DeviceId).eq(device_id))
                .and_where(Expr::col(DataTimestampIndex::ModelId).eq(model_id))
                .to_owned();
        },
        DataIndexing::TimestampMicros => {
            stmt = stmt
                .columns([
                    DataTimestampMicros::DeviceId,
                    DataTimestampMicros::ModelId,
                    DataTimestampMicros::Timestamp,
                    DataTimestampMicros::Data
                ])
                .from(DataTimestampMicros::Table)
                .and_where(Expr::col(DataTimestampMicros::DeviceId).eq(device_id))
                .and_where(Expr::col(DataTimestampMicros::ModelId).eq(model_id))
                .to_owned();
        }
    }
    match (indexing, selector) {
        (DataIndexing::Timestamp, DataSelector::Time(time)) => {
            stmt = stmt.and_where(Expr::col(DataTimestamp::Timestamp).eq(time)).to_owned();
        },
        (DataIndexing::TimestampIndex, DataSelector::Time(time)) => {
            stmt = stmt.and_where(Expr::col(DataTimestampIndex::Timestamp).eq(time)).to_owned();
            if let Some(i) = index {
                stmt = stmt.and_where(Expr::col(DataTimestampIndex::Index).eq(i)).to_owned();
            }
        },
        (DataIndexing::TimestampMicros, DataSelector::Time(time)) => {
            stmt = stmt.and_where(Expr::col(DataTimestampMicros::Timestamp).eq(time)).to_owned();
        },
        (DataIndexing::Timestamp, DataSelector::Last(last)) => {
            stmt = stmt.and_where(Expr::col(DataTimestamp::Timestamp).gt(last)).to_owned();
        },
        (DataIndexing::TimestampIndex, DataSelector::Last(last)) => {
            stmt = stmt.and_where(Expr::col(DataTimestampIndex::Timestamp).gt(last)).to_owned();
        },
        (DataIndexing::TimestampMicros, DataSelector::Last(last)) => {
            stmt = stmt.and_where(Expr::col(DataTimestampMicros::Timestamp).gt(last)).to_owned();
        },
        (DataIndexing::Timestamp, DataSelector::Range(begin, end)) => {
            stmt = stmt
                .and_where(Expr::col(DataTimestamp::Timestamp).gte(begin))
                .and_where(Expr::col(DataTimestamp::Timestamp).lte(end))
                .to_owned();
        },
        (DataIndexing::TimestampIndex, DataSelector::Range(begin, end)) => {
            stmt = stmt
                .and_where(Expr::col(DataTimestampIndex::Timestamp).gte(begin))
                .and_where(Expr::col(DataTimestampIndex::Timestamp).lte(end))
                .to_owned();
        },
        (DataIndexing::TimestampMicros, DataSelector::Range(begin, end)) => {
            stmt = stmt
                .and_where(Expr::col(DataTimestampMicros::Timestamp).gte(begin))
                .and_where(Expr::col(DataTimestampMicros::Timestamp).lte(end))
                .to_owned();
        },
        (DataIndexing::Timestamp, DataSelector::NumberBefore(time, limit)) => {
            stmt = stmt
                .and_where(Expr::col(DataTimestamp::Timestamp).lte(time))
                .order_by(DataTimestamp::Timestamp, Order::Desc)
                .limit(limit.into())
                .to_owned();
        },
        (DataIndexing::TimestampIndex, DataSelector::NumberBefore(time, limit)) => {
            stmt = stmt
                .and_where(Expr::col(DataTimestampIndex::Timestamp).lte(time))
                .order_by(DataTimestampIndex::Timestamp, Order::Desc)
                .limit(limit.into())
                .to_owned();
        },
        (DataIndexing::TimestampMicros, DataSelector::NumberBefore(time, limit)) => {
            stmt = stmt
                .and_where(Expr::col(DataTimestampMicros::Timestamp).lte(time))
                .order_by(DataTimestampMicros::Timestamp, Order::Desc)
                .limit(limit.into())
                .to_owned();
        },
        (DataIndexing::Timestamp, DataSelector::NumberAfter(time, limit)) => {
            stmt = stmt
                .and_where(Expr::col(DataTimestamp::Timestamp).gte(time))
                .order_by(DataTimestamp::Timestamp, Order::Asc)
                .limit(limit.into())
                .to_owned();
        },
        (DataIndexing::TimestampIndex, DataSelector::NumberAfter(time, limit)) => {
            stmt = stmt
                .and_where(Expr::col(DataTimestampIndex::Timestamp).gte(time))
                .order_by(DataTimestampIndex::Timestamp, Order::Asc)
                .limit(limit.into())
                .to_owned();
        },
        (DataIndexing::TimestampMicros, DataSelector::NumberAfter(time, limit)) => {
            stmt = stmt
                .and_where(Expr::col(DataTimestampMicros::Timestamp).gte(time))
                .order_by(DataTimestampMicros::Timestamp, Order::Asc)
                .limit(limit.into())
                .to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: MySqlRow| {
            DataBytesSchema {
                device_id: row.get(0),
                model_id: row.get(1),
                timestamp: row.get(2),
                index: row.try_get(4).ok(),
                bytes: row.get(3)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_data_model(pool: &Pool<MySql>,
    model_id: u32
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
        .build_sqlx(MysqlQueryBuilder);

    let mut data_type = DataModel { id: model_id, indexing: DataIndexing::Timestamp, types: Vec::new() };

    sqlx::query_with(&sql, values)
        .map(|row: MySqlRow| {
            data_type.indexing = DataIndexing::from_str(row.get(0));
            data_type.types.push(DataType::from_str(row.get(1)))
        })
        .fetch_all(pool)
        .await?;

    Ok(data_type)
}

pub(crate) async fn select_data_by_time(pool: &Pool<MySql>,
    model: DataModel,
    device_id: u64,
    timestamp: DateTime<Utc>,
    index: Option<u16>
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::Time(timestamp);
    let bytes = select_data_bytes(pool, model.indexing, selector, device_id, model.id, index).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn select_data_by_last_time(pool: &Pool<MySql>,
    model: DataModel,
    device_id: u64,
    last: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::Last(last);
    let bytes = select_data_bytes(pool, model.indexing, selector, device_id, model.id, None).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn select_data_by_range_time(pool: &Pool<MySql>,
    model: DataModel,
    device_id: u64,
    begin: DateTime<Utc>,
    end: DateTime<Utc>
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::Range(begin, end);
    let bytes = select_data_bytes(pool, model.indexing, selector, device_id, model.id, None).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn select_data_by_number_before(pool: &Pool<MySql>,
    model: DataModel,
    device_id: u64,
    before: DateTime<Utc>,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::NumberBefore(before, number);
    let bytes = select_data_bytes(pool, model.indexing, selector, device_id, model.id, None).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn select_data_by_number_after(pool: &Pool<MySql>,
    model: DataModel,
    device_id: u64,
    after: DateTime<Utc>,
    number: u32
) -> Result<Vec<DataSchema>, Error>
{
    let selector = DataSelector::NumberAfter(after, number);
    let bytes = select_data_bytes(pool, model.indexing, selector, device_id, model.id, None).await?;
    Ok(
        bytes.into_iter().map(|el| el.to_data_schema(&model.types)).collect()
    )
}

pub(crate) async fn insert_data(pool: &Pool<MySql>,
    model: DataModel,
    device_id: u64,
    timestamp: DateTime<Utc>,
    index: Option<u16>,
    data: Vec<DataValue>
) -> Result<(), Error>
{
    let bytes = ArrayDataValue::from_vec(&data).to_bytes();

    let mut stmt = Query::insert().to_owned();
        match model.indexing {
            DataIndexing::Timestamp => {
                stmt = stmt
                    .into_table(DataTimestamp::Table)
                    .columns([
                        DataTimestamp::DeviceId,
                        DataTimestamp::ModelId,
                        DataTimestamp::Timestamp,
                        DataTimestamp::Data
                    ])
                    .values([
                        device_id.into(),
                        model.id.into(),
                        timestamp.into(),
                        bytes.into()
                    ])
                    .unwrap_or(&mut sea_query::InsertStatement::default())
                    .to_owned();
            },
            DataIndexing::TimestampIndex => {
                stmt = stmt
                    .into_table(DataTimestampIndex::Table)
                    .columns([
                        DataTimestampIndex::DeviceId,
                        DataTimestampIndex::ModelId,
                        DataTimestampIndex::Timestamp,
                        DataTimestampIndex::Index,
                        DataTimestampIndex::Data
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
            },
            DataIndexing::TimestampMicros => {
                stmt = stmt
                    .into_table(DataTimestampMicros::Table)
                    .columns([
                        DataTimestampMicros::DeviceId,
                        DataTimestampMicros::ModelId,
                        DataTimestampMicros::Timestamp,
                        DataTimestampMicros::Data
                    ])
                    .values([
                        device_id.into(),
                        model.id.into(),
                        timestamp.into(),
                        bytes.into()
                    ])
                    .unwrap_or(&mut sea_query::InsertStatement::default())
                    .to_owned();
            }
        }
        let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_data(pool: &Pool<MySql>,
    model: DataModel,
    device_id: u64,
    timestamp: DateTime<Utc>,
    index: Option<u16>,
) -> Result<(), Error>
{
    let mut stmt = Query::delete().to_owned();
    match model.indexing {
        DataIndexing::Timestamp => {
            stmt = stmt
                .from_table(DataTimestamp::Table)
                .and_where(Expr::col(DataTimestamp::DeviceId).eq(device_id))
                .and_where(Expr::col(DataTimestamp::ModelId).eq(model.id))
                .and_where(Expr::col(DataTimestamp::Timestamp).eq(timestamp))
                .to_owned();
        },
        DataIndexing::TimestampIndex => {
            stmt = stmt
                .from_table(DataTimestampIndex::Table)
                .and_where(Expr::col(DataTimestampIndex::DeviceId).eq(device_id))
                .and_where(Expr::col(DataTimestampIndex::ModelId).eq(model.id))
                .and_where(Expr::col(DataTimestampIndex::Timestamp).eq(timestamp))
                .and_where(Expr::col(DataTimestampIndex::Timestamp).eq(index.unwrap_or_default()))
                .to_owned();
        },
        DataIndexing::TimestampMicros => {
            stmt = stmt
                .from_table(DataTimestampMicros::Table)
                .and_where(Expr::col(DataTimestampMicros::DeviceId).eq(device_id))
                .and_where(Expr::col(DataTimestampMicros::ModelId).eq(model.id))
                .and_where(Expr::col(DataTimestampMicros::Timestamp).eq(timestamp))
                .to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
