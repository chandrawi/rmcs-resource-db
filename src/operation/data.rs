use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order, Condition};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataType, DataValue, ArrayDataValue};
use crate::schema::model::Model;
use crate::schema::data::{Data, DataSchema, DataSetSchema};
use crate::schema::set::SetMap;

pub(crate) enum DataSelector {
    Time(DateTime<Utc>),
    Last(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    NumberBefore(DateTime<Utc>, usize),
    NumberAfter(DateTime<Utc>, usize)
}

pub(crate) async fn select_data(pool: &Pool<Postgres>, 
    selector: DataSelector,
    device_ids: Vec<Uuid>,
    model_ids: Vec<Uuid>
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
        .to_owned();

    if device_ids.len() == 0 || model_ids.len() == 0 {
        return Ok(Vec::new());
    }
    if device_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).eq(device_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).is_in(device_ids)).to_owned();
    }
    if model_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).eq(model_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).is_in(model_ids)).to_owned();
    }

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
                .limit(limit as u64)
                .to_owned();
        },
        DataSelector::NumberAfter(time, limit) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(time))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .limit(limit as u64)
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

pub(crate) async fn select_timestamp(pool: &Pool<Postgres>,
    selector: DataSelector,
    device_ids: Vec<Uuid>,
    model_ids: Vec<Uuid>
) -> Result<Vec<DateTime<Utc>>, Error>
{
    let mut stmt = Query::select()
        .column((Data::Table, Data::Timestamp))
        .from(Data::Table)
        .to_owned();

    if device_ids.len() == 0 || model_ids.len() == 0 {
        return Ok(Vec::new());
    }
    if device_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).eq(device_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).is_in(device_ids)).to_owned();
    }
    if model_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).eq(model_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).is_in(model_ids)).to_owned();
    }

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
        }
        _ => {}
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let mut rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            row.get(0)
        })
        .fetch_all(pool)
        .await?;
    rows.dedup();

    Ok(rows)
}

pub(crate) async fn select_data_types(pool: &Pool<Postgres>,
    model_id: Uuid
) -> Result<Vec<DataType>, Error>
{
    let (sql, values) = Query::select()
        .column((Model::Table, Model::DataType))
        .from(Model::Table)
        .and_where(Expr::col((Model::Table, Model::ModelId)).eq(model_id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            row.get::<Vec<u8>,_>(0).into_iter().map(|ty| ty.into()).collect()
        })
        .fetch_one(pool)
        .await
}

pub(crate) async fn insert_data(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    data: Vec<DataValue>
) -> Result<(), Error>
{
    let types = select_data_types(pool, model_id).await?;
    let converted_values = ArrayDataValue::from_vec(&data).convert(&types);
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
    device_id: Uuid,
    model_id: Uuid,
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

pub(crate) async fn select_data_set(pool: &Pool<Postgres>, 
    selector: DataSelector,
    set_id: Uuid
) -> Result<(Vec<DataSchema>, Vec<DataSetSchema>), Error>
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
                .limit(limit as u64)
                .to_owned();
        },
        DataSelector::NumberAfter(time, limit) => {
            stmt = stmt
                .and_where(Expr::col((Data::Table, Data::Timestamp)).gte(time))
                .order_by((Data::Table, Data::Timestamp), Order::Asc)
                .limit(limit as u64)
                .to_owned();
        }
    }
    let (sql, values) = stmt
        .order_by((SetMap::Table, SetMap::SetPosition), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut data_schema_vec: Vec<DataSchema> = Vec::new();
    let mut data_set_schema_vec: Vec<DataSetSchema> = Vec::new();
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
            // get last data_set_schema in data_set_schema_vec
            let mut data_set_schema = data_set_schema_vec.pop().unwrap_or_default();
            // on every new timestamp found insert new data_set_schema to data_set_schema_vec
            if last_timestamp != Some(data_schema.timestamp) {
                if last_timestamp != None {
                    data_set_schema_vec.push(data_set_schema.clone());
                }
                // initialize data_set_schema data vector with Null
                let number: i16 = row.get(7);
                data_set_schema = DataSetSchema::default();
                for _i in 0..number {
                    data_set_schema.data.push(DataValue::Null);
                }
            }
            data_set_schema.set_id = set_id;
            data_set_schema.timestamp = data_schema.timestamp;
            let indexes: Vec<u8> = row.get(5);
            let position: i16 = row.get(6);
            // filter data vector by data_set data indexes of particular model
            // and replace data_set_schema data vector on the set position with filtered data vector
            for (position_offset, index) in indexes.into_iter().enumerate() {
                data_set_schema.data[position as usize + position_offset] = 
                    data_schema.data.get(index as usize).map(|value| value.to_owned()).unwrap_or_default()
            }
            last_timestamp = Some(data_schema.timestamp);
            // update data_set_schema_vec with updated data_set_schema
            data_set_schema_vec.push(data_set_schema);
        })
        .fetch_all(pool)
        .await?;

    Ok((data_schema_vec, data_set_schema_vec))
}

pub(crate) async fn select_timestamp_set(pool: &Pool<Postgres>,
    selector: DataSelector,
    set_id: Uuid
) -> Result<Vec<DateTime<Utc>>, Error>
{
    let mut stmt = Query::select()
        .column((Data::Table, Data::Timestamp))
        .from(Data::Table)
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
        }
        _ => {}
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let mut timestamps: Vec<DateTime<Utc>> = Vec::new();
    let mut last_timestamp: Option<DateTime<Utc>> = None;

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let timestamp = row.get(0);
            if Some(timestamp) != last_timestamp {
                timestamps.push(timestamp);
            }
            last_timestamp = Some(timestamp);
        })
        .fetch_all(pool)
        .await?;

    Ok(timestamps)
}

pub(crate) async fn count_data(pool: &Pool<Postgres>,
    selector: DataSelector,
    device_id: Uuid,
    model_id: Uuid
) -> Result<usize, Error>
{
    let mut stmt = Query::select()
        .expr(Expr::col(Data::Timestamp).count())
        .from(Data::Table)
        .and_where(Expr::col(Data::DeviceId).eq(device_id))
        .and_where(Expr::col(Data::ModelId).eq(model_id))
        .to_owned();

    match selector {
        DataSelector::Last(last) => {
            stmt = stmt.and_where(Expr::col(Data::Timestamp).gt(last)).to_owned();
        },
        DataSelector::Range(begin, end) => {
            stmt = stmt
                .and_where(Expr::col(Data::Timestamp).gte(begin))
                .and_where(Expr::col(Data::Timestamp).lte(end))
                .to_owned();
        },
        _ => {}
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
