use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sqlx::types::chrono::{DateTime, Utc};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order, Condition, Func};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataType, DataValue, ArrayDataValue};
use crate::schema::model::Model;
use crate::schema::buffer::{DataBuffer, BufferSchema};
use crate::schema::set::SetMap;
use crate::operation::data::select_data_types;
use crate::operation::model::select_tag_members;
use crate::utility::tag as Tag;
use super::{EMPTY_LENGTH_UNMATCH, DATA_TYPE_UNMATCH, MODEL_NOT_EXISTS};

pub(crate) enum BufferSelector {
    Time(DateTime<Utc>),
    TimeLast(DateTime<Utc>),
    TimeRange(DateTime<Utc>, DateTime<Utc>),
    NumberBefore(DateTime<Utc>, usize),
    NumberAfter(DateTime<Utc>, usize),
    First(usize, usize),
    Last(usize, usize),
    None
}

pub(crate) async fn select_buffer(pool: &Pool<Postgres>, 
    selector: BufferSelector,
    id: Option<i32>,
    device_ids: Option<Vec<Uuid>>,
    model_ids: Option<Vec<Uuid>>,
    tag: Option<i16>
) -> Result<Vec<BufferSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (DataBuffer::Table, DataBuffer::Id),
            (DataBuffer::Table, DataBuffer::DeviceId),
            (DataBuffer::Table, DataBuffer::ModelId),
            (DataBuffer::Table, DataBuffer::Timestamp),
            (DataBuffer::Table, DataBuffer::Tag),
            (DataBuffer::Table, DataBuffer::Data)
        ])
        .column((Model::Table, Model::DataType))
        .from(DataBuffer::Table)
        .inner_join(Model::Table, 
            Expr::col((DataBuffer::Table, DataBuffer::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(DataBuffer::Id).eq(id)).to_owned();
    }
    if let Some(ids) = device_ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).is_in(ids)).to_owned();
        }
    }
    if let Some(ids) = model_ids.clone() {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).is_in(ids)).to_owned();
        }
    }
    if let (Some(ids), Some(t)) = (model_ids, tag) {
        let tags = select_tag_members(pool, ids, t).await?;
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Tag)).is_in(tags)).to_owned();
    }

    match selector {
        BufferSelector::Time(timestamp) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).eq(timestamp)).to_owned();
        },
        BufferSelector::TimeLast(last) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gt(last))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        BufferSelector::TimeRange(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gte(begin))
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).lte(end))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        BufferSelector::NumberBefore(timestamp, number) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).lte(timestamp))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Desc)
                .limit(number as u64)
                .to_owned();
        },
        BufferSelector::NumberAfter(timestamp, number) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gte(timestamp))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .limit(number as u64)
                .to_owned();
        },
        BufferSelector::First(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Asc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        BufferSelector::Last(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Desc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        BufferSelector::None => {}
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(5);
            let types: Vec<DataType> = row.get::<Vec<u8>,_>(6).into_iter().map(|ty| ty.into()).collect();
            BufferSchema {
                id: row.get(0),
                device_id: row.get(1),
                model_id: row.get(2),
                timestamp: row.get(3),
                data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
                tag: row.get(4)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_timestamp(pool: &Pool<Postgres>,
    selector: BufferSelector,
    device_ids: Option<Vec<Uuid>>,
    model_ids: Option<Vec<Uuid>>,
    tag: Option<i16>
) -> Result<Vec<DateTime<Utc>>, Error>
{
    let mut stmt = Query::select()
        .column((DataBuffer::Table, DataBuffer::Timestamp))
        .from(DataBuffer::Table)
        .to_owned();

    if let Some(ids) = device_ids {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).is_in(ids)).to_owned();
        }
    }
    if let Some(ids) = model_ids.clone() {
        if ids.len() == 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).eq(ids[0])).to_owned();
        }
        else if ids.len() > 1 {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).is_in(ids)).to_owned();
        }
    }
    if let (Some(ids), Some(t)) = (model_ids, tag) {
        let tags = select_tag_members(pool, ids, t).await?;
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Tag)).is_in(tags)).to_owned();
    }

    match selector {
        BufferSelector::First(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Asc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        BufferSelector::Last(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Desc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
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
    tag: Option<i16>
) -> Result<i32, Error>
{
    let types_vec = select_data_types(pool, vec![model_id]).await?;
    let types = types_vec.into_iter().next().ok_or(Error::InvalidArgument(MODEL_NOT_EXISTS.to_string()))?;
    let bytes = match ArrayDataValue::from_vec(&data).convert(&types) {
        Some(value) => value.to_bytes(),
        None => return Err(Error::InvalidArgument(DATA_TYPE_UNMATCH.to_string()))
    };
    let tag = tag.unwrap_or(Tag::DEFAULT);

    let (sql, values) = Query::insert()
        .into_table(DataBuffer::Table)
        .columns([
            DataBuffer::DeviceId,
            DataBuffer::ModelId,
            DataBuffer::Timestamp,
            DataBuffer::Tag,
            DataBuffer::Data
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp.into(),
            tag.into(),
            bytes.into()
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

pub(crate) async fn insert_buffer_multiple(pool: &Pool<Postgres>,
    device_ids: Vec<Uuid>,
    model_ids: Vec<Uuid>,
    timestamps: Vec<DateTime<Utc>>,
    data: Vec<Vec<DataValue>>,
    tags: Option<Vec<i16>>
) -> Result<Vec<i32>, Error>
{
    let number = device_ids.len();
    let tags = match tags {
        Some(value) => value,
        None => (0..number).map(|_| Tag::DEFAULT).collect()
    };
    let numbers = vec![model_ids.len(), timestamps.len(), data.len(), tags.len()];
    if number == 0 || numbers.into_iter().any(|n| n != number) {
        return Err(Error::InvalidArgument(EMPTY_LENGTH_UNMATCH.to_string()))
    } 
    let mut model_ids_unique = model_ids.clone();
    model_ids_unique.sort();
    model_ids_unique.dedup();

    let types_vec = select_data_types(pool, model_ids.clone()).await?;
    if model_ids_unique.len() != types_vec.len() {
        return Err(Error::InvalidArgument(MODEL_NOT_EXISTS.to_string()));
    }
    let types: Vec<Vec<DataType>> = model_ids.clone().into_iter().map(|id| {
        let index = model_ids_unique.iter().position(|&el| el == id).unwrap_or_default();
        types_vec[index].clone()
    }).collect();

    let mut stmt = Query::insert()
        .into_table(DataBuffer::Table)
        .columns([
            DataBuffer::DeviceId,
            DataBuffer::ModelId,
            DataBuffer::Timestamp,
            DataBuffer::Tag,
            DataBuffer::Data
        ])
        .to_owned();
    for i in 0..number {
        let bytes = match ArrayDataValue::from_vec(&data[i]).convert(&types[i]) {
            Some(value) => value.to_bytes(),
            None => return Err(Error::InvalidArgument(DATA_TYPE_UNMATCH.to_string()))
        };
        stmt = stmt.values([
            device_ids[i].into(),
            model_ids[i].into(),
            timestamps[i].into(),
            tags[i].clone().into(),
            bytes.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

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
    let ids = (id-number as i32+1..id+1).collect();

    Ok(ids)
}

pub(crate) async fn update_buffer(pool: &Pool<Postgres>,
    id: i32,
    data: Option<Vec<DataValue>>,
    tag: Option<i16>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(DataBuffer::Table)
        .to_owned();

    if let Some(value) = data {
        let types = select_buffer_types(pool, id).await.map_err(|_| Error::InvalidArgument(MODEL_NOT_EXISTS.to_string()))?;
        let bytes = match ArrayDataValue::from_vec(&value).convert(&types) {
            Some(value) => value.to_bytes(),
            None => return Err(Error::InvalidArgument(DATA_TYPE_UNMATCH.to_string()))
        };
        stmt = stmt.value(DataBuffer::Data, bytes).to_owned();
    }
    if let Some(value) = tag {
        stmt = stmt.value(DataBuffer::Tag, i16::from(value)).to_owned();
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

pub(crate) async fn select_buffer_set(pool: &Pool<Postgres>, 
    selector: BufferSelector,
    set_id: Uuid
) -> Result<Vec<BufferSchema>, Error>
{
    let mut stmt = Query::select().to_owned();
    stmt = stmt
        .columns([
            (DataBuffer::Table, DataBuffer::Id),
            (DataBuffer::Table, DataBuffer::DeviceId),
            (DataBuffer::Table, DataBuffer::ModelId),
            (DataBuffer::Table, DataBuffer::Timestamp),
            (DataBuffer::Table, DataBuffer::Tag),
            (DataBuffer::Table, DataBuffer::Data)
        ])
        .column((Model::Table, Model::DataType))
        .from(DataBuffer::Table)
        .inner_join(Model::Table, 
            Expr::col((DataBuffer::Table, DataBuffer::ModelId))
            .equals((Model::Table, Model::ModelId)))
        .inner_join(SetMap::Table, 
            Condition::all()
            .add(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).equals((SetMap::Table, SetMap::DeviceId)))
            .add(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).equals((SetMap::Table, SetMap::ModelId)))
        )
        .and_where(Expr::col((SetMap::Table, SetMap::SetId)).eq(set_id))
        .to_owned();

    match selector {
        BufferSelector::Time(timestamp) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).eq(timestamp)).to_owned();
        },
        BufferSelector::TimeLast(last) => {
            stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gt(last))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        BufferSelector::TimeRange(begin, end) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gte(begin))
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).lte(end))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .to_owned();
        },
        BufferSelector::NumberBefore(timestamp, number) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).lte(timestamp))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Desc)
                .limit(number as u64)
                .to_owned();
        },
        BufferSelector::NumberAfter(timestamp, number) => {
            stmt = stmt
                .and_where(Expr::col((DataBuffer::Table, DataBuffer::Timestamp)).gte(timestamp))
                .order_by((DataBuffer::Table, DataBuffer::Timestamp), Order::Asc)
                .limit(number as u64)
                .to_owned();
        },
        BufferSelector::First(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Asc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        BufferSelector::Last(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Desc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        BufferSelector::None => {}
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(5);
            let types: Vec<DataType> = row.get::<Vec<u8>,_>(6).into_iter().map(|ty| ty.into()).collect();
            BufferSchema {
                id: row.get(0),
                device_id: row.get(1),
                model_id: row.get(2),
                timestamp: row.get(3),
                data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
                tag: row.get(5)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_timestamp_set(pool: &Pool<Postgres>,
    selector: BufferSelector,
    set_id: Uuid
) -> Result<Vec<DateTime<Utc>>, Error>
{
    let mut stmt = Query::select()
        .column((DataBuffer::Table, DataBuffer::Timestamp))
        .from(DataBuffer::Table)
        .inner_join(SetMap::Table, 
            Condition::all()
            .add(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).equals((SetMap::Table, SetMap::DeviceId)))
            .add(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).equals((SetMap::Table, SetMap::ModelId)))
        )
        .and_where(Expr::col((SetMap::Table, SetMap::SetId)).eq(set_id))
        .to_owned();

    match selector {
        BufferSelector::First(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Asc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
        BufferSelector::Last(number, offset) => {
            stmt = stmt
                .order_by((DataBuffer::Table, DataBuffer::Id), Order::Desc)
                .limit(number as u64)
                .offset(offset as u64)
                .to_owned();
        },
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

pub(crate) async fn count_buffer(pool: &Pool<Postgres>,
    device_ids: Option<Vec<Uuid>>,
    model_ids: Option<Vec<Uuid>>,
    set_id: Option<Uuid>,
    tag: Option<i16>
) -> Result<usize, Error>
{
    let mut stmt = Query::select()
        .expr(Expr::col((DataBuffer::Table, DataBuffer::Id)).count())
        .from(DataBuffer::Table)
        .to_owned();

    if let Some(set_id) = set_id {
        stmt = stmt
            .inner_join(SetMap::Table, 
                Condition::all()
                .add(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).equals((SetMap::Table, SetMap::DeviceId)))
                .add(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).equals((SetMap::Table, SetMap::ModelId)))
            )
            .and_where(Expr::col((SetMap::Table, SetMap::SetId)).eq(set_id)).to_owned()
            .to_owned();
    }
    else {
        if let Some(ids) = device_ids {
            if ids.len() == 1 {
                stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).eq(ids[0])).to_owned();
            }
            else if ids.len() > 1 {
                stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::DeviceId)).is_in(ids)).to_owned();
            }
        }
        if let Some(ids) = model_ids.clone() {
            if ids.len() == 1 {
                stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).eq(ids[0])).to_owned();
            }
            else if ids.len() > 1 {
                stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::ModelId)).is_in(ids)).to_owned();
            }
        }
    }
    if let (Some(ids), Some(t)) = (model_ids, tag) {
        let tags = select_tag_members(pool, ids, t).await?;
        stmt = stmt.and_where(Expr::col((DataBuffer::Table, DataBuffer::Tag)).is_in(tags)).to_owned();
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
