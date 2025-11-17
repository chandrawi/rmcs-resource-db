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
use crate::operation::model::{select_tag_members, select_tag_members_set};
use crate::utility::tag as Tag;
use super::{EMPTY_LENGTH_UNMATCH, DATA_TYPE_UNMATCH, MODEL_NOT_EXISTS};

pub(crate) enum DataSelector {
    Time(DateTime<Utc>),
    Last(DateTime<Utc>),
    Range(DateTime<Utc>, DateTime<Utc>),
    NumberBefore(DateTime<Utc>, usize),
    NumberAfter(DateTime<Utc>, usize)
}

pub(crate) async fn select_data(pool: &Pool<Postgres>, 
    selector: DataSelector,
    device_ids: &[Uuid],
    model_ids: &[Uuid],
    tag: Option<i16>
) -> Result<Vec<DataSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (Data::Table, Data::DeviceId),
            (Data::Table, Data::ModelId),
            (Data::Table, Data::Timestamp),
            (Data::Table, Data::Tag),
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
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).is_in(device_ids.to_vec())).to_owned();
    }
    if model_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).eq(model_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).is_in(model_ids.to_vec())).to_owned();
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

    if let Some(t) = tag {
        let tags = select_tag_members(pool, model_ids, t).await?;
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).is_in(tags)).to_owned();
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(4);
            let types: Vec<DataType> = row.get::<Vec<u8>,_>(5).into_iter().map(|ty| ty.into()).collect();
            DataSchema {
                device_id: row.get(0),
                model_id: row.get(1),
                timestamp: row.get(2),
                data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
                tag: row.get(3)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_timestamp(pool: &Pool<Postgres>,
    selector: DataSelector,
    device_ids: &[Uuid],
    model_ids: &[Uuid],
    tag: Option<i16>
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
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).is_in(device_ids.to_vec())).to_owned();
    }
    if model_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).eq(model_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).is_in(model_ids.to_vec())).to_owned();
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

    if let Some(t) = tag {
        let tags = select_tag_members(pool, model_ids, t).await?;
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).is_in(tags)).to_owned();
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
    model_ids: &[Uuid]
) -> Result<Vec<Vec<DataType>>, Error>
{
    let (sql, values) = Query::select()
        .column((Model::Table, Model::DataType))
        .from(Model::Table)
        .and_where(Expr::col((Model::Table, Model::ModelId)).is_in(model_ids.to_vec()))
        .order_by((Model::Table, Model::ModelId), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            row.get::<Vec<u8>,_>(0).into_iter().map(|ty| ty.into()).collect()
        })
        .fetch_all(pool)
        .await
}

pub(crate) async fn insert_data(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    data: &[DataValue],
    tag: Option<i16>
) -> Result<(), Error>
{
    let types_vec = select_data_types(pool, &[model_id]).await?;
    let types = types_vec.into_iter().next().ok_or(Error::InvalidArgument(MODEL_NOT_EXISTS.to_string()))?;
    let bytes = match ArrayDataValue::from_vec(data).convert(&types) {
        Some(value) => value.to_bytes(),
        None => return Err(Error::InvalidArgument(DATA_TYPE_UNMATCH.to_string()))
    };
    let tag = tag.unwrap_or(Tag::DEFAULT);

    let stmt = Query::insert()
        .into_table(Data::Table)
        .columns([
            Data::DeviceId,
            Data::ModelId,
            Data::Timestamp,
            Data::Tag,
            Data::Data
        ])
        .values([
            device_id.into(),
            model_id.into(),
            timestamp.into(),
            tag.into(),
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

pub(crate) async fn insert_data_multiple(pool: &Pool<Postgres>,
    device_ids: &[Uuid],
    model_ids: &[Uuid],
    timestamps: &[DateTime<Utc>],
    data: &[&[DataValue]],
    tags: Option<&[i16]>
) -> Result<(), Error>
{
    let number = device_ids.len();
    let tags = match tags {
        Some(value) => value.to_vec(),
        None => (0..number).map(|_| Tag::DEFAULT).collect()
    };
    let numbers = vec![model_ids.len(), timestamps.len(), data.len(), tags.len()];
    if number == 0 || numbers.into_iter().any(|n| n != number) {
        return Err(Error::InvalidArgument(EMPTY_LENGTH_UNMATCH.to_string()))
    } 
    let mut model_ids_unique = model_ids.to_vec();
    model_ids_unique.sort();
    model_ids_unique.dedup();

    let types_vec = select_data_types(pool, model_ids).await?;
    if model_ids_unique.len() != types_vec.len() {
        return Err(Error::InvalidArgument(MODEL_NOT_EXISTS.to_string()));
    }
    let types: Vec<Vec<DataType>> = model_ids.into_iter().map(|id| {
        let index = model_ids_unique.iter().position(|el| el == id).unwrap_or_default();
        types_vec[index].clone()
    }).collect();

    let mut stmt = Query::insert()
        .into_table(Data::Table)
        .columns([
            Data::DeviceId,
            Data::ModelId,
            Data::Timestamp,
            Data::Tag,
            Data::Data
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
            tags[i].into(),
            bytes.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .to_owned();
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_data(pool: &Pool<Postgres>,
    device_id: Uuid,
    model_id: Uuid,
    timestamp: DateTime<Utc>,
    tag: Option<i16>
) -> Result<(), Error>
{
    let mut stmt = Query::delete()
        .from_table(Data::Table)
        .and_where(Expr::col(Data::DeviceId).eq(device_id))
        .and_where(Expr::col(Data::ModelId).eq(model_id))
        .and_where(Expr::col(Data::Timestamp).eq(timestamp))
        .to_owned();
    if let Some(t) = tag {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).eq(t)).to_owned();
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn select_data_set(pool: &Pool<Postgres>, 
    selector: DataSelector,
    set_id: Uuid,
    tag: Option<i16>
) -> Result<Vec<DataSetSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (Data::Table, Data::DeviceId),
            (Data::Table, Data::ModelId),
            (Data::Table, Data::Timestamp),
            (Data::Table, Data::Tag),
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
        _ => {}
    }

    if let Some(t) = tag {
        let tags = select_tag_members_set(pool, set_id, t).await?;
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).is_in(tags)).to_owned();
    }
    let (sql, values) = stmt
        .order_by((Data::Table, Data::Tag), Order::Asc)
        .order_by((SetMap::Table, SetMap::SetPosition), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut data_set_schema_vec: Vec<DataSetSchema> = Vec::new();
    let mut last_timestamp: Option<DateTime<Utc>> = None;
    let mut last_tag: Option<i16> = None;

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            // construct a data_schema
            let bytes: Vec<u8> = row.get(4);
            let types: Vec<DataType> = row.get::<Vec<u8>,_>(5).into_iter().map(|ty| ty.into()).collect();
            let data_schema = DataSchema {
                device_id: row.get(0),
                model_id: row.get(1),
                timestamp: row.get(2),
                data: ArrayDataValue::from_bytes(&bytes, &types).to_vec(),
                tag: row.get(3)
            };
            // get last data_set_schema in data_set_schema_vec
            let mut data_set_schema = data_set_schema_vec.pop().unwrap_or_default();
            // on every new timestamp or tag found, insert new data_set_schema to data_set_schema_vec
            if last_timestamp != Some(data_schema.timestamp) || last_tag != Some(data_schema.tag) {
                if last_timestamp != None {
                    data_set_schema_vec.push(data_set_schema.clone());
                }
                // initialize data_set_schema data vector with Null
                let number: i16 = row.get(8);
                data_set_schema = DataSetSchema::default();
                for _i in 0..number {
                    data_set_schema.data.push(DataValue::Null);
                }
            }
            data_set_schema.set_id = set_id;
            data_set_schema.timestamp = data_schema.timestamp;
            data_set_schema.tag = data_schema.tag;
            let indexes: Vec<u8> = row.get(6);
            let position: i16 = row.get(7);
            // filter data vector by data_set data indexes of particular model
            // and replace data_set_schema data vector on the set position with filtered data vector
            for (position_offset, index) in indexes.into_iter().enumerate() {
                data_set_schema.data[position as usize + position_offset] = 
                    data_schema.data.get(index as usize).map(|value| value.to_owned()).unwrap_or_default()
            }
            last_timestamp = Some(data_schema.timestamp);
            last_tag = Some(data_schema.tag);
            // update data_set_schema_vec with updated data_set_schema
            data_set_schema_vec.push(data_set_schema);
        })
        .fetch_all(pool)
        .await?;

    Ok(data_set_schema_vec)
}

pub(crate) async fn count_data(pool: &Pool<Postgres>,
    selector: DataSelector,
    device_ids: &[Uuid],
    model_ids: &[Uuid],
    tag: Option<i16>
) -> Result<usize, Error>
{
    let mut stmt = Query::select()
        .expr(Expr::col((Data::Table, Data::Timestamp)).count())
        .from(Data::Table)
        .to_owned();

    if device_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).eq(device_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::DeviceId)).is_in(device_ids.to_vec())).to_owned();
    }
    if model_ids.len() == 1 {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).eq(model_ids[0])).to_owned();
    }
    else {
        stmt = stmt.and_where(Expr::col((Data::Table, Data::ModelId)).is_in(model_ids.to_vec())).to_owned();
    }

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

    if let Some(t) = tag {
        let tags = select_tag_members(pool, model_ids, t).await?;
        stmt = stmt.and_where(Expr::col((Data::Table, Data::Tag)).is_in(tags)).to_owned();
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
