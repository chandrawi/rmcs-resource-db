use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataValue, DataType};
use crate::schema::model::{Model, ModelTag, ModelConfig, ModelSchema, ModelConfigSchema, TagSchema, ModelSchemaFlat};
use crate::schema::device::DeviceTypeModel;
use crate::schema::set::SetMap;

pub(crate) async fn select_model(pool: &Pool<Postgres>, 
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    type_id: Option<Uuid>,
    name: Option<&str>,
    category: Option<&str>
) -> Result<Vec<ModelSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (Model::Table, Model::ModelId),
            (Model::Table, Model::Name),
            (Model::Table, Model::Category),
            (Model::Table, Model::Description),
            (Model::Table, Model::DataType)
        ])
        .columns([
            (ModelTag::Table, ModelTag::Tag),
            (ModelTag::Table, ModelTag::Name),
            (ModelTag::Table, ModelTag::Members)
        ])
        .columns([
            (ModelConfig::Table, ModelConfig::Id),
            (ModelConfig::Table, ModelConfig::Index),
            (ModelConfig::Table, ModelConfig::Name),
            (ModelConfig::Table, ModelConfig::Value),
            (ModelConfig::Table, ModelConfig::Type),
            (ModelConfig::Table, ModelConfig::Category)
        ])
        .from(Model::Table)
        .left_join(ModelTag::Table, 
            Expr::col((Model::Table, Model::ModelId))
            .equals((ModelTag::Table, ModelTag::ModelId))
        )
        .left_join(ModelConfig::Table, 
            Expr::col((Model::Table, Model::ModelId))
            .equals((ModelConfig::Table, ModelConfig::ModelId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((Model::Table, Model::ModelId)).eq(id)).to_owned()
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((Model::Table, Model::ModelId)).is_in(ids.to_vec())).to_owned()
    }
    else {
        if let Some(type_id) = type_id {
            stmt = stmt.inner_join(DeviceTypeModel::Table, 
                    Expr::col((Model::Table, Model::ModelId))
                    .equals((DeviceTypeModel::Table, DeviceTypeModel::ModelId)))
                .and_where(Expr::col((DeviceTypeModel::Table, DeviceTypeModel::TypeId)).eq(type_id))
                .to_owned();
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((Model::Table, Model::Name)).like(name_like)).to_owned();
        }
        if let Some(category) = category {
            let category_like = String::from("%") + category + "%";
            stmt = stmt.and_where(Expr::col((Model::Table, Model::Category)).like(category_like)).to_owned();
        }
    }

    let (sql, values) = stmt
        .order_by((Model::Table, Model::ModelId), Order::Asc)
        .order_by((ModelTag::Table, ModelTag::Tag), Order::Asc)
        .order_by((ModelConfig::Table, ModelConfig::Id), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut last_id: Option<Uuid> = None;
    let mut last_tag: Option<i16> = None;
    let mut model_schema_vec: Vec<ModelSchemaFlat> = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            // get last model_schema in model_schema_vec or default
            let mut model_schema = model_schema_vec.pop().unwrap_or_default();
            // on every new id found insert model_schema to model_schema_vec and reset last_index
            let model_id: Uuid = row.get(0);
            if let Some(id) = last_id {
                if id != model_id {
                    model_schema_vec.push(model_schema.clone());
                    model_schema = ModelSchemaFlat::default();
                    last_tag = None;
                }
            }
            last_id = Some(model_id);
            model_schema.id = model_id;
            model_schema.name = row.get(1);
            model_schema.category = row.get(2);
            model_schema.description = row.get(3);
            model_schema.data_type = row.get::<Vec<u8>,_>(4).into_iter().map(|byte| byte.into()).collect();
            // on every new tag found, add a new tag schema to model schema and initialize a new config
            let tag_id = row.try_get(5).ok();
            let tag_name = row.try_get(6);
            let tag_bytes: Result<Vec<u8>,_> = row.try_get(7);
            if last_tag == None || last_tag != Some(tag_id.unwrap_or(0)) {
                if let (Some(tag), Ok(name), Ok(bytes)) = 
                    (tag_id, tag_name, tag_bytes) 
                {
                    let mut members = vec![tag];
                    for chunk in bytes.chunks_exact(2) {
                        members.push(i16::from_be_bytes([chunk[0], chunk[1]]));
                    }
                    model_schema.tags.push(TagSchema { model_id, tag, name, members });
                }
                model_schema.configs = Vec::new();
            }
            last_tag = Some(tag_id.unwrap_or(0));
            // update model_schema configs if non empty config found
            let config_id = row.try_get(8);
            let config_index = row.try_get(9);
            let config_name = row.try_get(10);
            let config_bytes: Result<Vec<u8>,_> = row.try_get(11);
            let config_type: Result<i16,_> = row.try_get(12);
            let config_category = row.try_get(13);
            if let (Ok(id), Ok(index), Ok(name), Ok(bytes), Ok(type_), Ok(category)) = 
                (config_id, config_index, config_name, config_bytes, config_type, config_category) 
            {
                let value = DataValue::from_bytes(&bytes, DataType::from(type_));
                model_schema.configs.push(ModelConfigSchema { id, model_id, index, name, value, category});
            }
            // update model_schema_vec with updated model_schema
            model_schema_vec.push(model_schema.clone());
        })
        .fetch_all(pool)
        .await?;

    Ok(model_schema_vec.into_iter().map(|schema| schema.into()).collect())
}

pub(crate) async fn insert_model(pool: &Pool<Postgres>,
    id: Uuid,
    data_type: &[DataType],
    category: &str,
    name: &str,
    description: Option<&str>,
) -> Result<Uuid, Error>
{
    let (sql, values) = Query::insert()
        .into_table(Model::Table)
        .columns([
            Model::ModelId,
            Model::Category,
            Model::Name,
            Model::Description,
            Model::DataType
        ])
        .values([
            id.into(),
            category.into(),
            name.into(),
            description.unwrap_or_default().into(),
            data_type.into_iter().map(|ty| {
                ty.to_owned().into()
            }).collect::<Vec<u8>>().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_model(pool: &Pool<Postgres>,
    id: Uuid,
    data_type: Option<&[DataType]>,
    category: Option<&str>,
    name: Option<&str>,
    description: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(Model::Table)
        .to_owned();

    if let Some(value) = category {
        stmt = stmt.value(Model::Category, value).to_owned();
    }
    if let Some(value) = name {
        stmt = stmt.value(Model::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(Model::Description, value).to_owned();
    }
    if let Some(value) = data_type {
        stmt = stmt.value(Model::DataType, value.into_iter().map(|ty| {
            ty.to_owned().into()
        }).collect::<Vec<u8>>()).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(Model::ModelId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_model(pool: &Pool<Postgres>, 
    id: Uuid
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(Model::Table)
        .and_where(Expr::col(Model::ModelId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn select_model_config(pool: &Pool<Postgres>,
    id: Option<i32>,
    model_id: Option<Uuid>
) -> Result<Vec<ModelConfigSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            ModelConfig::Id,
            ModelConfig::ModelId,
            ModelConfig::Index,
            ModelConfig::Name,
            ModelConfig::Value,
            ModelConfig::Type,
            ModelConfig::Category
        ])
        .from(ModelConfig::Table)
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col(ModelConfig::Id).eq(id)).to_owned();
    }
    else if let Some(model_id) = model_id {
        stmt = stmt.and_where(Expr::col(ModelConfig::ModelId).eq(model_id)).to_owned();
    }

    let (sql, values) = stmt
        .order_by(ModelConfig::ModelId, Order::Asc)
        .order_by(ModelConfig::Index, Order::Asc)
        .order_by(ModelConfig::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes = row.get(4);
            let type_ = DataType::from(row.get::<i16,_>(5));
            ModelConfigSchema {
                id: row.get(0),
                model_id: row.get(1),
                index: row.get(2),
                name: row.get(3),
                value: DataValue::from_bytes(bytes, type_),
                category: row.get(6)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn insert_model_config(pool: &Pool<Postgres>,
    model_id: Uuid,
    index: i32,
    name: &str,
    value: DataValue,
    category: &str
) -> Result<i32, Error>
{
    let config_value = value.to_bytes();
    let config_type = i16::from(value.get_type());
    let (sql, values) = Query::insert()
        .into_table(ModelConfig::Table)
        .columns([
            ModelConfig::ModelId,
            ModelConfig::Index,
            ModelConfig::Name,
            ModelConfig::Value,
            ModelConfig::Type,
            ModelConfig::Category
        ])
        .values([
            model_id.into(),
            index.into(),
            name.into(),
            config_value.into(),
            config_type.into(),
            category.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(ModelConfig::Id)))
        .from(ModelConfig::Table)
        .to_string(PostgresQueryBuilder);
    let id: i32 = sqlx::query(&sql)
        .map(|row: PgRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_model_config(pool: &Pool<Postgres>,
    id: i32,
    name: Option<&str>,
    value: Option<DataValue>,
    category: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(ModelConfig::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(ModelConfig::Name, value).to_owned();
    }
    if let Some(value) = value {
        let bytes = value.to_bytes();
        let type_ = i16::from(value.get_type());
        stmt = stmt
            .value(ModelConfig::Value, bytes)
            .value(ModelConfig::Type, type_).to_owned();
    }
    if let Some(value) = category {
        stmt = stmt.value(ModelConfig::Category, value).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(ModelConfig::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_model_config(pool: &Pool<Postgres>, 
    id: i32
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(ModelConfig::Table)
        .and_where(Expr::col(ModelConfig::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn select_model_tag(pool: &Pool<Postgres>, 
    model_id: Uuid,
    tag: Option<i16>
) -> Result<Vec<TagSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            ModelTag::ModelId,
            ModelTag::Tag,
            ModelTag::Name,
            ModelTag::Members
        ])
        .from(ModelTag::Table)
        .and_where(Expr::col(ModelTag::ModelId).eq(model_id))
        .to_owned();

    if let Some(t) = tag {
        stmt = stmt.and_where(Expr::col(ModelTag::Tag).eq(t)).to_owned();
    }
    let (sql, values) = stmt
        .order_by(ModelTag::Tag, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let mut tags: Vec<i16> = vec![row.get(1)];
            let bytes: Vec<u8> = row.get(3);
            for chunk in bytes.chunks_exact(2) {
                tags.push(i16::from_be_bytes([chunk[0], chunk[1]]));
            }
            TagSchema {
                model_id: row.get(0),
                tag: tags[0],
                name: row.get(2),
                members: tags
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_tag_members(pool: &Pool<Postgres>, 
    model_ids: &[Uuid],
    tag: i16
) -> Result<Vec<i16>, Error>
{
    let (sql, values) = Query::select()
        .column(ModelTag::Members)
        .from(ModelTag::Table)
        .and_where(Expr::col(ModelTag::ModelId).is_in(model_ids.to_vec()))
        .and_where(Expr::col(ModelTag::Tag).eq(tag))
        .build_sqlx(PostgresQueryBuilder);

    let mut tags: Vec<i16> = vec![tag];
    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(0);
            for chunk in bytes.chunks_exact(2) {
                tags.push(i16::from_be_bytes([chunk[0], chunk[1]]));
            }
        })
        .fetch_all(pool)
        .await?;

    tags.sort();
    tags.dedup();
    Ok(tags)
}

pub(crate) async fn select_tag_members_set(pool: &Pool<Postgres>, 
    set_id: Uuid,
    tag: i16
) -> Result<Vec<i16>, Error>
{
    let (sql, values) = Query::select()
        .column(ModelTag::Members)
        .from(ModelTag::Table)
        .inner_join(SetMap::Table, 
            Expr::col((ModelTag::Table, ModelTag::ModelId))
            .equals((SetMap::Table, SetMap::ModelId)))
        .and_where(Expr::col(SetMap::SetId).eq(set_id))
        .and_where(Expr::col(ModelTag::Tag).eq(tag))
        .build_sqlx(PostgresQueryBuilder);

    let mut tags: Vec<i16> = vec![tag];
    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: Vec<u8> = row.get(0);
            for chunk in bytes.chunks_exact(2) {
                tags.push(i16::from_be_bytes([chunk[0], chunk[1]]));
            }
        })
        .fetch_all(pool)
        .await?;

    tags.sort();
    tags.dedup();
    Ok(tags)
}

pub(crate) async fn insert_model_tag(pool: &Pool<Postgres>,
    model_id: Uuid,
    tag: i16,
    name: &str,
    members: &[i16]
) -> Result<(), Error>
{
    let mut bytes: Vec<u8> = Vec::new();
    for member in members {
        bytes.append(member.to_be_bytes().to_vec().as_mut());
    }
    let (sql, values) = Query::insert()
        .into_table(ModelTag::Table)
        .columns([
            ModelTag::ModelId,
            ModelTag::Tag,
            ModelTag::Name,
            ModelTag::Members
        ])
        .values([
            model_id.into(),
            tag.into(),
            name.into(),
            bytes.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn update_model_tag(pool: &Pool<Postgres>,
    model_id: Uuid,
    tag: i16,
    name: Option<&str>,
    members: Option<&[i16]>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(ModelTag::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(ModelTag::Name, value).to_owned();
    }
    if let Some(value) = members {
        let mut bytes: Vec<u8> = Vec::new();
        for member in value {
            bytes.append(member.to_be_bytes().to_vec().as_mut());
        }
        stmt = stmt.value(ModelTag::Members, bytes).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(ModelTag::ModelId).eq(model_id))
        .and_where(Expr::col(ModelTag::Tag).eq(tag))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_model_tag(pool: &Pool<Postgres>,
    model_id: Uuid,
    tag: i16
) -> Result<(), Error>
{
    let (sql, values) = Query::delete()
        .from_table(ModelTag::Table)
        .and_where(Expr::col(ModelTag::ModelId).eq(model_id))
        .and_where(Expr::col(ModelTag::Tag).eq(tag))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
