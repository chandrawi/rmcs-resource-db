use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sea_query::{PostgresQueryBuilder, Query, Expr, Cond, Order, Func};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{ConfigType, ConfigValue, DataIndexing, DataType};
use crate::schema::model::{Model, ModelType, ModelConfig, ModelSchema, ModelConfigSchema};

enum ModelSelector {
    Id(Uuid),
    Name(String),
    Category(String),
    NameCategory(String, String)
}

enum ConfigSelector {
    Id(i32),
    Model(Uuid)
}

async fn select_join_model(pool: &Pool<Postgres>, 
    selector: ModelSelector
) -> Result<Vec<ModelSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (Model::Table, Model::ModelId),
            (Model::Table, Model::Indexing),
            (Model::Table, Model::Category),
            (Model::Table, Model::Name),
            (Model::Table, Model::Description)
        ])
        .columns([
            (ModelType::Table, ModelType::Index),
            (ModelType::Table, ModelType::Type)
        ])
        .columns([
            (ModelConfig::Table, ModelConfig::Id),
            (ModelConfig::Table, ModelConfig::Name),
            (ModelConfig::Table, ModelConfig::Value),
            (ModelConfig::Table, ModelConfig::Type),
            (ModelConfig::Table, ModelConfig::Category)
        ])
        .from(Model::Table)
        .inner_join(ModelType::Table, 
            Expr::col((Model::Table, Model::ModelId))
            .equals((ModelType::Table, ModelType::ModelId))
        )
        .left_join(ModelConfig::Table, 
            Cond::all()
            .add(Expr::col((ModelType::Table, ModelType::ModelId))
                .equals((ModelConfig::Table, ModelConfig::ModelId))
            )
            .add(Expr::col((ModelType::Table, ModelType::Index))
                .equals((ModelConfig::Table, ModelConfig::Index))
            )
        )
        .to_owned();

    match selector {
        ModelSelector::Id(id) => {
            stmt = stmt.and_where(Expr::col((Model::Table, Model::ModelId)).eq(id)).to_owned();
        },
        ModelSelector::Name(name) => {
            stmt = stmt.and_where(Expr::col((Model::Table, Model::Name)).like(name)).to_owned();
        },
        ModelSelector::Category(category) => {
            stmt = stmt.and_where(Expr::col((Model::Table, Model::Category)).eq(category)).to_owned();
        },
        ModelSelector::NameCategory(name, category) => {
            stmt = stmt
                .and_where(Expr::col((Model::Table, Model::Name)).like(name))
                .and_where(Expr::col((Model::Table, Model::Category)).eq(category))
                .to_owned();
        }
    }
    let (sql, values) = stmt
        .order_by((Model::Table, Model::ModelId), Order::Asc)
        .order_by((ModelType::Table, ModelType::Index), Order::Asc)
        .order_by((ModelConfig::Table, ModelConfig::Id), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut last_id: Option<Uuid> = None;
    let mut last_index: Option<i16> = None;
    let mut config_schema_vec: Vec<ModelConfigSchema> = Vec::new();
    let mut model_schema_vec: Vec<ModelSchema> = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            // get last model_schema in model_schema_vec or default
            let mut model_schema = model_schema_vec.pop().unwrap_or_default();
            // on every new id found insert model_schema to model_schema_vec and reset last_index
            let id: Uuid = row.get(0);
            if let Some(value) = last_id {
                if value != id {
                    model_schema_vec.push(model_schema.clone());
                    model_schema = ModelSchema::default();
                    last_index = None;
                }
            }
            last_id = Some(id);
            model_schema.id = id;
            model_schema.indexing = DataIndexing::from(row.get::<i16,_>(1));
            model_schema.category = row.get(2);
            model_schema.name = row.get(3);
            model_schema.description = row.get(4);
            // on every new index found update model_schema types and clear config_schema_vec
            let type_index: i16 = row.get(5);
            if last_index == None || last_index != Some(type_index) {
                model_schema.types.push(DataType::from(row.get::<i16,_>(6)));
                model_schema.configs.push(Vec::new());
                config_schema_vec.clear();
            }
            last_index = Some(type_index);
            // update model_schema configs if non empty config found
            let config_id: Option<i32> = row.try_get(7).ok();
            if let Some(cfg_id) = config_id {
                let bytes: Vec<u8> = row.try_get(9).unwrap_or_default();
                let type_ = ConfigType::from(row.try_get::<i16,_>(10).unwrap_or_default());
                config_schema_vec.push(ModelConfigSchema {
                    id: cfg_id,
                    model_id: id,
                    index: type_index,
                    name: row.try_get(8).unwrap_or_default(),
                    value: ConfigValue::from_bytes(bytes.as_slice(), type_),
                    category: row.try_get(11).unwrap_or_default()
                });
                model_schema.configs.pop();
                model_schema.configs.push(config_schema_vec.clone());
            }
            // update model_schema_vec with updated model_schema
            model_schema_vec.push(model_schema.clone());
        })
        .fetch_all(pool)
        .await?;

    Ok(model_schema_vec)
}

pub(crate) async fn select_join_model_by_id(pool: &Pool<Postgres>, 
    id: Uuid
) -> Result<ModelSchema, Error>
{
    let results = select_join_model(pool, ModelSelector::Id(id)).await?;
    match results.into_iter().next() {
        Some(value) => Ok(value),
        None => Err(Error::RowNotFound)
    }
}

pub(crate) async fn select_join_model_by_name(pool: &Pool<Postgres>, 
    name: &str
) -> Result<Vec<ModelSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_join_model(pool, ModelSelector::Name(name_like)).await
}

pub(crate) async fn select_join_model_by_category(pool: &Pool<Postgres>, 
    category: &str
) -> Result<Vec<ModelSchema>, Error>
{
    select_join_model(pool, ModelSelector::Category(String::from(category))).await
}

pub(crate) async fn select_join_model_by_name_category(pool: &Pool<Postgres>, 
    name: &str,
    category: &str
) -> Result<Vec<ModelSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_join_model(pool, ModelSelector::NameCategory(String::from(name_like), String::from(category))).await
}

pub(crate) async fn insert_model(pool: &Pool<Postgres>,
    id: Uuid,
    indexing: DataIndexing,
    category: &str,
    name: &str,
    description: Option<&str>,
) -> Result<Uuid, Error>
{
    let (sql, values) = Query::insert()
        .into_table(Model::Table)
        .columns([
            Model::ModelId,
            Model::Indexing,
            Model::Category,
            Model::Name,
            Model::Description
        ])
        .values([
            id.into(),
            i16::from(indexing).into(),
            category.into(),
            name.into(),
            description.unwrap_or_default().into()
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
    indexing: Option<DataIndexing>,
    category: Option<&str>,
    name: Option<&str>,
    description: Option<&str>,
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(Model::Table)
        .to_owned();

    if let Some(value) = indexing {
        stmt = stmt.value(Model::Indexing, i16::from(value)).to_owned();
    }
    if let Some(value) = category {
        stmt = stmt.value(Model::Category, value).to_owned();
    }
    if let Some(value) = name {
        stmt = stmt.value(Model::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(Model::Description, value).to_owned();
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

pub(crate) async fn insert_model_types(pool: &Pool<Postgres>,
    id: Uuid,
    types: &[DataType]
) -> Result<(), Error>
{
    let mut stmt = Query::insert()
        .into_table(ModelType::Table)
        .columns([
            ModelType::ModelId,
            ModelType::Index,
            ModelType::Type
        ])
        .to_owned();
    let mut i = 0;
    for ty in types {
        let t = i16::from(ty.clone());
        stmt = stmt.values([
                id.into(),
                i.into(),
                t.into()
            ])
            .unwrap_or(&mut sea_query::InsertStatement::default())
            .to_owned();
        i += 1;
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_model_types(pool: &Pool<Postgres>, 
    id: Uuid
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(ModelType::Table)
        .and_where(Expr::col(ModelType::ModelId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

async fn select_model_config(pool: &Pool<Postgres>,
    selector: ConfigSelector
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

    match selector {
        ConfigSelector::Id(id) => {
            stmt = stmt.and_where(Expr::col(ModelConfig::Id).eq(id)).to_owned();
        },
        ConfigSelector::Model(model_id) => {
            stmt = stmt.and_where(Expr::col(ModelConfig::ModelId).eq(model_id)).to_owned();
        }
    }
    let (sql, values) = stmt
        .order_by(ModelConfig::ModelId, Order::Asc)
        .order_by(ModelConfig::Index, Order::Asc)
        .order_by(ModelConfig::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes: &[u8] = row.get(4);
            let type_ = ConfigType::from(row.get::<i16,_>(5));
            ModelConfigSchema {
                id: row.get(0),
                model_id: row.get(1),
                index: row.get(2),
                name: row.get(3),
                value: ConfigValue::from_bytes(bytes, type_),
                category: row.get(6)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_model_config_by_id(pool: &Pool<Postgres>,
    id: i32
) -> Result<ModelConfigSchema, Error>
{
    let results = select_model_config(pool, ConfigSelector::Id(id)).await?;
    match results.into_iter().next() {
        Some(value) => Ok(value),
        None => Err(Error::RowNotFound)
    }
}

pub(crate) async fn select_model_config_by_model(pool: &Pool<Postgres>,
    model_id: Uuid
) -> Result<Vec<ModelConfigSchema>, Error>
{
    select_model_config(pool, ConfigSelector::Model(model_id)).await
}

pub(crate) async fn insert_model_config(pool: &Pool<Postgres>,
    model_id: Uuid,
    index: i32,
    name: &str,
    value: ConfigValue,
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
    value: Option<ConfigValue>,
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
