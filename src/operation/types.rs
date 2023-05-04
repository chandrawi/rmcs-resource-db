use sqlx::{Pool, Row, Error};
use sqlx::mysql::{MySql, MySqlRow};
use sea_query::{MysqlQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;

use crate::schema::device::{DeviceType, DeviceTypeModel, TypeSchema};

enum TypeSelector {
    Id(u32),
    Name(String)
}

async fn select_device_type(pool: &Pool<MySql>, 
    selector: TypeSelector
) -> Result<Vec<TypeSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (DeviceType::Table, DeviceType::TypeId),
            (DeviceType::Table, DeviceType::Name),
            (DeviceType::Table, DeviceType::Description)
        ])
        .columns([
            (DeviceTypeModel::Table, DeviceTypeModel::ModelId)
        ])
        .from(DeviceType::Table)
        .left_join(DeviceTypeModel::Table, 
            Expr::col((DeviceType::Table, DeviceType::TypeId))
            .equals((DeviceTypeModel::Table, DeviceTypeModel::TypeId))
        )
        .to_owned();

    match selector {
        TypeSelector::Id(id) => {
            stmt = stmt.and_where(Expr::col((DeviceType::Table, DeviceType::TypeId)).eq(id)).to_owned();
        },
        TypeSelector::Name(name) => {
            stmt = stmt.and_where(Expr::col((DeviceType::Table, DeviceType::Name)).like(name)).to_owned();
        }
    }
    let (sql, values) = stmt
        .order_by((DeviceType::Table, DeviceType::TypeId), Order::Asc)
        .build_sqlx(MysqlQueryBuilder);

    let mut id_vec: Vec<u32> = Vec::new();
    let mut type_schema: TypeSchema = TypeSchema::default();
    let mut type_schema_vec: Vec<TypeSchema> = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: MySqlRow| {
            let type_id: u32 = row.get(0);
            let name: String = row.get(1);
            let description: String = row.get(2);
            let model_id: Result<u32, Error> = row.try_get(3);

            // on every new type_id found add id_vec and update type_schema scalar member
            if id_vec.iter().filter(|el| **el == type_id).count() == 0 {
                id_vec.push(type_id);
                type_schema.id = type_id;
                type_schema.name = name;
                type_schema.description = description;
                // insert new type_schema to type_schema_vec
                type_schema_vec.push(type_schema.clone());
            }
            // update type_schema if non empty model_id found
            if let Ok(value) = model_id {
                type_schema.models.push(value);
            }
            // update type_schema_vec with updated type_schema
            type_schema_vec.pop();
            type_schema_vec.push(type_schema.clone());
        })
        .fetch_all(pool)
        .await?;

    Ok(type_schema_vec)
}

pub(crate) async fn select_device_type_by_id(pool: &Pool<MySql>, 
    id: u32
) -> Result<TypeSchema, Error>
{
    let results = select_device_type(pool, TypeSelector::Id(id)).await?;
    match results.into_iter().next() {
        Some(value) => Ok(value),
        None => Err(Error::RowNotFound)
    }
}

pub(crate) async fn select_device_type_by_name(pool: &Pool<MySql>, 
    name: &str
) -> Result<Vec<TypeSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_device_type(pool, TypeSelector::Name(name_like)).await
}

pub(crate) async fn insert_device_type(pool: &Pool<MySql>,
    name: &str,
    description: Option<&str>
) -> Result<u32, Error>
{
    let (sql, values) = Query::insert()
        .into_table(DeviceType::Table)
        .columns([
            DeviceType::Name,
            DeviceType::Description
        ])
        .values([
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(DeviceType::TypeId)))
        .from(DeviceType::Table)
        .to_string(MysqlQueryBuilder);
    let id: u32 = sqlx::query(&sql)
        .map(|row: MySqlRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_device_type(pool: &Pool<MySql>,
    id: u32,
    name: Option<&str>,
    description: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(DeviceType::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(DeviceType::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(DeviceType::Description, value).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(DeviceType::TypeId).eq(id))
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_device_type(pool: &Pool<MySql>, 
    id: u32
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(DeviceType::Table)
        .and_where(Expr::col(DeviceType::TypeId).eq(id))
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn insert_device_type_model(pool: &Pool<MySql>,
    id: u32,
    model_id: u32
) -> Result<(), Error>
{
    let (sql, values) = Query::insert()
        .into_table(DeviceTypeModel::Table)
        .columns([
            DeviceTypeModel::TypeId,
            DeviceTypeModel::ModelId
        ])
        .values([
            id.into(),
            model_id.into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_device_type_model(pool: &Pool<MySql>, 
    id: u32,
    model_id: u32
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(DeviceTypeModel::Table)
        .and_where(Expr::col(DeviceTypeModel::TypeId).eq(id))
        .and_where(Expr::col(DeviceTypeModel::ModelId).eq(model_id))
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
