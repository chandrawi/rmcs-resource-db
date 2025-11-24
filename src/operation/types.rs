use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::device::{DeviceType, DeviceTypeModel, TypeSchema};

pub(crate) async fn select_device_type(pool: &Pool<Postgres>, 
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    name: Option<&str>
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

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((DeviceType::Table, DeviceType::TypeId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((DeviceType::Table, DeviceType::TypeId)).is_in(ids.to_vec())).to_owned();
    }
    else {
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((DeviceType::Table, DeviceType::Name)).like(name_like)).to_owned();
        }
    }

    let (sql, values) = stmt
        .order_by((DeviceType::Table, DeviceType::TypeId), Order::Asc)
        .order_by((DeviceTypeModel::Table, DeviceTypeModel::ModelId), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut last_id: Option<Uuid> = None;
    let mut type_schema_vec: Vec<TypeSchema> = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            // get last type_schema in type_schema_vec or default
            let mut type_schema = type_schema_vec.pop().unwrap_or_default();
            // on every new type_id found insert type_schema to type_schema_vec
            let type_id: Uuid = row.get(0);
            if let Some(value) = last_id {
                if value != type_id {
                    // insert new type_schema to type_schema_vec
                    type_schema_vec.push(type_schema.clone());
                    type_schema = TypeSchema::default();
                }
            }
            last_id = Some(type_id);
            type_schema.id = type_id;
            type_schema.name = row.get(1);
            type_schema.description = row.get(2);
            // update type_schema if non empty model_id found
            let model_id: Result<Uuid, Error> = row.try_get(3);
            if let Ok(value) = model_id {
                type_schema.model_ids.push(value);
            }
            // update type_schema_vec with updated type_schema
            type_schema_vec.push(type_schema.clone());
        })
        .fetch_all(pool)
        .await?;

    Ok(type_schema_vec)
}

pub(crate) async fn insert_device_type(pool: &Pool<Postgres>,
    id: Uuid,
    name: &str,
    description: Option<&str>
) -> Result<Uuid, Error>
{
    let (sql, values) = Query::insert()
        .into_table(DeviceType::Table)
        .columns([
            DeviceType::TypeId,
            DeviceType::Name,
            DeviceType::Description
        ])
        .values([
            id.into(),
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

pub(crate) async fn update_device_type(pool: &Pool<Postgres>,
    id: Uuid,
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
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_device_type(pool: &Pool<Postgres>, 
    id: Uuid
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(DeviceType::Table)
        .and_where(Expr::col(DeviceType::TypeId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn insert_device_type_model(pool: &Pool<Postgres>,
    id: Uuid,
    model_id: Uuid
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
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_device_type_model(pool: &Pool<Postgres>, 
    id: Uuid,
    model_id: Uuid
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(DeviceTypeModel::Table)
        .and_where(Expr::col(DeviceTypeModel::TypeId).eq(id))
        .and_where(Expr::col(DeviceTypeModel::ModelId).eq(model_id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
