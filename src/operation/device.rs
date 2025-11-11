use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::value::{DataValue, DataType};
use crate::schema::device::{Device, DeviceType, DeviceTypeModel, DeviceConfig, DeviceKind, DeviceSchema, DeviceConfigSchema};

pub(crate) async fn select_device(pool: &Pool<Postgres>, 
    kind: DeviceKind,
    id: Option<Uuid>,
    serial_number: Option<&str>,
    ids: Option<&[Uuid]>,
    gateway_id: Option<Uuid>,
    type_id: Option<Uuid>,
    name: Option<&str>
) -> Result<Vec<DeviceSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (Device::Table, Device::DeviceId),
            (Device::Table, Device::GatewayId),
            (Device::Table, Device::TypeId),
            (Device::Table, Device::SerialNumber),
            (Device::Table, Device::Name),
            (Device::Table, Device::Description)
        ])
        .columns([
            (DeviceType::Table, DeviceType::Name),
            (DeviceType::Table, DeviceType::Description)
        ])
        .columns([
            (DeviceTypeModel::Table, DeviceTypeModel::ModelId)
        ])
        .columns([
            (DeviceConfig::Table, DeviceConfig::Id),
            (DeviceConfig::Table, DeviceConfig::Name),
            (DeviceConfig::Table, DeviceConfig::Value),
            (DeviceConfig::Table, DeviceConfig::Type),
            (DeviceConfig::Table, DeviceConfig::Category)
        ])
        .from(Device::Table)
        .inner_join(DeviceType::Table, 
            Expr::col((Device::Table, Device::TypeId))
            .equals((DeviceType::Table, DeviceType::TypeId))
        )
        .left_join(DeviceTypeModel::Table, 
            Expr::col((Device::Table, Device::TypeId))
            .equals((DeviceTypeModel::Table, DeviceTypeModel::TypeId))
        )
        .left_join(DeviceConfig::Table, 
            Expr::col((Device::Table, Device::DeviceId))
            .equals((DeviceConfig::Table, DeviceConfig::DeviceId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((Device::Table, Device::DeviceId)).eq(id)).to_owned();
    }
    else if let Some(sn) = serial_number {
        stmt = stmt.and_where(Expr::col((Device::Table, Device::SerialNumber)).eq(sn.to_owned())).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((Device::Table, Device::DeviceId)).is_in(ids.to_vec())).to_owned();
    }
    else {
        if let Some(gateway_id) = gateway_id {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::GatewayId)).eq(gateway_id)).to_owned();
        }
        if let Some(type_id) = type_id {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::TypeId)).eq(type_id)).to_owned();
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((Device::Table, Device::Name)).like(name_like)).to_owned();
        }
    }

    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(
            Expr::col((Device::Table, Device::DeviceId)).equals((Device::Table, Device::GatewayId))
        ).to_owned()
    }
    let (sql, values) = stmt
        .order_by((Device::Table, Device::DeviceId), Order::Asc)
        .order_by((DeviceType::Table, DeviceType::TypeId), Order::Asc)
        .order_by((DeviceTypeModel::Table, DeviceTypeModel::ModelId), Order::Asc)
        .order_by((DeviceConfig::Table, DeviceConfig::Id), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut last_id: Option<Uuid> = None;
    let mut last_model: Option<Uuid> = None;
    let mut device_schema_vec: Vec<DeviceSchema> = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            // get last device_schema in device_schema_vec or default
            let mut device_schema = device_schema_vec.pop().unwrap_or_default();
            // on every new id found insert device_schema to device_schema_vec and reset last_model
            let device_id: Uuid = row.get(0);
            if let Some(value) = last_id {
                if value != device_id {
                    device_schema_vec.push(device_schema.clone());
                    device_schema = DeviceSchema::default();
                    last_model = None;
                }
            }
            last_id = Some(device_id);
            device_schema.id = device_id;
            device_schema.gateway_id = row.get(1);
            device_schema.serial_number = row.get(3);
            device_schema.name = row.get(4);
            device_schema.description = row.get(5);
            device_schema.type_.id = row.get(2);
            device_schema.type_.name = row.get(6);
            device_schema.type_.description = row.get(7);
            // on every new model id found, add model id to type model and initialize a new config
            let model_id = row.try_get(8).ok();
            if last_model == None || last_model != Some(model_id.unwrap_or_default()) {
                if let Some(id) = model_id {
                    device_schema.type_.models.push(id);
                }
                device_schema.configs = Vec::new();
            }
            last_model = Some(model_id.unwrap_or_default());
            // update device_schema configs if non empty config found
            let config_id = row.try_get(9);
            let config_name = row.try_get(10);
            let config_bytes: Result<Vec<u8>,_> = row.try_get(11);
            let config_type: Result<i16,_> = row.try_get(12);
            let config_category = row.try_get(13);
            if let (Ok(id), Ok(name), Ok(bytes), Ok(type_), Ok(category)) = 
                (config_id, config_name, config_bytes, config_type, config_category) 
            {
                let value = DataValue::from_bytes(&bytes, DataType::from(type_));
                device_schema.configs.push(DeviceConfigSchema { id, device_id, name, value, category });
            }
            // update device_schema_vec with updated device_schema
            device_schema_vec.push(device_schema.clone());
        })
        .fetch_all(pool)
        .await?;

    Ok(device_schema_vec)
}

pub(crate) async fn insert_device(pool: &Pool<Postgres>,
    id: Uuid,
    gateway_id: Uuid,
    type_id: Uuid,
    serial_number: &str,
    name: &str,
    description: Option<&str>
) -> Result<Uuid, Error>
{
    let (sql, values) = Query::insert()
        .into_table(Device::Table)
        .columns([
            Device::DeviceId,
            Device::GatewayId,
            Device::TypeId,
            Device::SerialNumber,
            Device::Name,
            Device::Description
        ])
        .values([
            id.into(),
            gateway_id.into(),
            type_id.into(),
            serial_number.into(),
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

pub(crate) async fn update_device(pool: &Pool<Postgres>,
    kind: DeviceKind,
    id: Uuid,
    gateway_id: Option<Uuid>,
    type_id: Option<Uuid>,
    serial_number: Option<&str>,
    name: Option<&str>,
    description: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(Device::Table)
        .to_owned();

    if let Some(value) = gateway_id {
        stmt = stmt.value(Device::GatewayId, value).to_owned();
    }
    if let Some(value) = type_id {
        stmt = stmt.value(Device::TypeId, value).to_owned();
    }
    if let Some(value) = serial_number {
        stmt = stmt.value(Device::SerialNumber, value).to_owned();
    }
    if let Some(value) = name {
        stmt = stmt.value(Device::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(Device::Description, value).to_owned();
    }

    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(Expr::col(Device::GatewayId).eq(id)).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(Device::DeviceId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_device(pool: &Pool<Postgres>, 
    kind: DeviceKind,
    id: Uuid
) -> Result<(), Error> 
{
    let mut stmt = Query::delete()
        .from_table(Device::Table)
        .and_where(Expr::col(Device::DeviceId).eq(id))
        .to_owned();

    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(Expr::col(Device::GatewayId).eq(id)).to_owned();
    }
    let (sql, values) = stmt.build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn select_device_config(pool: &Pool<Postgres>,
    kind: DeviceKind,
    id: Option<i32>,
    device_id: Option<Uuid>
) -> Result<Vec<DeviceConfigSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (DeviceConfig::Table, DeviceConfig::Id),
            (DeviceConfig::Table, DeviceConfig::DeviceId),
            (DeviceConfig::Table, DeviceConfig::Name),
            (DeviceConfig::Table, DeviceConfig::Value),
            (DeviceConfig::Table, DeviceConfig::Type),
            (DeviceConfig::Table, DeviceConfig::Category)
        ])
        .columns([
            (Device::Table, Device::GatewayId)
        ])
        .from(DeviceConfig::Table)
        .inner_join(Device::Table, 
            Expr::col((DeviceConfig::Table, DeviceConfig::DeviceId))
            .equals((Device::Table, Device::DeviceId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((DeviceConfig::Table, DeviceConfig::Id)).eq(id)).to_owned();
    }
    else if let Some(device_id) = device_id {
        stmt = stmt.and_where(Expr::col((DeviceConfig::Table, DeviceConfig::DeviceId)).eq(device_id)).to_owned();
    }

    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(
            Expr::col((DeviceConfig::Table, DeviceConfig::DeviceId)).equals((Device::Table, Device::GatewayId))
        ).to_owned()
    }
    let (sql, values) = stmt
        .order_by((DeviceConfig::Table, DeviceConfig::DeviceId), Order::Asc)
        .order_by((DeviceConfig::Table, DeviceConfig::Id), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            let bytes = row.get(3);
            let type_ = DataType::from(row.get::<i16,_>(4));
            DeviceConfigSchema {
                id: row.get(0),
                device_id: row.get(1),
                name: row.get(2),
                value: DataValue::from_bytes(bytes, type_),
                category: row.get(5)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn insert_device_config(pool: &Pool<Postgres>,
    device_id: Uuid,
    name: &str,
    value: DataValue,
    category: &str
) -> Result<i32, Error>
{
    let config_value = value.to_bytes();
    let config_type = i16::from(value.get_type());
    let (sql, values) = Query::insert()
        .into_table(DeviceConfig::Table)
        .columns([
            DeviceConfig::DeviceId,
            DeviceConfig::Name,
            DeviceConfig::Value,
            DeviceConfig::Type,
            DeviceConfig::Category
        ])
        .values([
            device_id.into(),
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
        .expr(Func::max(Expr::col(DeviceConfig::Id)))
        .from(DeviceConfig::Table)
        .to_string(PostgresQueryBuilder);
    let id: i32 = sqlx::query(&sql)
        .map(|row: PgRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_device_config(pool: &Pool<Postgres>,
    id: i32,
    name: Option<&str>,
    value: Option<DataValue>,
    category: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(DeviceConfig::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(DeviceConfig::Name, value).to_owned();
    }
    if let Some(value) = value {
        let bytes = value.to_bytes();
        let type_ = i16::from(value.get_type());
        stmt = stmt
            .value(DeviceConfig::Value, bytes)
            .value(DeviceConfig::Type, type_).to_owned();
    }
    if let Some(value) = category {
        stmt = stmt.value(DeviceConfig::Category, value).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(DeviceConfig::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_device_config(pool: &Pool<Postgres>, 
    id: i32
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(DeviceConfig::Table)
        .and_where(Expr::col(DeviceConfig::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
