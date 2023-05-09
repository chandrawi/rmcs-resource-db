use sqlx::{Pool, Row, Error};
use sqlx::mysql::{MySql, MySqlRow};
use sea_query::{MysqlQueryBuilder, Query, Expr, Order, Func};
use sea_query_binder::SqlxBinder;

use crate::schema::value::{ConfigValue, ConfigType};
use crate::schema::device::{
    Device, DeviceType, DeviceTypeModel, DeviceConfig, DeviceKind,
    DeviceSchema, DeviceConfigSchema
};

enum DeviceSelector {
    Id(u64),
    Gateway(u64),
    Type(u32),
    SN(String),
    Name(String),
    GatewayType(u64, u32),
    GatewayName(u64, String)
}

enum ConfigSelector {
    Id(u32),
    Device(u64)
}

async fn select_join_device(pool: &Pool<MySql>, 
    kind: DeviceKind,
    selector: DeviceSelector
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

    match selector {
        DeviceSelector::Id(id) => {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::DeviceId)).eq(id)).to_owned();
        },
        DeviceSelector::Gateway(id) => {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::GatewayId)).eq(id)).to_owned();
        },
        DeviceSelector::Type(ty) => {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::TypeId)).eq(ty)).to_owned();
        },
        DeviceSelector::SN(sn) => {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::SerialNumber)).eq(sn)).to_owned();
        },
        DeviceSelector::Name(name) => {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::Name)).like(name)).to_owned();
        },
        DeviceSelector::GatewayType(id, ty) => {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::GatewayId)).eq(id))
                .and_where(Expr::col((Device::Table, Device::TypeId)).eq(ty))
                .to_owned();
        },
        DeviceSelector::GatewayName(id, name) => {
            stmt = stmt.and_where(Expr::col((Device::Table, Device::GatewayId)).eq(id))
                .and_where(Expr::col((Device::Table, Device::Name)).like(name))
                .to_owned();
        }
    }
    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(
            Expr::col((DeviceConfig::Table, DeviceConfig::DeviceId)).equals((Device::Table, Device::GatewayId))
        ).to_owned()
    }
    let (sql, values) = stmt
        .order_by((Device::Table, Device::DeviceId), Order::Asc)
        .order_by((DeviceType::Table, DeviceType::TypeId), Order::Asc)
        .order_by((DeviceTypeModel::Table, DeviceTypeModel::ModelId), Order::Asc)
        .order_by((DeviceConfig::Table, DeviceConfig::Id), Order::Asc)
        .build_sqlx(MysqlQueryBuilder);

    let mut last_id: Option<u64> = None;
    let mut last_model: Option<u32> = None;
    let mut device_schema_vec: Vec<DeviceSchema> = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: MySqlRow| {
            // get last device_schema in device_schema_vec or default
            let mut device_schema = device_schema_vec.pop().unwrap_or_default();
            // on every new id found insert device_schema to device_schema_vec and reset last_model
            let id: u64 = row.get(0);
            if let Some(value) = last_id {
                if value != id {
                    device_schema_vec.push(device_schema.clone());
                    device_schema = DeviceSchema::default();
                    last_model = None;
                }
            }
            last_id = Some(id);
            device_schema.id = id;
            device_schema.gateway_id = row.get(1);
            device_schema.serial_number = row.get(3);
            device_schema.name = row.get(4);
            device_schema.description = row.get(5);
            device_schema.types.id = row.get(2);
            device_schema.types.name = row.get(6);
            device_schema.types.description = row.get(7);
            // on every new model_id found add model_vec and update device_schema types
            let model_id = row.try_get(8).unwrap_or(0);
            if last_model == None || last_model != Some(model_id) {
                device_schema.types.models.push(model_id);
                device_schema.configs = Vec::new();
            }
            last_model = Some(model_id);
            // update device_schema configs if non empty config found
            let config_id: Result<u32, Error> = row.try_get(9);
            if let Ok(cfg_id) = config_id {
                let bytes: Vec<u8> = row.try_get(11).unwrap_or_default();
                let type_string = ConfigType::from_str(row.try_get(12).unwrap_or_default());
                device_schema.configs.push(DeviceConfigSchema {
                    id: cfg_id,
                    device_id: id,
                    name: row.try_get(10).unwrap_or_default(),
                    value: ConfigValue::from_bytes(bytes.as_slice(), type_string),
                    category: row.try_get(13).unwrap_or_default()
                });
            }
            // update device_schema_vec with updated device_schema
            device_schema_vec.push(device_schema.clone());
        })
        .fetch_all(pool)
        .await?;

    Ok(device_schema_vec)
}

pub(crate) async fn select_device(pool: &Pool<MySql>,
    kind: DeviceKind,
    id: u64
) -> Result<DeviceSchema, Error>
{
    let results = select_join_device(pool, kind, DeviceSelector::Id(id)).await?;
    match results.into_iter().next() {
        Some(value) => Ok(value),
        None => Err(Error::RowNotFound)
    }
}

pub(crate) async fn select_device_by_sn(pool: &Pool<MySql>,
    kind: DeviceKind,
    serial_number: &str
) -> Result<DeviceSchema, Error>
{
    let results = select_join_device(pool, kind, DeviceSelector::SN(String::from(serial_number))).await?;
    match results.into_iter().next() {
        Some(value) => Ok(value),
        None => Err(Error::RowNotFound)
    }
}

pub(crate) async fn select_device_by_gateway(pool: &Pool<MySql>,
    kind: DeviceKind,
    gateway_id: u64
) -> Result<Vec<DeviceSchema>, Error>
{
    select_join_device(pool, kind, DeviceSelector::Gateway(gateway_id)).await
}

pub(crate) async fn select_device_by_type(pool: &Pool<MySql>,
    kind: DeviceKind,
    type_id: u32
) -> Result<Vec<DeviceSchema>, Error>
{
    select_join_device(pool, kind, DeviceSelector::Type(type_id)).await
}

pub(crate) async fn select_device_by_name(pool: &Pool<MySql>,
    kind: DeviceKind,
    name: &str
) -> Result<Vec<DeviceSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_join_device(pool, kind, DeviceSelector::Name(String::from(name_like))).await
}

pub(crate) async fn select_device_by_gateway_type(pool: &Pool<MySql>,
    kind: DeviceKind,
    gateway_id: u64,
    type_id: u32
) -> Result<Vec<DeviceSchema>, Error>
{
    select_join_device(pool, kind, DeviceSelector::GatewayType(gateway_id, type_id)).await
}

pub(crate) async fn select_device_by_gateway_name(pool: &Pool<MySql>,
    kind: DeviceKind,
    gateway_id: u64,
    name: &str
) -> Result<Vec<DeviceSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_join_device(pool, kind, DeviceSelector::GatewayName(gateway_id, name_like)).await
}

pub(crate) async fn insert_device(pool: &Pool<MySql>,
    id: u64,
    gateway_id: u64,
    type_id: u32,
    serial_number: &str,
    name: &str,
    description: Option<&str>
) -> Result<(), Error>
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
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn update_device(pool: &Pool<MySql>,
    kind: DeviceKind,
    id: u64,
    gateway_id: Option<u64>,
    type_id: Option<u32>,
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
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_device(pool: &Pool<MySql>, 
    kind: DeviceKind,
    id: u64
) -> Result<(), Error> 
{
    let mut stmt = Query::delete()
        .from_table(Device::Table)
        .and_where(Expr::col(Device::DeviceId).eq(id))
        .to_owned();

    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(Expr::col(Device::GatewayId).eq(id)).to_owned();
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

async fn select_device_config(pool: &Pool<MySql>,
    kind: DeviceKind,
    selector: ConfigSelector
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

    match selector {
        ConfigSelector::Id(id) => {
            stmt = stmt.and_where(Expr::col((DeviceConfig::Table, DeviceConfig::Id)).eq(id)).to_owned();
        },
        ConfigSelector::Device(device_id) => {
            stmt = stmt.and_where(Expr::col((DeviceConfig::Table, DeviceConfig::DeviceId)).eq(device_id)).to_owned();
        }
    }
    if let DeviceKind::Gateway = kind {
        stmt = stmt.and_where(
            Expr::col((DeviceConfig::Table, DeviceConfig::DeviceId)).equals((Device::Table, Device::GatewayId))
        ).to_owned()
    }
    let (sql, values) = stmt
        .order_by((DeviceConfig::Table, DeviceConfig::DeviceId), Order::Asc)
        .order_by((DeviceConfig::Table, DeviceConfig::Id), Order::Asc)
        .build_sqlx(MysqlQueryBuilder);

    let rows = sqlx::query_with(&sql, values)
        .map(|row: MySqlRow| {
            let bytes = row.get(3);
            let type_string = ConfigType::from_str(row.get(4));
            DeviceConfigSchema {
                id: row.get(0),
                device_id: row.get(1),
                name: row.get(2),
                value: ConfigValue::from_bytes(bytes, type_string),
                category: row.get(5)
            }
        })
        .fetch_all(pool)
        .await?;

    Ok(rows)
}

pub(crate) async fn select_device_config_by_id(pool: &Pool<MySql>,
    kind: DeviceKind,
    id: u32
) -> Result<DeviceConfigSchema, Error>
{
    let results = select_device_config(pool, kind, ConfigSelector::Id(id)).await?;
    match results.into_iter().next() {
        Some(value) => Ok(value),
        None => Err(Error::RowNotFound)
    }
}

pub(crate) async fn select_device_config_by_device(pool: &Pool<MySql>,
    kind: DeviceKind,
    device_id: u64
) -> Result<Vec<DeviceConfigSchema>, Error>
{
    select_device_config(pool, kind, ConfigSelector::Device(device_id)).await
}

pub(crate) async fn insert_device_config(pool: &Pool<MySql>,
    device_id: u64,
    name: &str,
    value: ConfigValue,
    category: &str
) -> Result<u32, Error>
{
    let config_value = value.to_bytes();
    let config_type = value.get_type().to_string();
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
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let sql = Query::select()
        .expr(Func::max(Expr::col(DeviceConfig::Id)))
        .from(DeviceConfig::Table)
        .to_string(MysqlQueryBuilder);
    let id: u32 = sqlx::query(&sql)
        .map(|row: MySqlRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_device_config(pool: &Pool<MySql>,
    id: u32,
    name: Option<&str>,
    value: Option<ConfigValue>,
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
        let type_ = value.get_type().to_string();
        stmt = stmt
            .value(DeviceConfig::Value, bytes)
            .value(DeviceConfig::Type, type_).to_owned();
    }
    if let Some(value) = category {
        stmt = stmt.value(DeviceConfig::Category, value).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(DeviceConfig::Id).eq(id))
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_device_config(pool: &Pool<MySql>, 
    id: u32
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(DeviceConfig::Table)
        .and_where(Expr::col(DeviceConfig::Id).eq(id))
        .build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
