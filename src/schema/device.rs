use sea_query::Iden;
use crate::schema::value::ConfigValue;

#[derive(Iden)]
pub(crate) enum Device {
    Table,
    DeviceId,
    GatewayId,
    TypeId,
    SerialNumber,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum DeviceType {
    Table,
    TypeId,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum DeviceTypeModel {
    Table,
    TypeId,
    ModelId
}

#[derive(Iden)]
pub(crate) enum DeviceConfig {
    Table,
    Id,
    DeviceId,
    Name,
    Value,
    Type,
    Category
}

pub(crate) enum DeviceKind {
    Device,
    Gateway
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DeviceSchema {
    pub id: u64,
    pub gateway_id: u64,
    pub serial_number: String,
    pub name: String,
    pub description: String,
    pub types: TypeSchema,
    pub configs: Vec<DeviceConfigSchema>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GatewaySchema {
    pub id: u64,
    pub serial_number: String,
    pub name: String,
    pub description: String,
    pub types: TypeSchema,
    pub configs: Vec<GatewayConfigSchema>
}

impl DeviceSchema {
    pub(crate) fn into_gateway(self) -> GatewaySchema
    {
        GatewaySchema {
            id: self.gateway_id,
            serial_number: self.serial_number,
            name: self.name,
            description: self.description,
            types: self.types,
            configs: self.configs.into_iter().map(|el| el.into_gateway_config()).collect()
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct TypeSchema {
    pub id: u32,
    pub name: String,
    pub description: String,
    pub models: Vec<u32>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DeviceConfigSchema {
    pub id: u32,
    pub device_id: u64,
    pub name: String,
    pub value: ConfigValue,
    pub category: String
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GatewayConfigSchema {
    pub id: u32,
    pub gateway_id: u64,
    pub name: String,
    pub value: ConfigValue,
    pub category: String
}

impl DeviceConfigSchema {
    pub(crate) fn into_gateway_config(self) -> GatewayConfigSchema
    {
        GatewayConfigSchema {
            id: self.id,
            gateway_id: self.device_id,
            name: self.name,
            value: self.value,
            category: self.category
        }
    }
}
