use sea_query::Iden;
use uuid::Uuid;
use crate::schema::value::{DataValue, DataType};
use rmcs_resource_api::device;

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
    pub id: Uuid,
    pub gateway_id: Uuid,
    pub serial_number: String,
    pub name: String,
    pub description: String,
    pub type_: TypeSchema,
    pub configs: Vec<DeviceConfigSchema>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GatewaySchema {
    pub id: Uuid,
    pub serial_number: String,
    pub name: String,
    pub description: String,
    pub type_: TypeSchema,
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
            type_: self.type_,
            configs: self.configs.into_iter().map(|el| el.into_gateway_config()).collect()
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct TypeSchema {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub model_ids: Vec<Uuid>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DeviceConfigSchema {
    pub id: i32,
    pub device_id: Uuid,
    pub name: String,
    pub value: DataValue,
    pub category: String
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GatewayConfigSchema {
    pub id: i32,
    pub gateway_id: Uuid,
    pub name: String,
    pub value: DataValue,
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

impl From<device::DeviceSchema> for DeviceSchema {
    fn from(value: device::DeviceSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            gateway_id: Uuid::from_slice(&value.gateway_id).unwrap_or_default(),
            serial_number: value.serial_number,
            name: value.name,
            description: value.description,
            type_: value.device_type.map(|s| s.into()).unwrap_or_default(),
            configs: value.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<device::DeviceSchema> for DeviceSchema {
    fn into(self) -> device::DeviceSchema {
        device::DeviceSchema {
            id: self.id.as_bytes().to_vec(),
            gateway_id: self.gateway_id.as_bytes().to_vec(),
            serial_number: self.serial_number,
            name: self.name,
            description: self.description,
            device_type: Some(self.type_.into()),
            configs: self.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<device::GatewaySchema> for GatewaySchema {
    fn from(value: device::GatewaySchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            serial_number: value.serial_number,
            name: value.name,
            description: value.description,
            type_:  value.gateway_type.map(|s| s.into()).unwrap_or_default(),
            configs: value.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<device::GatewaySchema> for GatewaySchema {
    fn into(self) -> device::GatewaySchema {
        device::GatewaySchema {
            id: self.id.as_bytes().to_vec(),
            serial_number: self.serial_number,
            name: self.name,
            description: self.description,
            gateway_type: Some(self.type_.into()),
            configs: self.configs.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<device::ConfigSchema> for DeviceConfigSchema {
    fn from(value: device::ConfigSchema) -> Self {
        Self {
            id: value.id,
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            name: value.name,
            value: DataValue::from_bytes(
                &value.config_bytes,
                DataType::from(value.config_type)
            ),
            category: value.category
        }
    }
}

impl Into<device::ConfigSchema> for DeviceConfigSchema {
    fn into(self) -> device::ConfigSchema {
        device::ConfigSchema {
            id: self.id,
            device_id: self.device_id.as_bytes().to_vec(),
            name: self.name,
            config_bytes: self.value.to_bytes(),
            config_type: self.value.get_type().into(),
            category: self.category
        }
    }
}

impl From<device::ConfigSchema> for GatewayConfigSchema {
    fn from(value: device::ConfigSchema) -> Self {
        Self {
            id: value.id,
            gateway_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            name: value.name,
            value: DataValue::from_bytes(
                &value.config_bytes,
                DataType::from(value.config_type)
            ),
            category: value.category
        }
    }
}

impl Into<device::ConfigSchema> for GatewayConfigSchema {
    fn into(self) -> device::ConfigSchema {
        device::ConfigSchema {
            id: self.id,
            device_id: self.gateway_id.as_bytes().to_vec(),
            name: self.name,
            config_bytes: self.value.to_bytes(),
            config_type: self.value.get_type().into(),
            category: self.category
        }
    }
}

impl From<device::TypeSchema> for TypeSchema {
    fn from(value: device::TypeSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            description: value.description,
            model_ids: value.model_ids.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect()
        }
    }
}

impl Into<device::TypeSchema> for TypeSchema {
    fn into(self) -> device::TypeSchema {
        device::TypeSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            description: self.description,
            model_ids: self.model_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
        }
    }
}
