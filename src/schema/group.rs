use sea_query::Iden;
use rmcs_resource_api::group;

#[derive(Iden)]
pub enum GroupModel {
    Table,
    Name,
    GroupId,
    Category,
    Description
}

#[derive(Iden)]
pub enum GroupModelMap {
    Table,
    GroupId,
    ModelId
}

#[derive(Iden)]
pub enum GroupDevice {
    Table,
    GroupId,
    Name,
    Kind,
    Category,
    Description
}

#[derive(Iden)]
pub enum GroupDeviceMap {
    Table,
    GroupId,
    DeviceId
}

pub(crate) enum GroupKind {
    Model,
    Device,
    Gateway
}

impl std::string::ToString for GroupKind {
    fn to_string(&self) -> String {
        match &self {
            GroupKind::Device => String::from("DEVICE"),
            GroupKind::Gateway => String::from("GATEWAY"),
            _ => String::from("")
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct GroupSchema {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) category: String,
    pub(crate) description: String,
    pub(crate) members: Vec<u64>
}

impl GroupSchema {
    pub(crate) fn into_group_model(self) -> GroupModelSchema
    {
        GroupModelSchema {
            id: self.id,
            name: self.name,
            category: self.category,
            description: self.description,
            models: self.members.into_iter().map(|el| el as u32).collect()
        }
    }
    pub(crate) fn into_group_device(self) -> GroupDeviceSchema
    {
        GroupDeviceSchema {
            id: self.id,
            name: self.name,
            category: self.category,
            description: self.description,
            devices: self.members
        }
    }
    pub(crate) fn into_group_gateway(self) -> GroupGatewaySchema
    {
        GroupGatewaySchema {
            id: self.id,
            name: self.name,
            category: self.category,
            description: self.description,
            gateways: self.members
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupModelSchema {
    pub id: u32,
    pub name: String,
    pub category: String,
    pub description: String,
    pub models: Vec<u32>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupDeviceSchema {
    pub id: u32,
    pub name: String,
    pub category: String,
    pub description: String,
    pub devices: Vec<u64>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupGatewaySchema {
    pub id: u32,
    pub name: String,
    pub category: String,
    pub description: String,
    pub gateways: Vec<u64>
}

impl From<group::GroupModelSchema> for GroupModelSchema {
    fn from(value: group::GroupModelSchema) -> Self {
        Self {
            id: value.id,
            name: value.name,
            category: value.category,
            description: value.description,
            models: value.models
        }
    }
}

impl Into<group::GroupModelSchema> for GroupModelSchema {
    fn into(self) -> group::GroupModelSchema {
        group::GroupModelSchema {
            id: self.id,
            name: self.name,
            category: self.category,
            description: self.description,
            models: self.models
        }
    }
}

impl From<group::GroupDeviceSchema> for GroupDeviceSchema {
    fn from(value: group::GroupDeviceSchema) -> Self {
        Self {
            id: value.id,
            name: value.name,
            category: value.category,
            description: value.description,
            devices: value.devices
        }
    }
}

impl Into<group::GroupDeviceSchema> for GroupDeviceSchema {
    fn into(self) -> group::GroupDeviceSchema {
        group::GroupDeviceSchema {
            id: self.id,
            name: self.name,
            category: self.category,
            description: self.description,
            devices: self.devices
        }
    }
}
