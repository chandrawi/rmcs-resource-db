use sea_query::Iden;
use uuid::Uuid;
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
#[derive(Clone, PartialEq)]
pub(crate) enum GroupKind {
    Model,
    Device,
    Gateway
}

impl From<bool> for GroupKind {
    fn from(value: bool) -> Self {
        match value {
            false => Self::Device,
            true => Self::Gateway
        }
    }
}

impl From<GroupKind> for bool {
    fn from(value: GroupKind) -> Self {
        match value {
            GroupKind::Model => false,
            GroupKind::Device => false,
            GroupKind::Gateway => true
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct GroupSchema {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) category: String,
    pub(crate) description: String,
    pub(crate) members: Vec<Uuid>
}

impl GroupSchema {
    pub(crate) fn into_group_model(self) -> GroupModelSchema
    {
        GroupModelSchema {
            id: self.id,
            name: self.name,
            category: self.category,
            description: self.description,
            model_ids: self.members.into_iter().map(|el| el).collect()
        }
    }
    pub(crate) fn into_group_device(self) -> GroupDeviceSchema
    {
        GroupDeviceSchema {
            id: self.id,
            name: self.name,
            category: self.category,
            description: self.description,
            device_ids: self.members
        }
    }
    pub(crate) fn into_group_gateway(self) -> GroupGatewaySchema
    {
        GroupGatewaySchema {
            id: self.id,
            name: self.name,
            category: self.category,
            description: self.description,
            gateway_ids: self.members
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupModelSchema {
    pub id: Uuid,
    pub name: String,
    pub category: String,
    pub description: String,
    pub model_ids: Vec<Uuid>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupDeviceSchema {
    pub id: Uuid,
    pub name: String,
    pub category: String,
    pub description: String,
    pub device_ids: Vec<Uuid>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct GroupGatewaySchema {
    pub id: Uuid,
    pub name: String,
    pub category: String,
    pub description: String,
    pub gateway_ids: Vec<Uuid>
}

impl From<group::GroupModelSchema> for GroupModelSchema {
    fn from(value: group::GroupModelSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            category: value.category,
            description: value.description,
            model_ids: value.model_ids.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect()
        }
    }
}

impl Into<group::GroupModelSchema> for GroupModelSchema {
    fn into(self) -> group::GroupModelSchema {
        group::GroupModelSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            category: self.category,
            description: self.description,
            model_ids: self.model_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
        }
    }
}

impl From<group::GroupDeviceSchema> for GroupDeviceSchema {
    fn from(value: group::GroupDeviceSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            category: value.category,
            description: value.description,
            device_ids: value.device_ids.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect()
        }
    }
}

impl Into<group::GroupDeviceSchema> for GroupDeviceSchema {
    fn into(self) -> group::GroupDeviceSchema {
        group::GroupDeviceSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            category: self.category,
            description: self.description,
            device_ids: self.device_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
        }
    }
}

impl From<group::GroupDeviceSchema> for GroupGatewaySchema {
    fn from(value: group::GroupDeviceSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            category: value.category,
            description: value.description,
            gateway_ids: value.device_ids.into_iter().map(|u| Uuid::from_slice(&u).unwrap_or_default()).collect()
        }
    }
}

impl Into<group::GroupDeviceSchema> for GroupGatewaySchema {
    fn into(self) -> group::GroupDeviceSchema {
        group::GroupDeviceSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            category: self.category,
            description: self.description,
            device_ids: self.gateway_ids.into_iter().map(|u| u.as_bytes().to_vec()).collect()
        }
    }
}
