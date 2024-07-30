use sea_query::Iden;
use uuid::Uuid;
use rmcs_resource_api::set;

#[derive(Iden)]
pub(crate) enum Set {
    Table,
    SetId,
    TemplateId,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum SetMap {
    Table,
    SetId,
    DeviceId,
    ModelId,
    DataIndex,
    SetPosition,
    SetNumber
}

#[derive(Iden)]
pub(crate) enum SetTemplate {
    Table,
    TemplateId,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum SetTemplateMap {
    Table,
    TemplateId,
    TypeId,
    ModelId,
    DataIndex,
    TemplateIndex
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SetSchema {
    pub id: Uuid,
    pub template_id: Uuid,
    pub name: String,
    pub description: String,
    pub members: Vec<SetMember>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SetMember {
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub data_index: Vec<u8>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SetTemplateSchema {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub members: Vec<SetTemplateMember>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SetTemplateMember {
    pub type_id: Uuid,
    pub model_id: Uuid,
    pub data_index: Vec<u8>
}

impl From<set::SetSchema> for SetSchema {
    fn from(value: set::SetSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            template_id: Uuid::from_slice(&value.template_id).unwrap_or_default(),
            name: value.name,
            description: value.description,
            members: value.members.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<set::SetSchema> for SetSchema {
    fn into(self) -> set::SetSchema {
        set::SetSchema {
            id: self.id.as_bytes().to_vec(),
            template_id: self.template_id.as_bytes().to_vec(),
            name: self.name,
            description: self.description,
            members: self.members.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<set::SetMember> for SetMember {
    fn from(value: set::SetMember) -> Self {
        Self {
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            data_index: value.data_index
        }
    }
}

impl Into<set::SetMember> for SetMember {
    fn into(self) -> set::SetMember {
        set::SetMember {
            device_id: self.device_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            data_index: self.data_index
        }
    }
}

impl From<set::SetTemplateSchema> for SetTemplateSchema {
    fn from(value: set::SetTemplateSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            name: value.name,
            description: value.description,
            members: value.members.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl Into<set::SetTemplateSchema> for SetTemplateSchema {
    fn into(self) -> set::SetTemplateSchema {
        set::SetTemplateSchema {
            id: self.id.as_bytes().to_vec(),
            name: self.name,
            description: self.description,
            members: self.members.into_iter().map(|e| e.into()).collect()
        }
    }
}

impl From<set::SetTemplateMember> for SetTemplateMember {
    fn from(value: set::SetTemplateMember) -> Self {
        Self {
            type_id: Uuid::from_slice(&value.type_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            data_index: value.data_index
        }
    }
}

impl Into<set::SetTemplateMember> for SetTemplateMember {
    fn into(self) -> set::SetTemplateMember {
        set::SetTemplateMember {
            type_id: self.type_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            data_index: self.data_index
        }
    }
}
