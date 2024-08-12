use sea_query::Iden;
use uuid::Uuid;
use crate::schema::value::{DataValue, DataType};
use rmcs_resource_api::{common, model};

#[derive(Iden)]
pub(crate) enum Model {
    Table,
    ModelId,
    Category,
    Name,
    Description,
    DataType
}

#[derive(Iden)]
pub(crate) enum ModelConfig {
    Table,
    Id,
    ModelId,
    Index,
    Name,
    Value,
    Type,
    Category
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ModelSchema {
    pub id: Uuid,
    pub category: String,
    pub name: String,
    pub description: String,
    pub data_type: Vec<DataType>,
    pub configs: Vec<Vec<ModelConfigSchema>>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ModelConfigSchema {
    pub id: i32,
    pub model_id: Uuid,
    pub index: i16,
    pub name: String,
    pub value: DataValue,
    pub category: String
}

impl From<model::ModelSchema> for ModelSchema {
    fn from(value: model::ModelSchema) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            category: value.category,
            name: value.name,
            description: value.description,
            data_type: value.data_type.into_iter().map(|e| {
                    DataType::from(common::DataType::try_from(e).unwrap_or_default())
                }).collect(),
            configs: value.configs.into_iter().map(|e| {
                    e.configs.into_iter().map(|e| e.into()).collect()
                }).collect()
        }
    }
}

impl Into<model::ModelSchema> for ModelSchema {
    fn into(self) -> model::ModelSchema {
        model::ModelSchema {
            id: self.id.as_bytes().to_vec(),
            category: self.category,
            name: self.name,
            description: self.description,
            data_type: self.data_type.into_iter().map(|e| {
                    Into::<common::DataType>::into(e).into()
                }).collect(),
            configs: self.configs.into_iter().map(|e| model::ConfigSchemaVec {
                    configs: e.into_iter().map(|e| e.into()).collect()
                }).collect()
        }
    }
}

impl From<model::ConfigSchema> for ModelConfigSchema {
    fn from(value: model::ConfigSchema) -> Self {
        Self {
            id: value.id,
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            index: value.index as i16,
            name: value.name,
            value: DataValue::from_bytes(
                &value.config_bytes, 
                DataType::from(common::DataType::try_from(value.config_type).unwrap_or_default())
            ),
            category: value.category
        }
    }
}

impl Into<model::ConfigSchema> for ModelConfigSchema {
    fn into(self) -> model::ConfigSchema {
        model::ConfigSchema {
            id: self.id,
            model_id: self.model_id.as_bytes().to_vec(),
            index: self.index as i32,
            name: self.name,
            config_bytes: self.value.to_bytes(),
            config_type: Into::<common::DataType>::into(self.value.get_type()).into(),
            category: self.category
        }
    }
}
