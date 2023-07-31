use sea_query::Iden;
use crate::schema::value::{ConfigValue, ConfigType, DataType, DataIndexing};
use rmcs_resource_api::{common, model};

#[derive(Iden)]
pub(crate) enum Model {
    Table,
    ModelId,
    Indexing,
    Category,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum ModelType {
    Table,
    ModelId,
    Index,
    Type,
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
    pub id: i32,
    pub indexing: DataIndexing,
    pub category: String,
    pub name: String,
    pub description: String,
    pub types: Vec<DataType>,
    pub configs: Vec<Vec<ModelConfigSchema>>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ModelConfigSchema {
    pub id: i32,
    pub model_id: i32,
    pub index: i16,
    pub name: String,
    pub value: ConfigValue,
    pub category: String
}

impl From<model::ModelSchema> for ModelSchema {
    fn from(value: model::ModelSchema) -> Self {
        Self {
            id: value.id,
            indexing: DataIndexing::from(common::DataIndexing::from_i32(value.indexing).unwrap_or_default()),
            category: value.category,
            name: value.name,
            description: value.description,
            types: value.types.into_iter().map(|e| {
                    DataType::from(common::DataType::from_i32(e).unwrap_or_default())
                })
                .collect(),
            configs: value.configs.into_iter().map(|e| {
                    e.configs.into_iter().map(|e| e.into()).collect()
                }).collect()
        }
    }
}

impl Into<model::ModelSchema> for ModelSchema {
    fn into(self) -> model::ModelSchema {
        model::ModelSchema {
            id: self.id,
            indexing: Into::<common::DataIndexing>::into(self.indexing).into(),
            category: self.category,
            name: self.name,
            description: self.description,
            types: self.types.into_iter().map(|e| {
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
            model_id: value.model_id,
            index: value.index as i16,
            name: value.name,
            value: ConfigValue::from_bytes(
                &value.config_bytes, 
                ConfigType::from(common::ConfigType::from_i32(value.config_type).unwrap_or_default())
            ),
            category: value.category
        }
    }
}

impl Into<model::ConfigSchema> for ModelConfigSchema {
    fn into(self) -> model::ConfigSchema {
        model::ConfigSchema {
            id: self.id,
            model_id: self.model_id,
            index: self.index as i32,
            name: self.name,
            config_bytes: self.value.to_bytes(),
            config_type: Into::<common::ConfigType>::into(self.value.get_type()).into(),
            category: self.category
        }
    }
}
