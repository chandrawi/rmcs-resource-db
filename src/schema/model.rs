use sea_query::Iden;
use crate::schema::value::{ConfigValue, DataType, DataIndexing};

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
    pub id: u32,
    pub indexing: DataIndexing,
    pub category: String,
    pub name: String,
    pub description: String,
    pub types: Vec<DataType>,
    pub configs: Vec<Vec<ModelConfigSchema>>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct ModelConfigSchema {
    pub id: u32,
    pub model_id: u32,
    pub index: u32,
    pub name: String,
    pub value: ConfigValue,
    pub category: String
}
