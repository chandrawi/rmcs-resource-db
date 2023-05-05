use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc};
use crate::schema::value::{DataValue, ArrayDataValue, DataType, DataIndexing};
use crate::schema::model::ModelSchema;

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum DataTimestamp {
    Table,
    DeviceId,
    ModelId,
    Timestamp,
    Data
}

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum DataTimestampIndex {
    Table,
    DeviceId,
    ModelId,
    Timestamp,
    Index,
    Data
}

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum DataTimestampMicros {
    Table,
    DeviceId,
    ModelId,
    Timestamp,
    Data
}

#[derive(Debug, PartialEq, Clone)]
pub struct DataModel {
    pub(crate) id: u32,
    pub(crate) indexing: DataIndexing,
    pub(crate) types: Vec<DataType>
}

impl std::convert::From<ModelSchema> for DataModel {
    fn from(value: ModelSchema) -> Self {
        DataModel {
            id: value.id,
            indexing: value.indexing,
            types: value.types
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DataSchema {
    pub device_id: u64,
    pub model_id: u32,
    pub timestamp: DateTime<Utc>,
    pub index: Option<u16>,
    pub data: Vec<DataValue>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct DataBytesSchema {
    pub(crate) device_id: u64,
    pub(crate) model_id: u32,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) index: Option<u16>,
    pub(crate) bytes: Vec<u8>
}

impl DataBytesSchema {
    pub(crate) fn to_data_schema(self, types: &[DataType]) -> DataSchema
    {
        DataSchema {
            device_id: self.device_id,
            model_id: self.model_id,
            timestamp: self.timestamp,
            index: self.index,
            data: ArrayDataValue::from_bytes(&self.bytes, types).to_vec()
        }
    }
}
