use sea_query::Iden;
use sqlx::types::chrono::NaiveDateTime;
use uuid::Uuid;
use crate::schema::value::{DataValue, ArrayDataValue, DataType, DataIndexing};
use crate::schema::model::ModelSchema;
use rmcs_resource_api::{common, data};

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum Data {
    Table,
    DeviceId,
    ModelId,
    Timestamp,
    Index,
    Data
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DataModel {
    pub(crate) id: Uuid,
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
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub timestamp: NaiveDateTime,
    pub index: i32,
    pub data: Vec<DataValue>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct DataBytesSchema {
    pub(crate) device_id: Uuid,
    pub(crate) model_id: Uuid,
    pub(crate) timestamp: NaiveDateTime,
    pub(crate) index: i32,
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

impl From<data::DataModel> for DataModel {
    fn from(value: data::DataModel) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            indexing: DataIndexing::from(common::DataIndexing::from_i32(value.indexing).unwrap_or_default()),
            types: value.types.into_iter().map(|e| {
                    DataType::from(common::DataType::from_i32(e).unwrap_or_default())
                }).collect()
        }
    }
}

impl Into<data::DataModel> for DataModel {
    fn into(self) -> data::DataModel {
        data::DataModel {
            id: self.id.as_bytes().to_vec(),
            indexing: Into::<common::DataIndexing>::into(self.indexing).into(),
            types: self.types.into_iter().map(|e| {
                    Into::<common::DataType>::into(e).into()
                }).collect()
        }
    }
}

impl From<data::DataSchema> for DataSchema {
    fn from(value: data::DataSchema) -> Self {
        Self {
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            timestamp: NaiveDateTime::from_timestamp_micros(value.timestamp).unwrap_or_default(),
            index: value.index,
            data: ArrayDataValue::from_bytes(
                    &value.data_bytes,
                    value.data_type.into_iter().map(|e| {
                        DataType::from(common::DataType::from_i32(e).unwrap_or_default())
                    })
                    .collect::<Vec<DataType>>()
                    .as_slice()
                ).to_vec()
        }
    }
}

impl Into<data::DataSchema> for DataSchema {
    fn into(self) -> data::DataSchema {
        data::DataSchema {
            device_id: self.device_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            timestamp: self.timestamp.timestamp_micros(),
            index: self.index as i32,
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| {
                    Into::<common::DataType>::into(e.get_type()).into()
                }).collect()
        }
    }
}
