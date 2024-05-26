use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use crate::schema::value::{DataValue, ArrayDataValue, DataType};
use crate::schema::model::ModelSchema;
use rmcs_resource_api::{common, data};

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum Data {
    Table,
    DeviceId,
    ModelId,
    Timestamp,
    Data
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DataModel {
    pub(crate) id: Uuid,
    pub(crate) types: Vec<DataType>
}

impl std::convert::From<ModelSchema> for DataModel {
    fn from(value: ModelSchema) -> Self {
        DataModel {
            id: value.id,
            types: value.data_type
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DataSchema {
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub data: Vec<DataValue>
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct DataBytesSchema {
    pub(crate) device_id: Uuid,
    pub(crate) model_id: Uuid,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) bytes: Vec<u8>
}

impl DataBytesSchema {
    pub(crate) fn to_data_schema(self, types: &[DataType]) -> DataSchema
    {
        DataSchema {
            device_id: self.device_id,
            model_id: self.model_id,
            timestamp: self.timestamp,
            data: ArrayDataValue::from_bytes(&self.bytes, types).to_vec()
        }
    }
}

impl From<data::DataModel> for DataModel {
    fn from(value: data::DataModel) -> Self {
        Self {
            id: Uuid::from_slice(&value.id).unwrap_or_default(),
            types: value.types.into_iter().map(|e| {
                    DataType::from(common::DataType::try_from(e).unwrap_or_default())
                }).collect()
        }
    }
}

impl Into<data::DataModel> for DataModel {
    fn into(self) -> data::DataModel {
        data::DataModel {
            id: self.id.as_bytes().to_vec(),
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
            timestamp: Utc.timestamp_nanos(value.timestamp * 1000),
            data: ArrayDataValue::from_bytes(
                    &value.data_bytes,
                    value.data_type.into_iter().map(|e| {
                        DataType::from(common::DataType::try_from(e).unwrap_or_default())
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
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| {
                    Into::<common::DataType>::into(e.get_type()).into()
                }).collect()
        }
    }
}
