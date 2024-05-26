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
    pub(crate) data_type: Vec<DataType>
}

impl std::convert::From<ModelSchema> for DataModel {
    fn from(value: ModelSchema) -> Self {
        DataModel {
            id: value.id,
            data_type: value.data_type
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
