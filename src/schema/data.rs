use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use crate::schema::value::{DataValue, ArrayDataValue, DataType};
use rmcs_resource_api::data;

#[derive(Iden)]
pub(crate) enum Data {
    Table,
    DeviceId,
    ModelId,
    Timestamp,
    Tag,
    Data
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DataSchema {
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub data: Vec<DataValue>,
    pub tag: i16
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct DataSetSchema {
    pub set_id: Uuid,
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
                    value.data_type.into_iter().map(|e| DataType::from(e))
                    .collect::<Vec<DataType>>()
                    .as_slice()
                ).to_vec(),
            tag: value.tag as i16
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
            data_type: self.data.into_iter().map(|e| e.get_type().into()).collect(),
            tag: self.tag as i32
        }
    }
}

impl From<data::DataSetSchema> for DataSetSchema {
    fn from(value: data::DataSetSchema) -> Self {
        Self {
            set_id: Uuid::from_slice(&value.set_id).unwrap_or_default(),
            timestamp: Utc.timestamp_nanos(value.timestamp * 1000),
            data: ArrayDataValue::from_bytes(
                    &value.data_bytes,
                    value.data_type.into_iter().map(|e| DataType::from(e))
                    .collect::<Vec<DataType>>()
                    .as_slice()
                ).to_vec()
        }
    }
}

impl Into<data::DataSetSchema> for DataSetSchema {
    fn into(self) -> data::DataSetSchema {
        data::DataSetSchema {
            set_id: self.set_id.as_bytes().to_vec(),
            timestamp: self.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| e.get_type().into()).collect()
        }
    }
}
