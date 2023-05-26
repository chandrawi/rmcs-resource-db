use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use crate::schema::value::{DataType, DataValue, ArrayDataValue};
use rmcs_resource_api::{common, buffer};

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum BufferData {
    Table,
    Id,
    DeviceId,
    ModelId,
    Timestamp,
    Index,
    Data,
    Status
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct BufferSchema {
    pub id: u32,
    pub device_id: u64,
    pub model_id: u32,
    pub timestamp: DateTime<Utc>,
    pub index: u16,
    pub data: Vec<DataValue>,
    pub status: String
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct BufferBytesSchema {
    pub(crate) id: u32,
    pub(crate) device_id: u64,
    pub(crate) model_id: u32,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) index: u16,
    pub(crate) bytes: Vec<u8>,
    pub(crate) status: String
}

impl BufferBytesSchema {
    pub(crate) fn to_buffer_schema(self, types: &[DataType]) -> BufferSchema
    {
        BufferSchema {
            id: self.id,
            device_id: self.device_id,
            model_id: self.model_id,
            timestamp: self.timestamp,
            index: self.index,
            data: ArrayDataValue::from_bytes(&self.bytes, types).to_vec(),
            status: self.status
        }
    }
}

impl From<buffer::BufferSchema> for BufferSchema {
    fn from(value: buffer::BufferSchema) -> Self {
        Self {
            id: value.id,
            device_id: value.device_id,
            model_id: value.model_id,
            timestamp: Utc.timestamp_nanos(value.timestamp),
            index: value.index as u16,
            data: ArrayDataValue::from_bytes(
                    &value.data_bytes,
                    value.data_type.into_iter().map(|e| {
                        DataType::from(common::DataType::from_i32(e).unwrap_or_default())
                    })
                    .collect::<Vec<DataType>>()
                    .as_slice()
                ).to_vec(),
            status: buffer::BufferStatus::from_i32(value.status).unwrap_or_default().as_str_name().to_owned()
        }
    }
}

impl Into<buffer::BufferSchema> for BufferSchema {
    fn into(self) -> buffer::BufferSchema {
        buffer::BufferSchema {
            id: self.id,
            device_id: self.device_id,
            model_id: self.model_id,
            timestamp: self.timestamp.timestamp_nanos(),
            index: self.index as u32,
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| {
                    Into::<common::DataType>::into(e.get_type()).into()
                }).collect(),
            status: buffer::BufferStatus::from_str_name(&self.status).unwrap_or_default().into()
        }
    }
}
