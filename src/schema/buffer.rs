use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc};
use crate::schema::value::{DataType, DataValue, ArrayDataValue};

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
    pub index: Option<u16>,
    pub data: Vec<DataValue>,
    pub status: String
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct BufferBytesSchema {
    pub(crate) id: u32,
    pub(crate) device_id: u64,
    pub(crate) model_id: u32,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) index: Option<u16>,
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
