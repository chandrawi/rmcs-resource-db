use std::str::FromStr;

use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use crate::schema::value::{DataType, DataValue, ArrayDataValue};
use rmcs_resource_api::{common, buffer};

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum DataBuffer {
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
    pub id: i32,
    pub device_id: i64,
    pub model_id: i32,
    pub timestamp: DateTime<Utc>,
    pub index: i32,
    pub data: Vec<DataValue>,
    pub status: String
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct BufferBytesSchema {
    pub(crate) id: i32,
    pub(crate) device_id: i64,
    pub(crate) model_id: i32,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) index: i32,
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
            index: value.index,
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
            index: self.index as i32,
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| {
                    Into::<common::DataType>::into(e.get_type()).into()
                }).collect(),
            status: buffer::BufferStatus::from_str_name(&self.status).unwrap_or_default().into()
        }
    }
}

pub(crate) enum BufferStatus {
    Default,
    Error,
    Convert,
    AnalyzeGateway,
    AnalyzeServer,
    TransferGateway,
    TransferServer,
    Backup,
    Delete,
}

impl From<i16> for BufferStatus {
    fn from(value: i16) -> Self {
        match value {
            1 => Self::Error,
            2 => Self::Convert,
            3 => Self::AnalyzeGateway,
            4 => Self::AnalyzeServer,
            5 => Self::TransferGateway,
            6 => Self::TransferServer,
            7 => Self::Backup,
            8 => Self::Delete,
            _ => Self::Default
        }
    }
}

impl From<BufferStatus> for i16 {
    fn from(value: BufferStatus) -> Self {
        match value {
            BufferStatus::Default => 0,
            BufferStatus::Error => 1,
            BufferStatus::Convert => 2,
            BufferStatus::AnalyzeGateway => 3,
            BufferStatus::AnalyzeServer => 4,
            BufferStatus::TransferGateway => 5,
            BufferStatus::TransferServer => 6,
            BufferStatus::Backup => 7,
            BufferStatus::Delete => 8
        }
    }
}

impl FromStr for BufferStatus {
    type Err = std::string::ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ERROR" => Ok(Self::Error),
            "CONVERT" => Ok(Self::Convert),
            "ANALYZE_GATEWAY" => Ok(Self::AnalyzeGateway),
            "ANALYZE_SERVER" => Ok(Self::AnalyzeServer),
            "TRANSFER_GATEWAY" => Ok(Self::TransferGateway),
            "TRANSFER_SERVER" => Ok(Self::TransferServer),
            "BACKUP" => Ok(Self::Backup),
            "DELETE" => Ok(Self::Delete),
            _ => Ok(Self::Default)
        }
    }
}

impl ToString for BufferStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Default => String::from("DEFAULT"),
            Self::Error => String::from("ERROR"),
            Self::Convert => String::from("CONVERT"),
            Self::AnalyzeGateway => String::from("ANALYZE_GATEWAY"),
            Self::AnalyzeServer => String::from("ANALYZE_SERVER"),
            Self::TransferGateway => String::from("TRANSFER_GATEWAY"),
            Self::TransferServer => String::from("TRANSFER_SERVER"),
            Self::Backup => String::from("BACKUP"),
            Self::Delete => String::from("DELETE")
        }
    }
}
