use std::str::FromStr;
use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
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
    Data,
    Status
}

#[derive(Debug, Default, PartialEq, Clone)]
pub enum BufferStatus {
    #[default]
    Default,
    Error,
    Delete,
    Hold,
    SendUplink,
    SendDownlink,
    TransferLocal,
    TransferGateway,
    TransferServer,
    Backup,
    Restore,
    Analysis1,
    Analysis2,
    Analysis3,
    Analysis4,
    Analysis5,
    Analysis6,
    Analysis7,
    Analysis8,
    Analysis9,
    Analysis10,
    ExternalInput,
    ExternalOutput,
    BufferCode(i16)
}

impl From<i16> for BufferStatus {
    fn from(value: i16) -> Self {
        match value {
            0 => Self::Default,
            1 => Self::Error,
            2 => Self::Delete,
            3 => Self::Hold,
            4 => Self::SendUplink,
            5 => Self::SendDownlink,
            6 => Self::TransferLocal,
            7 => Self::TransferGateway,
            8 => Self::TransferServer,
            9 => Self::Backup,
            10 => Self::Restore,
            11 => Self::Analysis1,
            12 => Self::Analysis2,
            13 => Self::Analysis3,
            14 => Self::Analysis4,
            15 => Self::Analysis5,
            16 => Self::Analysis6,
            17 => Self::Analysis7,
            18 => Self::Analysis8,
            19 => Self::Analysis9,
            20 => Self::Analysis10,
            21 => Self::ExternalInput,
            22 => Self::ExternalOutput,
            _ => Self::BufferCode(value)
        }
    }
}

impl From<BufferStatus> for i16 {
    fn from(value: BufferStatus) -> Self {
        match value {
            BufferStatus::Default => 0,
            BufferStatus::Error => 1,
            BufferStatus::Delete => 2,
            BufferStatus::Hold => 3,
            BufferStatus::SendUplink => 4,
            BufferStatus::SendDownlink => 5,
            BufferStatus::TransferLocal => 6,
            BufferStatus::TransferGateway => 7,
            BufferStatus::TransferServer => 8,
            BufferStatus::Backup => 9,
            BufferStatus::Restore => 10,
            BufferStatus::Analysis1 => 11,
            BufferStatus::Analysis2 => 12,
            BufferStatus::Analysis3 => 13,
            BufferStatus::Analysis4 => 14,
            BufferStatus::Analysis5 => 15,
            BufferStatus::Analysis6 => 16,
            BufferStatus::Analysis7 => 17,
            BufferStatus::Analysis8 => 18,
            BufferStatus::Analysis9 => 19,
            BufferStatus::Analysis10 => 20,
            BufferStatus::ExternalInput => 21,
            BufferStatus::ExternalOutput => 22,
            BufferStatus::BufferCode(i) => i
        }
    }
}

impl FromStr for BufferStatus {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DEFAULT" => Ok(Self::Default),
            "ERROR" => Ok(Self::Error),
            "DELETE" => Ok(Self::Delete),
            "HOLD" => Ok(Self::Hold),
            "SEND_UPLINK" => Ok(Self::SendUplink),
            "SEND_DOWNLINK" => Ok(Self::SendDownlink),
            "TRANSFER_LOCAL" => Ok(Self::TransferLocal),
            "TRANSFER_GATEWAY" => Ok(Self::TransferGateway),
            "TRANSFER_SERVER" => Ok(Self::TransferServer),
            "BACKUP" => Ok(Self::Backup),
            "RESTORE" => Ok(Self::Restore),
            "ANALYSIS_1" => Ok(Self::Analysis1),
            "ANALYSIS_2" => Ok(Self::Analysis2),
            "ANALYSIS_3" => Ok(Self::Analysis3),
            "ANALYSIS_4" => Ok(Self::Analysis4),
            "ANALYSIS_5" => Ok(Self::Analysis5),
            "ANALYSIS_6" => Ok(Self::Analysis6),
            "ANALYSIS_7" => Ok(Self::Analysis7),
            "ANALYSIS_8" => Ok(Self::Analysis8),
            "ANALYSIS_9" => Ok(Self::Analysis9),
            "ANALYSIS_10" => Ok(Self::Analysis10),
            "EXTERNAL_INPUT" => Ok(Self::ExternalInput),
            "EXTERNAL_OUTPUT" => Ok(Self::ExternalOutput),
            _ => s.parse::<i16>().map(|i| Self::BufferCode(i))
        }
    }
}

impl ToString for BufferStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Default => String::from("DEFAULT"),
            Self::Error => String::from("ERROR"),
            Self::Delete => String::from("DELETE"),
            Self::Hold => String::from("HOLD"),
            Self::SendUplink => String::from("SEND_UPLINK"),
            Self::SendDownlink => String::from("SEND_DOWNLINK"),
            Self::TransferLocal => String::from("TRANSFER_LOCAL"),
            Self::TransferGateway => String::from("TRANSFER_GATEWAY"),
            Self::TransferServer => String::from("TRANSFER_SERVER"),
            Self::Backup => String::from("BACKUP"),
            Self::Restore => String::from("RESTORE"),
            Self::Analysis1 => String::from("ANALYSIS1"),
            Self::Analysis2 => String::from("ANALYSIS2"),
            Self::Analysis3 => String::from("ANALYSIS3"),
            Self::Analysis4 => String::from("ANALYSIS4"),
            Self::Analysis5 => String::from("ANALYSIS5"),
            Self::Analysis6 => String::from("ANALYSIS6"),
            Self::Analysis7 => String::from("ANALYSIS7"),
            Self::Analysis8 => String::from("ANALYSIS8"),
            Self::Analysis9 => String::from("ANALYSIS9"),
            Self::Analysis10 => String::from("ANALYSIS10"),
            Self::ExternalInput => String::from("EXTERNAL_INPUT"),
            Self::ExternalOutput => String::from("EXTERNAL_OUTPUT"),
            Self::BufferCode(i) => i.to_string()
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct BufferSchema {
    pub id: i32,
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub data: Vec<DataValue>,
    pub status: BufferStatus
}

impl From<buffer::BufferSchema> for BufferSchema {
    fn from(value: buffer::BufferSchema) -> Self {
        Self {
            id: value.id,
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
                ).to_vec(),
            status: BufferStatus::from(value.status as i16)
        }
    }
}

impl Into<buffer::BufferSchema> for BufferSchema {
    fn into(self) -> buffer::BufferSchema {
        buffer::BufferSchema {
            id: self.id,
            device_id: self.device_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            timestamp: self.timestamp.timestamp_micros(),
            data_bytes: ArrayDataValue::from_vec(&self.data).to_bytes(),
            data_type: self.data.into_iter().map(|e| {
                    Into::<common::DataType>::into(e.get_type()).into()
                }).collect(),
            status: i16::from(self.status).into()
        }
    }
}
