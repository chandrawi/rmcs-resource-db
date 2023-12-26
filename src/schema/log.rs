use std::str::FromStr;
use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use crate::schema::value::{LogValue, LogType};
use rmcs_resource_api::{common, log};

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum SystemLog {
    Table,
    DeviceId,
    Timestamp,
    Status,
    Value,
    Type
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct LogSchema {
    pub timestamp: DateTime<Utc>,
    pub device_id: Uuid,
    pub status: String,
    pub value: LogValue
}

impl From<log::LogSchema> for LogSchema {
    fn from(value: log::LogSchema) -> Self {
        Self {
            timestamp: Utc.timestamp_nanos(value.timestamp * 1000),
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            status: log::LogStatus::try_from(value.status).unwrap_or_default().as_str_name().to_owned(),
            value: LogValue::from_bytes(
                &value.log_bytes,
                LogType::from(common::ConfigType::try_from(value.log_type).unwrap_or_default())
            )
        }
    }
}

impl Into<log::LogSchema> for LogSchema {
    fn into(self) -> log::LogSchema {
        log::LogSchema {
            timestamp: self.timestamp.timestamp_micros(),
            device_id: self.device_id.as_bytes().to_vec(),
            status: log::LogStatus::from_str_name(&self.status).unwrap_or_default().into(),
            log_bytes: self.value.to_bytes(),
            log_type: Into::<common::ConfigType>::into(self.value.get_type()).into()
        }
    }
}

#[derive(Default)]
pub enum LogStatus {
    #[default]
    Default,
    Success,
    ErrorSend,
    ErrorTransfer,
    ErrorAnalysis,
    ErrorNetwork,
    FailRead,
    FailCreate,
    FailUpdate,
    FailDelete,
    InvalidToken,
    InvalidRequest,
    UnknownError,
    UnknownStatus,
    LogCode(i16)
}

impl From<i16> for LogStatus {
    fn from(value: i16) -> Self {
        match value {
            0 => Self::Default,
            1 => Self::Success,
            2 => Self::ErrorSend,
            3 => Self::ErrorTransfer,
            4 => Self::ErrorAnalysis,
            5 => Self::ErrorNetwork,
            6 => Self::FailRead,
            7 => Self::FailCreate,
            8 => Self::FailUpdate,
            9 => Self::FailDelete,
            10 => Self::InvalidToken,
            11 => Self::InvalidRequest,
            12 => Self::UnknownError,
            13 => Self::UnknownStatus,
            _ => Self::LogCode(value)
        }
    }
}

impl From<LogStatus> for i16 {
    fn from(value: LogStatus) -> Self {
        match value {
            LogStatus::Default => 0,
            LogStatus::Success => 1,
            LogStatus::ErrorSend => 2,
            LogStatus::ErrorTransfer => 3,
            LogStatus::ErrorAnalysis => 4,
            LogStatus::ErrorNetwork => 5,
            LogStatus::FailRead => 6,
            LogStatus::FailCreate => 7,
            LogStatus::FailUpdate => 8,
            LogStatus::FailDelete => 9,
            LogStatus::InvalidToken => 10,
            LogStatus::InvalidRequest => 11,
            LogStatus::UnknownError => 12,
            LogStatus::UnknownStatus => 13,
            LogStatus::LogCode(i) => i
        }
    }
}

impl FromStr for LogStatus {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DEFAULT" => Ok(Self::Default),
            "SUCCESS" => Ok(Self::Success),
            "ERROR_SEND" => Ok(Self::ErrorSend),
            "ERROR_TRANSFER" => Ok(Self::ErrorTransfer),
            "ERROR_ANALYSIS" => Ok(Self::ErrorAnalysis),
            "ERROR_NETWORK" => Ok(Self::ErrorNetwork),
            "FAIL_READ" => Ok(Self::FailRead),
            "FAIL_CREATE" => Ok(Self::FailCreate),
            "FAIL_UPDATE" => Ok(Self::FailUpdate),
            "FAIL_DELETE" => Ok(Self::FailDelete),
            "INVALID_TOKEN" => Ok(Self::InvalidToken),
            "INVALID_REQUEST" => Ok(Self::InvalidRequest),
            "UNKNOWN_ERROR" => Ok(Self::UnknownError),
            "UNKNOWN_STATUS" => Ok(Self::UnknownStatus),
            _ => s.parse::<i16>().map(|i| Self::LogCode(i))
        }
    }
}

impl ToString for LogStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Default => String::from("DEFAULT"),
            Self::Success => String::from("SUCCESS"),
            Self::ErrorSend => String::from("ERROR_SEND"),
            Self::ErrorTransfer => String::from("ERROR_TRANSFER"),
            Self::ErrorAnalysis => String::from("ERROR_ANALYSIS"),
            Self::ErrorNetwork => String::from("ERROR_NETWORK"),
            Self::FailRead => String::from("FAIL_READ"),
            Self::FailCreate => String::from("FAIL_CREATE"),
            Self::FailUpdate => String::from("FAIL_UPDATE"),
            Self::FailDelete => String::from("FAIL_DELETE"),
            Self::InvalidToken => String::from("INVALID_TOKEN"),
            Self::InvalidRequest => String::from("INVALID_REQUEST"),
            Self::UnknownError => String::from("UNKNOWN_ERROR"),
            Self::UnknownStatus => String::from("UNKNOWN_STATUS"),
            Self::LogCode(i) => i.to_string()
        }
    }
}
