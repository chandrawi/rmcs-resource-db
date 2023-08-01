use std::str::FromStr;

use sea_query::Iden;
use sqlx::types::chrono::NaiveDateTime;
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
    pub timestamp: NaiveDateTime,
    pub device_id: i64,
    pub status: String,
    pub value: LogValue
}

impl From<log::LogSchema> for LogSchema {
    fn from(value: log::LogSchema) -> Self {
        Self {
            timestamp: NaiveDateTime::from_timestamp_micros(value.timestamp).unwrap_or_default(),
            device_id: value.device_id,
            status: log::LogStatus::from_i32(value.status).unwrap_or_default().as_str_name().to_owned(),
            value: LogValue::from_bytes(
                &value.log_bytes,
                LogType::from(common::ConfigType::from_i32(value.log_type).unwrap_or_default())
            )
        }
    }
}

impl Into<log::LogSchema> for LogSchema {
    fn into(self) -> log::LogSchema {
        log::LogSchema {
            timestamp: self.timestamp.timestamp_micros(),
            device_id: self.device_id,
            status: log::LogStatus::from_str_name(&self.status).unwrap_or_default().into(),
            log_bytes: self.value.to_bytes(),
            log_type: Into::<common::ConfigType>::into(self.value.get_type()).into()
        }
    }
}

pub(crate) enum LogStatus {
    Default,
    Success,
    ErrorRaw,
    ErrorMissing,
    ErrorConversion,
    ErrorAnalyze,
    ErrorNetwork,
    FailRead,
    FailCreate,
    FailUpdate,
    FailDelete,
    InvalidToken,
    InvalidRequest,
    NotFound,
    MethodNotAllowed,
    UnknownError,
    UnknownStatus,
}

impl From<i16> for LogStatus {
    fn from(value: i16) -> Self {
        match value {
            1 => Self::Success,
            2 => Self::ErrorRaw,
            3 => Self::ErrorMissing,
            4 => Self::ErrorConversion,
            5 => Self::ErrorAnalyze,
            6 => Self::ErrorNetwork,
            7 => Self::FailRead,
            8 => Self::FailCreate,
            9 => Self::FailUpdate,
            10 => Self::FailDelete,
            11 => Self::InvalidToken,
            12 => Self::InvalidRequest,
            13 => Self::NotFound,
            14 => Self::MethodNotAllowed,
            15 => Self::UnknownError,
            16 => Self::UnknownStatus,
            _ => Self::Default
        }
    }
}

impl From<LogStatus> for i16 {
    fn from(value: LogStatus) -> Self {
        match value {
            LogStatus::Default => 0,
            LogStatus::Success => 1,
            LogStatus::ErrorRaw => 2,
            LogStatus::ErrorMissing => 3,
            LogStatus::ErrorConversion => 4,
            LogStatus::ErrorAnalyze => 5,
            LogStatus::ErrorNetwork => 6,
            LogStatus::FailRead => 7,
            LogStatus::FailCreate => 8,
            LogStatus::FailUpdate => 9,
            LogStatus::FailDelete => 10,
            LogStatus::InvalidToken => 11,
            LogStatus::InvalidRequest => 12,
            LogStatus::NotFound => 13,
            LogStatus::MethodNotAllowed => 14,
            LogStatus::UnknownError => 15,
            LogStatus::UnknownStatus => 16
        }
    }
}

impl FromStr for LogStatus {
    type Err = std::string::ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "SUCCESS" => Ok(Self::Success),
            "ERROR_RAW" => Ok(Self::ErrorRaw),
            "ERROR_MISSING" => Ok(Self::ErrorMissing),
            "ERROR_CONVERSION" => Ok(Self::ErrorConversion),
            "ERROR_ANALYZE" => Ok(Self::ErrorAnalyze),
            "ERROR_NETWORK" => Ok(Self::ErrorNetwork),
            "FAIL_READ" => Ok(Self::FailRead),
            "FAIL_CREATE" => Ok(Self::FailCreate),
            "FAIL_UPDATE" => Ok(Self::FailUpdate),
            "FAIL_DELETE" => Ok(Self::FailDelete),
            "INVALID_TOKEN" => Ok(Self::InvalidToken),
            "INVALID_REQUEST" => Ok(Self::InvalidRequest),
            "NOT_FOUND" => Ok(Self::NotFound),
            "METHOD_NOT_ALLOWED" => Ok(Self::MethodNotAllowed),
            "UNKNOWN_ERROR" => Ok(Self::UnknownError),
            "UNKNOWN_STATUS" => Ok(Self::UnknownStatus),
            _ => Ok(Self::Default)
        }
    }
}

impl ToString for LogStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Default => String::from("DEFAULT"),
            Self::Success => String::from("SUCCESS"),
            Self::ErrorRaw => String::from("ERROR_RAW"),
            Self::ErrorMissing => String::from("ERROR_MISSING"),
            Self::ErrorConversion => String::from("ERROR_CONVERSION"),
            Self::ErrorAnalyze => String::from("ERROR_ANALYZE"),
            Self::ErrorNetwork => String::from("ERROR_NETWORK"),
            Self::FailRead => String::from("FAIL_READ"),
            Self::FailCreate => String::from("FAIL_CREATE"),
            Self::FailUpdate => String::from("FAIL_UPDATE"),
            Self::FailDelete => String::from("FAIL_DELETE"),
            Self::InvalidToken => String::from("INVALID_TOKEN"),
            Self::InvalidRequest => String::from("INVALID_REQUEST"),
            Self::NotFound => String::from("NOT_FOUND"),
            Self::MethodNotAllowed => String::from("METHOD_NOT_ALLOWED"),
            Self::UnknownError => String::from("UNKNOWN_ERROR"),
            Self::UnknownStatus => String::from("UNKNOWN_STATUS")
        }
    }
}
