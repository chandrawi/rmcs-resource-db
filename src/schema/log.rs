use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
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
    pub device_id: u64,
    pub status: String,
    pub value: LogValue
}

impl From<log::LogSchema> for LogSchema {
    fn from(value: log::LogSchema) -> Self {
        Self {
            timestamp: Utc.timestamp_nanos(value.timestamp),
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
            timestamp: self.timestamp.timestamp_nanos(),
            device_id: self.device_id,
            status: log::LogStatus::from_str_name(&self.status).unwrap_or_default().into(),
            log_bytes: self.value.to_bytes(),
            log_type: Into::<common::ConfigType>::into(self.value.get_type()).into()
        }
    }
}
