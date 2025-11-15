use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use crate::schema::value::{DataValue, DataType};
use rmcs_resource_api::log;

#[derive(Iden)]
pub(crate) enum SystemLog {
    Table,
    Id,
    Timestamp,
    DeviceId,
    ModelId,
    Tag,
    Value,
    Type
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct LogSchema {
    pub id: i32,
    pub timestamp: DateTime<Utc>,
    pub device_id: Option<Uuid>,
    pub model_id: Option<Uuid>,
    pub value: DataValue,
    pub tag: i16
}

impl From<log::LogSchema> for LogSchema {
    fn from(value: log::LogSchema) -> Self {
        Self {
            id: value.id,
            timestamp: Utc.timestamp_nanos(value.timestamp * 1000),
            device_id: value.device_id.map(|id| Uuid::from_slice(&id).unwrap_or_default()),
            model_id: value.model_id.map(|id| Uuid::from_slice(&id).unwrap_or_default()),
            value: DataValue::from_bytes(
                &value.log_bytes,
                DataType::from(value.log_type)
            ),
            tag: value.tag as i16
        }
    }
}

impl Into<log::LogSchema> for LogSchema {
    fn into(self) -> log::LogSchema {
        log::LogSchema {
            id: self.id,
            timestamp: self.timestamp.timestamp_micros(),
            device_id: self.device_id.map(|id| id.as_bytes().to_vec()),
            model_id: self.model_id.map(|id| id.as_bytes().to_vec()),
            log_bytes: self.value.to_bytes(),
            log_type: self.value.get_type().into(),
            tag: self.tag.into()
        }
    }
}
