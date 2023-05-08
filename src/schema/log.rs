use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc};
use crate::schema::value::LogValue;

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
    pub device_id: u64,
    pub timestamp: DateTime<Utc>,
    pub status: String,
    pub value: LogValue
}
