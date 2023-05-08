use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc};

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum SliceData {
    Table,
    Id,
    DeviceId,
    ModelId,
    TimestampBegin,
    TimestampEnd,
    IndexBegin,
    IndexEnd,
    Name,
    Description
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SliceSchema {
    pub id: u32,
    pub device_id: u64,
    pub model_id: u32,
    pub timestamp_begin: DateTime<Utc>,
    pub timestamp_end: DateTime<Utc>,
    pub index_begin: u16,
    pub index_end: u16,
    pub name: String,
    pub description: String
}
