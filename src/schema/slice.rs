use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use rmcs_resource_api::slice;

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

impl From<slice::SliceSchema> for SliceSchema {
    fn from(value: slice::SliceSchema) -> Self {
        Self {
            id: value.id,
            device_id: value.device_id,
            model_id: value.model_id,
            timestamp_begin: Utc.timestamp_nanos(value.timestamp_begin),
            timestamp_end: Utc.timestamp_nanos(value.timestamp_end),
            index_begin: value.index_begin as u16,
            index_end: value.index_end as u16,
            name: value.name,
            description: value.description
        }
    }
}

impl Into<slice::SliceSchema> for SliceSchema {
    fn into(self) -> slice::SliceSchema {
        slice::SliceSchema {
            id: self.id,
            device_id: self.device_id,
            model_id: self.model_id,
            timestamp_begin: self.timestamp_begin.timestamp_nanos(),
            timestamp_end: self.timestamp_end.timestamp_nanos(),
            index_begin: self.index_begin as u32,
            index_end: self.index_end as u32,
            name: self.name,
            description: self.description
        }
    }
}
