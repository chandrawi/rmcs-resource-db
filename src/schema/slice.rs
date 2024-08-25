use sea_query::Iden;
use sqlx::types::chrono::{DateTime, Utc, TimeZone};
use uuid::Uuid;
use rmcs_resource_api::slice;

#[derive(Iden)]
pub(crate) enum SliceData {
    Table,
    Id,
    DeviceId,
    ModelId,
    TimestampBegin,
    TimestampEnd,
    Name,
    Description
}

#[derive(Iden)]
pub(crate) enum SliceDataSet {
    Table,
    Id,
    SetId,
    TimestampBegin,
    TimestampEnd,
    Name,
    Description
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SliceSchema {
    pub id: i32,
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub timestamp_begin: DateTime<Utc>,
    pub timestamp_end: DateTime<Utc>,
    pub name: String,
    pub description: String
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct SliceSetSchema {
    pub id: i32,
    pub set_id: Uuid,
    pub timestamp_begin: DateTime<Utc>,
    pub timestamp_end: DateTime<Utc>,
    pub name: String,
    pub description: String
}

impl From<slice::SliceSchema> for SliceSchema {
    fn from(value: slice::SliceSchema) -> Self {
        Self {
            id: value.id,
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            timestamp_begin: Utc.timestamp_nanos(value.timestamp_begin * 1000),
            timestamp_end: Utc.timestamp_nanos(value.timestamp_end * 1000),
            name: value.name,
            description: value.description
        }
    }
}

impl Into<slice::SliceSchema> for SliceSchema {
    fn into(self) -> slice::SliceSchema {
        slice::SliceSchema {
            id: self.id,
            device_id: self.device_id.as_bytes().to_vec(),
            model_id: self.model_id.as_bytes().to_vec(),
            timestamp_begin: self.timestamp_begin.timestamp_micros(),
            timestamp_end: self.timestamp_end.timestamp_micros(),
            name: self.name,
            description: self.description
        }
    }
}

impl From<slice::SliceSetSchema> for SliceSetSchema {
    fn from(value: slice::SliceSetSchema) -> Self {
        Self {
            id: value.id,
            set_id: Uuid::from_slice(&value.set_id).unwrap_or_default(),
            timestamp_begin: Utc.timestamp_nanos(value.timestamp_begin * 1000),
            timestamp_end: Utc.timestamp_nanos(value.timestamp_end * 1000),
            name: value.name,
            description: value.description
        }
    }
}

impl Into<slice::SliceSetSchema> for SliceSetSchema {
    fn into(self) -> slice::SliceSetSchema {
        slice::SliceSetSchema {
            id: self.id,
            set_id: self.set_id.as_bytes().to_vec(),
            timestamp_begin: self.timestamp_begin.timestamp_micros(),
            timestamp_end: self.timestamp_end.timestamp_micros(),
            name: self.name,
            description: self.description
        }
    }
}
