use sea_query::Iden;
use sqlx::types::chrono::NaiveDateTime;
use uuid::Uuid;
use rmcs_resource_api::slice;

#[allow(unused)]
#[derive(Iden)]
pub(crate) enum DataSlice {
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
    pub id: i32,
    pub device_id: Uuid,
    pub model_id: Uuid,
    pub timestamp_begin: NaiveDateTime,
    pub timestamp_end: NaiveDateTime,
    pub index_begin: i32,
    pub index_end: i32,
    pub name: String,
    pub description: String
}

impl From<slice::SliceSchema> for SliceSchema {
    fn from(value: slice::SliceSchema) -> Self {
        Self {
            id: value.id,
            device_id: Uuid::from_slice(&value.device_id).unwrap_or_default(),
            model_id: Uuid::from_slice(&value.model_id).unwrap_or_default(),
            timestamp_begin: NaiveDateTime::from_timestamp_micros(value.timestamp_begin).unwrap_or_default(),
            timestamp_end: NaiveDateTime::from_timestamp_micros(value.timestamp_end).unwrap_or_default(),
            index_begin: value.index_begin as i32,
            index_end: value.index_end as i32,
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
            index_begin: self.index_begin as i32,
            index_end: self.index_end as i32,
            name: self.name,
            description: self.description
        }
    }
}
