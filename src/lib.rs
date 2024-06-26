pub mod schema;
pub(crate) mod operation;
pub mod utility;

use sqlx::{Pool, Error};
use sqlx::postgres::{Postgres, PgPoolOptions};
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;

pub use schema::value::{ConfigType, ConfigValue, LogType, LogValue, DataType, DataValue, ArrayDataValue};
pub use schema::model::{ModelSchema, ModelConfigSchema};
pub use schema::device::{DeviceSchema, GatewaySchema, TypeSchema, DeviceConfigSchema, GatewayConfigSchema};
use schema::device::DeviceKind;
pub use schema::group::{GroupModelSchema, GroupDeviceSchema, GroupGatewaySchema};
use schema::group::GroupKind;
pub use schema::data::{DataSchema, DataModel};
pub use schema::buffer::{BufferSchema, BufferStatus};
pub use schema::slice::SliceSchema;
pub use schema::log::{LogSchema, LogStatus};
use operation::model;
use operation::device;
use operation::types;
use operation::group;
use operation::data;
use operation::buffer;
use operation::slice;
use operation::log;

#[derive(Debug, Clone)]
pub struct Resource {
    pub pool: Pool<Postgres>,
    options: ResourceOptions
}

#[derive(Debug, Clone)]
pub struct ResourceOptions {
    limit: u32,
    with_description: bool,
    order: Vec<OrderOption>
}

#[derive(Debug, Clone)]
pub enum OrderOption {
    IdAsc,
    IdDesc,
    NameAsc,
    NameDesc
}

impl Default for ResourceOptions {
    fn default() -> Self {
        ResourceOptions { 
            limit: 10000, 
            with_description: false, 
            order: vec![] 
        }
    }
}

impl Resource {

    pub async fn new(host: &str, username: &str, password: &str, database: &str) -> Resource {
        let url = format!("mysql://{}:{}@{}/{}", username, password, host, database);
        Resource::new_with_url(&url).await
    }

    pub async fn new_with_url(url: &str) -> Resource {
        let pool = PgPoolOptions::new()
            .max_connections(100)
            .connect(url)
            .await
            .expect(&format!("Error connecting to {}", url));
        Resource {
            pool,
            options: ResourceOptions::default()
        }
    }

    pub fn new_with_pool(pool: Pool<Postgres>) -> Resource {
        Resource {
            pool,
            options: ResourceOptions::default()
        }
    }

    pub fn set_limit(mut self, limit: u32) {
        self.options.limit = limit;
    }

    pub fn set_with_description(mut self, with_description: bool) {
        self.options.with_description = with_description;
    }

    pub fn set_order(mut self, order: Vec<OrderOption>) {
        self.options.order = order;
    }

    pub async fn read_model(&self, id: Uuid)
        -> Result<ModelSchema, Error>
    {
        model::select_join_model_by_id(&self.pool, id)
        .await
    }

    pub async fn list_model_by_name(&self, name: &str)
        -> Result<Vec<ModelSchema>, Error>
    {
        model::select_join_model_by_name(&self.pool, name)
        .await
    }

    pub async fn list_model_by_category(&self, category: &str)
        -> Result<Vec<ModelSchema>, Error>
    {
        model::select_join_model_by_category(&self.pool, category)
        .await
    }

    pub async fn list_model_by_name_category(&self, name: &str, category: &str)
        -> Result<Vec<ModelSchema>, Error>
    {
        model::select_join_model_by_name_category(&self.pool, name, category)
        .await
    }

    pub async fn list_model_by_type(&self, type_id: Uuid)
        -> Result<Vec<ModelSchema>, Error>
    {
        model::select_join_model_by_type(&self.pool, type_id)
        .await
    }

    pub async fn create_model(&self, id: Uuid, data_type: &[DataType], category: &str, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        model::insert_model(&self.pool, id, data_type, category, name, description)
        .await
    }

    pub async fn update_model(&self, id: Uuid, data_type: Option<&[DataType]>, category: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        model::update_model(&self.pool, id, data_type, category, name, description)
        .await
    }

    pub async fn delete_model(&self, id: Uuid)
        -> Result<(), Error>
    {
        model::delete_model(&self.pool, id)
        .await
    }

    pub async fn read_model_config(&self, id: i32)
        -> Result<ModelConfigSchema, Error>
    {
        model::select_model_config_by_id(&self.pool, id)
        .await
    }

    pub async fn list_model_config_by_model(&self, model_id: Uuid)
        -> Result<Vec<ModelConfigSchema>, Error>
    {
        model::select_model_config_by_model(&self.pool, model_id)
        .await
    }

    pub async fn create_model_config(&self, model_id: Uuid, index: i32, name: &str, value: ConfigValue, category: &str)
        -> Result<i32, Error>
    {
        model::insert_model_config(&self.pool, model_id, index, name, value, category)
        .await
    }

    pub async fn update_model_config(&self, id: i32, name: Option<&str>, value: Option<ConfigValue>, category: Option<&str>)
        -> Result<(), Error>
    {
        model::update_model_config(&self.pool, id, name, value, category)
        .await
    }

    pub async fn delete_model_config(&self, id: i32)
        -> Result<(), Error>
    {
        model::delete_model_config(&self.pool, id)
        .await
    }

    pub async fn read_device(&self, id: Uuid)
        -> Result<DeviceSchema, Error>
    {
        device::select_device(&self.pool, DeviceKind::Device, id)
        .await
    }

    pub async fn read_device_by_sn(&self, serial_number: &str)
        -> Result<DeviceSchema, Error>
    {
        device::select_device_by_sn(&self.pool, DeviceKind::Device, serial_number)
        .await
    }

    pub async fn list_device_by_gateway(&self, gateway_id: Uuid)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device_by_gateway(&self.pool, DeviceKind::Device, gateway_id)
        .await
    }

    pub async fn list_device_by_type(&self, type_id: Uuid)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device_by_type(&self.pool, DeviceKind::Device, type_id)
        .await
    }

    pub async fn list_device_by_name(&self, name: &str)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device_by_name(&self.pool, DeviceKind::Device, name)
        .await
    }

    pub async fn list_device_by_gateway_type(&self, gateway_id: Uuid, type_id: Uuid)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device_by_gateway_type(&self.pool, DeviceKind::Device, gateway_id, type_id)
        .await
    }

    pub async fn list_device_by_gateway_name(&self, gateway_id: Uuid, name: &str)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device_by_gateway_name(&self.pool, DeviceKind::Device, gateway_id, name)
        .await
    }

    pub async fn create_device(&self, id: Uuid, gateway_id: Uuid, type_id: Uuid, serial_number: &str, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        device::insert_device(&self.pool, id, gateway_id, type_id, serial_number, name, description)
        .await
    }

    pub async fn update_device(&self, id: Uuid, gateway_id: Option<Uuid>, type_id: Option<Uuid>, serial_number: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        device::update_device(&self.pool, DeviceKind::Device, id, gateway_id, type_id, serial_number, name, description)
        .await
    }

    pub async fn delete_device(&self, id: Uuid)
        -> Result<(), Error>
    {
        device::delete_device(&self.pool, DeviceKind::Device, id)
        .await
    }

    pub async fn read_gateway(&self, id: Uuid)
        -> Result<GatewaySchema, Error>
    {
        match device::select_device(&self.pool, DeviceKind::Gateway, id).await {
            Ok(value) => Ok(value.into_gateway()),
            Err(error) => Err(error)
        }
    }

    pub async fn read_gateway_by_sn(&self, serial_number: &str)
        -> Result<GatewaySchema, Error>
    {
        match device::select_device_by_sn(&self.pool, DeviceKind::Gateway, serial_number).await {
            Ok(value) => Ok(value.into_gateway()),
            Err(error) => Err(error)
        }
    }

    pub async fn list_gateway_by_type(&self, type_id: Uuid)
        -> Result<Vec<GatewaySchema>, Error>
    {
        match device::select_device_by_type(&self.pool, DeviceKind::Gateway, type_id).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_gateway())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn list_gateway_by_name(&self, name: &str)
        -> Result<Vec<GatewaySchema>, Error>
    {
        match device::select_device_by_name(&self.pool, DeviceKind::Gateway, name).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_gateway())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn create_gateway(&self, id: Uuid, type_id: Uuid, serial_number: &str, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        device::insert_device(&self.pool, id, id, type_id, serial_number, name, description)
        .await
    }

    pub async fn update_gateway(&self, id: Uuid, type_id: Option<Uuid>, serial_number: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        device::update_device(&self.pool, DeviceKind::Gateway, id, None, type_id, serial_number, name, description)
        .await
    }

    pub async fn delete_gateway(&self, id: Uuid)
        -> Result<(), Error>
    {
        device::delete_device(&self.pool, DeviceKind::Gateway, id)
        .await
    }

    pub async fn read_device_config(&self, id: i32)
        -> Result<DeviceConfigSchema, Error>
    {
        device::select_device_config_by_id(&self.pool, DeviceKind::Device, id)
        .await
    }

    pub async fn list_device_config_by_device(&self, device_id: Uuid)
        -> Result<Vec<DeviceConfigSchema>, Error>
    {
        device::select_device_config_by_device(&self.pool, DeviceKind::Device, device_id)
        .await
    }

    pub async fn create_device_config(&self, device_id: Uuid, name: &str, value: ConfigValue, category: &str)
        -> Result<i32, Error>
    {
        device::insert_device_config(&self.pool, device_id, name, value, category)
        .await
    }

    pub async fn update_device_config(&self, id: i32, name: Option<&str>, value: Option<ConfigValue>, category: Option<&str>)
        -> Result<(), Error>
    {
        device::update_device_config(&self.pool, id, name, value, category)
        .await
    }

    pub async fn delete_device_config(&self, id: i32)
        -> Result<(), Error>
    {
        device::delete_device_config(&self.pool, id)
        .await
    }

    pub async fn read_gateway_config(&self, id: i32)
        -> Result<GatewayConfigSchema, Error>
    {
        match device::select_device_config_by_id(&self.pool, DeviceKind::Gateway, id).await {
            Ok(value) => Ok(value.into_gateway_config()),
            Err(error) => Err(error)
        }
    }

    pub async fn list_gateway_config_by_gateway(&self, gateway_id: Uuid)
        -> Result<Vec<GatewayConfigSchema>, Error>
    {
        match device::select_device_config_by_device(&self.pool, DeviceKind::Gateway, gateway_id).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_gateway_config())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn create_gateway_config(&self, gateway_id: Uuid, name: &str, value: ConfigValue, category: &str)
        -> Result<i32, Error>
    {
        device::insert_device_config(&self.pool, gateway_id, name, value, category)
        .await
    }

    pub async fn update_gateway_config(&self, id: i32, name: Option<&str>, value: Option<ConfigValue>, category: Option<&str>)
        -> Result<(), Error>
    {
        device::update_device_config(&self.pool, id, name, value, category)
        .await
    }

    pub async fn delete_gateway_config(&self, id: i32)
        -> Result<(), Error>
    {
        device::delete_device_config(&self.pool, id)
        .await
    }

    pub async fn read_type(&self, id: Uuid)
        -> Result<TypeSchema, Error>
    {
        types::select_device_type_by_id(&self.pool, id)
        .await
    }

    pub async fn list_type_by_name(&self, name: &str)
        -> Result<Vec<TypeSchema>, Error>
    {
        types::select_device_type_by_name(&self.pool, name)
        .await
    }

    pub async fn create_type(&self, id: Uuid, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        types::insert_device_type(&self.pool, id, name, description)
        .await
    }

    pub async fn update_type(&self, id: Uuid, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        types::update_device_type(&self.pool, id, name, description)
        .await
    }

    pub async fn delete_type(&self, id: Uuid)
        -> Result<(), Error>
    {
        types::delete_device_type(&self.pool, id)
        .await
    }

    pub async fn add_type_model(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        types::insert_device_type_model(&self.pool, id, model_id)
        .await
    }

    pub async fn remove_type_model(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        types::delete_device_type_model(&self.pool, id, model_id)
        .await
    }

    pub async fn read_group_model(&self, id: Uuid)
        -> Result<GroupModelSchema, Error>
    {
        match group::select_group_by_id(&self.pool, GroupKind::Model, id).await {
            Ok(value) => Ok(value.into_group_model()),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_model_by_name(&self, name: &str)
        -> Result<Vec<GroupModelSchema>, Error>
    {
        match group::select_group_by_name(&self.pool, GroupKind::Model, name).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_group_model())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_model_by_category(&self, category: &str)
        -> Result<Vec<GroupModelSchema>, Error>
    {
        match group::select_group_by_category(&self.pool, GroupKind::Model, category).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_group_model())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_model_by_name_category(&self, name: &str, category: &str)
        -> Result<Vec<GroupModelSchema>, Error>
    {
        match group::select_group_by_name_category(&self.pool, GroupKind::Model, name, category).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_group_model())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn create_group_model(&self, id: Uuid, name: &str, category: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        group::insert_group(&self.pool, GroupKind::Model, id, name, category, description)
        .await
    }

    pub async fn update_group_model(&self, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        group::update_group(&self.pool, GroupKind::Model, id, name, category, description)
        .await
    }

    pub async fn delete_group_model(&self, id: Uuid)
        -> Result<(), Error>
    {
        group::delete_group(&self.pool, GroupKind::Model, id)
        .await
    }

    pub async fn add_group_model_member(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        group::insert_group_map(&self.pool, GroupKind::Model, id, model_id)
        .await
    }

    pub async fn remove_group_model_member(&self, id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        group::delete_group_map(&self.pool, GroupKind::Model, id, model_id)
        .await
    }

    pub async fn read_group_device(&self, id: Uuid)
        -> Result<GroupDeviceSchema, Error>
    {
        match group::select_group_by_id(&self.pool, GroupKind::Device, id).await {
            Ok(value) => Ok(value.into_group_device()),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_device_by_name(&self, name: &str)
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        match group::select_group_by_name(&self.pool, GroupKind::Device, name).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_group_device())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_device_by_category(&self, category: &str)
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        match group::select_group_by_category(&self.pool, GroupKind::Device, category).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_group_device())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_device_by_name_category(&self, name: &str, category: &str)
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        match group::select_group_by_name_category(&self.pool, GroupKind::Device, name, category).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_group_device())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn create_group_device(&self, id: Uuid, name: &str, category: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        group::insert_group(&self.pool, GroupKind::Device, id, name, category, description)
        .await
    }

    pub async fn update_group_device(&self, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        group::update_group(&self.pool, GroupKind::Device, id, name, category, description)
        .await
    }

    pub async fn delete_group_device(&self, id: Uuid)
        -> Result<(), Error>
    {
        group::delete_group(&self.pool, GroupKind::Device, id)
        .await
    }

    pub async fn add_group_device_member(&self, id: Uuid, device_id: Uuid)
        -> Result<(), Error>
    {
        group::insert_group_map(&self.pool, GroupKind::Device, id, device_id)
        .await
    }

    pub async fn remove_group_device_member(&self, id: Uuid, device_id: Uuid)
        -> Result<(), Error>
    {
        group::delete_group_map(&self.pool, GroupKind::Device, id, device_id)
        .await
    }

    pub async fn read_group_gateway(&self, id: Uuid)
        -> Result<GroupGatewaySchema, Error>
    {
        match group::select_group_by_id(&self.pool, GroupKind::Gateway, id).await {
            Ok(value) => Ok(value.into_group_gateway()),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_gateway_by_name(&self, name: &str)
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        match group::select_group_by_name(&self.pool, GroupKind::Gateway, name).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_group_gateway())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_gateway_by_category(&self, category: &str)
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        match group::select_group_by_category(&self.pool, GroupKind::Gateway, category).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_group_gateway())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_gateway_by_name_category(&self, name: &str, category: &str)
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        match group::select_group_by_name_category(&self.pool, GroupKind::Gateway, name, category).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_group_gateway())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn create_group_gateway(&self, id: Uuid, name: &str, category: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        group::insert_group(&self.pool, GroupKind::Gateway, id, name, category, description)
        .await
    }

    pub async fn update_group_gateway(&self, id: Uuid, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        group::update_group(&self.pool, GroupKind::Gateway, id, name, category, description)
        .await
    }

    pub async fn delete_group_gateway(&self, id: Uuid)
        -> Result<(), Error>
    {
        group::delete_group(&self.pool, GroupKind::Gateway, id)
        .await
    }

    pub async fn add_group_gateway_member(&self, id: Uuid, gateway_id: Uuid)
        -> Result<(), Error>
    {
        group::insert_group_map(&self.pool, GroupKind::Gateway, id, gateway_id)
        .await
    }

    pub async fn remove_group_gateway_member(&self, id: Uuid, gateway_id: Uuid)
        -> Result<(), Error>
    {
        group::delete_group_map(&self.pool, GroupKind::Gateway, id, gateway_id)
        .await
    }

    pub async fn read_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>)
        -> Result<DataSchema, Error>
    {
        data::select_data_by_time(&self.pool, model_id, device_id, timestamp).await?.into_iter().next()
            .ok_or(Error::RowNotFound)
    }

    pub async fn list_data_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>)
        -> Result<Vec<DataSchema>, Error>
    {
        data::select_data_by_time(&self.pool, model_id, device_id, timestamp)
        .await
    }

    pub async fn list_data_by_last_time(&self, device_id: Uuid, model_id: Uuid, last: DateTime<Utc>)
        -> Result<Vec<DataSchema>, Error>
    {
        data::select_data_by_last_time(&self.pool, model_id, device_id, last)
        .await
    }

    pub async fn list_data_by_range_time(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<DataSchema>, Error>
    {
        data::select_data_by_range_time(&self.pool, model_id, device_id, begin, end)
        .await
    }

    pub async fn list_data_by_number_before(&self, device_id: Uuid, model_id: Uuid, before: DateTime<Utc>, number: u32)
        -> Result<Vec<DataSchema>, Error>
    {
        data::select_data_by_number_before(&self.pool, model_id, device_id, before, number)
        .await
    }

    pub async fn list_data_by_number_after(&self, device_id: Uuid, model_id: Uuid, after: DateTime<Utc>, number: u32)
        -> Result<Vec<DataSchema>, Error>
    {
        data::select_data_by_number_after(&self.pool, model_id, device_id, after, number)
        .await
    }

    pub async fn create_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: Vec<DataValue>)
        -> Result<(), Error>
    {
        data::insert_data(&self.pool, model_id, device_id, timestamp, data)
        .await
    }

    pub async fn delete_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>)
        -> Result<(), Error>
    {
        data::delete_data(&self.pool, model_id, device_id, timestamp)
        .await
    }

    pub async fn read_buffer(&self, id: i32)
        -> Result<BufferSchema, Error>
    {
        buffer::select_buffer_by_id(&self.pool, id).await
    }

    pub async fn read_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, status: Option<BufferStatus>)
        -> Result<BufferSchema, Error>
    {
        buffer::select_buffer_by_time(&self.pool, device_id, model_id, timestamp, status).await
    }

    pub async fn read_buffer_first(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, status: Option<BufferStatus>)
        -> Result<BufferSchema, Error>
    {
        buffer::select_buffer_first(&self.pool, 1, device_id, model_id, status)
            .await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_buffer_last(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, status: Option<BufferStatus>)
        -> Result<BufferSchema, Error>
    {
        buffer::select_buffer_last(&self.pool, 1, device_id, model_id, status)
            .await?.into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_first(&self, number: u32, device_id: Option<Uuid>, model_id: Option<Uuid>, status: Option<BufferStatus>)
        -> Result<Vec<BufferSchema>, Error>
    {
        buffer::select_buffer_first(&self.pool, number, device_id, model_id, status)
        .await
    }

    pub async fn list_buffer_last(&self, number: u32, device_id: Option<Uuid>, model_id: Option<Uuid>, status: Option<BufferStatus>)
        -> Result<Vec<BufferSchema>, Error>
    {
        buffer::select_buffer_last(&self.pool, number, device_id, model_id, status)
        .await
    }

    pub async fn create_buffer(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: Vec<DataValue>, status: BufferStatus)
        -> Result<i32, Error>
    {
        buffer::insert_buffer(&self.pool, device_id, model_id, timestamp, data, status)
        .await
    }

    pub async fn update_buffer(&self, id: i32, data: Option<Vec<DataValue>>, status: Option<BufferStatus>)
        -> Result<(), Error>
    {
        buffer::update_buffer(&self.pool, id, data, status)
        .await
    }

    pub async fn delete_buffer(&self, id: i32)
        -> Result<(), Error>
    {
        buffer::delete_buffer(&self.pool, id).await
    }

    pub async fn read_slice(&self, id: i32)
        -> Result<SliceSchema, Error>
    {
        slice::select_slice_by_id(&self.pool, id).await
    }

    pub async fn list_slice_by_name(&self, name: &str)
        -> Result<Vec<SliceSchema>, Error>
    {
        slice::select_slice_by_name(&self.pool, name).await
    }

    pub async fn list_slice_by_device(&self, device_id: Uuid)
        -> Result<Vec<SliceSchema>, Error>
    {
        slice::select_slice_by_device(&self.pool, device_id).await
    }

    pub async fn list_slice_by_model(&self, model_id: Uuid)
        -> Result<Vec<SliceSchema>, Error>
    {
        slice::select_slice_by_model(&self.pool, model_id).await
    }

    pub async fn list_slice_by_device_model(&self, device_id: Uuid, model_id: Uuid)
        -> Result<Vec<SliceSchema>, Error>
    {
        slice::select_slice_by_device_model(&self.pool, device_id, model_id).await
    }

    pub async fn create_slice(&self, device_id: Uuid, model_id: Uuid, timestamp_begin: DateTime<Utc>, timestamp_end: DateTime<Utc>, name: &str, description: Option<&str>)
        -> Result<i32, Error>
    {
        slice::insert_slice(&self.pool, device_id, model_id, timestamp_begin, timestamp_end, name, description)
        .await
    }

    pub async fn update_slice(&self, id: i32, timestamp_begin: Option<DateTime<Utc>>, timestamp_end: Option<DateTime<Utc>>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        slice::update_slice(&self.pool, id, timestamp_begin, timestamp_end, name, description)
        .await
    }

    pub async fn delete_slice(&self, id: i32)
        -> Result<(), Error>
    {
        slice::delete_slice(&self.pool, id).await
    }

    pub async fn read_log(&self, timestamp: DateTime<Utc>, device_id: Uuid)
        -> Result<LogSchema, Error>
    {
        log::select_log_by_id(&self.pool, timestamp, device_id).await
    }

    pub async fn list_log_by_time(&self, timestamp: DateTime<Utc>, device_id: Option<Uuid>, status: Option<LogStatus>)
        -> Result<Vec<LogSchema>, Error>
    {
        log::select_log_by_time(&self.pool, timestamp, device_id, status).await
    }

    pub async fn list_log_by_last_time(&self, last: DateTime<Utc>, device_id: Option<Uuid>, status: Option<LogStatus>)
        -> Result<Vec<LogSchema>, Error>
    {
        log::select_log_by_last_time(&self.pool, last, device_id, status).await
    }

    pub async fn list_log_by_range_time(&self, begin: DateTime<Utc>, end: DateTime<Utc>, device_id: Option<Uuid>, status: Option<LogStatus>)
        -> Result<Vec<LogSchema>, Error>
    {
        log::select_log_by_range_time(&self.pool, begin, end, device_id, status).await
    }

    pub async fn create_log(&self, timestamp: DateTime<Utc>, device_id: Uuid, status: LogStatus, value: ConfigValue)
        -> Result<(), Error>
    {
        log::insert_log(&self.pool, timestamp, device_id, status, value)
        .await
    }

    pub async fn update_log(&self, timestamp: DateTime<Utc>, device_id: Uuid, status: Option<LogStatus>, value: Option<ConfigValue>)
        -> Result<(), Error>
    {
        log::update_log(&self.pool, timestamp, device_id, status, value)
        .await
    }

    pub async fn delete_log(&self, timestamp: DateTime<Utc>, device_id: Uuid)
        -> Result<(), Error>
    {
        log::delete_log(&self.pool, timestamp, device_id).await
    }

}
