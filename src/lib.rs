pub mod schema;
pub(crate) mod operation;
pub mod utility;

use sqlx::{Pool, Error};
use sqlx::postgres::{Postgres, PgPoolOptions};
use sqlx::types::chrono::{DateTime, Utc};
use uuid::Uuid;
use std::slice::from_ref;

use operation::model;
use operation::device;
use operation::types;
use operation::group;
use operation::set;
use operation::data;
use operation::buffer;
use operation::slice;
use operation::log;
pub use schema::value::{DataType, DataValue, ArrayDataValue};
pub use schema::model::{ModelSchema, TagSchema, ModelConfigSchema};
pub use schema::device::{DeviceSchema, GatewaySchema, TypeSchema, DeviceConfigSchema, GatewayConfigSchema};
use schema::device::DeviceKind;
pub use schema::group::{GroupModelSchema, GroupDeviceSchema, GroupGatewaySchema};
use schema::group::GroupKind;
pub use schema::set::{SetSchema, SetTemplateSchema, SetMember, SetTemplateMember};
pub use schema::data::{DataSchema, DataSetSchema};
use data::DataSelector;
pub use schema::buffer::{BufferSchema, BufferSetSchema};
use buffer::BufferSelector;
pub use schema::slice::{SliceSchema, SliceSetSchema};
use slice::SliceSelector;
pub use schema::log::LogSchema;
use log::LogSelector;
pub use utility::tag;

#[derive(Debug, Clone)]
pub struct Resource {
    pub pool: Pool<Postgres>,
    options: ResourceOptions
}

#[derive(Debug, Clone)]
pub struct ResourceOptions {
    limit: usize,
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

    pub fn set_limit(mut self, limit: usize) {
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
        match model::select_model(&self.pool, Some(id), None, None, None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_model_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<ModelSchema>, Error>
    {
        model::select_model(&self.pool, None, Some(ids), None, None, None)
        .await
    }

    pub async fn list_model_by_type(&self, type_id: Uuid)
        -> Result<Vec<ModelSchema>, Error>
    {
        model::select_model(&self.pool, None, None, Some(type_id), None, None)
        .await
    }

    pub async fn list_model_by_name(&self, name: &str)
        -> Result<Vec<ModelSchema>, Error>
    {
        model::select_model(&self.pool, None, None, None, Some(name), None)
        .await
    }

    pub async fn list_model_by_category(&self, category: &str)
        -> Result<Vec<ModelSchema>, Error>
    {
        model::select_model(&self.pool, None, None, None, None, Some(category))
        .await
    }

    pub async fn list_model_option(&self, type_id: Option<Uuid>, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<ModelSchema>, Error>
    {
        model::select_model(&self.pool, None, None, type_id, name, category)
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
        match model::select_model_config(&self.pool, Some(id), None).await?
        .into_iter().next() {
            Some(value) => Ok(value),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_model_config_by_model(&self, model_id: Uuid)
        -> Result<Vec<ModelConfigSchema>, Error>
    {
        model::select_model_config(&self.pool, None, Some(model_id))
        .await
    }

    pub async fn create_model_config(&self, model_id: Uuid, index: i32, name: &str, value: DataValue, category: &str)
        -> Result<i32, Error>
    {
        model::insert_model_config(&self.pool, model_id, index, name, value, category)
        .await
    }

    pub async fn update_model_config(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
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

    pub async fn read_tag(&self, model_id: Uuid, tag: i16)
        -> Result<TagSchema, Error>
    {
        match model::select_model_tag(&self.pool, model_id, Some(tag)).await?
        .into_iter().next() {
            Some(value) => Ok(value),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_tag_by_model(&self, model_id: Uuid)
        -> Result<Vec<TagSchema>, Error>
    {
        model::select_model_tag(&self.pool, model_id, None)
        .await
    }

    pub async fn create_tag(&self, model_id: Uuid, tag: i16, name: &str, members: &[i16])
        -> Result<(), Error>
    {
        model::insert_model_tag(&self.pool, model_id, tag, name, members)
        .await
    }

    pub async fn update_tag(&self, model_id: Uuid, tag: i16, name: Option<&str>, members: Option<&[i16]>)
        -> Result<(), Error>
    {
        model::update_model_tag(&self.pool, model_id, tag, name, members)
        .await
    }

    pub async fn delete_tag(&self, model_id: Uuid, tag: i16)
        -> Result<(), Error>
    {
        model::delete_model_tag(&self.pool, model_id, tag)
        .await
    }

    pub async fn read_device(&self, id: Uuid)
        -> Result<DeviceSchema, Error>
    {
        match device::select_device(&self.pool, DeviceKind::Device, Some(id), None, None, None, None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn read_device_by_sn(&self, serial_number: &str)
        -> Result<DeviceSchema, Error>
    {
        match device::select_device(&self.pool, DeviceKind::Device, None, Some(serial_number), None, None, None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_device_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device(&self.pool, DeviceKind::Device, None, None, Some(ids), None, None, None)
        .await
    }

    pub async fn list_device_by_gateway(&self, gateway_id: Uuid)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device(&self.pool, DeviceKind::Device, None, None, None, Some(gateway_id), None, None)
        .await
    }

    pub async fn list_device_by_type(&self, type_id: Uuid)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device(&self.pool, DeviceKind::Device, None, None, None, None, Some(type_id), None)
        .await
    }

    pub async fn list_device_by_name(&self, name: &str)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device(&self.pool, DeviceKind::Device, None, None, None, None, None, Some(name))
        .await
    }

    pub async fn list_device_option(&self, gateway_id: Option<Uuid>, type_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device(&self.pool, DeviceKind::Device, None, None, None, gateway_id, type_id, name)
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
        match device::select_device(&self.pool, DeviceKind::Gateway, Some(id), None, None, None, None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value.into_gateway()),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn read_gateway_by_sn(&self, serial_number: &str)
        -> Result<GatewaySchema, Error>
    {
        match device::select_device(&self.pool, DeviceKind::Gateway, None, Some(serial_number), None, None, None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value.into_gateway()),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_gateway_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GatewaySchema>, Error>
    {
        match device::select_device(&self.pool, DeviceKind::Gateway, None, None, Some(ids), None, None, None).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_gateway())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_gateway_by_type(&self, type_id: Uuid)
        -> Result<Vec<GatewaySchema>, Error>
    {
        match device::select_device(&self.pool, DeviceKind::Gateway, None, None, None, None, Some(type_id), None).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_gateway())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_gateway_by_name(&self, name: &str)
        -> Result<Vec<GatewaySchema>, Error>
    {
        match device::select_device(&self.pool, DeviceKind::Gateway, None, None, None, None, None, Some(name)).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_gateway())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_gateway_option(&self, type_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<GatewaySchema>, Error>
    {
        match device::select_device(&self.pool, DeviceKind::Gateway, None, None, None, None, type_id, name).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_gateway())).collect(),
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
        match device::select_device_config(&self.pool, DeviceKind::Device, Some(id), None).await?
        .into_iter().next() {
            Some(value) => Ok(value),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_device_config_by_device(&self, device_id: Uuid)
        -> Result<Vec<DeviceConfigSchema>, Error>
    {
        device::select_device_config(&self.pool, DeviceKind::Device, None, Some(device_id))
        .await
    }

    pub async fn create_device_config(&self, device_id: Uuid, name: &str, value: DataValue, category: &str)
        -> Result<i32, Error>
    {
        device::insert_device_config(&self.pool, device_id, name, value, category)
        .await
    }

    pub async fn update_device_config(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
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
        match device::select_device_config(&self.pool, DeviceKind::Gateway, Some(id), None).await?
        .into_iter().next() {
            Some(value) => Ok(value.into_gateway_config()),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_gateway_config_by_gateway(&self, gateway_id: Uuid)
        -> Result<Vec<GatewayConfigSchema>, Error>
    {
        match device::select_device_config(&self.pool, DeviceKind::Gateway, None, Some(gateway_id)).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_gateway_config())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn create_gateway_config(&self, gateway_id: Uuid, name: &str, value: DataValue, category: &str)
        -> Result<i32, Error>
    {
        device::insert_device_config(&self.pool, gateway_id, name, value, category)
        .await
    }

    pub async fn update_gateway_config(&self, id: i32, name: Option<&str>, value: Option<DataValue>, category: Option<&str>)
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
        match types::select_device_type(&self.pool, Some(id), None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_type_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<TypeSchema>, Error>
    {
        types::select_device_type(&self.pool, None, Some(ids), None)
        .await
    }

    pub async fn list_type_by_name(&self, name: &str)
        -> Result<Vec<TypeSchema>, Error>
    {
        types::select_device_type(&self.pool, None, None, Some(name))
        .await
    }

    pub async fn list_type_option(&self, name: Option<&str>)
        -> Result<Vec<TypeSchema>, Error>
    {
        types::select_device_type(&self.pool, None, None, name)
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
        match group::select_group(&self.pool, GroupKind::Model, Some(id), None, None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value.into_group_model()),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_group_model_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GroupModelSchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Model, None, Some(ids), None, None).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_model())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_model_by_name(&self, name: &str)
        -> Result<Vec<GroupModelSchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Model, None, None, Some(name), None).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_model())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_model_by_category(&self, category: &str)
        -> Result<Vec<GroupModelSchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Model, None, None, None, Some(category)).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_model())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_model_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<GroupModelSchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Model, None, None, name, category).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_model())).collect(),
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
        match group::select_group(&self.pool, GroupKind::Device, Some(id), None, None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value.into_group_device()),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_group_device_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Device, None, Some(ids), None, None).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_device())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_device_by_name(&self, name: &str)
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Device, None, None, Some(name), None).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_device())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_device_by_category(&self, category: &str)
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Device, None, None, None, Some(category)).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_device())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_device_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<GroupDeviceSchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Device, None, None, name, category).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_device())).collect(),
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
        match group::select_group(&self.pool, GroupKind::Gateway, Some(id), None, None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value.into_group_gateway()),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_group_gateway_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Gateway, None, Some(ids), None, None).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_gateway())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_gateway_by_name(&self, name: &str)
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Gateway, None, None, Some(name), None).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_gateway())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_gateway_by_category(&self, category: &str)
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Gateway, None, None, None, Some(category)).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_gateway())).collect(),
            Err(error) => Err(error)
        }
    }

    pub async fn list_group_gateway_option(&self, name: Option<&str>, category: Option<&str>)
        -> Result<Vec<GroupGatewaySchema>, Error>
    {
        match group::select_group(&self.pool, GroupKind::Gateway, None, None, name, category).await {
            Ok(value) => value.into_iter().map(|el| Ok(el.into_group_gateway())).collect(),
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

    pub async fn read_set(&self, id: Uuid)
        -> Result<SetSchema, Error>
    {
        match set::select_set(&self.pool, Some(id), None, None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_set_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<SetSchema>, Error>
    {
        set::select_set(&self.pool, None, Some(ids), None, None)
        .await
    }

    pub async fn list_set_by_template(&self, template_id: Uuid)
        -> Result<Vec<SetSchema>, Error>
    {
        set::select_set(&self.pool, None, None, Some(template_id), None)
        .await
    }

    pub async fn list_set_by_name(&self, name: &str)
        -> Result<Vec<SetSchema>, Error>
    {
        set::select_set(&self.pool, None, None, None, Some(name))
        .await
    }

    pub async fn list_set_option(&self, template_id: Option<Uuid>, name: Option<&str>)
        -> Result<Vec<SetSchema>, Error>
    {
        set::select_set(&self.pool, None, None, template_id, name)
        .await
    }

    pub async fn create_set(&self, id: Uuid, template_id: Uuid, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        set::insert_set(&self.pool, id, template_id, name, description)
        .await
    }

    pub async fn update_set(&self, id: Uuid, template_id: Option<Uuid>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        set::update_set(&self.pool, id, template_id, name, description)
        .await
    }

    pub async fn delete_set(&self, id: Uuid)
        -> Result<(), Error>
    {
        set::delete_set(&self.pool, id)
        .await
    }

    pub async fn add_set_member(&self, id: Uuid, device_id: Uuid, model_id: Uuid, data_index: &[u8])
        -> Result<(), Error>
    {
        set::insert_set_member(&self.pool, id, device_id, model_id, data_index)
        .await
    }

    pub async fn remove_set_member(&self, id: Uuid, device_id: Uuid, model_id: Uuid)
        -> Result<(), Error>
    {
        set::delete_set_member(&self.pool, id, device_id, model_id)
        .await
    }

    pub async fn swap_set_member(&self, id: Uuid, device_id_1: Uuid, model_id_1: Uuid, device_id_2: Uuid, model_id_2: Uuid)
        -> Result<(), Error>
    {
        set::swap_set_member(&self.pool, id, device_id_1, model_id_1, device_id_2, model_id_2)
        .await
    }

    pub async fn read_set_template(&self, id: Uuid)
        -> Result<SetTemplateSchema, Error>
    {
        match set::select_set_template(&self.pool, Some(id), None, None).await?
        .into_iter().next() {
            Some(value) => Ok(value),
            None => Err(Error::RowNotFound)
        }
    }

    pub async fn list_set_template_by_ids(&self, ids: &[Uuid])
        -> Result<Vec<SetTemplateSchema>, Error>
    {
        set::select_set_template(&self.pool, None, Some(ids), None)
        .await
    }

    pub async fn list_set_template_by_name(&self, name: &str)
        -> Result<Vec<SetTemplateSchema>, Error>
    {
        set::select_set_template(&self.pool, None, None, Some(name))
        .await
    }

    pub async fn list_set_template_option(&self, name: Option<&str>)
        -> Result<Vec<SetTemplateSchema>, Error>
    {
        set::select_set_template(&self.pool, None, None, name)
        .await
    }

    pub async fn create_set_template(&self, id: Uuid, name: &str, description: Option<&str>)
        -> Result<Uuid, Error>
    {
        set::insert_set_template(&self.pool, id, name, description)
        .await
    }

    pub async fn update_set_template(&self, id: Uuid, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        set::update_set_template(&self.pool, id, name, description)
        .await
    }

    pub async fn delete_set_template(&self, id: Uuid)
        -> Result<(), Error>
    {
        set::delete_set_template(&self.pool, id)
        .await
    }

    pub async fn add_set_template_member(&self, id: Uuid, type_id: Uuid, model_id: Uuid, data_index: &[u8])
        -> Result<(), Error>
    {
        set::insert_set_template_member(&self.pool, id, type_id, model_id, data_index)
        .await
    }

    pub async fn remove_set_template_member(&self, id: Uuid, index: usize)
        -> Result<(), Error>
    {
        set::delete_set_template_member(&self.pool, id, index)
        .await
    }

    pub async fn swap_set_template_member(&self, id: Uuid, index_1: usize, index_2: usize)
        -> Result<(), Error>
    {
        set::swap_set_template_member(&self.pool, id, index_1, index_2)
        .await
    }

    pub async fn read_slice(&self, id: i32)
        -> Result<SliceSchema, Error>
    {
        slice::select_slice(&self.pool, SliceSelector::None, Some(id), None, None, None, None).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_slice_by_ids(&self, ids: &[i32])
        -> Result<Vec<SliceSchema>, Error>
    {
        slice::select_slice(&self.pool, SliceSelector::None, None, Some(ids), None, None, None)
        .await
    }

    pub async fn list_slice_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Time(timestamp);
        slice::select_slice(&self.pool, selector, None, None, Some(device_id), Some(model_id), None)
        .await
    }

    pub async fn list_slice_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Range(begin, end);
        slice::select_slice(&self.pool, selector, None, None, Some(device_id), Some(model_id), None)
        .await
    }

    pub async fn list_slice_by_name_time(&self, name: &str, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Time(timestamp);
        slice::select_slice(&self.pool, selector, None, None, None, None, Some(name))
        .await
    }

    pub async fn list_slice_by_name_range(&self, name: &str, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = SliceSelector::Range(begin, end);
        slice::select_slice(&self.pool, selector, None, None, None, None, Some(name))
        .await
    }

    pub async fn list_slice_option(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
        -> Result<Vec<SliceSchema>, Error>
    {
        let selector = match (begin_or_timestamp, end) {
            (Some(begin), Some(end)) => SliceSelector::Range(begin, end),
            (Some(timestamp), None) => SliceSelector::Time(timestamp),
            _ => SliceSelector::None
        };
        slice::select_slice(&self.pool, selector, None, None, device_id, model_id, name).await
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

    pub async fn read_slice_set(&self, id: i32)
        -> Result<SliceSetSchema, Error>
    {
        slice::select_slice_set(&self.pool, SliceSelector::None, Some(id), None, None, None).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_slice_set_by_ids(&self, ids: &[i32])
        -> Result<Vec<SliceSetSchema>, Error>
    {
        slice::select_slice_set(&self.pool, SliceSelector::None, None, Some(ids), None, None)
        .await
    }

    pub async fn list_slice_set_by_time(&self, set_id: Uuid, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = SliceSelector::Time(timestamp);
        slice::select_slice_set(&self.pool, selector, None, None, Some(set_id), None)
        .await
    }

    pub async fn list_slice_set_by_range(&self, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = SliceSelector::Range(begin, end);
        slice::select_slice_set(&self.pool, selector, None, None, Some(set_id), None)
        .await
    }

    pub async fn list_slice_set_by_name_time(&self, name: &str, timestamp: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = SliceSelector::Time(timestamp);
        slice::select_slice_set(&self.pool, selector, None, None, None, Some(name))
        .await
    }

    pub async fn list_slice_set_by_name_range(&self, name: &str, begin: DateTime<Utc>, end: DateTime<Utc>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = SliceSelector::Range(begin, end);
        slice::select_slice_set(&self.pool, selector, None, None, None, Some(name))
        .await
    }

    pub async fn list_slice_set_option(&self, set_id: Option<Uuid>, name: Option<&str>, begin_or_timestamp: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>)
        -> Result<Vec<SliceSetSchema>, Error>
    {
        let selector = match (begin_or_timestamp, end) {
            (Some(begin), Some(end)) => SliceSelector::Range(begin, end),
            (Some(timestamp), None) => SliceSelector::Time(timestamp),
            _ => SliceSelector::None
        };
        slice::select_slice_set(&self.pool, selector, None, None, set_id, name).await
    }

    pub async fn create_slice_set(&self, set_id: Uuid, timestamp_begin: DateTime<Utc>, timestamp_end: DateTime<Utc>, name: &str, description: Option<&str>)
        -> Result<i32, Error>
    {
        slice::insert_slice_set(&self.pool, set_id, timestamp_begin, timestamp_end, name, description)
        .await
    }

    pub async fn update_slice_set(&self, id: i32, timestamp_begin: Option<DateTime<Utc>>, timestamp_end: Option<DateTime<Utc>>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        slice::update_slice_set(&self.pool, id, timestamp_begin, timestamp_end, name, description)
        .await
    }

    pub async fn delete_slice_set(&self, id: i32)
        -> Result<(), Error>
    {
        slice::delete_slice_set(&self.pool, id).await
    }

    pub async fn read_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DataSchema, Error>
    {
        let selector = DataSelector::Time(timestamp);
        data::select_data(&self.pool, selector, &[device_id], &[model_id], tag).await?.into_iter().next()
            .ok_or(Error::RowNotFound)
    }

    pub async fn list_data_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::Time(timestamp);
        data::select_data(&self.pool, selector, &[device_id], &[model_id], tag)
        .await
    }

    pub async fn list_data_by_latest(&self, device_id: Uuid, model_id: Uuid, latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::Latest(latest);
        data::select_data(&self.pool, selector, &[device_id], &[model_id], tag)
        .await
    }

    pub async fn list_data_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::Range(begin, end);
        data::select_data(&self.pool, selector, &[device_id], &[model_id], tag)
        .await
    }

    pub async fn list_data_by_number_before(&self, device_id: Uuid, model_id: Uuid, before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::NumberBefore(before, number);
        data::select_data(&self.pool, selector, &[device_id], &[model_id], tag)
        .await
    }

    pub async fn list_data_by_number_after(&self, device_id: Uuid, model_id: Uuid, after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::NumberAfter(after, number);
        data::select_data(&self.pool, selector, &[device_id], &[model_id], tag)
        .await
    }

    pub async fn list_data_group_by_time(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::Time(timestamp);
        data::select_data(&self.pool, selector, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_data_group_by_latest(&self, device_ids: &[Uuid], model_ids: &[Uuid], latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::Latest(latest);
        data::select_data(&self.pool, selector, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_data_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::Range(begin, end);
        data::select_data(&self.pool, selector, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_data_group_by_number_before(&self, device_ids: &[Uuid], model_ids: &[Uuid], before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::NumberBefore(before, number);
        data::select_data(&self.pool, selector, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_data_group_by_number_after(&self, device_ids: &[Uuid], model_ids: &[Uuid], after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<DataSchema>, Error>
    {
        let selector = DataSelector::NumberAfter(after, number);
        data::select_data(&self.pool, selector, device_ids, model_ids, tag)
        .await
    }

    pub async fn read_data_set(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DataSetSchema, Error>
    {
        let selector = DataSelector::Time(timestamp);
        data::select_data_set(&self.pool, selector, set_id, tag)
        .await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_data_set_by_time(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Error>
    {
        let selector = DataSelector::Time(timestamp);
        data::select_data_set(&self.pool, selector, set_id, tag)
        .await
    }

    pub async fn list_data_set_by_latest(&self, set_id: Uuid, latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Error>
    {
        let selector = DataSelector::Latest(latest);
        data::select_data_set(&self.pool, selector, set_id, tag)
        .await
    }

    pub async fn list_data_set_by_range(&self, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DataSetSchema>, Error>
    {
        let selector = DataSelector::Range(begin, end);
        data::select_data_set(&self.pool, selector, set_id, tag)
        .await
    }

    pub async fn create_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: &[DataValue], tag: Option<i16>)
        -> Result<(), Error>
    {
        data::insert_data(&self.pool, device_id, model_id, timestamp, data, tag)
        .await
    }

    pub async fn create_data_multiple(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamps: &[DateTime<Utc>], data: &[&[DataValue]], tags: Option<&[i16]>)
        -> Result<(), Error>
    {
        data::insert_data_multiple(&self.pool, device_ids, model_ids, timestamps, data, tags)
        .await
    }

    pub async fn delete_data(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<(), Error>
    {
        data::delete_data(&self.pool, device_id, model_id, timestamp, tag)
        .await
    }

    pub async fn read_data_timestamp(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Error>
    {
        let selector = DataSelector::Time(timestamp);
        data::select_timestamp(&self.pool, selector, &[device_id], &[model_id], tag).await?.into_iter().next()
            .ok_or(Error::RowNotFound)
    }

    pub async fn list_data_timestamp_by_latest(&self, device_id: Uuid, model_id: Uuid, latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let selector = DataSelector::Latest(latest);
        data::select_timestamp(&self.pool, selector, &[device_id], &[model_id], tag)
        .await
    }

    pub async fn list_data_timestamp_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let selector = DataSelector::Range(begin, end);
        data::select_timestamp(&self.pool, selector, &[device_id], &[model_id], tag)
        .await
    }

    pub async fn read_data_group_timestamp(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Error>
    {
        let selector = DataSelector::Time(timestamp);
        data::select_timestamp(&self.pool, selector, device_ids, model_ids, tag).await?.into_iter().next()
            .ok_or(Error::RowNotFound)
    }

    pub async fn list_data_group_timestamp_by_latest(&self, device_ids: &[Uuid], model_ids: &[Uuid], latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let selector = DataSelector::Latest(latest);
        data::select_timestamp(&self.pool, selector, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_data_group_timestamp_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let selector = DataSelector::Range(begin, end);
        data::select_timestamp(&self.pool, selector, device_ids, model_ids, tag)
        .await
    }

    pub async fn count_data(&self, device_id: Uuid, model_id: Uuid, tag: Option<i16>)
        -> Result<usize, Error>
    {
        data::count_data(&self.pool, DataSelector::Time(DateTime::default()), &[device_id], &[model_id], tag)
        .await
    }

    pub async fn count_data_by_latest(&self, device_id: Uuid, model_id: Uuid, latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        data::count_data(&self.pool, DataSelector::Latest(latest), &[device_id], &[model_id], tag)
        .await
    }

    pub async fn count_data_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        data::count_data(&self.pool, DataSelector::Range(begin, end), &[device_id], &[model_id], tag)
        .await
    }

    pub async fn count_data_group(&self, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
        -> Result<usize, Error>
    {
        data::count_data(&self.pool, DataSelector::Time(DateTime::default()), device_ids, model_ids, tag)
        .await
    }

    pub async fn count_data_group_by_latest(&self, device_ids: &[Uuid], model_ids: &[Uuid], latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        data::count_data(&self.pool, DataSelector::Latest(latest), device_ids, model_ids, tag)
        .await
    }

    pub async fn count_data_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        data::count_data(&self.pool, DataSelector::Range(begin, end), device_ids, model_ids, tag)
        .await
    }

    pub async fn read_buffer(&self, id: i32)
        -> Result<BufferSchema, Error>
    {
        buffer::select_buffer(&self.pool, BufferSelector::None, Some(&[id]), None, None, None).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let selector = BufferSelector::Time(timestamp);
        buffer::select_buffer(&self.pool, selector, None, Some(&[device_id]), Some(&[model_id]), tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_by_ids(&self, ids: &[i32])
        -> Result<Vec<BufferSchema>, Error>
    {
        buffer::select_buffer(&self.pool, BufferSelector::None, Some(ids), None, None, None)
        .await
    }

    pub async fn list_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Time(timestamp);
        buffer::select_buffer(&self.pool, selector, None, Some(&[device_id]), Some(&[model_id]), tag)
        .await
    }

    pub async fn list_buffer_by_latest(&self, device_id: Uuid, model_id: Uuid, latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Latest(latest);
        buffer::select_buffer(&self.pool, selector, None, Some(&[device_id]), Some(&[model_id]), tag)
        .await
    }

    pub async fn list_buffer_by_range(&self, device_id: Uuid, model_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Range(begin, end);
        buffer::select_buffer(&self.pool, selector, None, Some(&[device_id]), Some(&[model_id]), tag)
        .await
    }

    pub async fn list_buffer_by_number_before(&self, device_id: Uuid, model_id: Uuid, before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::NumberBefore(before, number);
        buffer::select_buffer(&self.pool, selector, None, Some(&[device_id]), Some(&[model_id]), tag)
        .await
    }

    pub async fn list_buffer_by_number_after(&self, device_id: Uuid, model_id: Uuid, after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::NumberAfter(after, number);
        buffer::select_buffer(&self.pool, selector, None, Some(&[device_id]), Some(&[model_id]), tag)
        .await
    }

    pub async fn read_buffer_first(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let selector = BufferSelector::First(1, 0);
        buffer::select_buffer(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_buffer_last(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let selector = BufferSelector::Last(1, 0);
        buffer::select_buffer(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_first(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::First(number, 0);
        buffer::select_buffer(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_buffer_first_offset(&self, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::First(number, offset);
        buffer::select_buffer(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_buffer_last(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Last(number, 0);
        buffer::select_buffer(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_buffer_last_offset(&self, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Last(number, offset);
        buffer::select_buffer(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_buffer_group_by_time(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Time(timestamp);
        buffer::select_buffer(&self.pool, selector, None, Some(device_ids), Some(model_ids), tag)
        .await
    }

    pub async fn list_buffer_group_by_latest(&self, device_ids: &[Uuid], model_ids: &[Uuid], latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Latest(latest);
        buffer::select_buffer(&self.pool, selector, None, Some(device_ids), Some(model_ids), tag)
        .await
    }

    pub async fn list_buffer_group_by_range(&self, device_ids: &[Uuid], model_ids: &[Uuid], begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Range(begin, end);
        buffer::select_buffer(&self.pool, selector, None, Some(device_ids), Some(model_ids), tag)
        .await
    }

    pub async fn list_buffer_group_by_number_before(&self, device_ids: &[Uuid], model_ids: &[Uuid], before: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::NumberBefore(before, number);
        buffer::select_buffer(&self.pool, selector, None, Some(device_ids), Some(model_ids), tag)
        .await
    }

    pub async fn list_buffer_group_by_number_after(&self, device_ids: &[Uuid], model_ids: &[Uuid], after: DateTime<Utc>, number: usize, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::NumberAfter(after, number);
        buffer::select_buffer(&self.pool, selector, None, Some(device_ids), Some(model_ids), tag)
        .await
    }

    pub async fn read_buffer_group_first(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let selector = BufferSelector::First(1, 0);
        buffer::select_buffer(&self.pool, selector, None, device_ids, model_ids, tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_buffer_group_last(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<BufferSchema, Error>
    {
        let selector = BufferSelector::Last(1, 0);
        buffer::select_buffer(&self.pool, selector, None, device_ids, model_ids, tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_group_first(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::First(number, 0);
        buffer::select_buffer(&self.pool, selector, None, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_buffer_group_first_offset(&self, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::First(number, offset);
        buffer::select_buffer(&self.pool, selector, None, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_buffer_group_last(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Last(number, 0);
        buffer::select_buffer(&self.pool, selector, None, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_buffer_group_last_offset(&self, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<BufferSchema>, Error>
    {
        let selector = BufferSelector::Last(number, offset);
        buffer::select_buffer(&self.pool, selector, None, device_ids, model_ids, tag)
        .await
    }

    pub async fn read_buffer_set(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<BufferSetSchema, Error>
    {
        let selector = BufferSelector::Time(timestamp);
        buffer::select_buffer_set(&self.pool, selector, set_id, tag)
        .await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_set_by_time(&self, set_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Error>
    {
        let selector = BufferSelector::Time(timestamp);
        buffer::select_buffer_set(&self.pool, selector, set_id, tag)
        .await
    }

    pub async fn list_buffer_set_by_latest(&self, set_id: Uuid, latest: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Error>
    {
        let selector = BufferSelector::Latest(latest);
        buffer::select_buffer_set(&self.pool, selector, set_id, tag)
        .await
    }

    pub async fn list_buffer_set_by_range(&self, set_id: Uuid, begin: DateTime<Utc>, end: DateTime<Utc>, tag: Option<i16>)
        -> Result<Vec<BufferSetSchema>, Error>
    {
        let selector = BufferSelector::Range(begin, end);
        buffer::select_buffer_set(&self.pool, selector, set_id, tag)
        .await
    }

    pub async fn create_buffer(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: &[DataValue], tag: Option<i16>)
        -> Result<i32, Error>
    {
        buffer::insert_buffer(&self.pool, device_id, model_id, timestamp, data, tag)
        .await
    }

    pub async fn create_buffer_multiple(&self, device_ids: &[Uuid], model_ids: &[Uuid], timestamps: &[DateTime<Utc>], data: &[&[DataValue]], tags: Option<&[i16]>)
        -> Result<Vec<i32>, Error>
    {
        buffer::insert_buffer_multiple(&self.pool, device_ids, model_ids, timestamps, data, tags)
        .await
    }

    pub async fn update_buffer(&self, id: i32, data: Option<&[DataValue]>, tag: Option<i16>)
        -> Result<(), Error>
    {
        buffer::update_buffer(&self.pool, Some(id), None, None, None, data, tag)
        .await
    }

    pub async fn update_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, data: Option<&[DataValue]>, tag: Option<i16>)
        -> Result<(), Error>
    {
        buffer::update_buffer(&self.pool, None, Some(device_id), Some(model_id), Some(timestamp), data, tag)
        .await
    }

    pub async fn delete_buffer(&self, id: i32)
        -> Result<(), Error>
    {
        buffer::delete_buffer(&self.pool, Some(id), None, None, None, None).await
    }

    pub async fn delete_buffer_by_time(&self, device_id: Uuid, model_id: Uuid, timestamp: DateTime<Utc>, tag: Option<i16>)
        -> Result<(), Error>
    {
        buffer::delete_buffer(&self.pool, None, Some(device_id), Some(model_id), Some(timestamp), tag).await
    }

    pub async fn read_buffer_timestamp_first(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Error>
    {
        let selector = BufferSelector::First(1, 0);
        buffer::select_timestamp(&self.pool, selector, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_buffer_timestamp_last(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<DateTime<Utc>, Error>
    {
        let selector = BufferSelector::Last(1, 0);
        buffer::select_timestamp(&self.pool, selector, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_buffer_timestamp_first(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let selector = BufferSelector::First(number, 0);
        buffer::select_timestamp(&self.pool, selector, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_buffer_timestamp_last(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let selector = BufferSelector::Last(number, 0);
        buffer::select_timestamp(&self.pool, selector, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_buffer_group_timestamp_first(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let selector = BufferSelector::First(number, 0);
        buffer::select_timestamp(&self.pool, selector, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_buffer_group_timestamp_last(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<DateTime<Utc>>, Error>
    {
        let selector = BufferSelector::Last(number, 0);
        buffer::select_timestamp(&self.pool, selector, device_ids, model_ids, tag)
        .await
    }

    pub async fn count_buffer(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        buffer::count_buffer(&self.pool, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn count_buffer_group(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<usize, Error>
    {
        buffer::count_buffer(&self.pool, device_ids, model_ids, tag)
        .await
    }

    pub async fn read_log(&self, id: i32)
        -> Result<LogSchema, Error>
    {
        log::select_log(&self.pool, LogSelector::None, Some(&[id]), None, None, None).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_log_by_time(&self, timestamp: DateTime<Utc>, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<LogSchema, Error>
    {
        let selector = LogSelector::Time(timestamp);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_log_by_ids(&self, ids: &[i32])
        -> Result<Vec<LogSchema>, Error>
    {
        log::select_log(&self.pool, LogSelector::None, Some(ids), None, None, None)
        .await
    }

    pub async fn list_log_by_time(&self, timestamp: DateTime<Utc>, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Time(timestamp);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_log_by_latest(&self, latest: DateTime<Utc>, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Latest(latest);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_log_by_range(&self, begin: DateTime<Utc>, end: DateTime<Utc>, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Range(begin, end);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn read_log_first(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<LogSchema, Error>
    {
        let selector = LogSelector::First(1, 0);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_log_last(&self, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<LogSchema, Error>
    {
        let selector = LogSelector::Last(1, 0);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_log_first(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::First(number, 0);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_log_first_offset(&self, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::First(number, offset);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_log_last(&self, number: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Last(number, 0);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_log_last_offset(&self, number: usize, offset: usize, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Last(number, offset);
        log::select_log(&self.pool, selector, None, device_id.as_ref().map(|id| from_ref(id)), model_id.as_ref().map(|id| from_ref(id)), tag)
        .await
    }

    pub async fn list_log_group_by_time(&self, timestamp: DateTime<Utc>, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Time(timestamp);
        log::select_log(&self.pool, selector, None, Some(device_ids), Some(model_ids), tag)
        .await
    }

    pub async fn list_log_group_by_latest(&self, latest: DateTime<Utc>, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Latest(latest);
        log::select_log(&self.pool, selector, None, Some(device_ids), Some(model_ids), tag)
        .await
    }

    pub async fn list_log_group_by_range(&self, begin: DateTime<Utc>, end: DateTime<Utc>, device_ids: &[Uuid], model_ids: &[Uuid], tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Range(begin, end);
        log::select_log(&self.pool, selector, None, Some(device_ids), Some(model_ids), tag)
        .await
    }

    pub async fn read_log_group_first(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<LogSchema, Error>
    {
        let selector = LogSelector::First(1, 0);
        log::select_log(&self.pool, selector, None, device_ids, model_ids, tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn read_log_group_last(&self, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<LogSchema, Error>
    {
        let selector = LogSelector::Last(1, 0);
        log::select_log(&self.pool, selector, None, device_ids, model_ids, tag).await?
        .into_iter().next().ok_or(Error::RowNotFound)
    }

    pub async fn list_log_group_first(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::First(number, 0);
        log::select_log(&self.pool, selector, None, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_log_group_first_offset(&self, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::First(number, offset);
        log::select_log(&self.pool, selector, None, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_log_group_last(&self, number: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Last(number, 0);
        log::select_log(&self.pool, selector, None, device_ids, model_ids, tag)
        .await
    }

    pub async fn list_log_group_last_offset(&self, number: usize, offset: usize, device_ids: Option<&[Uuid]>, model_ids: Option<&[Uuid]>, tag: Option<i16>)
        -> Result<Vec<LogSchema>, Error>
    {
        let selector = LogSelector::Last(number, offset);
        log::select_log(&self.pool, selector, None, device_ids, model_ids, tag)
        .await
    }

    pub async fn create_log(&self, timestamp: DateTime<Utc>, device_id: Option<Uuid>, model_id: Option<Uuid>, value: DataValue, tag: Option<i16>)
        -> Result<i32, Error>
    {
        log::insert_log(&self.pool, timestamp, device_id, model_id, value, tag)
        .await
    }

    pub async fn update_log(&self, id: i32, value: Option<DataValue>, tag: Option<i16>)
        -> Result<(), Error>
    {
        log::update_log(&self.pool, Some(id), None, None, None, value, tag)
        .await
    }

    pub async fn update_log_by_time(&self, timestamp: DateTime<Utc>, device_id: Option<Uuid>, model_id: Option<Uuid>, value: Option<DataValue>, tag: Option<i16>)
        -> Result<(), Error>
    {
        log::update_log(&self.pool, None, Some(timestamp), device_id, model_id, value, tag)
        .await
    }

    pub async fn delete_log(&self, id: i32)
        -> Result<(), Error>
    {
        log::delete_log(&self.pool, Some(id), None, None, None, None).await
    }

    pub async fn delete_log_by_time(&self, timestamp: DateTime<Utc>, device_id: Option<Uuid>, model_id: Option<Uuid>, tag: Option<i16>)
        -> Result<(), Error>
    {
        log::delete_log(&self.pool, None, Some(timestamp), device_id, model_id, tag).await
    }

}
