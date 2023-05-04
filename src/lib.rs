pub mod schema;
pub mod operation;

use sqlx::{Pool, Error};
use sqlx::mysql::{MySql, MySqlPoolOptions};

pub use schema::value::{ConfigValue, DataValue};
pub use schema::model::{ModelSchema, ModelConfigSchema};
pub use schema::device::{DeviceSchema, GatewaySchema, TypeSchema, DeviceConfigSchema, GatewayConfigSchema};
use schema::device::DeviceKind;
pub use schema::group::{GroupModelSchema, GroupDeviceSchema, GroupGatewaySchema};
use schema::group::GroupKind;
use operation::model;
use operation::device;
use operation::types;
use operation::group;

pub struct Resource {
    pub pool: Pool<MySql>,
    options: ResourceOptions
}

#[derive(Debug)]
pub struct ResourceOptions {
    limit: u32,
    with_description: bool,
    order: Vec<OrderOption>
}

#[derive(Debug)]
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
        let pool = MySqlPoolOptions::new()
            .max_connections(100)
            .connect(url)
            .await
            .expect(&format!("Error connecting to {}", url));
        Resource {
            pool,
            options: ResourceOptions::default()
        }
    }

    pub fn new_with_pool(pool: Pool<MySql>) -> Resource {
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

    pub async fn read_model(&self, id: u32)
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

    pub async fn create_model(&self, indexing: &str, category: &str, name: &str, description: Option<&str>)
        -> Result<u32, Error>
    {
        model::insert_model(&self.pool, indexing, category, name, description)
        .await
    }

    pub async fn update_model(&self, id: u32, indexing: Option<&str>, category: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        model::update_model(&self.pool, id, indexing, category, name, description)
        .await
    }

    pub async fn delete_model(&self, id: u32)
        -> Result<(), Error>
    {
        model::delete_model(&self.pool, id)
        .await
    }

    pub async fn add_model_type(&self, id: u32, types: &[&str])
        -> Result<(), Error>
    {
        model::insert_model_types(&self.pool, id, types)
        .await
    }

    pub async fn remove_model_type(&self, id: u32)
        -> Result<(), Error>
    {
        model::delete_model_types(&self.pool, id)
        .await
    }

    pub async fn read_model_config(&self, id: u32)
        -> Result<ModelConfigSchema, Error>
    {
        model::select_model_config_by_id(&self.pool, id)
        .await
    }

    pub async fn list_model_config_by_model(&self, model_id: u32)
        -> Result<Vec<ModelConfigSchema>, Error>
    {
        model::select_model_config_by_model(&self.pool, model_id)
        .await
    }

    pub async fn create_model_config(&self, model_id: u32, index: u32, name: &str, value: ConfigValue, category: &str)
        -> Result<u32, Error>
    {
        model::insert_model_config(&self.pool, model_id, index, name, value, category)
        .await
    }

    pub async fn update_model_config(&self, id: u32, name: Option<&str>, value: Option<ConfigValue>, category: Option<&str>)
        -> Result<(), Error>
    {
        model::update_model_config(&self.pool, id, name, value, category)
        .await
    }

    pub async fn delete_model_config(&self, id: u32)
        -> Result<(), Error>
    {
        model::delete_model_config(&self.pool, id)
        .await
    }

    pub async fn read_device(&self, id: u64)
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

    pub async fn list_device_by_gateway(&self, gateway_id: u64)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device_by_gateway(&self.pool, DeviceKind::Device, gateway_id)
        .await
    }

    pub async fn list_device_by_type(&self, type_id: u32)
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

    pub async fn list_device_by_gateway_type(&self, gateway_id: u64, type_id: u32)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device_by_gateway_type(&self.pool, DeviceKind::Device, gateway_id, type_id)
        .await
    }

    pub async fn list_device_by_gateway_name(&self, gateway_id: u64, name: &str)
        -> Result<Vec<DeviceSchema>, Error>
    {
        device::select_device_by_gateway_name(&self.pool, DeviceKind::Device, gateway_id, name)
        .await
    }

    pub async fn create_device(&self, id: u64, gateway_id: u64, type_id: u32, serial_number: &str, name: &str, description: Option<&str>)
        -> Result<(), Error>
    {
        device::insert_device(&self.pool, id, gateway_id, type_id, serial_number, name, description)
        .await
    }

    pub async fn update_device(&self, id: u64, gateway_id: Option<u64>, type_id: Option<u32>, serial_number: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        device::update_device(&self.pool, DeviceKind::Device, id, gateway_id, type_id, serial_number, name, description)
        .await
    }

    pub async fn delete_device(&self, id: u64)
        -> Result<(), Error>
    {
        device::delete_device(&self.pool, DeviceKind::Device, id)
        .await
    }

    pub async fn read_gateway(&self, id: u64)
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

    pub async fn list_gateway_by_type(&self, type_id: u32)
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

    pub async fn create_gateway(&self, id: u64, type_id: u32, serial_number: &str, name: &str, description: Option<&str>)
        -> Result<(), Error>
    {
        device::insert_device(&self.pool, id, id, type_id, serial_number, name, description)
        .await
    }

    pub async fn update_gateway(&self, id: u64, type_id: Option<u32>, serial_number: Option<&str>, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        device::update_device(&self.pool, DeviceKind::Gateway, id, None, type_id, serial_number, name, description)
        .await
    }

    pub async fn delete_gateway(&self, id: u64)
        -> Result<(), Error>
    {
        device::delete_device(&self.pool, DeviceKind::Gateway, id)
        .await
    }

    pub async fn read_device_config(&self, id: u32)
        -> Result<DeviceConfigSchema, Error>
    {
        device::select_device_config_by_id(&self.pool, DeviceKind::Device, id)
        .await
    }

    pub async fn list_device_config_by_device(&self, device_id: u64)
        -> Result<Vec<DeviceConfigSchema>, Error>
    {
        device::select_device_config_by_device(&self.pool, DeviceKind::Device, device_id)
        .await
    }

    pub async fn create_device_config(&self, device_id: u64, name: &str, value: ConfigValue, category: &str)
        -> Result<u32, Error>
    {
        device::insert_device_config(&self.pool, device_id, name, value, category)
        .await
    }

    pub async fn update_device_config(&self, id: u32, name: Option<&str>, value: Option<ConfigValue>, category: Option<&str>)
        -> Result<(), Error>
    {
        device::update_device_config(&self.pool, id, name, value, category)
        .await
    }

    pub async fn delete_device_config(&self, id: u32)
        -> Result<(), Error>
    {
        device::delete_device_config(&self.pool, id)
        .await
    }

    pub async fn read_gateway_config(&self, id: u32)
        -> Result<GatewayConfigSchema, Error>
    {
        match device::select_device_config_by_id(&self.pool, DeviceKind::Gateway, id).await {
            Ok(value) => Ok(value.into_gateway_config()),
            Err(error) => Err(error)
        }
    }

    pub async fn list_gateway_config_by_gateway(&self, gateway_id: u64)
        -> Result<Vec<GatewayConfigSchema>, Error>
    {
        match device::select_device_config_by_device(&self.pool, DeviceKind::Gateway, gateway_id).await {
            Ok(value) => {
                value.into_iter().map(|el| Ok(el.into_gateway_config())).collect()
            },
            Err(error) => Err(error)
        }
    }

    pub async fn create_gateway_config(&self, gateway_id: u64, name: &str, value: ConfigValue, category: &str)
        -> Result<u32, Error>
    {
        device::insert_device_config(&self.pool, gateway_id, name, value, category)
        .await
    }

    pub async fn update_gateway_config(&self, id: u32, name: Option<&str>, value: Option<ConfigValue>, category: Option<&str>)
        -> Result<(), Error>
    {
        device::update_device_config(&self.pool, id, name, value, category)
        .await
    }

    pub async fn delete_gateway_config(&self, id: u32)
        -> Result<(), Error>
    {
        device::delete_device_config(&self.pool, id)
        .await
    }

    pub async fn read_type(&self, id: u32)
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

    pub async fn create_type(&self, name: &str, description: Option<&str>)
        -> Result<u32, Error>
    {
        types::insert_device_type(&self.pool, name, description)
        .await
    }

    pub async fn update_type(&self, id: u32, name: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        types::update_device_type(&self.pool, id, name, description)
        .await
    }

    pub async fn delete_type(&self, id: u32)
        -> Result<(), Error>
    {
        types::delete_device_type(&self.pool, id)
        .await
    }

    pub async fn add_type_model(&self, id: u32, model_id: u32)
        -> Result<(), Error>
    {
        types::insert_device_type_model(&self.pool, id, model_id)
        .await
    }

    pub async fn remove_type_model(&self, id: u32, model_id: u32)
        -> Result<(), Error>
    {
        types::delete_device_type_model(&self.pool, id, model_id)
        .await
    }

    pub async fn read_group_model(&self, id: u32)
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

    pub async fn create_group_model(&self, name: &str, category: &str, description: Option<&str>)
        -> Result<u32, Error>
    {
        group::insert_group(&self.pool, GroupKind::Model, name, category, description)
        .await
    }

    pub async fn update_group_model(&self, id: u32, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        group::update_group(&self.pool, GroupKind::Model, id, name, category, description)
        .await
    }

    pub async fn delete_group_model(&self, id: u32)
        -> Result<(), Error>
    {
        group::delete_group(&self.pool, GroupKind::Model, id)
        .await
    }

    pub async fn add_group_model_member(&self, id: u32, model_id: u32)
        -> Result<(), Error>
    {
        group::insert_group_map(&self.pool, GroupKind::Model, id, model_id as u64)
        .await
    }

    pub async fn remove_group_model_member(&self, id: u32, model_id: u32)
        -> Result<(), Error>
    {
        group::delete_group_map(&self.pool, GroupKind::Model, id, model_id as u64)
        .await
    }

    pub async fn read_group_device(&self, id: u32)
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

    pub async fn create_group_device(&self, name: &str, category: &str, description: Option<&str>)
        -> Result<u32, Error>
    {
        group::insert_group(&self.pool, GroupKind::Device, name, category, description)
        .await
    }

    pub async fn update_group_device(&self, id: u32, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        group::update_group(&self.pool, GroupKind::Device, id, name, category, description)
        .await
    }

    pub async fn delete_group_device(&self, id: u32)
        -> Result<(), Error>
    {
        group::delete_group(&self.pool, GroupKind::Device, id)
        .await
    }

    pub async fn add_group_device_member(&self, id: u32, device_id: u64)
        -> Result<(), Error>
    {
        group::insert_group_map(&self.pool, GroupKind::Device, id, device_id)
        .await
    }

    pub async fn remove_group_device_member(&self, id: u32, device_id: u64)
        -> Result<(), Error>
    {
        group::delete_group_map(&self.pool, GroupKind::Device, id, device_id)
        .await
    }

    pub async fn read_group_gateway(&self, id: u32)
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

    pub async fn create_group_gateway(&self, name: &str, category: &str, description: Option<&str>)
        -> Result<u32, Error>
    {
        group::insert_group(&self.pool, GroupKind::Gateway, name, category, description)
        .await
    }

    pub async fn update_group_gateway(&self, id: u32, name: Option<&str>, category: Option<&str>, description: Option<&str>)
        -> Result<(), Error>
    {
        group::update_group(&self.pool, GroupKind::Gateway, id, name, category, description)
        .await
    }

    pub async fn delete_group_gateway(&self, id: u32)
        -> Result<(), Error>
    {
        group::delete_group(&self.pool, GroupKind::Gateway, id)
        .await
    }

    pub async fn add_group_gateway_member(&self, id: u32, device_id: u64)
        -> Result<(), Error>
    {
        group::insert_group_map(&self.pool, GroupKind::Gateway, id, device_id)
        .await
    }

    pub async fn remove_group_gateway_member(&self, id: u32, device_id: u64)
        -> Result<(), Error>
    {
        group::delete_group_map(&self.pool, GroupKind::Gateway, id, device_id)
        .await
    }

}
