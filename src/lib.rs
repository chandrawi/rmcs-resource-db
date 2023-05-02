pub mod schema;
pub mod operation;

use sqlx::{Pool, Error};
use sqlx::mysql::{MySql, MySqlPoolOptions};

pub use schema::value::{ConfigValue, DataValue};
pub use schema::model::{ModelSchema, ModelConfigSchema};
use operation::model;

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

}
