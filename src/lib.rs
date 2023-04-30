use sqlx::Pool;
use sqlx::mysql::{MySql, MySqlPoolOptions};

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

}
