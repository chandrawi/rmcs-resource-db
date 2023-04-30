#[cfg(test)]
mod tests {
    use std::vec;

    use sqlx::{Pool, Row, Error};
    use sqlx::mysql::{MySql, MySqlRow, MySqlPoolOptions};
    use rmcs_resource_db::Resource;

    async fn get_connection_pool() -> Result<Pool<MySql>, Error>
    {
        dotenvy::dotenv().ok();
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        MySqlPoolOptions::new()
            .max_connections(100)
            .connect(&url)
            .await
    }

    async fn check_tables_exist(pool: &Pool<MySql>) -> Result<bool, Error>
    {
        let sql = "SHOW TABLES;";
        let tables: Vec<String> = sqlx::query(sql)
            .map(|row: MySqlRow| row.get(0))
            .fetch_all(pool)
            .await?;

        Ok(tables == vec![
            String::from("_sqlx_migrations"),
            String::from("buffer_index"),
            String::from("buffer_timestamp"),
            String::from("buffer_timestamp_index"),
            String::from("buffer_timestamp_micros"),
            String::from("data_index"),
            String::from("data_timestamp"),
            String::from("data_timestamp_index"),
            String::from("data_timestamp_micros"),
            String::from("device"),
            String::from("device_config"),
            String::from("device_type"),
            String::from("device_type_model"),
            String::from("group_device"),
            String::from("group_device_map"),
            String::from("group_model"),
            String::from("group_model_map"),
            String::from("log_device"),
            String::from("log_server"),
            String::from("model"),
            String::from("model_config"),
            String::from("model_type"),
            String::from("slice_index"),
            String::from("slice_timestamp"),
            String::from("slice_timestamp_index"),
            String::from("slice_timestamp_micros")
        ])
    }

    #[sqlx::test]
    async fn test_resource()
    {
        // std::env::set_var("RUST_BACKTRACE", "1");

        let pool = get_connection_pool().await.unwrap();
        let resource = Resource::new_with_pool(pool);

        // drop tables from previous test if exist
        if check_tables_exist(&resource.pool).await.unwrap() {
            sqlx::migrate!().undo(&resource.pool, 2).await.unwrap();
        }
        // create tables for testing
        sqlx::migrate!().run(&resource.pool).await.unwrap();
        // check if all tables successfully created
        if !check_tables_exist(&resource.pool).await.unwrap() {
            panic!("Database migration failed!");
        }

        // drop tables after testing
        sqlx::migrate!().undo(&resource.pool, 7).await.unwrap();
    }

}
