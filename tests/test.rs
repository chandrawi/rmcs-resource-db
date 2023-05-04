#[cfg(test)]
mod tests {
    use std::vec;

    use sqlx::{Pool, Row, Error};
    use sqlx::mysql::{MySql, MySqlRow, MySqlPoolOptions};
    use rmcs_resource_db::{Resource, ConfigValue::*};

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

        // create new data model and add data types
        let model_id = resource.create_model("timestamp", "UPLINK", "speed and direction", None).await.unwrap();
        let model_buffer_id = resource.create_model("timestamp", "UPLINK", "buffer8", None).await.unwrap();
        resource.add_model_type(model_id, &["f32","f32"]).await.unwrap();
        resource.add_model_type(model_buffer_id, &["u8","u8","u8","u8","u8","u8","u8","u8"]).await.unwrap();
        // create scale, symbol, and threshold configurations for new created model
        resource.create_model_config(model_id, 0, "scale_0", Str("speed".to_owned()), "SCALE").await.unwrap();
        resource.create_model_config(model_id, 1, "scale_1", Str("direction".to_owned()), "SCALE").await.unwrap();
        resource.create_model_config(model_id, 0, "unit_0", Str("meter/second".to_owned()), "UNIT").await.unwrap();
        resource.create_model_config(model_id, 1, "unit_1", Str("degree".to_owned()), "UNIT").await.unwrap();
        let model_cfg_id = resource.create_model_config(model_id, 0, "upper_threshold", Int(250), "THRESHOLD").await.unwrap();

        // Create new type and link it to newly created model
        let type_id = resource.create_type("Speedometer Compass", None).await.unwrap();
        resource.add_type_model(type_id, model_id).await.unwrap();
        resource.add_type_model(type_id, model_buffer_id).await.unwrap();

        // create new devices with newly created type as its type 
        let gateway_id = 0x87AD915C32B89D09;
        let device_id1 = 0xA07C2589F301DB46;
        let device_id2 = 0x2C8B82061E8F10A2;
        resource.create_device(device_id1, gateway_id, type_id, "TEST01", "Speedometer Compass 1", None).await.unwrap();
        resource.create_device(device_id2, gateway_id, type_id, "TEST02", "Speedometer Compass 2", None).await.unwrap();
        // create device configurations
        resource.create_device_config(device_id1, "coef_0", Int(-21), "CONVERSION").await.unwrap();
        resource.create_device_config(device_id1, "coef_1", Float(0.1934), "CONVERSION").await.unwrap();
        resource.create_device_config(device_id1, "period", Int(60), "NETWORK").await.unwrap();
        resource.create_device_config(device_id2, "coef_0", Int(44), "CONVERSION").await.unwrap();
        resource.create_device_config(device_id2, "coef_1", Float(0.2192), "CONVERSION").await.unwrap();
        let device_cfg_id = resource.create_device_config(device_id2, "period", Int(120), "NETWORK").await.unwrap();

        // create new group and register newly created models as its member
        let group_model_id = resource.create_group_model("data", "APPLICATION", None).await.unwrap();
        resource.add_group_model_member(group_model_id, model_id).await.unwrap();
        // create new group and register newly created devices as its member
        let group_device_id = resource.create_group_device("sensor", "APPLICATION", None).await.unwrap();
        resource.add_group_device_member(group_device_id, device_id1).await.unwrap();
        resource.add_group_device_member(group_device_id, device_id2).await.unwrap();

        // read model
        let model = resource.read_model(model_id).await.unwrap();
        let models = resource.list_model_by_name("speed").await.unwrap();
        let last_model = models.into_iter().last().unwrap();
        assert_eq!(model, last_model);
        assert_eq!(model.name, "speed and direction");
        assert_eq!(model.indexing, "timestamp");
        assert_eq!(model.category, "UPLINK");
        assert_eq!(model.types, ["f32","f32"]);
        // read model configurations
        let model_configs = resource.list_model_config_by_model(model_id).await.unwrap();
        let mut config_vec: Vec<rmcs_resource_db::ModelConfigSchema> = Vec::new();
        for cfg_vec in model.configs {
            for cfg in cfg_vec {
                config_vec.push(cfg);
            }
        }
        assert_eq!(model_configs, config_vec);

        // read device
        let device1 = resource.read_device(device_id1).await.unwrap();
        let devices = resource.list_device_by_gateway(gateway_id).await.unwrap();
        let last_device = devices.into_iter().last().unwrap();
        assert_eq!(device1, last_device); // device_id1 > device_id2, so device1 in second (last) order
        assert_eq!(device1.serial_number, "TEST01");
        assert_eq!(device1.name, "Speedometer Compass 1");
        // read type
        let types = resource.list_type_by_name("Speedometer").await.unwrap();
        assert_eq!(device1.types, types.into_iter().next().unwrap());
        // read device configurations
        let device_configs = resource.list_device_config_by_device(device_id1).await.unwrap();
        assert_eq!(device1.configs, device_configs);

        // read group model
        let groups = resource.list_group_model_by_category("APPLICATION").await.unwrap();
        let group = groups.into_iter().next().unwrap();
        assert_eq!(group.models, [model_id]);
        assert_eq!(group.name, "data");
        assert_eq!(group.category, "APPLICATION");
        // read group device
        let groups = resource.list_group_device_by_name("sensor").await.unwrap();
        let group = groups.into_iter().next().unwrap();
        assert_eq!(group.devices, [device_id2, device_id1]);
        assert_eq!(group.name, "sensor");
        assert_eq!(group.category, "APPLICATION");

        // update model
        resource.update_model(model_buffer_id, None, None, Some("buffer 10 bytes"), Some("Model for store 10 bytes temporary data")).await.unwrap();
        resource.remove_model_type(model_buffer_id).await.unwrap();
        resource.add_model_type(model_buffer_id, &["u8","u8","u8","u8","u8","u8","u8","u8","u8","u8"]).await.unwrap();
        let model = resource.read_model(model_buffer_id).await.unwrap();
        assert_eq!(model.name, "buffer 10 bytes");
        assert_eq!(model.types, ["u8","u8","u8","u8","u8","u8","u8","u8","u8","u8"]);
        // update model configurations
        resource.update_model_config(model_cfg_id, None, Some(Int(238)), None).await.unwrap();
        let config = resource.read_model_config(model_cfg_id).await.unwrap();
        assert_eq!(config.value, Int(238));

        // update type
        resource.update_type(type_id, None, Some("Speedometer and compass sensor")).await.unwrap();
        let type_ = resource.read_type(type_id).await.unwrap();
        assert_eq!(type_.description, "Speedometer and compass sensor");

        // update device
        resource.update_device(device_id2, None, None, None, None, Some("E-bike speedometer and compass sensor 2")).await.unwrap();
        let device2 = resource.read_device(device_id2).await.unwrap();
        assert_eq!(device2.description, "E-bike speedometer and compass sensor 2");
        // update device config
        resource.update_device_config(device_cfg_id, None, Some(Int(60)), None).await.unwrap();
        let config = resource.read_device_config(device_cfg_id).await.unwrap();
        assert_eq!(config.value, Int(60));

        // update group model
        resource.update_group_model(group_model_id, None, None, Some("Data models")).await.unwrap();
        let group = resource.read_group_model(group_model_id).await.unwrap();
        assert_eq!(group.description, "Data models");
        // update group device
        resource.update_group_device(group_device_id, None, None, Some("Sensor devices")).await.unwrap();
        let group = resource.read_group_device(group_device_id).await.unwrap();
        assert_eq!(group.description, "Sensor devices");

        // delete model config
        let config_id = model_configs.iter().next().map(|el| el.id).unwrap();
        resource.delete_model_config(config_id).await.unwrap();
        let result = resource.read_model_config(config_id).await;
        assert!(result.is_err());
        // delete model
        resource.delete_model(model_id).await.unwrap();
        let result = resource.read_model(model_id).await;
        assert!(result.is_err());
        // check if all model config also deleted
        let configs = resource.list_model_config_by_model(model_id).await.unwrap();
        assert_eq!(configs.len(), 0);

        // delete device config
        let config_id = device_configs.iter().next().map(|el| el.id).unwrap();
        resource.delete_device_config(config_id).await.unwrap();
        let result = resource.read_device_config(config_id).await;
        assert!(result.is_err());
        // delete device
        resource.delete_device(device_id1).await.unwrap();
        let result = resource.read_device(device_id1).await;
        assert!(result.is_err());
        // check if all device config also deleted
        let configs = resource.list_device_config_by_device(device_id1).await.unwrap();
        assert_eq!(configs.len(), 0);

        // delete type
        let result = resource.delete_type(type_id).await;
        assert!(result.is_err()); // error because a device associated with the type still exists
        let devices = resource.list_device_by_type(type_id).await.unwrap();
        for device in devices {
            resource.delete_device(device.id).await.unwrap();
        }
        resource.delete_type(type_id).await.unwrap();

        // check number of member of the group
        let group = resource.read_group_model(group_model_id).await.unwrap();
        assert_eq!(group.models.len(), 0);
        let group = resource.read_group_device(group_device_id).await.unwrap();
        assert_eq!(group.devices.len(), 0);
        // delete group model and device
        resource.delete_group_model(group_model_id).await.unwrap();
        resource.delete_group_device(group_device_id).await.unwrap();
        let result = resource.read_group_model(group_model_id).await;
        assert!(result.is_err());
        let result = resource.read_group_device(group_device_id).await;
        assert!(result.is_err());

    }

}
