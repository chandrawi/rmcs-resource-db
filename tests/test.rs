#[cfg(test)]
mod tests {
    use std::vec;
    use sqlx::{Pool, Error};
    use sqlx::postgres::{Postgres, PgPoolOptions};
    use sqlx::types::chrono::{Utc, DateTime};
    use uuid::Uuid;
    use rmcs_resource_db::{ModelConfigSchema, DeviceConfigSchema};
    use rmcs_resource_db::{Resource, ConfigValue::{*, self}, DataType::*, DataValue::*};
    use rmcs_resource_db::{BufferStatus::*, LogStatus::*};

    async fn get_connection_pool() -> Result<Pool<Postgres>, Error>
    {
        dotenvy::dotenv().ok();
        let url = std::env::var("DATABASE_URL_RESOURCE_TEST").unwrap();
        PgPoolOptions::new()
            .max_connections(100)
            .connect(&url)
            .await
    }

    async fn truncate_tables(pool: &Pool<Postgres>) -> Result<(), Error>
    {
        let sql = "TRUNCATE TABLE \"system_log\", \"data_slice\", \"data_buffer\", \"data\", \"group_model_map\", \"group_device_map\", \"group_model\", \"group_device\", \"device_config\", \"device\", \"device_type_model\", \"device_type\", \"model_config\", \"model\";";
        sqlx::query(sql)
            .execute(pool)
            .await?;
        Ok(())
    }

    #[sqlx::test]
    async fn test_resource()
    {
        std::env::set_var("RUST_BACKTRACE", "1");

        let pool = get_connection_pool().await.unwrap();
        let resource = Resource::new_with_pool(pool);

        // truncate all resource database tables before test
        truncate_tables(&resource.pool).await.unwrap();

        // create new data model and add data types
        let model_id = resource.create_model(Uuid::new_v4(), &[F32T,F32T], "UPLINK", "speed and direction", None).await.unwrap();
        let model_buf_id = resource.create_model(Uuid::new_v4(), &[U8T,U8T,U8T,U8T], "UPLINK", "buffer 4", None).await.unwrap();
        // create scale, symbol, and threshold configurations for new created model
        resource.create_model_config(model_id, 0, "scale_0", Str("speed".to_owned()), "SCALE").await.unwrap();
        resource.create_model_config(model_id, 1, "scale_1", Str("direction".to_owned()), "SCALE").await.unwrap();
        resource.create_model_config(model_id, 0, "unit_0", Str("meter/second".to_owned()), "UNIT").await.unwrap();
        resource.create_model_config(model_id, 1, "unit_1", Str("degree".to_owned()), "UNIT").await.unwrap();
        let model_cfg_id = resource.create_model_config(model_id, 0, "upper_threshold", Int(250), "THRESHOLD").await.unwrap();

        // Create new type and link it to newly created model
        let type_id = resource.create_type(Uuid::new_v4(), "Speedometer Compass", None).await.unwrap();
        resource.add_type_model(type_id, model_id).await.unwrap();
        resource.add_type_model(type_id, model_buf_id).await.unwrap();

        // create new devices with newly created type as its type 
        let gateway_id = Uuid::parse_str("bfc01f2c-8b2c-47cf-912a-f95f6f41a1e6").unwrap();
        let device_id1 = Uuid::parse_str("74768a42-bc29-40eb-8934-2effcbf34f8f").unwrap();
        let device_id2 = Uuid::parse_str("150a0a77-2d9b-4672-9253-3d42fd0f0940").unwrap();
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
        let group_model_id = resource.create_group_model(Uuid::new_v4(), "data", "APPLICATION", None).await.unwrap();
        resource.add_group_model_member(group_model_id, model_id).await.unwrap();
        // create new group and register newly created devices as its member
        let group_device_id = resource.create_group_device(Uuid::new_v4(), "sensor", "APPLICATION", None).await.unwrap();
        resource.add_group_device_member(group_device_id, device_id1).await.unwrap();
        resource.add_group_device_member(group_device_id, device_id2).await.unwrap();

        // read model
        let model = resource.read_model(model_id).await.unwrap();
        let models = resource.list_model_by_name("speed").await.unwrap();
        let model_ids: Vec<Uuid> = models.iter().map(|u| u.id).collect();
        assert!(model_ids.contains(&model_id));
        assert_eq!(model.name, "speed and direction");
        assert_eq!(model.category, "UPLINK");
        assert_eq!(model.data_type, [F32T,F32T]);
        // read model configurations
        let model_configs = resource.list_model_config_by_model(model_id).await.unwrap();
        let mut config_vec: Vec<ModelConfigSchema> = Vec::new();
        for cfg_vec in model.configs {
            for cfg in cfg_vec {
                config_vec.push(cfg);
            }
        }
        assert_eq!(model_configs, config_vec);

        // read device
        let device1 = resource.read_device(device_id1).await.unwrap();
        let devices = resource.list_device_by_gateway(gateway_id).await.unwrap();
        let device_ids: Vec<Uuid> = devices.iter().map(|u| u.id).collect();
        assert!(device_ids.contains(&device_id1)); // device_id1 > device_id2, so device1 in second (last) order
        assert_eq!(device1.serial_number, "TEST01");
        assert_eq!(device1.name, "Speedometer Compass 1");
        // read type
        let types = resource.list_type_by_name("Speedometer").await.unwrap();
        let device_type = types.iter().filter(|x| x.id == type_id).next().unwrap();
        assert_eq!(device1.type_, device_type.to_owned());
        // read device configurations
        let device_configs = resource.list_device_config_by_device(device_id1).await.unwrap();
        assert_eq!(device1.configs, device_configs);

        // read group model
        let groups = resource.list_group_model_by_category("APPLICATION").await.unwrap();
        let group_model = groups.iter().filter(|x| x.models.contains(&model_id)).next().unwrap();
        assert_eq!(group_model.name, "data");
        assert_eq!(group_model.category, "APPLICATION");
        // read group device
        let groups = resource.list_group_device_by_name("sensor").await.unwrap();
        let group_device = groups.iter().filter(|x| x.devices.contains(&device_id1)).next().unwrap();
        assert_eq!(group_device.devices, [device_id1, device_id2]);
        assert_eq!(group_device.name, "sensor");
        assert_eq!(group_device.category, "APPLICATION");

        // update model
        resource.update_model(model_buf_id, Some(&[I32T,I32T]), None, Some("buffer 2 integer"), Some("Model for store 2 i32 temporary data")).await.unwrap();
        let model = resource.read_model(model_buf_id).await.unwrap();
        assert_eq!(model.name, "buffer 2 integer");
        assert_eq!(model.data_type, [I32T,I32T]);
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

        // generate raw data and create buffers
        let timestamp = DateTime::parse_from_str("2023-05-07 07:08:48.123456 +0000", "%Y-%m-%d %H:%M:%S.%6f %z").unwrap().into();
        let raw_1 = vec![I32(1231),I32(890)];
        let raw_2 = vec![I32(1452),I32(-341)];
        resource.create_buffer(device_id1, model_buf_id, timestamp, raw_1.clone(), Analysis1).await.unwrap();
        resource.create_buffer(device_id2, model_buf_id, timestamp, raw_2.clone(), Analysis1).await.unwrap();

        // read buffer
        let buffers = resource.list_buffer_first(100, None, None, None).await.unwrap();
        assert_eq!(buffers[0].data, raw_1);
        assert_eq!(buffers[1].data, raw_2);

        // get model config value then convert buffer data
        let conf_val = |model_configs: Vec<DeviceConfigSchema>, name: &str| -> ConfigValue {
            model_configs.iter().filter(|&cfg| cfg.name == name.to_owned())
                .next().unwrap().value.clone()
        };
        let convert = |raw: i32, coef0: i64, coef1: f64| -> f64 {
            (raw as f64 - coef0 as f64) * coef1
        };
        let coef0 = conf_val(device_configs.clone(), "coef_0").try_into().unwrap();
        let coef1 = conf_val(device_configs.clone(), "coef_1").try_into().unwrap();
        let speed = convert(raw_1[0].clone().try_into().unwrap(), coef0, coef1) as f32;
        let direction = convert(raw_1[1].clone().try_into().unwrap(), coef0, coef1) as f32;
        // create data
        resource.create_data(device_id1, model_id, timestamp, vec![F32(speed), F32(direction)]).await.unwrap();

        // read data
        let datas = resource.list_data_by_number_before(device_id1, model_id, timestamp, 100).await.unwrap();
        let data = datas.iter().filter(|x| x.device_id == device_id1 && x.model_id == model_id).next().unwrap();
        assert_eq!(vec![F32(speed), F32(direction)], data.data);
        assert_eq!(timestamp, data.timestamp);

        // delete data
        resource.delete_data(device_id1, model_id, timestamp).await.unwrap();
        let result = resource.read_data(device_id1, model_id, timestamp).await;
        assert!(result.is_err());

        // update buffer status
        resource.update_buffer(buffers[0].id, None, Some(Delete)).await.unwrap();
        let buffer = resource.read_buffer(buffers[0].id).await.unwrap();
        assert_eq!(buffers[0].data, buffer.data);
        assert_eq!(buffer.status, Delete);

        // delete buffer data
        resource.delete_buffer(buffers[0].id).await.unwrap();
        resource.delete_buffer(buffers[1].id).await.unwrap();
        let result = resource.read_buffer(buffers[0].id).await;
        assert!(result.is_err());

        // create data slice
        let slice_id = resource.create_slice(device_id1, model_id, timestamp, timestamp, "Speed and compass slice", None).await.unwrap();
        // read data
        let slices = resource.list_slice_by_name("slice").await.unwrap();
        let slice = slices.iter().filter(|x| x.device_id == device_id1 && x.model_id == model_id).next().unwrap();
        assert_eq!(slice.timestamp_begin, timestamp);
        assert_eq!(slice.name, "Speed and compass slice");

        // update data slice
        resource.update_slice(slice_id, None, None, None, Some("Speed and compass sensor 1 at '2023-05-07 07:08:48'")).await.unwrap();
        let slice = resource.read_slice(slice_id).await.unwrap();
        assert_eq!(slice.description, "Speed and compass sensor 1 at '2023-05-07 07:08:48'");

        // delete data slice
        resource.delete_slice(slice_id).await.unwrap();
        let result = resource.read_slice(slice_id).await;
        assert!(result.is_err());

        // create system log
        resource.create_log(timestamp, device_id1, UnknownError, Str("testing success".to_owned())).await.unwrap();
        // read log
        let logs = resource.list_log_by_range_time(timestamp, Utc::now(), None, None).await.unwrap();
        let log = logs.iter().filter(|x| x.device_id == device_id1 && x.timestamp == timestamp).next().unwrap();
        assert_eq!(log.value, Str("testing success".to_owned()));

        // update system log
        resource.update_log(timestamp, device_id1, Some(Success), None).await.unwrap();
        let log = resource.read_log(timestamp, device_id1).await.unwrap();
        assert_eq!(log.status, Success);

        // delete system log
        resource.delete_log(timestamp, device_id1).await.unwrap();
        let result = resource.read_log(timestamp, device_id1).await;
        assert!(result.is_err());

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
