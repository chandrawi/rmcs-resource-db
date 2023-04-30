use ConfigValue::{Int, Float, Str};
use DataValue::{I8, I16, I32, I64, U8, U16, U32, U64, F32, F64, Char, Bool};

pub trait BytesValue {
    fn from_bytes(bytes: &[u8], type_string: &str) -> Self;
    fn into_bytes(&self) -> Vec<u8>;
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum ConfigValue {
    Int(i64),
    Float(f64),
    Str(String),
    #[default]
    Null
}

impl BytesValue for ConfigValue {
    fn from_bytes(bytes: &[u8], type_string: &str) -> Self
    {
        match type_string {
            "int" => Int(i64::from_be_bytes(bytes.try_into().unwrap_or_default())),
            "float" => Float(f64::from_be_bytes(bytes.try_into().unwrap_or_default())),
            "str" => Str(std::str::from_utf8(bytes).unwrap_or_default().to_owned()),
            _ => ConfigValue::Null
        }
    }
    fn into_bytes(&self) -> Vec<u8>
    {
        match self {
            Int(value) => value.to_be_bytes().to_vec(),
            Float(value) => value.to_be_bytes().to_vec(),
            Str(value) => value.as_bytes().to_vec(),
            ConfigValue::Null => vec![]
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum DataValue {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Char(char),
    Bool(bool),
    #[default]
    Null
}

impl BytesValue for DataValue {
    fn from_bytes(bytes: &[u8], type_string: &str) -> Self
    {
        if bytes.len() == 0 {
            return DataValue::Null;
        }
        let first_el = bytes[0];
        let sel_val = |n: usize, v: DataValue| -> DataValue {
            if bytes.len() == n {
                v
            } else {
                DataValue::Null
            }
        };
        match type_string {
            "i8" => sel_val(1, I8(i8::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "i16" => sel_val(2, I16(i16::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "i32" => sel_val(4, I32(i32::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "i64" => sel_val(8, I64(i64::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "u8" => sel_val(1, U8(u8::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "u16" => sel_val(2, U16(u16::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "u32" => sel_val(4, U32(u32::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "u64" => sel_val(8, U64(u64::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "f32" => sel_val(4, F32(f32::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "f64" => sel_val(8, F64(f64::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            "char" => sel_val(1, Char(char::from_u32(first_el as u32).unwrap_or_default())),
            "bool" => sel_val(1, Bool(bool::from(first_el > 0))),
            _ => DataValue::Null
        }
    }
    fn into_bytes(&self) -> Vec<u8> {
        match self {
            I8(value) => value.to_be_bytes().to_vec(),
            I16(value) => value.to_be_bytes().to_vec(),
            I32(value) => value.to_be_bytes().to_vec(),
            I64(value) => value.to_be_bytes().to_vec(),
            U8(value) => value.to_be_bytes().to_vec(),
            U16(value) => value.to_be_bytes().to_vec(),
            U32(value) => value.to_be_bytes().to_vec(),
            U64(value) => value.to_be_bytes().to_vec(),
            F32(value) => value.to_be_bytes().to_vec(),
            F64(value) => value.to_be_bytes().to_vec(),
            Char(value) => vec![*value as u8],
            Bool(value) => vec![*value as u8],
            _ => vec![]
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayDataValue(Vec<DataValue>);

impl ArrayDataValue {
    pub fn from_bytes(bytes: &[u8], types: &[&str]) -> Self
    {
        let mut values = vec![];
        let mut index = 0;
        for t in types {
            let len = match *t {
                "i8" | "u8" | "char" | "bool" => 1,
                "i16" | "u16" => 2,
                "i32" | "u32" | "f32" => 4,
                "i64" | "u64" | "f64" => 8,
                _ => 0
            };
            if index + len > bytes.len() {
                break;
            }
            if len == 0 {
                continue;
            }
            values.push(DataValue::from_bytes(&bytes[index..index + len], t));
            index += len;
        }
        ArrayDataValue(values)
    }
    pub fn into_bytes(&self) -> Vec<u8>
    {
        let mut bytes = vec![];
        for value in &self.0 {
            bytes.append(&mut value.into_bytes());
        }
        bytes
    }
}

macro_rules! value_impl_from {
    ($from:ty, $value:ty, $variant:path, $into:ty) => {
        impl From<$from> for $value {
            fn from(value: $from) -> Self {
                $variant(value as $into)
            }
        }
    };
    ($from:ty, $value:ty, $variant:path) => {
        impl From<$from> for $value {
            fn from(value: $from) -> Self {
                $variant(value.into())
            }
        }
    };
}

value_impl_from!(i8, ConfigValue, Int, i64);
value_impl_from!(i16, ConfigValue, Int, i64);
value_impl_from!(i32, ConfigValue, Int, i64);
value_impl_from!(i64, ConfigValue, Int, i64);
value_impl_from!(u8, ConfigValue, Int, i64);
value_impl_from!(u16, ConfigValue, Int, i64);
value_impl_from!(u32, ConfigValue, Int, i64);
value_impl_from!(u64, ConfigValue, Int, i64);
value_impl_from!(f32, ConfigValue, Float, f64);
value_impl_from!(f64, ConfigValue, Float, f64);
value_impl_from!(String, ConfigValue, Str);
value_impl_from!(&str, ConfigValue, Str);

value_impl_from!(i8, DataValue, I8);
value_impl_from!(i16, DataValue, I16);
value_impl_from!(i32, DataValue, I32);
value_impl_from!(i64, DataValue, I64);
value_impl_from!(u8, DataValue, U8);
value_impl_from!(u16, DataValue, U16);
value_impl_from!(u32, DataValue, U32);
value_impl_from!(u64, DataValue, U64);
value_impl_from!(f32, DataValue, F32);
value_impl_from!(f64, DataValue, F64);
value_impl_from!(char, DataValue, Char);
value_impl_from!(bool, DataValue, Bool);

macro_rules! value_impl_try_into {
    ($into:ty, $value:ty, $variant:path) => {
        impl TryInto<$into> for $value {
            type Error = String;
            fn try_into(self) -> Result<$into, Self::Error> {
                match self {
                    $variant(value) => Ok(value as $into),
                    _ => Err(String::from("conversion error"))
                }
            }
        }
    };
}

value_impl_try_into!(i8, ConfigValue, Int);
value_impl_try_into!(i16, ConfigValue, Int);
value_impl_try_into!(i32, ConfigValue, Int);
value_impl_try_into!(i64, ConfigValue, Int);
value_impl_try_into!(u8, ConfigValue, Int);
value_impl_try_into!(u16, ConfigValue, Int);
value_impl_try_into!(u32, ConfigValue, Int);
value_impl_try_into!(u64, ConfigValue, Int);
value_impl_try_into!(f32, ConfigValue, Float);
value_impl_try_into!(f64, ConfigValue, Float);
value_impl_try_into!(String, ConfigValue, Str);

value_impl_try_into!(i8, DataValue, I8);
value_impl_try_into!(i16, DataValue, I16);
value_impl_try_into!(i32, DataValue, I32);
value_impl_try_into!(i64, DataValue, I64);
value_impl_try_into!(u8, DataValue, U8);
value_impl_try_into!(u16, DataValue, U16);
value_impl_try_into!(u32, DataValue, U32);
value_impl_try_into!(u64, DataValue, U64);
value_impl_try_into!(f32, DataValue, F32);
value_impl_try_into!(f64, DataValue, F64);
value_impl_try_into!(char, DataValue, Char);
value_impl_try_into!(bool, DataValue, Bool);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_value_conversion()
    {
        let value: u8 = 1;
        let conf = ConfigValue::from(value);
        assert_eq!(value, conf.try_into().unwrap());
        let value: i16 = -256;
        let conf = ConfigValue::from(value);
        assert_eq!(value, conf.try_into().unwrap());
        let value: u32 = 65536;
        let conf = ConfigValue::from(value);
        assert_eq!(value, conf.try_into().unwrap());
        let value: i64 = -4294967296;
        let conf = ConfigValue::from(value);
        assert_eq!(value, conf.try_into().unwrap());

        let value: f32 = 65536.65536;
        let conf = ConfigValue::from(value);
        assert_eq!(value, conf.try_into().unwrap());
        let value: f64 = 4294967296.4294967296;
        let conf = ConfigValue::from(value);
        assert_eq!(value, conf.try_into().unwrap());

        let value: &str = "slice_value";
        let conf = ConfigValue::from(value);
        let convert: String = conf.try_into().unwrap();
        assert_eq!(String::from("slice_value"), convert);
    }

    #[test]
    fn config_value_bytes() 
    {
        let bytes = [255, 255, 255, 255, 255, 255, 255, 0];
        let conf = ConfigValue::from_bytes(&bytes, "int");
        assert_eq!(bytes.to_vec(), conf.into_bytes());
        assert_eq!(conf, Int(-256));

        let bytes = [63, 136, 0, 0, 0, 0, 0, 0];
        let conf = ConfigValue::from_bytes(&bytes, "float");
        assert_eq!(bytes.to_vec(), conf.into_bytes());
        assert_eq!(conf, Float(0.01171875));

        let bytes = [97, 98, 99, 100];
        let conf = ConfigValue::from_bytes(&bytes, "string");
        assert_eq!(bytes.to_vec(), conf.into_bytes());
        assert_eq!(conf, Str(String::from("abcd")));
    }

    #[test]
    fn data_value_conversion()
    {
        let value: i8 = -1;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());
        let value: i16 = -256;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());
        let value: i32 = -65536;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());
        let value: i64 = -4294967296;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());

        let value: u8 = 1;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());
        let value: u16 = 256;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());
        let value: u32 = 65536;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());
        let value: u64 = 4294967296;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());

        let value: f32 = 65536.65536;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());
        let value: f64 = 4294967296.4294967296;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());

        let value: bool = true;
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());
        let value: char = 'a';
        let data = DataValue::from(value);
        assert_eq!(value, data.try_into().unwrap());
    }

    #[test]
    fn data_value_bytes() 
    {
        let bytes = [255];
        let value = DataValue::from_bytes(&bytes, "i8");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, I8(-1));
        let bytes = [255, 0];
        let value = DataValue::from_bytes(&bytes, "i16");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, I16(-256));
        let bytes = [255, 255, 255, 0];
        let value = DataValue::from_bytes(&bytes, "i32");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, I32(-256));
        let bytes = [255, 255, 255, 255, 255, 255, 255, 0];
        let value = DataValue::from_bytes(&bytes, "i64");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, I64(-256));

        let bytes = [1];
        let value = DataValue::from_bytes(&bytes, "u8");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, U8(1));
        let bytes = [1, 0];
        let value = DataValue::from_bytes(&bytes, "u16");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, U16(256));
        let bytes = [1, 0, 0, 0];
        let value = DataValue::from_bytes(&bytes, "u32");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, U32(16777216));
        let bytes = [1, 0, 0, 0, 0, 0, 0, 0];
        let value = DataValue::from_bytes(&bytes, "u64");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, U64(72057594037927936));

        let bytes = [62, 32, 0, 0];
        let value = DataValue::from_bytes(&bytes, "f32");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, F32(0.15625));
        let bytes = [63, 136, 0, 0, 0, 0, 0, 0];
        let value = DataValue::from_bytes(&bytes, "f64");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, F64(0.01171875));

        let bytes = [97];
        let value = DataValue::from_bytes(&bytes, "char");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, Char('a'));
        let bytes = [1];
        let value = DataValue::from_bytes(&bytes, "bool");
        assert_eq!(bytes.to_vec(), value.into_bytes());
        assert_eq!(value, Bool(true));

        // wrong bytes length
        let bytes = [1, 0];
        assert_eq!(DataValue::from_bytes(&bytes, "u8"), DataValue::Null);
    }

    #[test]
    fn array_data_value_bytes() 
    {
        let bytes = [1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let types = ["u8", "i16", "u32", "i64"];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(data.0, [
            U8(1),
            I16(256),
            U32(16777216),
            I64(72057594037927936)
        ]);

        let bytes = [62, 32, 0, 0, 63, 136, 0, 0, 0, 0, 0, 0];
        let types = ["f32", "f64"];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(data.0, [
            F32(0.15625),
            F64(0.01171875)
        ]);

        let bytes = [97, 1];
        let types = ["char", "bool"];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(data.0, [
            Char('a'),
            Bool(true)
        ]);
    }

}
