use ConfigValue::{Int, Float, Str};
use DataValue::{I8, I16, I32, I64, U8, U16, U32, U64, F32, F64, Char, Bool};
use ConfigType::{IntT, FloatT, StrT};
use DataType::{I8T, I16T, I32T, I64T, U8T, U16T, U32T, U64T, F32T, F64T, CharT, BoolT};

#[derive(Debug)]
pub enum ConfigType {
    IntT,
    FloatT,
    StrT,
    NullT
}

impl ConfigType {
    pub(crate) fn from_str(value: &str) -> Self {
        match value {
            "int" => IntT,
            "float" => FloatT,
            "str" => StrT,
            _ => Self::NullT
        }
    }
    pub(crate) fn to_string(self) -> String {
        match self {
            IntT => String::from("int"),
            FloatT => String::from("float"),
            StrT => String::from("str"),
            Self::NullT => String::new()
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum ConfigValue {
    Int(i64),
    Float(f64),
    Str(String),
    #[default]
    Null
}

impl ConfigValue {
    pub fn from_bytes(bytes: &[u8], type_: ConfigType) -> Self {
        match type_ {
            IntT => Int(i64::from_be_bytes(bytes.try_into().unwrap_or_default())),
            FloatT => Float(f64::from_be_bytes(bytes.try_into().unwrap_or_default())),
            StrT => Str(std::str::from_utf8(bytes).unwrap_or_default().to_owned()),
            _ => Self::Null
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Int(value) => value.to_be_bytes().to_vec(),
            Float(value) => value.to_be_bytes().to_vec(),
            Str(value) => value.as_bytes().to_vec(),
            Self::Null => Vec::new()
        }
    }
    pub fn get_type(&self) -> ConfigType {
        match self {
            Int(_) => IntT,
            Float(_) => FloatT,
            Str(_) => StrT,
            Self::Null => ConfigType::NullT
        }
    }
}

pub type LogValue = ConfigValue;

#[derive(Debug, Default, PartialEq, Clone)]
pub enum DataIndexing {
    #[default]
    Timestamp,
    TimestampIndex,
    TimestampMicros
}

impl DataIndexing {
    pub fn from_str(value: &str) -> Self {
        match value {
            "timestamp_index" => Self::TimestampIndex,
            "timestamp_micros" => Self::TimestampMicros,
            _ => Self::Timestamp,
        }
    }
    pub fn to_string(self) -> String {
        match self {
            Self::Timestamp => String::from("timestamp"),
            Self::TimestampIndex => String::from("timestamp_index"),
            Self::TimestampMicros => String::from("timestamp_micros")
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    I8T,
    I16T,
    I32T,
    I64T,
    U8T,
    U16T,
    U32T,
    U64T,
    F32T,
    F64T,
    CharT,
    BoolT,
    NullT
}

impl DataType {
    pub(crate) fn from_str(value: &str) -> Self {
        match value {
            "i8" => I8T,
            "i16" => I16T,
            "i32" => I32T,
            "i64" => I64T,
            "u8" => U8T,
            "u16" => U16T,
            "u32" => U32T,
            "u64" => U64T,
            "f32" => F32T,
            "f64" => F64T,
            "char" => CharT,
            "bool" => BoolT,
            _ => Self::NullT
        }
    }
    pub(crate) fn to_string(self) -> String {
        match self {
            I8T => String::from("i8"),
            I16T => String::from("i16"),
            I32T => String::from("i32"),
            I64T => String::from("i64"),
            U8T => String::from("u8"),
            U16T => String::from("u16"),
            U32T => String::from("u32"),
            U64T => String::from("u64"),
            F32T => String::from("f32"),
            F64T => String::from("f64"),
            CharT => String::from("char"),
            BoolT => String::from("bool"),
            Self::NullT => String::new()
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

impl DataValue {
    pub fn from_bytes(bytes: &[u8], type_: DataType) -> Self {
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
        match type_ {
            I8T => sel_val(1, I8(i8::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            I16T => sel_val(2, I16(i16::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            I32T => sel_val(4, I32(i32::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            I64T => sel_val(8, I64(i64::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            U8T => sel_val(1, U8(u8::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            U16T => sel_val(2, U16(u16::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            U32T => sel_val(4, U32(u32::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            U64T => sel_val(8, U64(u64::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            F32T => sel_val(4, F32(f32::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            F64T => sel_val(8, F64(f64::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            CharT => sel_val(1, Char(char::from_u32(first_el as u32).unwrap_or_default())),
            BoolT => sel_val(1, Bool(bool::from(first_el > 0))),
            _ => Self::Null
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
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
            Char(value) => Vec::from([*value as u8]),
            Bool(value) => Vec::from([*value as u8]),
            _ => Vec::new()
        }
    }
    pub fn get_type(&self) -> DataType {
        match self {
            I8(_) => I8T,
            I16(_) => I16T,
            I32(_) => I32T,
            I64(_) => I64T,
            U8(_) => U8T,
            U16(_) => U16T,
            U32(_) => U32T,
            U64(_) => U64T,
            F32(_) => F32T,
            F64(_) => F64T,
            Char(_) => CharT,
            Bool(_) => BoolT,
            Self::Null => DataType::NullT
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayDataValue(Vec<DataValue>);

impl ArrayDataValue {
    pub fn from_bytes(bytes: &[u8], types: &[DataType]) -> Self {
        let mut values = Vec::new();
        let mut index = 0;
        for t in types {
            let len = match t {
                I8T | U8T | CharT | BoolT => 1,
                I16T | U16T => 2,
                I32T | U32T | F32T => 4,
                I64T | U64T | F64T => 8,
                _ => 0
            };
            if index + len > bytes.len() {
                break;
            }
            if len == 0 {
                continue;
            }
            values.push(DataValue::from_bytes(&bytes[index..index + len], t.clone()));
            index += len;
        }
        ArrayDataValue(values)
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in &self.0 {
            bytes.append(&mut value.to_bytes());
        }
        bytes
    }
    pub fn from_vec(data_vec: &[DataValue]) -> Self {
        Self(data_vec.to_vec())
    }
    pub fn to_vec(self) -> Vec<DataValue> {
        self.0
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
        let conf = ConfigValue::from_bytes(&bytes, IntT);
        assert_eq!(bytes.to_vec(), conf.to_bytes());
        assert_eq!(conf, Int(-256));

        let bytes = [63, 136, 0, 0, 0, 0, 0, 0];
        let conf = ConfigValue::from_bytes(&bytes, FloatT);
        assert_eq!(bytes.to_vec(), conf.to_bytes());
        assert_eq!(conf, Float(0.01171875));

        let bytes = [97, 98, 99, 100];
        let conf = ConfigValue::from_bytes(&bytes, StrT);
        assert_eq!(bytes.to_vec(), conf.to_bytes());
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
        let value = DataValue::from_bytes(&bytes, I8T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, I8(-1));
        let bytes = [255, 0];
        let value = DataValue::from_bytes(&bytes, I16T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, I16(-256));
        let bytes = [255, 255, 255, 0];
        let value = DataValue::from_bytes(&bytes, I32T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, I32(-256));
        let bytes = [255, 255, 255, 255, 255, 255, 255, 0];
        let value = DataValue::from_bytes(&bytes, I64T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, I64(-256));

        let bytes = [1];
        let value = DataValue::from_bytes(&bytes, U8T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, U8(1));
        let bytes = [1, 0];
        let value = DataValue::from_bytes(&bytes, U16T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, U16(256));
        let bytes = [1, 0, 0, 0];
        let value = DataValue::from_bytes(&bytes, U32T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, U32(16777216));
        let bytes = [1, 0, 0, 0, 0, 0, 0, 0];
        let value = DataValue::from_bytes(&bytes, U64T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, U64(72057594037927936));

        let bytes = [62, 32, 0, 0];
        let value = DataValue::from_bytes(&bytes, F32T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, F32(0.15625));
        let bytes = [63, 136, 0, 0, 0, 0, 0, 0];
        let value = DataValue::from_bytes(&bytes, F64T);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, F64(0.01171875));

        let bytes = [97];
        let value = DataValue::from_bytes(&bytes, CharT);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, Char('a'));
        let bytes = [1];
        let value = DataValue::from_bytes(&bytes, BoolT);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, Bool(true));

        // wrong bytes length
        let bytes = [1, 0];
        assert_eq!(DataValue::from_bytes(&bytes, U8T), DataValue::Null);
    }

    #[test]
    fn array_data_value_bytes() 
    {
        let bytes = [1, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let types = [U8T, I16T, U32T, I64T];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(data.0, [
            U8(1),
            I16(256),
            U32(16777216),
            I64(72057594037927936)
        ]);

        let bytes = [62, 32, 0, 0, 63, 136, 0, 0, 0, 0, 0, 0];
        let types = [F32T, F64T];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(data.0, [
            F32(0.15625),
            F64(0.01171875)
        ]);

        let bytes = [97, 1];
        let types = [CharT, BoolT];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(data.0, [
            Char('a'),
            Bool(true)
        ]);
    }

}
