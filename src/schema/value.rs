use ConfigValue::{Int, Float, Str};
use DataValue::{I8, I16, I32, I64, U8, U16, U32, U64, F32, F64, Char, Bool};
use ConfigType::{IntT, FloatT, StrT};
use DataType::{I8T, I16T, I32T, I64T, U8T, U16T, U32T, U64T, F32T, F64T, CharT, BoolT};
use rmcs_resource_api::common;

#[derive(Debug)]
pub enum ConfigType {
    IntT,
    FloatT,
    StrT,
    NullT
}

impl From<i16> for ConfigType {
    fn from(value: i16) -> Self {
        match value {
            1 => IntT,
            2 => FloatT,
            3 => StrT,
            _ => Self::NullT
        }
    }
}

impl From<ConfigType> for i16 {
    fn from(value: ConfigType) -> Self {
        match value {
            IntT => 1,
            FloatT => 2,
            StrT => 3,
            ConfigType::NullT => 0
        }
    }
}

impl From<common::ConfigType> for ConfigType {
    fn from(value: common::ConfigType) -> Self {
        match value {
            common::ConfigType::Nullc => Self::NullT,
            common::ConfigType::Int => Self::IntT,
            common::ConfigType::Float => Self::FloatT,
            common::ConfigType::Str => Self::StrT
        }
    }
}

impl Into<common::ConfigType> for ConfigType {
    fn into(self) -> common::ConfigType {
        match self {
            Self::NullT => common::ConfigType::Nullc,
            Self::IntT => common::ConfigType::Int,
            Self::FloatT => common::ConfigType::Float,
            Self::StrT => common::ConfigType::Str
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

pub type LogType = ConfigType;
pub type LogValue = ConfigValue;

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

impl From<i16> for DataType {
    fn from(value: i16) -> Self {
        match value {
            1 => I8T,
            2 => I16T,
            3 => I32T,
            4 => I64T,
            5 => U8T,
            6 => U16T,
            7 => U32T,
            8 => U64T,
            9 => F32T,
            10 => F64T,
            11 => CharT,
            12 => BoolT,
            _ => Self::NullT
        }
    }
}

impl From<DataType> for i16 {
    fn from(value: DataType) -> Self {
        match value {
            I8T => 1,
            I16T => 2,
            I32T => 3,
            I64T => 4,
            U8T => 5,
            U16T => 6,
            U32T => 7,
            U64T => 8,
            F32T => 9,
            F64T => 10,
            CharT => 11,
            BoolT => 12,
            DataType::NullT => 0
        }
    }
}

impl From<common::DataType> for DataType {
    fn from(value: common::DataType) -> Self {
        match value {
            common::DataType::Nulld => Self::NullT,
            common::DataType::I8 => Self::I8T,
            common::DataType::I16 => Self::I16T,
            common::DataType::I32 => Self::I32T,
            common::DataType::I64 => Self::I64T,
            common::DataType::U8 => Self::U8T,
            common::DataType::U16 => Self::U16T,
            common::DataType::U32 => Self::U32T,
            common::DataType::U64 => Self::U64T,
            common::DataType::F32 => Self::F32T,
            common::DataType::F64 => Self::F64T,
            common::DataType::Char => Self::CharT,
            common::DataType::Bool => Self::BoolT
        }
    }
}

impl Into<common::DataType> for DataType {
    fn into(self) -> common::DataType {
        match self {
            Self::I8T => common::DataType::I8,
            Self::I16T => common::DataType::I16,
            Self::I32T => common::DataType::I32,
            Self::I64T => common::DataType::I64,
            Self::U8T => common::DataType::U8,
            Self::U16T => common::DataType::U16,
            Self::U32T => common::DataType::U32,
            Self::U64T => common::DataType::U64,
            Self::F32T => common::DataType::F32,
            Self::F64T => common::DataType::F64,
            Self::CharT => common::DataType::Char,
            Self::BoolT => common::DataType::Bool,
            Self::NullT => common::DataType::Nulld
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
    fn to_int(&self) -> Option<u64> {
        match *self {
            I8(value) => Some(value as u64),
            I16(value) => Some(value as u64),
            I32(value) => Some(value as u64),
            I64(value) => Some(value as u64),
            U8(value) => Some(value as u64),
            U16(value) => Some(value as u64),
            U32(value) => Some(value as u64),
            U64(value) => Some(value as u64),
            _ => None
        }
    }
    fn to_float(&self) -> Option<f64> {
        match *self {
            F32(value) => Some(value as f64),
            F64(value) => Some(value as f64),
            _ => None
        }
    }
    pub fn convert(self, type_: DataType) -> Option<Self> {
        let type_group = | t: DataType | -> u8 {
            match t {
                I8T | I16T | I32T | I64T | U8T | U16T | U32T | U64T => 1,
                F32T | F64T => 2,
                CharT => 3,
                BoolT => 4,
                _ => 0
            }
        };
        if type_group(self.get_type()) != type_group(type_.clone()) {
            return None
        }
        match type_ {
            I8T => Some(I8(self.to_int().unwrap() as i8)),
            I16T => Some(I16(self.to_int().unwrap() as i16)),
            I32T => Some(I32(self.to_int().unwrap() as i32)),
            I64T => Some(I64(self.to_int().unwrap() as i64)),
            U8T => Some(U8(self.to_int().unwrap() as u8)),
            U16T => Some(U16(self.to_int().unwrap() as u16)),
            U32T => Some(U32(self.to_int().unwrap() as u32)),
            U64T => Some(U64(self.to_int().unwrap())),
            F32T => Some(F32(self.to_float().unwrap() as f32)),
            F64T => Some(F64(self.to_float().unwrap())),
            _ => Some(self)
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
    pub fn get_types(&self) -> Vec<DataType> {
        let mut types = Vec::new();
        for value in &self.0 {
            types.push(value.get_type());
        }
        types
    }
    pub fn from_vec(data_vec: &[DataValue]) -> Self {
        Self(data_vec.to_vec())
    }
    pub fn to_vec(self) -> Vec<DataValue> {
        self.0
    }
    pub fn convert(self, types: &[DataType]) -> Option<Self> {
        let mut data_array = Vec::new();
        let mut it_value = self.0.iter();
        for ty in types {
            match it_value.next() {
                Some(value) => {
                    match value.clone().convert(ty.clone()) {
                        Some(v) => data_array.push(v),
                        None => return None
                    }
                },
                None => return None
            }
        }
        return Some(Self(data_array));
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
        assert_eq!(value, TryInto::<u8>::try_into(conf).unwrap());
        let value: i16 = -256;
        let conf = ConfigValue::from(value);
        assert_eq!(value, TryInto::<i16>::try_into(conf).unwrap());
        let value: u32 = 65536;
        let conf = ConfigValue::from(value);
        assert_eq!(value, TryInto::<u32>::try_into(conf).unwrap());
        let value: i64 = -4294967296;
        let conf = ConfigValue::from(value);
        assert_eq!(value, TryInto::<i64>::try_into(conf).unwrap());

        let value: f32 = 65536.65536;
        let conf = ConfigValue::from(value);
        assert_eq!(value, TryInto::<f32>::try_into(conf).unwrap());
        let value: f64 = 4294967296.4294967296;
        let conf = ConfigValue::from(value);
        assert_eq!(value, TryInto::<f64>::try_into(conf).unwrap());

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
        assert_eq!(value, TryInto::<i8>::try_into(data).unwrap());
        let value: i16 = -256;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<i16>::try_into(data).unwrap());
        let value: i32 = -65536;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<i32>::try_into(data).unwrap());
        let value: i64 = -4294967296;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<i64>::try_into(data).unwrap());

        let value: u8 = 1;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<u8>::try_into(data).unwrap());
        let value: u16 = 256;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<u16>::try_into(data).unwrap());
        let value: u32 = 65536;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<u32>::try_into(data).unwrap());
        let value: u64 = 4294967296;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<u64>::try_into(data).unwrap());

        let value: f32 = 65536.65536;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<f32>::try_into(data).unwrap());
        let value: f64 = 4294967296.4294967296;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<f64>::try_into(data).unwrap());

        let value: bool = true;
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<bool>::try_into(data).unwrap());
        let value: char = 'a';
        let data = DataValue::from(value);
        assert_eq!(value, TryInto::<char>::try_into(data).unwrap());
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
