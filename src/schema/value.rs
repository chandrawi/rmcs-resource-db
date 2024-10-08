use DataValue::{I8, I16, I32, I64, I128, U8, U16, U32, U64, U128, F32, F64, Bool, Char};
use DataType::{I8T, I16T, I32T, I64T, I128T, U8T, U16T, U32T, U64T, U128T, F32T, F64T, BoolT, CharT, StringT, BytesT};

#[derive(Debug, Default, Clone, PartialEq)]
pub enum DataType {
    #[default]
    NullT,
    I8T,
    I16T,
    I32T,
    I64T,
    I128T,
    U8T,
    U16T,
    U32T,
    U64T,
    U128T,
    F32T,
    F64T,
    BoolT,
    CharT,
    StringT,
    BytesT
}

impl From<u8> for DataType {
    fn from(value: u8) -> Self {
        match value {
            1 => I8T,
            2 => I16T,
            3 => I32T,
            4 => I64T,
            5 => I128T,
            6 => U8T,
            7 => U16T,
            8 => U32T,
            9 => U64T,
            10 => U128T,
            12 => F32T,
            13 => F64T,
            15 => BoolT,
            16 => CharT,
            17 => StringT,
            18 => BytesT,
            _ => Self::NullT
        }
    }
}

impl From<i16> for DataType {
    fn from(value: i16) -> Self {
        Self::from(value as u8)
    }
}

impl From<u32> for DataType {
    fn from(value: u32) -> Self {
        Self::from(value as u8)
    }
}

impl From<DataType> for u8 {
    fn from(value: DataType) -> Self {
        match value {
            I8T => 1,
            I16T => 2,
            I32T => 3,
            I64T => 4,
            I128T => 5,
            U8T => 6,
            U16T => 7,
            U32T => 8,
            U64T => 9,
            U128T => 10,
            F32T => 12,
            F64T => 13,
            BoolT => 15,
            CharT => 16,
            StringT => 17,
            BytesT => 18,
            DataType::NullT => 0
        }
    }
}

impl From<DataType> for i16 {
    fn from(value: DataType) -> Self {
        u8::from(value) as i16
    }
}

impl From<DataType> for u32 {
    fn from(value: DataType) -> Self {
        u8::from(value) as u32
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum DataValue {
    #[default]
    Null,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(f32),
    F64(f64),
    Bool(bool),
    Char(char),
    String(String),
    Bytes(Vec<u8>)
}

impl DataValue {
    pub fn from_bytes(bytes: &[u8], type_: DataType) -> Self {
        if bytes.len() == 0 {
            return Self::Null;
        }
        let first_el = bytes[0];
        let sel_val = |n: usize, v: DataValue| -> DataValue {
            if bytes.len() == n {
                v
            } else {
                Self::Null
            }
        };
        match type_ {
            I8T => sel_val(1, I8(i8::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            I16T => sel_val(2, I16(i16::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            I32T => sel_val(4, I32(i32::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            I64T => sel_val(8, I64(i64::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            I128T => sel_val(16, I128(i128::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            U8T => sel_val(1, U8(u8::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            U16T => sel_val(2, U16(u16::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            U32T => sel_val(4, U32(u32::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            U64T => sel_val(8, U64(u64::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            U128T => sel_val(16, U128(u128::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            F32T => sel_val(4, F32(f32::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            F64T => sel_val(8, F64(f64::from_be_bytes(bytes.try_into().unwrap_or_default()))),
            BoolT => sel_val(1, Bool(bool::from(first_el > 0))),
            CharT => sel_val(1, Char(char::from_u32(first_el as u32).unwrap_or_default())),
            StringT => match String::from_utf8(bytes.to_owned()).ok() {
                Some(value) => Self::String(value),
                None => Self::Null
            },
            BytesT => Self::Bytes(bytes.to_owned()),
            _ => Self::Null
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            I8(value) => value.to_be_bytes().to_vec(),
            I16(value) => value.to_be_bytes().to_vec(),
            I32(value) => value.to_be_bytes().to_vec(),
            I64(value) => value.to_be_bytes().to_vec(),
            I128(value) => value.to_be_bytes().to_vec(),
            U8(value) => value.to_be_bytes().to_vec(),
            U16(value) => value.to_be_bytes().to_vec(),
            U32(value) => value.to_be_bytes().to_vec(),
            U64(value) => value.to_be_bytes().to_vec(),
            U128(value) => value.to_be_bytes().to_vec(),
            F32(value) => value.to_be_bytes().to_vec(),
            F64(value) => value.to_be_bytes().to_vec(),
            Bool(value) => Vec::from([*value as u8]),
            Char(value) => Vec::from([*value as u8]),
            Self::String(value) => value.to_owned().as_bytes().to_vec(),
            Self::Bytes(value) => value.to_owned(),
            _ => Vec::new()
        }
    }
    pub fn get_type(&self) -> DataType {
        match self {
            I8(_) => I8T,
            I16(_) => I16T,
            I32(_) => I32T,
            I64(_) => I64T,
            I128(_) => I128T,
            U8(_) => U8T,
            U16(_) => U16T,
            U32(_) => U32T,
            U64(_) => U64T,
            U128(_) => U128T,
            F32(_) => F32T,
            F64(_) => F64T,
            Char(_) => CharT,
            Bool(_) => BoolT,
            Self::String(_) => StringT,
            Self::Bytes(_) => BytesT,
            Self::Null => DataType::NullT
        }
    }
    fn to_int(&self) -> Option<u64> {
        match *self {
            I8(value) => Some(value as u64),
            I16(value) => Some(value as u64),
            I32(value) => Some(value as u64),
            I64(value) => Some(value as u64),
            I128(value) => Some(value as u64),
            U8(value) => Some(value as u64),
            U16(value) => Some(value as u64),
            U32(value) => Some(value as u64),
            U64(value) => Some(value as u64),
            U128(value) => Some(value as u64),
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
                I8T | I16T | I32T | I64T | I128T | U8T | U16T | U32T | U64T | U128T => 1,
                F32T | F64T => 2,
                BoolT => 3,
                CharT => 4,
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
            I128T => Some(I128(self.to_int().unwrap() as i128)),
            U8T => Some(U8(self.to_int().unwrap() as u8)),
            U16T => Some(U16(self.to_int().unwrap() as u16)),
            U32T => Some(U32(self.to_int().unwrap() as u32)),
            U64T => Some(U64(self.to_int().unwrap())),
            U128T => Some(U128(self.to_int().unwrap() as u128)),
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
                I128T | U128T => 16,
                StringT | BytesT => {
                    let length = bytes.get(index).unwrap_or(&0).to_owned(); // first element is the length
                    index += 1;  // skip first element
                    length as usize
                },
                _ => 0
            };
            if index + len > bytes.len() {
                break;
            }
            values.push(DataValue::from_bytes(&bytes[index..index + len], t.clone()));
            index += len;
        }
        ArrayDataValue(values)
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in &self.0 {
            let mut bytes_value = value.to_bytes();
            match value {
                DataValue::String(_) | DataValue::Bytes(_) => {
                    bytes.push(bytes_value.len() as u8) // insert length at first element
                },
                _ => {}
            }
            bytes.append(&mut bytes_value);
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
    ($from:ty, $value:ty, $variant:path) => {
        impl From<$from> for $value {
            fn from(value: $from) -> Self {
                $variant(value.into())
            }
        }
    };
}

value_impl_from!(i8, DataValue, I8);
value_impl_from!(i16, DataValue, I16);
value_impl_from!(i32, DataValue, I32);
value_impl_from!(i64, DataValue, I64);
value_impl_from!(i128, DataValue, I128);
value_impl_from!(u8, DataValue, U8);
value_impl_from!(u16, DataValue, U16);
value_impl_from!(u32, DataValue, U32);
value_impl_from!(u64, DataValue, U64);
value_impl_from!(u128, DataValue, U128);
value_impl_from!(f32, DataValue, F32);
value_impl_from!(f64, DataValue, F64);
value_impl_from!(bool, DataValue, Bool);
value_impl_from!(char, DataValue, Char);
value_impl_from!(String, DataValue, DataValue::String);
value_impl_from!(Vec<u8>, DataValue, DataValue::Bytes);

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

value_impl_try_into!(i8, DataValue, I8);
value_impl_try_into!(i16, DataValue, I16);
value_impl_try_into!(i32, DataValue, I32);
value_impl_try_into!(i64, DataValue, I64);
value_impl_try_into!(i128, DataValue, I128);
value_impl_try_into!(u8, DataValue, U8);
value_impl_try_into!(u16, DataValue, U16);
value_impl_try_into!(u32, DataValue, U32);
value_impl_try_into!(u64, DataValue, U64);
value_impl_try_into!(u128, DataValue, U128);
value_impl_try_into!(f32, DataValue, F32);
value_impl_try_into!(f64, DataValue, F64);
value_impl_try_into!(bool, DataValue, Bool);
value_impl_try_into!(char, DataValue, Char);
value_impl_try_into!(String, DataValue, DataValue::String);
value_impl_try_into!(Vec<u8>, DataValue, DataValue::Bytes);

#[cfg(test)]
mod tests {
    use super::*;

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

        let value: String = "xyz".to_owned();
        let data = DataValue::from(value.clone());
        assert_eq!(value, TryInto::<String>::try_into(data).unwrap());
        let value: Vec<u8> = vec![101, 102, 103, 104, 105];
        let data = DataValue::from(value.clone());
        assert_eq!(value, TryInto::<Vec<u8>>::try_into(data).unwrap());
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

        let bytes = [97, 98, 99];
        let value = DataValue::from_bytes(&bytes, StringT);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, DataValue::String("abc".to_owned()));
        let bytes = [10, 20, 30, 40];
        let value = DataValue::from_bytes(&bytes, BytesT);
        assert_eq!(bytes.to_vec(), value.to_bytes());
        assert_eq!(value, DataValue::Bytes(vec![10, 20, 30, 40]));

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
        assert_eq!(bytes.to_vec(), data.to_bytes());

        let bytes = [62, 32, 0, 0, 63, 136, 0, 0, 0, 0, 0, 0];
        let types = [F32T, F64T];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(data.0, [
            F32(0.15625),
            F64(0.01171875)
        ]);
        assert_eq!(bytes.to_vec(), data.to_bytes());

        let bytes = [97, 1, 3, 97, 98, 99, 4, 10, 20, 30, 40];
        let types = [CharT, BoolT, StringT, BytesT];
        let data = ArrayDataValue::from_bytes(&bytes, &types);
        assert_eq!(data.0, [
            Char('a'),
            Bool(true),
            DataValue::String("abc".to_owned()),
            DataValue::Bytes(vec![10, 20, 30, 40])
        ]);
        assert_eq!(bytes.to_vec(), data.to_bytes());
    }

}
