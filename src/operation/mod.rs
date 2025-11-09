pub(crate) mod model;
pub(crate) mod device;
pub(crate) mod types;
pub(crate) mod group;
pub(crate) mod set;
pub(crate) mod data;
pub(crate) mod buffer;
pub(crate) mod slice;
pub(crate) mod log;

const EMPTY_LENGTH_UNMATCH: &str = "One or more input array arguments are empty or doesn't have the same length";
const MODEL_NOT_EXISTS: &str = "Input model argument doesn't exist";
const DATA_TYPE_UNMATCH: &str = "The type of input data argument doesn't match with the model";
