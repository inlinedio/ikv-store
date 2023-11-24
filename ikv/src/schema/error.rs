use core::fmt;

pub enum SchemaError {
    /// Usually thrown for a new field-type
    /// which is unknown or unsuported.
    UnsupportedField,

    /// Cannot support more than 2^16 fields.
    RangeExhausted,
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SchemaError::UnsupportedField => write!(
                f,
                "unsupported field-type, most likely you need to version bump ikv-client"
            ),
            SchemaError::RangeExhausted => write!(f, "cannot support more than 2^16 fields"),
        }
    }
}
