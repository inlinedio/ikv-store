use super::{field::Field, primary_key_index::PrimaryKeyIndex};
use std::{collections::HashMap, sync::RwLock};

const NUM_SEGMENTS: usize = 16;

pub struct ColumnarKVIndex {
    // hash(key) -> PrimaryKeyIndex
    segments: [RwLock<PrimaryKeyIndex>; NUM_SEGMENTS],

    // field-id -> Field
    fieldid_field_table: Vec<Field>,

    // field-name -> Field
    fieldname_field_table: HashMap<String, Field>,
}

impl ColumnarKVIndex {
    // create(..mount_directory.., schema)
    // open(..mount_directory..)
    // get_value(key, field_id) -> Option<val>
    // write_value(key, val)
}
