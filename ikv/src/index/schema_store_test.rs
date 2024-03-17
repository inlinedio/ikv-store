use std::collections::HashMap;

use crate::{
    index::schema_store::CKVIndexSchema, proto::generated_proto::common::FieldValue,
    utils::testing::string_to_field_value,
};

const PRIMARY_KEY_FIELD_NAME: &str = "userid";

fn create_document(user_id: u32) -> HashMap<String, FieldValue> {
    let mut document = HashMap::new();
    document.insert(
        PRIMARY_KEY_FIELD_NAME.to_string(),
        string_to_field_value(&format!("id:{}", user_id)),
    );
    document.insert(
        "name".to_string(),
        string_to_field_value(&format!("name:{}", user_id)),
    );
    document.insert(
        "location".to_string(),
        string_to_field_value(&format!("location:{}", user_id)),
    );
    document.insert(
        "gender".to_string(),
        string_to_field_value(&format!("gender:{}", user_id)),
    );
    document
}

#[test]
pub fn test_lifecycle() {
    // create mount dir
    let mount_directory = "/tmp/schema_store_test_test_lifecycle";
    let _ = std::fs::remove_dir_all(&mount_directory);
    std::fs::create_dir_all(&mount_directory).unwrap();

    // not present
    assert!(CKVIndexSchema::index_not_present(&mount_directory));
    // invalid
    assert!(CKVIndexSchema::is_valid_index(&mount_directory).is_err());

    // create new
    assert!(
        CKVIndexSchema::open_or_create(&mount_directory, PRIMARY_KEY_FIELD_NAME.to_string())
            .is_ok()
    );
    assert_eq!(CKVIndexSchema::index_not_present(&mount_directory), false);
    assert!(CKVIndexSchema::is_valid_index(&mount_directory).is_ok());
    assert!(
        CKVIndexSchema::open_or_create(&mount_directory, PRIMARY_KEY_FIELD_NAME.to_string())
            .is_ok()
    );

    // delete
    assert!(CKVIndexSchema::delete_all(&mount_directory).is_ok());

    // not present
    assert!(CKVIndexSchema::index_not_present(&mount_directory));

    // invalid
    assert!(CKVIndexSchema::is_valid_index(&mount_directory).is_err());

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn empty_store() {
    // create mount dir
    let mount_directory = "/tmp/schema_store_test_empty_store";
    let _ = std::fs::remove_dir_all(&mount_directory);
    std::fs::create_dir_all(&mount_directory).unwrap();

    let index =
        CKVIndexSchema::open_or_create(&mount_directory, PRIMARY_KEY_FIELD_NAME.to_string())
            .unwrap();
    assert_eq!(index.fetch_id_by_name(PRIMARY_KEY_FIELD_NAME).unwrap(), 0);

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn write_then_read() {
    // create mount dir
    let mount_directory = "/tmp/schema_store_test_write_then_read";
    let _ = std::fs::remove_dir_all(&mount_directory);
    std::fs::create_dir_all(&mount_directory).unwrap();

    let document0 = create_document(0);
    let document1 = create_document(1);
    let document2 = create_document(2);

    let mut index =
        CKVIndexSchema::open_or_create(&mount_directory, PRIMARY_KEY_FIELD_NAME.to_string())
            .unwrap();
    assert!(index.upsert_schema(&document0).is_ok());
    assert!(index.upsert_schema(&document1).is_ok());

    // fetch ids by name
    assert_eq!(
        index.extract_primary_key_value(&document2).unwrap(),
        &string_to_field_value(&"id:2".to_string())
    );
    assert_eq!(index.fetch_id_by_name(PRIMARY_KEY_FIELD_NAME).unwrap(), 0);
    assert!(index.fetch_id_by_name("name").unwrap() > 0);
    assert!(index.fetch_id_by_name("location").unwrap() > 0);
    assert!(index.fetch_id_by_name("gender").unwrap() > 0);
    assert!(index.fetch_id_by_name("unknown").is_none());

    index.close();

    // re open and read
    let index =
        CKVIndexSchema::open_or_create(&mount_directory, PRIMARY_KEY_FIELD_NAME.to_string())
            .unwrap();
    assert!(CKVIndexSchema::is_valid_index(&mount_directory).is_ok());

    assert_eq!(
        index.extract_primary_key_value(&document2).unwrap(),
        &string_to_field_value(&"id:2".to_string())
    );
    assert_eq!(index.fetch_id_by_name(PRIMARY_KEY_FIELD_NAME).unwrap(), 0);
    assert!(index.fetch_id_by_name("name").unwrap() > 0);
    assert!(index.fetch_id_by_name("location").unwrap() > 0);
    assert!(index.fetch_id_by_name("gender").unwrap() > 0);
    assert!(index.fetch_id_by_name("unknown").is_none());

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
fn drop_fields() {
    // create mount dir
    let mount_directory = "/tmp/schema_store_test_drop_fields";
    let _ = std::fs::remove_dir_all(&mount_directory);
    std::fs::create_dir_all(&mount_directory).unwrap();

    let document0 = create_document(0);
    let mut index =
        CKVIndexSchema::open_or_create(&mount_directory, PRIMARY_KEY_FIELD_NAME.to_string())
            .unwrap();
    assert!(index.upsert_schema(&document0).is_ok());

    // fields are indexed
    assert!(index.fetch_id_by_name(PRIMARY_KEY_FIELD_NAME).unwrap() == 0);
    assert!(index.fetch_id_by_name("name").unwrap() > 0);
    assert!(index.fetch_id_by_name("location").unwrap() > 0);
    assert!(index.fetch_id_by_name("gender").unwrap() > 0);
    let field_id_loc = index.fetch_id_by_name("location").unwrap();

    // drop fields (name, gender)
    index
        .drop_fields(
            &vec![
                "name".to_string(),
                "foo".to_string(),
                PRIMARY_KEY_FIELD_NAME.to_string(),
            ],
            &vec!["gen".to_string(), "LOC".to_string()],
        )
        .unwrap();
    assert!(index.fetch_id_by_name(PRIMARY_KEY_FIELD_NAME).unwrap() == 0);
    assert!(index.fetch_id_by_name("name").is_none());
    assert!(index.fetch_id_by_name("location").unwrap() == field_id_loc);
    assert!(index.fetch_id_by_name("gender").is_none());

    // drop all fields
    index.drop_all_fields().unwrap();

    assert!(index.fetch_id_by_name(PRIMARY_KEY_FIELD_NAME).unwrap() == 0);
    assert!(index.fetch_id_by_name("name").is_none());
    assert!(index.fetch_id_by_name("location").is_none());
    assert!(index.fetch_id_by_name("gender").is_none());

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}
