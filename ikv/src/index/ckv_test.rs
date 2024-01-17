use std::collections::HashMap;

use crate::utils::testing::{bytes_to_field_value, i32_to_field_value, string_to_field_value};
use crate::{
    index::ckv::CKVIndex,
    proto::generated_proto::common::{FieldValue, IKVStoreConfig},
};

const PRIMARY_KEY_FIELD_NAME: &str = "userid";
const DOCFIELD1: &str = "name";
const DOCFIELD2: &str = "embedding";
const DOCFIELD3: &str = "age";

fn create_document(user_id: u32) -> HashMap<String, FieldValue> {
    let mut document = HashMap::new();

    // string userid
    document.insert(
        PRIMARY_KEY_FIELD_NAME.to_string(),
        string_to_field_value(&format!("id:{}", user_id)),
    );

    // string name
    document.insert(
        DOCFIELD1.to_string(),
        string_to_field_value(&format!("name:{}", user_id)),
    );

    // bytes embedding
    document.insert(
        DOCFIELD2.to_string(),
        bytes_to_field_value(&format!("embedding:{}", user_id).as_bytes()),
    );

    // int age
    document.insert(DOCFIELD3.to_string(), i32_to_field_value(user_id as i32));

    document
}

fn setup_cfg(mount_dir: &str) -> IKVStoreConfig {
    let mut ikv_config = IKVStoreConfig::new();
    ikv_config.stringConfigs = HashMap::new();
    ikv_config
        .stringConfigs
        .insert("mount_directory".to_string(), mount_dir.to_string());
    ikv_config
        .stringConfigs
        .insert("store_name".to_string(), "ckv_test_store".to_string());
    ikv_config
        .stringConfigs
        .insert("primary_key_field_name".to_string(), "userid".to_string());

    ikv_config.intConfigs = HashMap::new();
    ikv_config.intConfigs.insert("partition".to_string(), 0);

    ikv_config
}

#[test]
pub fn test_lifecycle() {
    let mount_directory = "/tmp/ckv_test_test_lifecycle";
    let ikv_config = setup_cfg(&mount_directory);
    let _ = std::fs::remove_dir_all(&mount_directory);

    // empty
    assert!(CKVIndex::index_not_present(&ikv_config).unwrap());

    // invalid index
    assert!(CKVIndex::is_valid_index(&ikv_config).is_err());

    // create new and close
    let index = CKVIndex::open_or_create(&ikv_config).unwrap();
    assert!(index.close().is_ok());

    // re-open and close
    let index = CKVIndex::open_or_create(&ikv_config).unwrap();
    assert!(index.close().is_ok());

    // valid and not empty
    assert_eq!(CKVIndex::index_not_present(&ikv_config).unwrap(), false);
    assert!(CKVIndex::is_valid_index(&ikv_config).is_ok());

    // delete
    assert!(CKVIndex::delete_all(&ikv_config).is_ok());
    assert!(CKVIndex::is_valid_index(&ikv_config).is_err());

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn test_full_document_operations() {
    let mount_directory: &str = "/tmp/ckv_test_test_full_document_operations";
    let ikv_config = setup_cfg(&mount_directory);
    let _ = std::fs::remove_dir_all(&mount_directory);

    // generate documents and primary keys
    let doc0 = create_document(0);
    let pkey0 = doc0.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone();

    let doc1 = create_document(1);
    let pkey1 = doc1.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone();

    let doc2 = create_document(2);
    let pkey2 = doc2.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone();

    let index = CKVIndex::open_or_create(&ikv_config).unwrap();

    // upsert
    assert!(index.upsert_field_values(&doc0).is_ok());

    assert_eq!(
        index
            .get_field_value(&pkey0, PRIMARY_KEY_FIELD_NAME)
            .unwrap(),
        doc0.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone()
    );

    // more upserts
    assert!(index.upsert_field_values(&doc1).is_ok());
    assert!(index.upsert_field_values(&doc2).is_ok());
    assert!(index.flush_writes().is_ok());

    assert_eq!(
        index.get_field_value(&pkey1, DOCFIELD1).unwrap(),
        doc1.get(DOCFIELD1).unwrap().value.clone()
    );

    assert_eq!(
        index.get_field_value(&pkey2, DOCFIELD2).unwrap(),
        doc2.get(DOCFIELD2).unwrap().value.clone()
    );

    assert_eq!(
        index.get_field_value(&pkey2, DOCFIELD3).unwrap(),
        doc2.get(DOCFIELD3).unwrap().value.clone()
    );

    // read unknown field
    assert!(index.get_field_value(&pkey0, "foo").is_none());
    // read unknown pkey
    assert!(index.get_field_value(b"foo", DOCFIELD1).is_none());

    // delete doc1
    assert!(index.delete_document(&doc1).is_ok());
    assert!(index
        .get_field_value(&pkey1, PRIMARY_KEY_FIELD_NAME)
        .is_none()); // doc1 access returns empty
    assert!(index.get_field_value(&pkey1, DOCFIELD1).is_none()); // doc1 access returns empty
    assert_eq!(
        index
            .get_field_value(&pkey0, PRIMARY_KEY_FIELD_NAME)
            .unwrap(),
        doc0.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone()
    ); // doc0 access is ok

    // delete pkey and DOCFIELD1 from doc2
    assert!(index
        .delete_field_values(
            &doc2,
            &[PRIMARY_KEY_FIELD_NAME.to_string(), DOCFIELD1.to_string()]
        )
        .is_ok());
    assert!(index
        .get_field_value(&pkey2, PRIMARY_KEY_FIELD_NAME)
        .is_none()); // doc2#PRIMARY_KEY_FIELD_NAME access returns empty
    assert!(index.get_field_value(&pkey2, DOCFIELD1).is_none()); // doc2#DOCFIELD1 access returns empty
    assert_eq!(
        index.get_field_value(&pkey2, DOCFIELD2).unwrap(),
        doc2.get(DOCFIELD2).unwrap().value.clone()
    );
    assert_eq!(
        index.get_field_value(&pkey2, DOCFIELD3).unwrap(),
        doc2.get(DOCFIELD3).unwrap().value.clone()
    );

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn test_projected_document_operations() {
    let mount_directory: &str = "/tmp/ckv_test_test_projected_document_operations";
    let ikv_config = setup_cfg(&mount_directory);
    let _ = std::fs::remove_dir_all(&mount_directory);

    // create doc with userid and name
    let mut document = HashMap::new();
    document.insert(
        PRIMARY_KEY_FIELD_NAME.to_string(),
        string_to_field_value("id:0"),
    );
    document.insert(DOCFIELD1.to_string(), string_to_field_value("name:0"));
    let primary_key = document.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone();

    // open index
    let index = CKVIndex::open_or_create(&ikv_config).unwrap();

    // upsert
    assert!(index.upsert_field_values(&document).is_ok());

    assert_eq!(
        index
            .get_field_value(&primary_key, PRIMARY_KEY_FIELD_NAME)
            .unwrap(),
        document.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone()
    );

    assert_eq!(
        index.get_field_value(&primary_key, DOCFIELD1).unwrap(),
        document.get(DOCFIELD1).unwrap().value.clone()
    );

    assert!(index.get_field_value(&primary_key, DOCFIELD2).is_none());

    // add more fields to document
    document.insert(DOCFIELD2.to_string(), bytes_to_field_value(b"embedding:0"));
    document.insert(DOCFIELD3.to_string(), i32_to_field_value(0));
    assert!(index.upsert_field_values(&document).is_ok());

    assert_eq!(
        index.get_field_value(&primary_key, DOCFIELD2).unwrap(),
        document.get(DOCFIELD2).unwrap().value.clone()
    );

    assert_eq!(
        index.get_field_value(&primary_key, DOCFIELD3).unwrap(),
        document.get(DOCFIELD3).unwrap().value.clone()
    );

    assert_eq!(
        index.get_field_value(&primary_key, DOCFIELD1).unwrap(),
        document.get(DOCFIELD1).unwrap().value.clone()
    );

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

// TODO: close and reopen for reads
