use std::collections::HashMap;

use crate::index::ckv::CKVIndex;
use crate::utils;
use crate::utils::testing::{bytes_to_field_value, i32_to_field_value, string_to_field_value};

const PRIMARY_KEY_FIELD_NAME: &str = utils::testing::PRIMARY_KEY_FIELD_NAME;
const DOCFIELD1: &str = utils::testing::DOCFIELD1;
const DOCFIELD2: &str = utils::testing::DOCFIELD2;
const DOCFIELD3: &str = utils::testing::DOCFIELD3;

#[test]
pub fn test_lifecycle() {
    let mount_directory = "/tmp/ckv_test_test_lifecycle";
    let ikv_config = utils::testing::setup_index_cfg(&mount_directory);
    let _ = std::fs::remove_dir_all(&mount_directory);

    // empty
    assert!(CKVIndex::index_not_present(&ikv_config).unwrap());

    // invalid index
    assert!(CKVIndex::is_valid_index(&ikv_config).is_err());

    // create new and close
    CKVIndex::open_or_create(&ikv_config).unwrap();

    // re-open and close
    CKVIndex::open_or_create(&ikv_config).unwrap();

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
    let ikv_config = utils::testing::setup_index_cfg(&mount_directory);
    let _ = std::fs::remove_dir_all(&mount_directory);

    // generate documents and primary keys
    let doc0 = utils::testing::create_document(0);
    let pkey0 = doc0.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone();

    let doc1 = utils::testing::create_document(1);
    let pkey1 = doc1.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone();

    let doc2 = utils::testing::create_document(2);
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

    // close previous handle and reopen
    let index = CKVIndex::open_or_create(&ikv_config).unwrap();

    // read doc0
    assert_eq!(
        index
            .get_field_value(&pkey0, PRIMARY_KEY_FIELD_NAME)
            .unwrap(),
        doc0.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone()
    );

    // cleanup mount dir
    index.close().unwrap();
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn test_drop_fields() {
    let mount_directory: &str = "/tmp/ckv_test_test_drop_fields";
    let ikv_config = utils::testing::setup_index_cfg(&mount_directory);
    let _ = std::fs::remove_dir_all(&mount_directory);

    let doc0 = utils::testing::create_document(0);
    let pkey0 = doc0.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone();

    let doc1 = utils::testing::create_document(1);
    let pkey1 = doc1.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone();

    let doc2 = utils::testing::create_document(2);
    let pkey2 = doc2.get(PRIMARY_KEY_FIELD_NAME).unwrap().value.clone();

    let index = CKVIndex::open_or_create(&ikv_config).unwrap();

    // upsert and flush
    index.upsert_field_values(&doc0).unwrap();
    index.upsert_field_values(&doc1).unwrap();
    index.upsert_field_values(&doc2).unwrap();
    index.flush_writes().unwrap();

    // fields exist -
    assert_eq!(
        index.get_field_value(&pkey0, DOCFIELD1).unwrap(),
        doc0.get(DOCFIELD1).unwrap().value.clone()
    );
    assert_eq!(
        index.get_field_value(&pkey0, DOCFIELD2).unwrap(),
        doc0.get(DOCFIELD2).unwrap().value.clone()
    );

    // drop "embedding"
    index
        .drop_fields(&vec![DOCFIELD2.to_string()], &vec![])
        .unwrap();
    assert!(index.get_field_value(&pkey0, DOCFIELD2).is_none());
    assert!(index.get_field_value(&pkey1, DOCFIELD2).is_none());
    assert!(index.get_field_value(&pkey2, DOCFIELD2).is_none());

    // drop all
    index.drop_all_documents().unwrap();
    assert!(index
        .get_field_value(&pkey0, PRIMARY_KEY_FIELD_NAME)
        .is_none());
    assert!(index.get_field_value(&pkey0, DOCFIELD1).is_none());
    assert!(index.get_field_value(&pkey0, DOCFIELD2).is_none());
    assert!(index.get_field_value(&pkey0, DOCFIELD3).is_none());

    // upsert again and check
    index.upsert_field_values(&doc0).unwrap();
    assert_eq!(
        index.get_field_value(&pkey0, DOCFIELD1).unwrap(),
        doc0.get(DOCFIELD1).unwrap().value.clone()
    );

    index.close().unwrap();

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn test_projected_document_operations() {
    let mount_directory: &str = "/tmp/ckv_test_test_projected_document_operations";
    let ikv_config = utils::testing::setup_index_cfg(&mount_directory);
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
    index.close().unwrap();
    let _ = std::fs::remove_dir_all(&mount_directory);
}

// TODO: close and reopen for reads
