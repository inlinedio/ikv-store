use std::collections::HashMap;

use crate::utils::{
    self,
    testing::{i32_to_field_value, string_to_field_value, DOCFIELD1, DOCFIELD3},
};

use super::CKVIndex;

#[test]
pub fn drop_all_documents() {
    let mount_directory: &str = "/tmp/compactions_test_drop_all_documents";
    let ikv_config = utils::testing::setup_index_cfg(&mount_directory);
    let _ = std::fs::remove_dir_all(&mount_directory);
    let index = CKVIndex::open_or_create(&ikv_config).unwrap();

    // insert 1000 docs
    for docid in 0..1000 {
        let doc = utils::testing::create_document(docid);
        index.upsert_field_values(&doc).unwrap();
    }
    index.close().unwrap();

    // re open and delete all documents
    let index = CKVIndex::open_or_create(&ikv_config).unwrap();
    for docid in 0..1000 {
        let doc = utils::testing::create_document(docid);
        index.delete_document(&doc).unwrap();
    }

    let (pre_stats, post_stats) = index.compact_and_close().unwrap();

    assert!(pre_stats.offset_table_size_bytes > 0);
    assert!(pre_stats.mmap_file_size_bytes > 0);

    assert_eq!(post_stats.offset_table_size_bytes, 0);
    assert_eq!(post_stats.mmap_file_size_bytes, 0);

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn live_drop_all_documents() {
    let mount_directory: &str = "/tmp/compactions_test_live_drop_all_documents";
    let ikv_config = utils::testing::setup_index_cfg(&mount_directory);
    let _ = std::fs::remove_dir_all(&mount_directory);
    let index = CKVIndex::open_or_create(&ikv_config).unwrap();

    // insert 100 docs
    for docid in 0..100 {
        let doc = utils::testing::create_document(docid);
        index.upsert_field_values(&doc).unwrap();
    }
    let (stats, _) = index.compact_and_close().unwrap();
    assert!(stats.offset_table_size_bytes > 0);
    assert!(stats.mmap_file_size_bytes > 0);

    // re open and delete all documents
    let index = CKVIndex::open_or_create(&ikv_config).unwrap();
    index.drop_all_documents().unwrap();

    let (stats, _) = index.compact_and_close().unwrap();
    assert_eq!(stats.offset_table_size_bytes, 0);
    assert_eq!(stats.mmap_file_size_bytes, 0);

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn change_fields() {
    let mount_directory: &str = "/tmp/compactions_test_change_fields";
    let ikv_config = utils::testing::setup_index_cfg(&mount_directory);
    let _ = std::fs::remove_dir_all(&mount_directory);
    let index = CKVIndex::open_or_create(&ikv_config).unwrap();

    // add 100 docs in the following format:
    // {"field0": pkey-as-str, "field1": str, "field2": bytes, "field3": int}
    // drop field1 and field3, add field4 - an int column
    // new format: {"field0": pkey-as-str, "field2": bytes, "field4": int}
    // compact

    for docid in 0..100 {
        let doc = utils::testing::create_document(docid);
        index.upsert_field_values(&doc).unwrap();
    }

    index
        .drop_fields(&vec![DOCFIELD1.to_string()], &vec![])
        .unwrap();

    for docid in 0..100 {
        let mut doc = HashMap::new();
        doc.insert(
            "field0".to_string(),
            string_to_field_value(&format!("field0:{}", docid)),
        );
        doc.insert("field4".to_string(), i32_to_field_value(docid as i32));
        index.upsert_field_values(&doc).unwrap();
    }

    index
        .drop_fields(&vec![DOCFIELD3.to_string()], &vec![])
        .unwrap();

    index.compact_and_close().unwrap();

    let index = CKVIndex::open_or_create(&ikv_config).unwrap();

    // perform reads on the remaining fields
    // new format: {"field0": pkey-as-str, "field2": bytes, "field4": int}
    for docid in 0..100 {
        let pkey = string_to_field_value(&format!("field0:{}", docid)).value;
        let expected_value = i32_to_field_value(docid as i32).value;
        assert!(index.get_field_value(&pkey, "field2").is_some());
        assert_eq!(
            index.get_field_value(&pkey, "field4").unwrap(),
            expected_value
        );
    }

    // cleanup mount dir
    index.close().unwrap();
    let _ = std::fs::remove_dir_all(&mount_directory);
}
