use rdkafka::TopicPartitionList;

use crate::index::offset_store::OffsetStore;

#[test]
pub fn test_lifecycle() {
    // create mount dir
    let mount_directory = "/tmp/offset_store_test_test_lifecycle";
    let _ = std::fs::remove_dir_all(&mount_directory);
    std::fs::create_dir_all(&mount_directory).unwrap();

    // not valid
    assert!(OffsetStore::is_valid_index(&mount_directory).is_err());

    // create new, then re-open
    assert!(OffsetStore::open_or_create(mount_directory.to_string()).is_ok());
    assert!(OffsetStore::open_or_create(mount_directory.to_string()).is_ok());

    // is valid now
    assert!(OffsetStore::is_valid_index(&mount_directory).is_ok());

    // delete all
    assert!(OffsetStore::delete_all(&mount_directory).is_ok());
    assert!(OffsetStore::is_valid_index(&mount_directory).is_err());

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn empty_store() {
    // create mount dir
    let mount_directory = "/tmp/offset_store_test_empty_store";
    let _ = std::fs::remove_dir_all(&mount_directory);
    std::fs::create_dir_all(&mount_directory).unwrap();

    let offset_store = OffsetStore::open_or_create(mount_directory.to_string()).unwrap();
    assert_eq!(offset_store.read_all_offsets().unwrap().len(), 0);

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}

#[test]
pub fn write_then_read() {
    // create mount dir
    let mount_directory = "/tmp/offset_store_test_write_then_read";
    let _ = std::fs::remove_dir_all(&mount_directory);
    std::fs::create_dir_all(&mount_directory).unwrap();

    let mut list = TopicPartitionList::new();
    list.add_partition_offset("topic_a", 0, rdkafka::Offset::Offset(100))
        .unwrap();
    list.add_partition_offset("topic_b", 1, rdkafka::Offset::Offset(200))
        .unwrap();
    list.add_partition_offset("topic_c", 2, rdkafka::Offset::Offset(300))
        .unwrap();

    let offset_store = OffsetStore::open_or_create(mount_directory.to_string()).unwrap();

    // write multiple times, intermediate writes have no affect
    assert!(offset_store.write_all_offsets(&list).is_ok());
    assert!(offset_store
        .write_all_offsets(&TopicPartitionList::new())
        .is_ok());
    assert!(offset_store.write_all_offsets(&list).is_ok());

    // is valid
    assert!(OffsetStore::is_valid_index(&mount_directory).is_ok());

    // read
    let mut list = offset_store.read_all_offsets().unwrap();
    assert_eq!(list.len(), 3);

    list.sort_by(|e1, e2| e1.topic.cmp(&e2.topic));

    assert_eq!(list[0].topic, "topic_a");
    assert_eq!(list[0].partition, 0);
    assert_eq!(list[0].offset, 100);

    assert_eq!(list[1].topic, "topic_b");
    assert_eq!(list[1].partition, 1);
    assert_eq!(list[1].offset, 200);

    assert_eq!(list[2].topic, "topic_c");
    assert_eq!(list[2].partition, 2);
    assert_eq!(list[2].offset, 300);

    // cleanup mount dir
    let _ = std::fs::remove_dir_all(&mount_directory);
}
