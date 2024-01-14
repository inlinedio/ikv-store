use std::collections::HashMap;

use crate::proto::generated_proto::common::IKVStoreConfig;

use super::Controller;

#[test]
fn test_fetch_server_configs() {
    let mut user_supplied_cfg = IKVStoreConfig::new();
    user_supplied_cfg.stringConfigs = HashMap::new();
    user_supplied_cfg
        .stringConfigs
        .insert("account_id".to_string(), "testing-account-v1".to_string());
    user_supplied_cfg.stringConfigs.insert(
        "account_passkey".to_string(),
        "testing-account-passkey".to_string(),
    );
    user_supplied_cfg
        .stringConfigs
        .insert("store_name".to_string(), "testing-store".to_string());

    let server_supplied_cfg = Controller::fetch_server_configs(&user_supplied_cfg).unwrap();

    println!("Server config: {}", server_supplied_cfg);
}
