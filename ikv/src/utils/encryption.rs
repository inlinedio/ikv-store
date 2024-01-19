use crate::proto::generated_proto::common::IKVStoreConfig;

// Base64 encoded string from 256-bit/32-byte AES256 key constructed from config.
pub fn sse_key_and_digest(config: &IKVStoreConfig) -> anyhow::Result<(String, String)> {
    let account_passkey = config.stringConfigs.get("account_passkey").ok_or(
        rdkafka::error::KafkaError::ClientCreation(
            "account_passkey is a required client-specified config".to_string(),
        ),
    )?;

    let mut account_passkey_utf8 = account_passkey.as_bytes().to_vec();
    while account_passkey_utf8.len() < 32 {
        account_passkey_utf8.append(&mut account_passkey_utf8.clone());
    }

    let key = &account_passkey_utf8[0..32];
    let key_md5_digest = md5::compute(&key);

    // https://www.reddit.com/r/programmingcirclejerk/comments/16zkmnl/base64s_rust_create_maintainer_bravely_defends/
    let base64_key = base64::encode(key);
    let base64_key_md5_digest = base64::encode(key_md5_digest.as_slice());

    Ok((base64_key, base64_key_md5_digest))
}
