use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;

use aws_sdk_s3::primitives::ByteStream;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use log::info;
use tar::Archive;

use crate::index::ckv::CKVIndex;
use crate::proto::generated_proto::common::IKVStoreConfig;
use crate::utils;

const REFRESH_BASE_INDEX_AGE_MILLIS: u128 = 7 * 24 * 60 * 60 * 1000; // 7 days

#[tokio::main(flavor = "current_thread")]
pub async fn load_index(config: &IKVStoreConfig) -> anyhow::Result<()> {
    let working_mount_directory = crate::utils::paths::get_working_mount_directory_fqn(config)?;
    let index_mount_directory = crate::utils::paths::get_index_mount_directory_fqn(config)?;

    // create paths if not exists
    std::fs::create_dir_all(&working_mount_directory)?;
    std::fs::create_dir_all(&index_mount_directory)?;

    if base_index_download_required(config).await? {
        info!("Removing existing base index on disk.");
        CKVIndex::delete_all(config)?;

        info!("Starting base index download from S3 repository.");
        orchestrate_index_download(&working_mount_directory, &index_mount_directory, config)
            .await?;
    }

    Ok(())
}

/// New index downloaded when:
/// 1) No index is present on disk (ex. bootstrapping new hardware)
/// 2) Index is corrupt/invalida
/// 3) Base index age is old and we should refresh it.
async fn base_index_download_required(config: &IKVStoreConfig) -> anyhow::Result<bool> {
    if CKVIndex::index_not_present(config)? {
        info!("No base index present in mount directory, needs download.");
        return Ok(true);
    }

    if let Err(e) = CKVIndex::is_valid_index(config) {
        info!(
            "Base index found in inconsistent state: {}, needs download.",
            e
        );
        return Ok(true);
    }

    // Check for download based on age.
    let base_index_epoch_millis = local_base_index_epoch_millis(config)?.unwrap_or(0); // 0 if missing
    let curr_time_milis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    if (curr_time_milis - base_index_epoch_millis) >= REFRESH_BASE_INDEX_AGE_MILLIS {
        // Eligible for refresh, check if newer index is available
        if let Some((remote_index_key, remote_index_age_epoch_millis)) =
            find_latest_base_index(config).await?
        {
            if (curr_time_milis - remote_index_age_epoch_millis) < REFRESH_BASE_INDEX_AGE_MILLIS {
                info!("Current base index is old with age: {}, found more recent index on S3: {} with age: {}, needs download.", base_index_epoch_millis, remote_index_key, remote_index_age_epoch_millis);
                return Ok(true);
            }
        }
        info!(
            "Current base index is old with age: {}, but no eligible index on S3.",
            base_index_epoch_millis
        );
    }

    return Ok(false);
}

fn local_base_index_epoch_millis(config: &IKVStoreConfig) -> anyhow::Result<Option<u128>> {
    let ckv_index = CKVIndex::open_or_create(config)?;
    let header = ckv_index.read_index_header()?;
    if header.base_index_epoch_millis == 0 {
        return Ok(None);
    }

    Ok(Some(header.base_index_epoch_millis as u128))
}

#[tokio::main(flavor = "current_thread")]
pub async fn upload_index(config: &IKVStoreConfig) -> anyhow::Result<()> {
    let working_mount_directory = crate::utils::paths::get_working_mount_directory_fqn(config)?;
    let index_mount_directory = crate::utils::paths::get_index_mount_directory_fqn(config)?;
    // create paths if not exists
    std::fs::create_dir_all(&working_mount_directory)?;
    std::fs::create_dir_all(&index_mount_directory)?;

    // check if index exists, error if not
    if let Err(e) = CKVIndex::is_valid_index(config) {
        bail!("Cannot upload bad index, error: {}", e);
    }

    // upload as base index
    orchestrate_index_upload(&working_mount_directory, &index_mount_directory, config).await
}

async fn orchestrate_index_upload(
    working_mount_directory: &str,
    index_mount_directory: &str,
    config: &IKVStoreConfig,
) -> anyhow::Result<()> {
    let tarball_index_filename = format!("{}/base_index.tar.gz", working_mount_directory);

    // clear any old base index tar archives
    if Path::new(&tarball_index_filename).exists() {
        std::fs::remove_file(&tarball_index_filename)?;
    }

    pack_tarball(index_mount_directory, &tarball_index_filename)?;

    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_PutObject_section.html
    // TODO: need to handle large file uploads!!
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_Scenario_UsingLargeFiles_section.html

    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region("eu-north-1")
        .load()
        .await;
    let client = S3Client::new(&aws_config);

    // ikv-base-indexes-v1
    let bucket_name = config
        .stringConfigs
        .get("base_index_s3_bucket_name")
        .ok_or(anyhow!(
            "base_index_s3_bucket_name is a required gateway-specified config"
        ))?
        .to_string();

    let account_id = config
        .stringConfigs
        .get("account_id")
        .ok_or(anyhow!("account_id is a required client-specified config"))?
        .to_string();

    let store_name = config
        .stringConfigs
        .get("store_name")
        .ok_or(anyhow!("store_name is a required client-specified config"))?
        .to_string();

    let partition = config
        .intConfigs
        .get("partition")
        .copied()
        .ok_or(anyhow!("partition is a required client-specified config"))?;

    let epoch = local_base_index_epoch_millis(config)?
        .ok_or(anyhow!("base_index_epoch_millis missing from index header"))?;

    // key: <account_id>/<storename>/<partition>/<epoch>
    let base_index_s3_key = format!("{}/{}/{}/{}", &account_id, &store_name, partition, &epoch);

    // upload!
    let sse_key_objects = utils::encryption::sse_key_and_digest(config)?;
    let body = ByteStream::from_path(Path::new(&tarball_index_filename)).await?;

    client
        .put_object()
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html
        .sse_customer_algorithm(aws_sdk_s3::types::ServerSideEncryption::Aes256.as_str())
        .sse_customer_key(sse_key_objects.0)
        .sse_customer_key_md5(sse_key_objects.1)
        .bucket(bucket_name)
        .key(base_index_s3_key)
        .body(body)
        .send()
        .await?;

    // Remove tarball
    std::fs::remove_file(tarball_index_filename)?;
    Ok(())
}

/// Latest index present in S3.
/// If present, returns the full S3-key and base-index epoch.
async fn find_latest_base_index(config: &IKVStoreConfig) -> anyhow::Result<Option<(String, u128)>> {
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region("eu-north-1")
        .load()
        .await;
    let client = S3Client::new(&aws_config);

    let bucket_name = config
        .stringConfigs
        .get("base_index_s3_bucket_name")
        .ok_or(anyhow!(
            "base_index_s3_bucket_name is a required gateway-specified config"
        ))?
        .to_string();

    let account_id = config
        .stringConfigs
        .get("account_id")
        .ok_or(anyhow!("account_id is a required client-specified config"))?
        .to_string();

    let store_name = config
        .stringConfigs
        .get("store_name")
        .ok_or(anyhow!("store_name is a required client-specified config"))?
        .to_string();

    let partition = config
        .intConfigs
        .get("partition")
        .copied()
        .ok_or(anyhow!("partition is a required client-specified config"))?;

    // <account-id>/<store-name>/<partition>
    let s3_key_prefix = format!("{}/{}/{}", account_id, store_name, partition);

    // list objects based on prefix
    let mut response = client
        .list_objects_v2()
        .bucket(bucket_name.clone())
        .max_keys(3)
        .prefix(&s3_key_prefix)
        .into_paginator()
        .send();

    let mut maybe_base_index_key = None;
    let mut max_epoch = u128::MIN;

    while let Some(result) = response.next().await {
        for object in result?.contents() {
            // key format: <account_id>/<storename>/<partition>/<epoch>
            if let Some(key) = object.key() {
                let key_parts = key.split('/').collect::<Vec<&str>>();

                let epoch = key_parts.get(3).ok_or(anyhow!(
                    "malformed base index key: {}, expecting epoch",
                    key
                ))?;

                let epoch: u128 = epoch.parse::<u128>()?;
                if maybe_base_index_key.is_none() || max_epoch < epoch {
                    max_epoch = epoch;
                    maybe_base_index_key = Some(key.to_string());
                }
            }
        }
    }

    if maybe_base_index_key.is_none() {
        info!("No base index found in S3, key-prefix: {}", &s3_key_prefix);
        return Ok(None);
    }

    Ok(Some((maybe_base_index_key.unwrap(), max_epoch)))
}

async fn orchestrate_index_download(
    working_mount_directory: &str,
    index_mount_directory: &str,
    config: &IKVStoreConfig,
) -> anyhow::Result<()> {
    // Find latest remote base index.
    let maybe_base_index = find_latest_base_index(config).await?;
    if maybe_base_index.is_none() {
        return Ok(());
    }

    let key = maybe_base_index.unwrap().0;
    info!("Found base index, base-index-key: {}", &key);

    // References:
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_Scenario_UsingLargeFiles_section.html
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_ListObjects_section.html

    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region("eu-north-1")
        .load()
        .await;
    let client = S3Client::new(&aws_config);

    let bucket_name = config
        .stringConfigs
        .get("base_index_s3_bucket_name")
        .ok_or(anyhow!(
            "base_index_s3_bucket_name is a required gateway-specified config"
        ))?
        .to_string();

    // download, unpack and delete tarred file
    let tarball_index_filename = format!("{}/base_index.tar.gz", working_mount_directory);

    // clear any old base index tar archives
    if Path::new(&tarball_index_filename).exists() {
        std::fs::remove_file(&tarball_index_filename)?;
    }

    download_from_s3(&client, config, &bucket_name, &key, &tarball_index_filename).await?;
    unpack_tarball(&tarball_index_filename, working_mount_directory)?;

    // after unpacking, the decompressed index is in <mount-dir>/base_index, move it to <mount-dir>
    std::fs::rename(
        format!("{}/base_index", working_mount_directory),
        index_mount_directory,
    )?;

    std::fs::remove_file(tarball_index_filename)?;

    Ok(())
}

async fn download_from_s3(
    client: &S3Client,
    config: &IKVStoreConfig,
    bucket: &str,
    key: &str,
    destination: &str,
) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(destination)?;

    let sse_key_objects = utils::encryption::sse_key_and_digest(config)?;
    let mut writer = BufWriter::new(&file);
    let mut result = client
        .get_object()
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html
        .sse_customer_algorithm(aws_sdk_s3::types::ServerSideEncryption::Aes256.as_str())
        .sse_customer_key(sse_key_objects.0)
        .sse_customer_key_md5(sse_key_objects.1)
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    while let Some(bytes) = result.body.try_next().await? {
        writer.write_all(&bytes)?;
    }

    Ok(())
}

fn unpack_tarball(input_filepath: &str, destination_dir: &str) -> anyhow::Result<()> {
    // Unzip working_mount_dir/base_index.tar.gz to working_mount_dir/base_index
    // Reference: https://rust-lang-nursery.github.io/rust-cookbook/compression/tar.html
    let file = OpenOptions::new().read(true).open(input_filepath)?;
    let tar = GzDecoder::new(file);
    let mut archive = Archive::new(tar);
    archive.unpack(destination_dir)?;

    Ok(())
}

// input_dir: directory to tarball (ex. the index directory)
// output_filepath: of the the tarball file
fn pack_tarball(input_dir: &str, output_filepath: &str) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(output_filepath)?;
    let enc = GzEncoder::new(file, Compression::default());
    let mut tar = tar::Builder::new(enc);
    tar.append_dir_all("base_index", input_dir)?;
    Ok(())
}
