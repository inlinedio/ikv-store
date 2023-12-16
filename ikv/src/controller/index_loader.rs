use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};

use aws_config::imds::Client;
use aws_sdk_s3::Client as S3Client;

use aws_sdk_s3::operation::{
    create_multipart_upload::CreateMultipartUploadOutput, get_object::GetObjectOutput,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use tar::Archive;

use crate::proto::generated_proto::common::IKVStoreConfig;

pub fn load_index(config: &IKVStoreConfig) -> anyhow::Result<()> {
    let mount_directory = crate::utils::paths::create_mount_directory(&config)?;

    // create mount directory if it does not exist
    std::fs::create_dir_all(&mount_directory)?;

    // check if index already exists
    let index_path = format!("{}/index", &mount_directory);
    if Path::new(&index_path).exists() {
        // index is already available
        return Ok(());
    }

    // download base index
    orchestrate_index_download(&mount_directory, config)
}

pub fn upload_index(config: &IKVStoreConfig) -> anyhow::Result<()> {
    let mount_directory = crate::utils::paths::create_mount_directory(&config)?;

    // check if index exists, error if not
    let index_path = format!("{}/index", &mount_directory);
    if !Path::new(&index_path).exists() {
        bail!(
            "Cannot upload empty index, nothing found at: {}",
            index_path
        );
    }

    // upload as base index
    orchestrate_index_upload(&mount_directory, config)
}

#[tokio::main]
async fn orchestrate_index_upload(
    mount_directory: &str,
    config: &IKVStoreConfig,
) -> anyhow::Result<()> {
    let tarball_index_filename = format!("{}/base_index.tar.gz", mount_directory);
    pack_tarball(mount_directory, &tarball_index_filename)?;

    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_PutObject_section.html
    // TODO: need to handle large file uploads!!
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_Scenario_UsingLargeFiles_section.html

    let aws_config = aws_config::load_from_env().await;
    let client = S3Client::new(&aws_config);

    // ikv-base-indexes-v1
    let bucket_name = config
        .stringConfigs
        .get("s3_bucket_name")
        .ok_or(anyhow!("s3_bucket_name is a required config"))?
        .to_string();

    let s3_key_prefix = config
        .stringConfigs
        .get("base_index_s3_bucket_prefix")
        .ok_or(anyhow!("base_index_s3_bucket_prefix is a required config"))?
        .to_string();

    let partition = config
        .numericConfigs
        .get("partition")
        .copied()
        .ok_or(anyhow!("partition is a required config"))?;

    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();

    // key: <account_id>/<storename>/<epoch>/<partition>
    let base_index_s3_key = format!("{}/{}/{}", &s3_key_prefix, &epoch, partition);

    // upload!
    // TODO: use encryption keys
    let body = ByteStream::from_path(Path::new(&tarball_index_filename)).await?;
    client
        .put_object()
        .bucket(bucket_name)
        .key(base_index_s3_key)
        .body(body)
        .send()
        .await?;

    // Remove tarball
    // throws if file does not exist or permission issue
    std::fs::remove_file(tarball_index_filename)?;
    Ok(())
}

#[tokio::main]
async fn orchestrate_index_download(
    mount_directory: &str,
    config: &IKVStoreConfig,
) -> anyhow::Result<()> {
    // References:
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_Scenario_UsingLargeFiles_section.html
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_ListObjects_section.html
    let aws_config = aws_config::load_from_env().await;
    let client = S3Client::new(&aws_config);

    // ikv-base-indexes-v1
    let bucket_name = config
        .stringConfigs
        .get("s3_bucket_name")
        .ok_or(anyhow!("s3_bucket_name is a required config"))?
        .to_string();

    // <account-id>/<store-name>
    let s3_key_prefix = config
        .stringConfigs
        .get("base_index_s3_bucket_prefix")
        .ok_or(anyhow!("base_index_s3_bucket_prefix is a required config"))?
        .to_string();

    let partition = config
        .numericConfigs
        .get("partition")
        .copied()
        .ok_or(anyhow!("partition is a required config"))?;

    // list objects based on prefix
    let mut response = client
        .list_objects_v2()
        .bucket(bucket_name.clone())
        .max_keys(3)
        .prefix(&s3_key_prefix)
        .into_paginator()
        .send();

    let mut base_index_key: Option<String> = None;
    let mut max_epoch = u64::MIN;

    while let Some(result) = response.next().await {
        let output = result?;
        for object in output.contents() {
            // key format: <account_id>/<storename>/<epoch>/<partition>
            if let Some(key) = object.key() {
                let parts = key.split("/").collect::<Vec<&str>>();

                let key_partition = parts.get(3).ok_or(anyhow!(
                    "malformed base index key: {}, expecting partition",
                    key
                ))?;
                let key_partition: i64 = key_partition.parse::<i64>()?;

                if partition != key_partition {
                    continue;
                }

                let key_epoch = parts.get(2).ok_or(anyhow!(
                    "malformed base index key: {}, expecting epoch",
                    key
                ))?;
                let key_epoch: u64 = key_epoch.parse::<u64>()?;
                if max_epoch < key_epoch {
                    max_epoch = key_epoch;
                    base_index_key = Some(key.to_string());
                }
            }
        }
    }

    if base_index_key.is_none() {
        // TODO! use logging
        println!(
            "Did not find base index, bucket: {} prefix: {}",
            &bucket_name, &s3_key_prefix
        );
        return Ok(());
    }

    let key = base_index_key.unwrap();
    println!("Found base index, bucket: {} key: {}", &bucket_name, &key);

    // download, unpack and delete
    let tarball_index_filename = format!("{}/base_index.tar.gz", mount_directory);
    download_from_s3(&client, &bucket_name, &key, &tarball_index_filename).await?;

    unpack_tarball(&tarball_index_filename, mount_directory)?;

    // after unpacking, the decompressed index is in <mount-dir>/index, move it to <mount-dir>
    std::fs::rename(format!("{}/index", mount_directory), mount_directory)?;

    std::fs::remove_file(tarball_index_filename)?; // throws if file does not exist or permission issue

    Ok(())
}

async fn download_from_s3(
    client: &S3Client,
    bucket: &str,
    key: &str,
    destination: &str,
) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(destination)?;

    // TODO! use decryption keys!!!
    // https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/operation/get_object/builders/struct.GetObjectFluentBuilder.html#method.sse_customer_key

    let mut writer = BufWriter::new(&file);
    let mut result = client.get_object().bucket(bucket).key(key).send().await?;
    while let Some(bytes) = result.body.try_next().await? {
        writer.write_all(&bytes)?;
    }

    Ok(())
}

fn unpack_tarball(input_filepath: &str, destination_dir: &str) -> anyhow::Result<()> {
    // Unzip mount_directory/<storename>/<partition>/base_index.tar.gz to mount_directory/<storename>/<partition>
    // Reference: https://rust-lang-nursery.github.io/rust-cookbook/compression/tar.html
    let file = OpenOptions::new().read(true).open(input_filepath)?;
    let tar = GzDecoder::new(file);
    let mut archive = Archive::new(tar);
    archive.unpack(destination_dir)?;

    Ok(())
}

fn pack_tarball(input_dir: &str, output_filepath: &str) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(output_filepath)?;
    let enc = GzEncoder::new(file, Compression::default());
    let mut tar = tar::Builder::new(enc);
    tar.append_dir_all("index", input_dir)?; // TODO! inspect???
    Ok(())
}
