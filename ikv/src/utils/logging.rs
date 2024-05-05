use std::sync::Mutex;

use crate::proto::generated_proto::common::IKVStoreConfig;

use anyhow::{anyhow, bail};
use log::LevelFilter;
use log4rs::{
    append::{console::ConsoleAppender, file::FileAppender},
    config::{Appender, Root},
    encode::pattern::PatternEncoder,
    Config,
};

static LOG_HANDLE: Mutex<Option<log4rs::Handle>> = Mutex::new(None);

pub fn configure_logging(config: &IKVStoreConfig) -> anyhow::Result<()> {
    let pattern_encoder = Box::new(PatternEncoder::new(
        "{d(%Y-%m-%d %H:%M:%S %Z)(utc)} {l} {t} {m}{n}",
    ));

    let appender;
    if use_console(config) {
        let stdout = ConsoleAppender::builder().encoder(pattern_encoder).build();
        appender = Appender::builder().build("default_appender", Box::new(stdout));
    } else {
        // TODO: (testing): Check if logs are appended and original contents are not truncated
        let filepath = use_file(config).ok_or(anyhow!("log output must be configured, either use rust_client_log_to_console or rust_client_log_file"))?;
        let file_appender = FileAppender::builder()
            .encoder(pattern_encoder)
            .build(filepath)?;
        appender = Appender::builder().build("default_appender", Box::new(file_appender));
    }

    let level_filter = level(config)?;
    let config = Config::builder().appender(appender).build(
        Root::builder()
            .appender("default_appender")
            .build(level_filter),
    )?;

    let mut log_handle = LOG_HANDLE.lock().unwrap();
    if log_handle.is_none() {
        let lh = log4rs::init_config(config)?;
        *log_handle = Some(lh);
    } else {
        log_handle.as_ref().unwrap().set_config(config);
    }

    Ok(())
}

fn level(config: &IKVStoreConfig) -> anyhow::Result<LevelFilter> {
    let string_level = config
        .stringConfigs
        .get("rust_client_log_level")
        .ok_or(anyhow!("rust_client_log_level is a required config"))?
        .to_lowercase();

    let level_filter = match string_level.as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        other => bail!(
            "Invalid logging level: {}. Allowed: error|warn|info|debug|trace",
            other
        ),
    };

    Ok(level_filter)
}

fn use_console(config: &IKVStoreConfig) -> bool {
    let yes = config
        .booleanConfigs
        .get("rust_client_log_to_console")
        .copied();
    yes.is_some() && yes.unwrap()
}

fn use_file(config: &IKVStoreConfig) -> Option<String> {
    config.stringConfigs.get("rust_client_log_file").cloned()
}
