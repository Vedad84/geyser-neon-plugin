use std::{fs::File, io::Read};

use ahash::AHashSet;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FilterConfig {
    // Filter by account owners in base58
    pub filter_include_owners: AHashSet<String>,
    // Alway include list for filter ( public keys from 32 to 44 characters in base58 )
    pub filter_include_pubkeys: AHashSet<String>,
}

pub fn read_filter_config(filer_path: &str) -> Result<FilterConfig> {
    let mut file = File::open(filer_path)?;
    let mut filter_config = String::new();
    file.read_to_string(&mut filter_config)?;

    Ok(serde_json::from_str(&filter_config)?)
}

pub async fn read_filter_config_async(filer_path: &Path) -> Result<FilterConfig> {
    let filter_config = fs::read_to_string(filer_path).await?;
    Ok(serde_json::from_str(&filter_config)?)
}
