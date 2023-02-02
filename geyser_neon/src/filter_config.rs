use std::{fs::File, io::Read};

use ahash::AHashSet;
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FilterConfig {
    // Filter by account owners in base58
    pub filter_include_owners: AHashSet<String>,
    // Alway include list for filter ( public keys from 32 to 44 characters in base58 )
    pub filter_include_pubkeys: AHashSet<String>,
}

pub fn read_filter_config(filer_path: &str) -> Result<FilterConfig, Box<dyn std::error::Error>> {
    let mut file = File::open(filer_path)?;
    let mut filter_config = String::new();
    file.read_to_string(&mut filter_config)?;

    Ok(serde_json::from_str(&filter_config)?)
}
