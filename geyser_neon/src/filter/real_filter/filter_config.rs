use ahash::AHashSet;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct FilterConfig {
    // Filter by account owners in base58
    pub filter_include_owners: Vec<String>,
    // Always include list for filter ( public keys from 32 to 44 characters in base58 )
    pub filter_include_pubkeys: Vec<String>,
}

impl FilterConfig {
    pub fn to_filter_config_keys(&self) -> bs58::decode::Result<FilterConfigKeys> {
        Ok(FilterConfigKeys {
            filter_include_owners: self
                .filter_include_owners
                .iter()
                .map(|key| bs58::decode(key).into_vec())
                .collect::<bs58::decode::Result<_>>()?,
            filter_include_pubkeys: self
                .filter_include_pubkeys
                .iter()
                .map(|key| bs58::decode(key).into_vec())
                .collect::<bs58::decode::Result<_>>()?,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct FilterConfigKeys {
    pub filter_include_owners: AHashSet<Vec<u8>>,
    pub filter_include_pubkeys: AHashSet<Vec<u8>>,
}

pub fn read_filter_config(path: &str) -> Result<FilterConfig> {
    Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;

    #[test]
    fn test_filter_config_similar_asserts() -> Result<()> {
        // fast_log::init(fast_log::Config::new().console())?;

        let filter_config1 = read_filter_config("filter_config_rpc1.json")?;

        let mut filter_config2 = read_filter_config("filter_config_rpc1.json")?;

        filter_config2.filter_include_owners = vec![];

        info!(
            "{}",
            similar_asserts::SimpleDiff::from_str(
                &serde_json::to_string_pretty(&filter_config1)?,
                &serde_json::to_string_pretty(&filter_config2)?,
                "left",
                "right"
            )
        );

        Ok(())
    }
}
