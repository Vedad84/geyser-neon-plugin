use std::sync::{Arc, RwLock};
use std::time::Duration;

use log::{error, info};
use notify::{
    event::ModifyKind, recommended_watcher, Event, EventKind, RecommendedWatcher, RecursiveMode,
    Watcher,
};

use crate::filter::real_filter::filter_config::{
    read_filter_config, FilterConfig, FilterConfigKeys,
};

pub struct WatcherHelper {
    filter_config_path: String,
    pub current_filter_config: RwLock<(FilterConfigKeys, FilterConfig)>,
    watcher: RwLock<Option<RecommendedWatcher>>,
}

pub fn watcher(filter_config_path: &str) -> anyhow::Result<Arc<WatcherHelper>> {
    let current_filter_config = read_filter_config(filter_config_path)?;

    let watcher_helper = Arc::new(WatcherHelper {
        filter_config_path: filter_config_path.to_string(),
        current_filter_config: RwLock::new((
            current_filter_config.to_filter_config_keys()?,
            current_filter_config,
        )),
        watcher: RwLock::new(None),
    });

    watcher_helper.clone().new_watcher()?;

    Ok(watcher_helper)
}

impl WatcherHelper {
    fn new_watcher(self: Arc<Self>) -> anyhow::Result<()> {
        let mut watcher = {
            let me = self.clone();
            recommended_watcher(move |event: notify::Result<Event>| {
                match me.clone().handle_event(event) {
                    Ok(()) => {}
                    Err(error) => {
                        error!("Watch error for {} {error:?}", me.filter_config_path)
                    }
                };
            })?
        };

        watcher.watch(self.filter_config_path.as_ref(), RecursiveMode::Recursive)?;

        *self.watcher.write().unwrap() = Some(watcher);

        Ok(())
    }

    fn handle_event(self: Arc<Self>, event: notify::Result<Event>) -> anyhow::Result<()> {
        info!("Received event {event:?}");
        match event?.kind {
            EventKind::Modify(ModifyKind::Data(_)) => {
                self.reload_filter_config()?;
            }
            EventKind::Remove(_) => {
                std::thread::sleep(Duration::from_millis(200));
                self.reload_filter_config()?;
                self.new_watcher()?;
            }
            _ => {}
        }
        Ok(())
    }

    fn reload_filter_config(&self) -> anyhow::Result<()> {
        let new_filter_config = read_filter_config(&self.filter_config_path)?;

        {
            let (_, current_filter_config) = &*self.current_filter_config.read().unwrap();

            info!(
                "Diff owners: {}",
                similar_asserts::SimpleDiff::from_str(
                    &serde_json::to_string_pretty(&current_filter_config.filter_include_owners)?,
                    &serde_json::to_string_pretty(&new_filter_config.filter_include_owners)?,
                    "current",
                    "new"
                )
            );

            info!(
                "Diff pubkeys: {}",
                similar_asserts::SimpleDiff::from_str(
                    &serde_json::to_string_pretty(&current_filter_config.filter_include_pubkeys)?,
                    &serde_json::to_string_pretty(&new_filter_config.filter_include_pubkeys)?,
                    "current",
                    "new"
                )
            );
        }

        *self.current_filter_config.write().unwrap() = (
            new_filter_config.to_filter_config_keys()?,
            new_filter_config,
        );

        info!("Filter config reloaded successfully");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_config_watcher_cp() -> anyhow::Result<()> {
        // fast_log::init(fast_log::Config::new().console())?;

        let path = "mutable-test-filter-config1.json";

        std::fs::copy("test-filter-config.json", path)?;

        let initial_config = read_filter_config(path)?;

        let watcher_helper = watcher(path)?;

        assert_eq!(
            *watcher_helper.current_filter_config.read().unwrap(),
            (initial_config.to_filter_config_keys()?, initial_config)
        );

        Command::new("./test_update_config.sh").output().unwrap();

        std::thread::sleep(Duration::from_millis(500));

        let updated_filter_config = read_filter_config(path)?;

        assert_eq!(
            *watcher_helper.current_filter_config.read().unwrap(),
            (
                updated_filter_config.to_filter_config_keys()?,
                updated_filter_config,
            )
        );

        std::fs::remove_file(path)?;

        Ok(())
    }

    #[test]
    fn test_config_watcher_rm() -> anyhow::Result<()> {
        // fast_log::init(fast_log::Config::new().console())?;

        let path = "mutable-test-filter-config2.json";

        std::fs::copy("test-filter-config.json", path)?;

        let initial_config = read_filter_config(path)?;

        let watcher_helper = watcher(path)?;

        assert_eq!(
            *watcher_helper.current_filter_config.read().unwrap(),
            (initial_config.to_filter_config_keys()?, initial_config)
        );

        std::fs::remove_file(path)?;

        std::fs::copy("updated-test-filter-config.json", path)?;

        std::thread::sleep(Duration::from_millis(500));

        let updated_filter_config = read_filter_config(path)?;

        assert_eq!(
            *watcher_helper.current_filter_config.read().unwrap(),
            (
                updated_filter_config.to_filter_config_keys()?,
                updated_filter_config,
            )
        );

        std::fs::remove_file(path)?;

        Ok(())
    }
}
