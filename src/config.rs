use std::path::Path;
use bindings::sdk::{DbConnectionBuilder, __codegen::SpacetimeModule};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Config {
    cluster_url: String,
    region:      String,
    token:       String,
}
impl Default for Config {
    fn default() -> Self {
        Self { cluster_url: String::default(), region: String::default(), token: String::default() }
    }
}
impl Config {
    pub fn from(path: &str) -> Result<Self> {
        let path = Path::new(path);
        if !path.exists() {
            let config = Config::default();
            let content = serde_json::to_string_pretty(&config)?;
            std::fs::write(path, content)?;
            Ok(config)
        } else {
            let content = std::fs::read(path)?;
            let config = serde_json::from_slice(&content)?;
            Ok(config)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.cluster_url.is_empty() || self.region.is_empty() || self.token.is_empty()
    }
}

pub trait Configurable {
    fn configure(self, config: &Config) -> Self;
}
impl <M: SpacetimeModule> Configurable for DbConnectionBuilder<M> {
    fn configure(self, config: &Config) -> Self {
        self.with_uri(&config.cluster_url)
            .with_module_name(&config.region)
            .with_token(Some(&config.token))
    }
}
