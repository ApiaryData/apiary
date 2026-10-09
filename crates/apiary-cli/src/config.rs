//! The `apiary node run` configuration file (TOML).
//!
//! ```toml
//! [node]
//! storage = "local:///mnt/drive/apiary"      # the site's comb (the cluster drive)
//! harvest = "s3://site-harvest/apiary"       # optional: where capped Cells go
//!
//! [flight]
//! listen = "0.0.0.0:50051"
//! token_env = "APIARY_TOKEN"                 # optional shared bearer token
//!
//! [mqtt]
//! host = "broker.local"
//! client_id = "apiary-pi-01"
//! [[mqtt.subscriptions]]
//! topic = "plant/+/readings"
//! frame = "factory.line1.readings"
//! ```
//!
//! Every setting not named takes the Node's default. Unknown keys are errors, so
//! a misspelt setting is not silently ignored.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use apiary_core::config::NodeConfig;
use apiary_entrance::MqttConfig;
use apiary_net::NetConfig;

/// The whole file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub node: NodeSection,
    #[serde(default)]
    pub flight: Option<FlightSection>,
    #[serde(default)]
    pub mqtt: Option<MqttConfig>,
    #[serde(default)]
    pub net: Option<NetConfig>,
}

/// `[node]`: storage and cadences.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSection {
    /// The site's comb: `local://<path>` or `s3://bucket/prefix`.
    pub storage: String,
    /// Where the crop and set-aside deposits live on this Node's own disk.
    pub cache_dir: Option<PathBuf>,
    pub deposit_interval_secs: Option<u64>,
    pub crop_max_mb: Option<u64>,
    pub crop_sync: Option<bool>,
    /// Where capped Cells are harvested to.
    pub harvest: Option<String>,
    pub harvest_interval_secs: Option<u64>,
    pub harvest_batch_mb: Option<u64>,
    pub cap_interval_secs: Option<u64>,
    pub cap_max_age_secs: Option<u64>,
    /// How long harvested Cells stay on the drive (unset: for ever).
    pub retention_secs: Option<u64>,
    pub clear_interval_secs: Option<u64>,
    pub clear_grace_secs: Option<u64>,
    /// The most commits a Frame's log takes per minute.
    pub commit_budget_per_min: Option<u32>,
    /// How different the Bees are from one another (default 0.5).
    pub colony_diversity: Option<f64>,
}

/// `[flight]`: the Flight SQL entrance.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlightSection {
    /// Address to listen on. Defaults to the loopback address, because there is
    /// no user authentication yet.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// A shared bearer token every call must carry.
    pub token: Option<String>,
    /// Or the name of an environment variable holding it (keeps it out of the file).
    pub token_env: Option<String>,
}

fn default_listen() -> String {
    "127.0.0.1:50051".to_string()
}

impl FileConfig {
    /// Read and check a configuration file.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("Cannot read {}: {e}", path.display()))?;
        Self::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Parse and check configuration text.
    pub fn parse(text: &str) -> Result<Self, String> {
        let config: Self = toml::from_str(text).map_err(|e| e.to_string())?;
        config.check()?;
        Ok(config)
    }

    fn check(&self) -> Result<(), String> {
        if let Some(flight) = &self.flight {
            flight.listen.parse::<SocketAddr>().map_err(|e| {
                format!("[flight] listen '{}' is not an address: {e}", flight.listen)
            })?;
            if flight.token.is_some() && flight.token_env.is_some() {
                return Err("[flight] set token or token_env, not both".into());
            }
        }
        if let Some(mqtt) = &self.mqtt
            && mqtt.subscriptions.is_empty()
        {
            return Err("[mqtt] needs at least one [[mqtt.subscriptions]]".into());
        }
        let storage = &self.node.storage;
        if apiary_comb::custom_store::drive_authority(storage).is_some() {
            let net = self.net.as_ref().ok_or(
                "[node] storage is another node's drive (apiary-drive://), which needs a [net] section",
            )?;
            if net.serve_comb {
                return Err("[net] serve_comb and a remote [node] storage cannot both be set: a node either has the drive or reaches it".into());
            }
        }
        if let Some(net) = &self.net
            && net.serve_comb
            && storage.starts_with("s3://")
        {
            return Err("[net] serve_comb needs [node] storage to be a local directory".into());
        }
        Ok(())
    }

    /// The Node's configuration: detected resources, then this file's settings.
    pub fn node_config(&self) -> NodeConfig {
        let n = &self.node;
        let mut config = NodeConfig::detect(&n.storage);
        let secs = Duration::from_secs;
        if let Some(dir) = &n.cache_dir {
            config.cache_dir = dir.clone();
        }
        if let Some(v) = n.deposit_interval_secs {
            config.deposit_interval = secs(v);
        }
        if let Some(v) = n.crop_max_mb {
            config.crop_max_bytes = v * 1024 * 1024;
        }
        if let Some(v) = n.crop_sync {
            config.crop_sync = v;
        }
        config.harvest_uri = n.harvest.clone();
        if let Some(v) = n.harvest_interval_secs {
            config.harvest_interval = secs(v);
        }
        if let Some(v) = n.harvest_batch_mb {
            config.harvest_batch_bytes = v * 1024 * 1024;
        }
        if let Some(v) = n.cap_interval_secs {
            config.cap_interval = secs(v);
        }
        if let Some(v) = n.cap_max_age_secs {
            config.cap_max_age = secs(v);
        }
        config.retention = n.retention_secs.map(secs);
        if let Some(v) = n.clear_interval_secs {
            config.clear_interval = secs(v);
        }
        if let Some(v) = n.clear_grace_secs {
            config.clear_grace = secs(v);
        }
        if let Some(v) = n.commit_budget_per_min {
            config.commit_budget_per_min = v;
        }
        if let Some(v) = n.colony_diversity {
            config.colony_diversity = v;
        }
        config
    }
}

impl FlightSection {
    /// The address to listen on (checked when the file was loaded).
    pub fn addr(&self) -> SocketAddr {
        self.listen
            .parse()
            .expect("the address was checked when the file was loaded")
    }

    /// The bearer token, from the file or the named environment variable.
    pub fn resolve_token(&self) -> Result<Option<String>, String> {
        match (&self.token, &self.token_env) {
            (Some(token), _) => Ok(Some(token.clone())),
            (None, Some(var)) => std::env::var(var)
                .map(Some)
                .map_err(|_| format!("[flight] token_env names {var}, which is not set")),
            (None, None) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"
[node]
storage = "local:///tmp/site"
cache_dir = "/tmp/cache"
deposit_interval_secs = 5
crop_max_mb = 8
crop_sync = false
harvest = "s3://bucket/apiary"
harvest_batch_mb = 100
cap_max_age_secs = 30
retention_secs = 86400
clear_grace_secs = 120

[flight]
listen = "0.0.0.0:6000"
token = "abc"

[mqtt]
host = "broker"
client_id = "pi-01"
batch_interval = 250
[[mqtt.subscriptions]]
topic = "plant/#"
frame = "f.l.readings"
"#;

    #[test]
    fn a_full_file_maps_onto_the_node_config() {
        let file = FileConfig::parse(FULL).unwrap();
        let config = file.node_config();
        assert_eq!(config.storage_uri, "local:///tmp/site");
        assert_eq!(config.cache_dir, PathBuf::from("/tmp/cache"));
        assert_eq!(config.deposit_interval, Duration::from_secs(5));
        assert_eq!(config.crop_max_bytes, 8 * 1024 * 1024);
        assert!(!config.crop_sync);
        assert_eq!(config.harvest_uri.as_deref(), Some("s3://bucket/apiary"));
        assert_eq!(config.harvest_batch_bytes, 100 * 1024 * 1024);
        assert_eq!(config.cap_max_age, Duration::from_secs(30));
        assert_eq!(config.retention, Some(Duration::from_secs(86_400)));
        assert_eq!(config.clear_grace, Duration::from_secs(120));

        let flight = file.flight.unwrap();
        assert_eq!(flight.addr().port(), 6000);
        assert_eq!(flight.resolve_token().unwrap().as_deref(), Some("abc"));
        let mqtt = file.mqtt.unwrap();
        assert_eq!(mqtt.port, 1883);
        assert_eq!(mqtt.batch_interval, Duration::from_millis(250));
    }

    #[test]
    fn only_storage_is_required_and_the_rest_defaults() {
        let file = FileConfig::parse("[node]\nstorage = \"local:///tmp/x\"\n").unwrap();
        let config = file.node_config();
        assert_eq!(config.retention, None);
        assert_eq!(config.harvest_uri, None);
        assert!(config.crop_sync);
        assert!(file.flight.is_none() && file.mqtt.is_none());
    }

    #[test]
    fn mistakes_are_refused_with_a_reason() {
        for (text, wanted) in [
            ("[node]\n", "storage"),
            ("[node]\nstorage = \"x\"\nstorgae = \"y\"\n", "storgae"),
            (
                "[node]\nstorage = \"x\"\n[flight]\nlisten = \"nowhere\"\n",
                "not an address",
            ),
            (
                "[node]\nstorage = \"x\"\n[flight]\ntoken = \"a\"\ntoken_env = \"B\"\n",
                "not both",
            ),
            (
                "[node]\nstorage = \"x\"\n[mqtt]\nhost = \"h\"\nclient_id = \"c\"\nsubscriptions = []\n",
                "at least one",
            ),
        ] {
            let err = FileConfig::parse(text).expect_err(text);
            assert!(err.contains(wanted), "{text:?}: {err}");
        }
    }

    #[test]
    fn the_token_can_come_from_the_environment() {
        let file = FileConfig::parse(
            "[node]\nstorage = \"x\"\n[flight]\ntoken_env = \"APIARY_TEST_TOKEN_UNSET\"\n",
        )
        .unwrap();
        let err = file.flight.unwrap().resolve_token().unwrap_err();
        assert!(err.contains("APIARY_TEST_TOKEN_UNSET"));
    }
}
