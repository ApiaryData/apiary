//! Keys: a Node's identity and the Apiary's signing key.
//!
//! Each Node generates an ed25519 key pair on first start and its public key is
//! its Node id. The key lives in a file (in Kubernetes, a mounted Secret), so a
//! restarted or rescheduled Node is the same Node.
//!
//! The Apiary has one signing key, held by the Beekeeper. It signs join tokens
//! and revocation lists; every Node holds only its public half.

use std::fs;
use std::io::Write;
use std::path::Path;

use iroh_base::{PublicKey, SecretKey, Signature};

use apiary_core::{ApiaryError, Result};

/// A Node's id: its public key.
pub type NodeId = PublicKey;

/// The Apiary's public key, which every Node trusts to sign tokens.
pub type ApiaryPublicKey = PublicKey;

/// A Node's key pair.
#[derive(Clone)]
pub struct NodeKey {
    secret: SecretKey,
}

impl NodeKey {
    /// Generate a new key.
    pub fn generate() -> Self {
        Self {
            secret: SecretKey::generate(),
        }
    }

    /// Load the key at `path`, creating and saving a new one if there is none.
    ///
    /// Safe to race: two processes starting together end up with the same key.
    pub fn load_or_create(path: &Path) -> Result<Self> {
        match read_secret(path) {
            Ok(secret) => Ok(Self { secret }),
            Err(ApiaryError::EntityNotFound { .. }) => {
                let key = Self::generate();
                match write_secret_new(path, &key.secret) {
                    Ok(()) => Ok(key),
                    // Someone else created it first: use theirs.
                    Err(ApiaryError::AlreadyExists { .. }) => Ok(Self {
                        secret: read_secret(path)?,
                    }),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// A key from its 32 secret bytes.
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self {
            secret: SecretKey::from_bytes(bytes),
        }
    }

    /// This Node's id.
    pub fn id(&self) -> NodeId {
        self.secret.public()
    }

    /// The secret key, for the transport.
    pub fn secret(&self) -> &SecretKey {
        &self.secret
    }
}

impl std::fmt::Debug for NodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeKey({})", self.id().fmt_short())
    }
}

/// The Apiary's signing key, held by the Beekeeper.
#[derive(Clone)]
pub struct ApiaryKey {
    secret: SecretKey,
}

impl ApiaryKey {
    /// Generate a new Apiary key.
    pub fn generate() -> Self {
        Self {
            secret: SecretKey::generate(),
        }
    }

    /// An Apiary key from its 32 secret bytes (the simulator derives keys from
    /// its seed so a run replays exactly).
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self {
            secret: SecretKey::from_bytes(bytes),
        }
    }

    /// Load an Apiary key from a file.
    pub fn load(path: &Path) -> Result<Self> {
        Ok(Self {
            secret: read_secret(path)?,
        })
    }

    /// Save this key to a new file (readable by its owner only). Fails if the
    /// file exists: an Apiary key is never overwritten.
    pub fn save_new(&self, path: &Path) -> Result<()> {
        write_secret_new(path, &self.secret)
    }

    /// The public key Nodes trust.
    pub fn public(&self) -> ApiaryPublicKey {
        self.secret.public()
    }

    /// Sign a message.
    pub fn sign(&self, message: &[u8]) -> Signature {
        self.secret.sign(message)
    }
}

impl std::fmt::Debug for ApiaryKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ApiaryKey({})", self.public().fmt_short())
    }
}

/// Parse a public key written as Node ids are (`NodeId::to_string`).
pub fn parse_public(text: &str) -> Result<PublicKey> {
    text.trim().parse().map_err(|e| ApiaryError::Config {
        message: format!("'{}' is not a valid public key: {e}", text.trim()),
    })
}

fn read_secret(path: &Path) -> Result<SecretKey> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(ApiaryError::EntityNotFound {
                entity_type: "Key file".into(),
                name: path.display().to_string(),
            });
        }
        Err(e) => {
            return Err(ApiaryError::storage(
                format!("Failed to read the key file {}", path.display()),
                e,
            ));
        }
    };
    let bytes = data_encoding::HEXLOWER_PERMISSIVE
        .decode(text.trim().as_bytes())
        .map_err(|e| ApiaryError::Config {
            message: format!("The key file {} is not hex: {e}", path.display()),
        })?;
    let array: [u8; 32] = bytes.try_into().map_err(|_| ApiaryError::Config {
        message: format!(
            "The key file {} must hold 32 bytes (64 hex characters)",
            path.display()
        ),
    })?;
    Ok(SecretKey::from_bytes(&array))
}

fn write_secret_new(path: &Path, secret: &SecretKey) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|e| {
            ApiaryError::storage(format!("Failed to create {}", parent.display()), e)
        })?;
    }
    // The key is written whole to a private temporary file and then linked into
    // place, which fails if the file already exists. So a reader never sees half
    // a key, and of two writers racing, one wins and the other finds its file.
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let io = |what: &str, e: std::io::Error| {
        ApiaryError::storage(
            format!("Failed to {what} the key file {}", path.display()),
            e,
        )
    };
    let hex = data_encoding::HEXLOWER.encode(&secret.to_bytes());
    let mut file = options.open(&tmp).map_err(|e| io("create", e))?;
    let written = file
        .write_all(hex.as_bytes())
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_all());
    drop(file);
    if let Err(e) = written {
        let _ = fs::remove_file(&tmp);
        return Err(io("write", e));
    }

    let linked = fs::hard_link(&tmp, path);
    let _ = fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(ApiaryError::AlreadyExists {
                entity_type: "Key file".into(),
                name: path.display().to_string(),
            })
        }
        Err(e) => Err(io("link", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_key_persists_and_is_the_same_node_after_a_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("keys").join("node.key");
        let first = NodeKey::load_or_create(&path).unwrap();
        let again = NodeKey::load_or_create(&path).unwrap();
        assert_eq!(first.id(), again.id());
        assert_eq!(first.secret().to_bytes(), again.secret().to_bytes());
    }

    #[cfg(unix)]
    #[test]
    fn key_files_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("node.key");
        NodeKey::load_or_create(&path).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "group and others have no access: {mode:o}");
    }

    #[test]
    fn two_starts_racing_to_create_the_key_agree() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("node.key");
        let ids: Vec<_> = (0..8)
            .map(|_| {
                let path = path.clone();
                std::thread::spawn(move || NodeKey::load_or_create(&path).unwrap().id())
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        assert!(ids.windows(2).all(|w| w[0] == w[1]), "{ids:?}");
    }

    #[test]
    fn an_apiary_key_is_never_overwritten() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("apiary.key");
        let key = ApiaryKey::generate();
        key.save_new(&path).unwrap();
        assert!(ApiaryKey::generate().save_new(&path).is_err());
        assert_eq!(ApiaryKey::load(&path).unwrap().public(), key.public());
    }

    #[test]
    fn a_bad_key_file_says_what_is_wrong() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bad.key");
        fs::write(&path, "not hex").unwrap();
        assert!(NodeKey::load_or_create(&path).is_err());
        fs::write(&path, "abcd").unwrap();
        let err = ApiaryKey::load(&path).unwrap_err().to_string();
        assert!(err.contains("32 bytes"), "{err}");
    }

    #[test]
    fn public_keys_round_trip_through_text() {
        let id = NodeKey::generate().id();
        assert_eq!(parse_public(&id.to_string()).unwrap(), id);
        assert!(parse_public("nope").is_err());
    }
}
