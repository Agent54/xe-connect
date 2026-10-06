use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use iroh::SecretKey;
use serde::{Deserialize, Serialize};

/// Human-readable metadata, independent of the cryptographic device identity.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub device_name: String,
    pub user_name: Option<String>,
}

/// All state belongs to a directory chosen by the embedding application or CLI.
#[derive(Clone, Debug)]
pub struct StateStore {
    directory: PathBuf,
}

impl StateStore {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref();
        if !directory.exists() {
            fs::create_dir_all(directory).context("create state directory")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
            }
        }
        Ok(Self {
            directory: directory
                .canonicalize()
                .context("resolve state directory")?,
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// The file is exactly a 32-byte secret key. Invalid files are never replaced.
    pub fn identity(&self) -> Result<SecretKey> {
        let _lock = self.lock("config.lock")?;
        let path = self.directory.join("identity.key");
        match File::open(&path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take(33).read_to_end(&mut bytes)?;
                let bytes: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("identity.key must contain exactly 32 bytes"))?;
                Ok(SecretKey::from_bytes(&bytes))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let key = SecretKey::generate();
                self.atomic_write("identity.key", &key.to_bytes())?;
                Ok(key)
            }
            Err(error) => Err(error).context("read identity.key"),
        }
    }

    pub fn read<T: for<'de> Deserialize<'de> + Default>(&self) -> Result<T> {
        let _lock = self.lock("config.lock")?;
        self.read_unlocked()
    }

    /// Serialize updates across CLI and agent processes, then replace atomically.
    pub fn update<T, R>(&self, update: impl FnOnce(&mut T) -> Result<R>) -> Result<R>
    where
        T: for<'de> Deserialize<'de> + Serialize + Default,
    {
        let _lock = self.lock("config.lock")?;
        let existed = self.directory.join("config.json").try_exists()?;
        let mut config = self.read_unlocked::<T>()?;
        let original = serde_json::to_value(&config)?;
        let result = update(&mut config)?;
        if !existed || original != serde_json::to_value(&config)? {
            let mut bytes = serde_json::to_vec_pretty(&config)?;
            bytes.push(b'\n');
            self.atomic_write("config.json", &bytes)?;
        }
        Ok(result)
    }

    pub fn agent_running(&self) -> Result<bool> {
        let file = self.open_lock("agent.lock")?;
        match file.try_lock() {
            Ok(()) => Ok(false),
            Err(std::fs::TryLockError::WouldBlock) => Ok(true),
            Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
        }
    }

    /// Hold this guard for the lifetime of a running endpoint.
    pub fn acquire_agent(&self) -> Result<File> {
        let file = self.open_lock("agent.lock")?;
        file.try_lock()
            .context("this state directory already has a running agent")?;
        Ok(file)
    }

    fn read_unlocked<T: for<'de> Deserialize<'de> + Default>(&self) -> Result<T> {
        match fs::read(self.directory.join("config.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).context("read config.json"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(T::default()),
            Err(error) => Err(error).context("read config.json"),
        }
    }

    fn lock(&self, name: &str) -> Result<File> {
        let file = self.open_lock(name)?;
        file.lock().context("lock state directory")?;
        Ok(file)
    }

    fn open_lock(&self, name: &str) -> Result<File> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(self.directory.join(name))
            .context("open state lock")
    }

    fn atomic_write(&self, name: &str, bytes: &[u8]) -> Result<()> {
        if name.contains('/') {
            bail!("invalid state filename");
        }
        let temporary = self
            .directory
            .join(format!(".{name}.{:016x}.new", rand::random::<u64>()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = (|| -> Result<()> {
            let mut file = options.open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, self.directory.join(name))?;
            File::open(&self.directory)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.with_context(|| format!("persist {name}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_survives_reopening_and_invalid_keys_are_not_replaced() -> Result<()> {
        let directory = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR"))?;
        let store = StateStore::open(directory.path())?;
        let id = store.identity()?.public();
        assert_eq!(StateStore::open(directory.path())?.identity()?.public(), id);
        fs::write(directory.path().join("identity.key"), b"invalid")?;
        assert!(store.identity().is_err());
        assert_eq!(fs::read(directory.path().join("identity.key"))?, b"invalid");
        Ok(())
    }

    #[test]
    fn updates_persist_and_a_second_agent_cannot_start() -> Result<()> {
        let directory = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR"))?;
        let store = StateStore::open(directory.path())?;
        store.update::<Profile, _>(|profile| {
            profile.device_name = "My Mac".into();
            profile.user_name = Some("Jan".into());
            Ok(())
        })?;
        assert_eq!(store.read::<Profile>()?.user_name.as_deref(), Some("Jan"));
        let first = store.acquire_agent()?;
        assert!(store.acquire_agent().is_err());
        drop(first);
        assert!(store.acquire_agent().is_ok());
        Ok(())
    }

    #[test]
    fn unchanged_and_rejected_updates_do_not_replace_the_file() -> Result<()> {
        let directory = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR"))?;
        let store = StateStore::open(directory.path())?;
        store.update::<Profile, _>(|config| {
            config.device_name = "Saved".into();
            Ok(())
        })?;
        let path = directory.path().join("config.json");
        let before = fs::metadata(&path)?;
        store.update::<Profile, _>(|_| Ok(()))?;
        assert!(
            store
                .update::<Profile, ()>(|config| {
                    config.device_name = "Rejected".into();
                    bail!("reject this update")
                })
                .is_err()
        );
        let after = fs::metadata(&path)?;
        assert_eq!(before.modified()?, after.modified()?);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(before.ino(), after.ino());
        }
        assert_eq!(store.read::<Profile>()?.device_name, "Saved");
        Ok(())
    }
}
