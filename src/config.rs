use std::{
    collections::BTreeMap,
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hyper::Uri;
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};

use crate::state::Profile;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub address: EndpointAddr,
    pub profile: Profile,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Invitation {
    pub expires_at: u64,
    /// Binding redemption to one identity makes a lost pairing reply retryable.
    pub redeemed_by: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u8,
    pub profile: Profile,
    pub listen: SocketAddr,
    pub upstream: String,
    pub probe_timeout_ms: u64,
    pub peers: BTreeMap<String, Peer>,
    pub routes: BTreeMap<String, String>,
    pub invitations: BTreeMap<String, Invitation>,
    pub last_address: Option<EndpointAddr>,
    pub control_token: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: 1,
            profile: Profile::default(),
            listen: "127.0.0.1:5198".parse().unwrap(),
            upstream: "http://127.0.0.1:5196".into(),
            probe_timeout_ms: 500,
            peers: BTreeMap::new(),
            routes: BTreeMap::new(),
            invitations: BTreeMap::new(),
            last_address: None,
            control_token: new_token(),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.version == 1, "unsupported configuration version");
        ensure!(
            self.listen.ip().is_loopback(),
            "HTTP listener must use a loopback address"
        );
        ensure!(self.listen.port() != 0, "choose a fixed HTTP listener port");
        ensure!(
            (1..=30_000).contains(&self.probe_timeout_ms),
            "probe timeout must be between 1 and 30000 milliseconds"
        );
        validate_profile(&self.profile)?;
        let uri: Uri = self.upstream.parse()?;
        ensure!(
            matches!(uri.scheme_str(), Some("http" | "https")),
            "upstream must use HTTP(S)"
        );
        let host = uri.host().unwrap_or_default();
        ensure!(
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback()),
            "upstream must be a local loopback gateway"
        );
        ensure!(
            uri.path() == "/" && uri.query().is_none(),
            "upstream must be an origin without a path or query"
        );
        ensure!(
            uri.authority().is_some_and(|a| !a.as_str().contains('@')),
            "upstream cannot contain credentials"
        );
        ensure!(
            self.control_token.len() >= 32,
            "invalid local control token"
        );
        for (id, peer) in &self.peers {
            ensure!(
                *id == peer.address.id.to_string(),
                "peer map key must match endpoint identity"
            );
            validate_profile(&peer.profile)?;
        }
        Ok(())
    }

    pub fn remove_peer(&mut self, id: &str) -> bool {
        self.routes.retain(|_, provider| provider != id);
        self.invitations
            .retain(|_, invitation| invitation.redeemed_by.as_deref() != Some(id));
        self.peers.remove(id).is_some()
    }

    pub(crate) fn invitation_authorizes(&self, token: &str, id: &str) -> bool {
        self.invitations.get(token).is_some_and(|invitation| {
            invitation.expires_at >= now()
                && match invitation.redeemed_by.as_deref() {
                    None => true,
                    Some(redeemer) => redeemer == id && self.peers.contains_key(id),
                }
        })
    }
}

/// Settings that a running parent or CLI can change without replacing peer state.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub listen: Option<SocketAddr>,
    pub upstream: Option<String>,
    pub probe_timeout_ms: Option<u64>,
    pub device_name: Option<String>,
    pub user_name: Option<String>,
}

impl Settings {
    pub fn is_empty(&self) -> bool {
        self.listen.is_none()
            && self.upstream.is_none()
            && self.probe_timeout_ms.is_none()
            && self.device_name.is_none()
            && self.user_name.is_none()
    }

    pub fn apply(self, config: &mut Config) -> Result<()> {
        if let Some(value) = self.listen {
            config.listen = value;
        }
        if let Some(value) = self.upstream {
            config.upstream = value;
        }
        if let Some(value) = self.probe_timeout_ms {
            config.probe_timeout_ms = value;
        }
        if let Some(value) = self.device_name {
            config.profile.device_name = value;
        }
        if let Some(value) = self.user_name {
            config.profile.user_name = Some(value);
        }
        config.validate()
    }
}

pub fn validate_profile(profile: &Profile) -> Result<()> {
    ensure!(profile.device_name.len() <= 256, "device name is too long");
    ensure!(
        profile
            .user_name
            .as_ref()
            .is_none_or(|name| name.len() <= 256),
        "user name is too long"
    );
    Ok(())
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn new_token() -> String {
    URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Ticket {
    pub version: u8,
    pub peer: Peer,
    pub token: String,
    pub expires_at: u64,
}

impl Ticket {
    pub fn encode(&self) -> Result<String> {
        Ok(format!(
            "xe1:{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }

    pub fn decode(text: &str) -> Result<Self> {
        ensure!(text.len() <= 32_768, "pairing ticket is too large");
        let encoded = text
            .strip_prefix("xe1:")
            .ok_or_else(|| anyhow::anyhow!("invalid pairing ticket prefix"))?;
        let ticket: Self = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?;
        ensure!(ticket.version == 1, "unsupported pairing ticket version");
        ensure!(ticket.expires_at >= now(), "pairing ticket has expired");
        ensure!(ticket.token.len() >= 32, "invalid pairing token");
        validate_profile(&ticket.peer.profile)?;
        Ok(ticket)
    }
}
