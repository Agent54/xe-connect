use anyhow::{Result, ensure};
use iroh::endpoint::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::Peer;

pub(crate) const ALPN: &[u8] = b"xe-connect/1";
const MAX_FRAME: usize = 16 * 1024;

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Scheme {
    #[default]
    Http,
    Https,
}

impl Scheme {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Operation {
    Pair {
        token: String,
        peer: Peer,
    },
    Resolve {
        hostname: String,
    },
    Http {
        hostname: String,
        #[serde(default)]
        scheme: Scheme,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reply {
    pub ok: bool,
    pub peer: Option<Peer>,
    pub error: Option<String>,
}

impl Reply {
    pub fn ok(peer: Option<Peer>) -> Self {
        Self {
            ok: true,
            peer,
            error: None,
        }
    }
    pub fn no(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            peer: None,
            error: Some(message.into()),
        }
    }
}

pub(crate) async fn write<T: Serialize>(stream: &mut SendStream, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= MAX_FRAME, "protocol frame too large");
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    Ok(())
}

pub(crate) async fn read<T: for<'de> Deserialize<'de>>(stream: &mut RecvStream) -> Result<T> {
    let length = stream.read_u32().await? as usize;
    ensure!(length <= MAX_FRAME, "protocol frame too large");
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
