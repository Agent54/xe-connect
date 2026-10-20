use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Method, Request};
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioTimer},
};
use iroh::EndpointAddr;
use tokio::{net::TcpListener, time::timeout};
use xe_connect::{
    Agent, Config, Peer, StateStore,
    config::{Settings, Ticket},
};

#[derive(Parser)]
#[command(
    version,
    about = "Noninteractive HTTP bridge between paired iroh computers"
)]
struct Args {
    /// Persistent identity and configuration directory; defaults to the working directory.
    #[arg(long, alias = "folder", global = true, default_value = ".")]
    state_dir: PathBuf,
    /// Persist the HTTP loopback listener address.
    #[arg(long, global = true)]
    listen: Option<SocketAddr>,
    /// Persist the local HTTP(S) gateway origin, preserving requested .localhost hosts.
    #[arg(long, global = true)]
    upstream: Option<String>,
    /// Persist the HEAD / probe deadline in milliseconds (default: 500).
    #[arg(long, global = true)]
    probe_timeout_ms: Option<u64>,
    #[arg(long, global = true)]
    device_name: Option<String>,
    #[arg(long, global = true)]
    user_name: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the agent (also the default when no command is supplied).
    Serve,
    /// Print a one-use pairing ticket, valid for 10 minutes by default.
    Invite {
        #[arg(long, default_value_t = 600)]
        expires_in: u64,
    },
    /// Redeem a ticket and persist the connection on both computers.
    Pair { ticket: String },
    /// Print the saved peers and cached hostname routes as JSON.
    Peers,
    /// Remove a peer, its cached routes, and active connections.
    Unpair { endpoint_id: String },
    /// Print this computer's persistent endpoint ID.
    Id,
    /// Print a private authenticated link to the running agent's diagnostics UI.
    Ui,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let settings = Settings {
        listen: args.listen,
        upstream: args.upstream,
        probe_timeout_ms: args.probe_timeout_ms,
        device_name: args.device_name,
        user_name: args.user_name,
    };
    let initial_settings = settings.clone();
    let (store, identity, running) = disk(move || {
        let store = StateStore::open(args.state_dir)?;
        let identity = store.identity()?;
        let running = store.agent_running()?;
        if !running {
            store.update::<Config, _>(|config| {
                if config.profile.device_name.is_empty() {
                    config.profile.device_name =
                        format!("computer-{}", &identity.public().to_string()[..8]);
                }
                initial_settings.apply(config)
            })?;
        }
        Ok((store, identity, running))
    })
    .await?;
    let id = identity.public().to_string();
    if running && !settings.is_empty() {
        control(
            &store,
            Method::POST,
            "/_xe/config",
            Some(serde_json::to_vec(&settings)?),
        )
        .await?;
    }

    match args.command.unwrap_or(Command::Serve) {
        Command::Serve => {
            let reader = store.clone();
            let config = disk(move || reader.read::<Config>()).await?;
            let listener = TcpListener::bind(config.listen)
                .await
                .context("bind HTTP listener")?;
            let agent = Agent::start(store.clone()).await?;
            println!(
                "{}",
                serde_json::json!({
                    "event": "ready", "endpoint_id": id, "listen": config.listen,
                    "state_dir": store.directory(), "device_name": config.profile.device_name,
                    "user_name": config.profile.user_name,
                })
            );
            let serving = agent.clone().serve_http(listener);
            tokio::select! {
                result = serving => result?,
                result = shutdown_signal() => result?,
            }
            agent.close().await;
        }
        Command::Invite { expires_in } => {
            let ticket: Ticket = if running {
                serde_json::from_slice(
                    &control(
                        &store,
                        Method::POST,
                        "/_xe/invite",
                        Some(serde_json::to_vec(&expires_in)?),
                    )
                    .await?,
                )?
            } else {
                disk(move || {
                    let config = store.read::<Config>()?;
                    let peer = Peer {
                        address: config
                            .last_address
                            .unwrap_or_else(|| EndpointAddr::new(identity.public())),
                        profile: config.profile,
                    };
                    xe_connect::agent::create_invitation(
                        &store,
                        peer,
                        Duration::from_secs(expires_in),
                    )
                })
                .await?
            };
            println!("{}", ticket.encode()?);
        }
        Command::Pair { ticket } => {
            let parsed = Ticket::decode(&ticket)?;
            let peer = if running {
                serde_json::from_slice::<Peer>(
                    &control(
                        &store,
                        Method::POST,
                        "/_xe/pair",
                        Some(serde_json::to_vec(&ticket)?),
                    )
                    .await?,
                )?
            } else {
                let agent = Agent::start(store).await?;
                let result = agent.pair(parsed).await;
                agent.close().await;
                result?
            };
            println!("{}", serde_json::to_string(&peer)?);
        }
        Command::Peers => {
            let config = disk(move || store.read::<Config>()).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &serde_json::json!({ "peers": config.peers, "routes": config.routes })
                )?
            );
        }
        Command::Unpair { endpoint_id } => {
            let id = endpoint_id.parse::<iroh::PublicKey>()?.to_string();
            if running {
                control(
                    &store,
                    Method::POST,
                    "/_xe/unpair",
                    Some(serde_json::to_vec(&id)?),
                )
                .await?;
            } else {
                let removed = id.clone();
                disk(move || {
                    store.update::<Config, _>(|config| {
                        config.remove_peer(&removed);
                        Ok(())
                    })
                })
                .await?;
            }
            println!("{}", serde_json::json!({ "removed": id }));
        }
        Command::Id => println!("{id}"),
        Command::Ui => {
            ensure!(
                cfg!(feature = "web-ui"),
                "this binary was built without the web-ui feature"
            );
            ensure!(running, "start the agent before opening its UI");
            let config = disk(move || store.read::<Config>()).await?;
            println!(
                "http://{}/_xe/ui/#token={}",
                config.listen, config.control_token
            );
        }
    }
    Ok(())
}

async fn disk<R: Send + 'static>(work: impl FnOnce() -> Result<R> + Send + 'static) -> Result<R> {
    tokio::task::spawn_blocking(work).await?
}

async fn control(
    store: &StateStore,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<Vec<u8>> {
    let reader = store.clone();
    let config = disk(move || reader.read::<Config>()).await?;
    let request = Request::builder()
        .method(method)
        .uri(format!("http://{}{path}", config.listen))
        .header("authorization", format!("Bearer {}", config.control_token))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.unwrap_or_default())))?;
    let client = Client::builder(TokioExecutor::new())
        .pool_timer(TokioTimer::new())
        .pool_max_idle_per_host(2)
        .build_http();
    timeout(Duration::from_secs(30), async {
        let response = client.request(request).await?;
        let status = response.status();
        let bytes = Limited::new(response.into_body(), 32_768)
            .collect()
            .await
            .map_err(anyhow::Error::from_boxed)?
            .to_bytes()
            .to_vec();
        ensure!(
            status.is_success(),
            "agent rejected command: {}",
            String::from_utf8_lossy(&bytes)
        );
        Ok::<_, anyhow::Error>(bytes)
    })
    .await?
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
