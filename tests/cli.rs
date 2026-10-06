use std::{
    convert::Infallible,
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
};
use serde_json::Value;
use tokio::{net::TcpListener, time::timeout};
use xe_connect::{Config, StateStore};

const BIN: &str = env!("CARGO_BIN_EXE_xe-connect");

async fn command(directory: &Path, args: &[&str]) -> Result<String> {
    let directory = directory.to_owned();
    let args: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(BIN)
            .arg("--state-dir")
            .arg(directory)
            .args(args)
            .stdin(Stdio::null())
            .output()
    })
    .await??;
    ensure!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

struct Process(Child);

impl Process {
    async fn start(directory: &Path, extra: &[&str]) -> Result<(Self, SocketAddr)> {
        let mut process = Self(
            Command::new(BIN)
                .arg("--state-dir")
                .arg(directory)
                .args(extra)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        let stdout = process
            .0
            .stdout
            .take()
            .context("missing readiness output")?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
            let _ = sender.send(result);
        });
        let line = timeout(Duration::from_secs(10), receiver).await???;
        let ready: Value = serde_json::from_str(&line).context("missing readiness JSON")?;
        ensure!(ready["event"] == "ready", "unexpected readiness event");
        let address = ready["listen"]
            .as_str()
            .context("missing listener")?
            .parse()?;
        Ok((process, address))
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn gateway(offers_service: bool) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| async move {
                    let mut response = Response::new(Full::new(Bytes::from_static(b"from peer")));
                    if !offers_service {
                        *response.status_mut() = StatusCode::NOT_FOUND;
                    }
                    if request.method() == Method::HEAD {
                        *response.body_mut() = Full::new(Bytes::new());
                    }
                    Ok::<_, Infallible>(response)
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Ok((address, task))
}

fn unused_address() -> Result<SocketAddr> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

async fn request(address: SocketAddr) -> Result<(StatusCode, Bytes)> {
    let request = Request::builder()
        .uri(format!("http://{address}/document"))
        .header("host", format!("notes.web.localhost:{}", address.port()))
        .body(Full::new(Bytes::new()))?;
    let response = timeout(
        Duration::from_secs(15),
        Client::builder(TokioExecutor::new())
            .build_http()
            .request(request),
    )
    .await??;
    let status = response.status();
    Ok((status, response.into_body().collect().await?.to_bytes()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_pairing_with_and_without_a_running_agent() -> Result<()> {
    let a_directory = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR"))?;
    let b_directory = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR"))?;
    let (a_gateway, a_task) = gateway(false).await?;
    let (b_gateway, b_task) = gateway(true).await?;
    let a_listen = unused_address()?.to_string();
    let b_listen = unused_address()?.to_string();
    let a_upstream = format!("http://{a_gateway}");
    let b_upstream = format!("http://{b_gateway}");

    let (b, _) = Process::start(
        b_directory.path(),
        &[
            "--device-name",
            "CLI Cloud",
            "--user-name",
            "Jan",
            "--listen",
            &b_listen,
            "--upstream",
            &b_upstream,
        ],
    )
    .await?;
    let b_id = command(b_directory.path(), &["id"]).await?;
    let a_id = command(
        a_directory.path(),
        &[
            "--device-name",
            "CLI Mac",
            "--user-name",
            "Jan",
            "--listen",
            &a_listen,
            "--upstream",
            &a_upstream,
            "id",
        ],
    )
    .await?;
    assert_eq!(command(a_directory.path(), &["id"]).await?, a_id);
    let ticket = command(b_directory.path(), &["invite"]).await?;
    let paired: Value =
        serde_json::from_str(&command(a_directory.path(), &["pair", &ticket]).await?)?;
    assert_eq!(paired["profile"]["device_name"], "CLI Cloud");
    assert_eq!(paired["profile"]["user_name"], "Jan");
    let b_store = StateStore::open(b_directory.path())?;
    assert!(b_store.read::<Config>()?.peers.contains_key(&a_id));

    let (a, a_address) = Process::start(a_directory.path(), &[]).await?;
    let (status, body) = request(a_address).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "from peer");
    let a_store = StateStore::open(a_directory.path())?;
    assert_eq!(
        a_store.read::<Config>()?.routes["notes.web.localhost"],
        b_id
    );

    assert_eq!(
        command(
            a_directory.path(),
            &[
                "--device-name",
                "CLI Mac Updated",
                "--probe-timeout-ms",
                "200",
                "id"
            ]
        )
        .await?,
        a_id
    );
    assert_eq!(a_store.read::<Config>()?.probe_timeout_ms, 200);
    let changed_listen = unused_address()?.to_string();
    assert!(
        command(a_directory.path(), &["--listen", &changed_listen, "id"])
            .await
            .is_err()
    );
    assert_eq!(a_store.read::<Config>()?.listen, a_address);

    let ticket = command(a_directory.path(), &["invite"]).await?;
    command(b_directory.path(), &["pair", &ticket]).await?;
    assert_eq!(
        b_store.read::<Config>()?.peers[&a_id].profile.device_name,
        "CLI Mac Updated"
    );
    let peers: Value = serde_json::from_str(&command(a_directory.path(), &["peers"]).await?)?;
    assert_eq!(peers["peers"][&b_id]["profile"]["device_name"], "CLI Cloud");
    command(a_directory.path(), &["unpair", &b_id]).await?;
    assert!(a_store.read::<Config>()?.peers.is_empty());
    assert!(a_store.read::<Config>()?.routes.is_empty());
    assert_eq!(request(a_address).await?.0, StatusCode::NOT_FOUND);
    drop(a);
    drop(b);
    a_task.abort();
    b_task.abort();
    Ok(())
}
