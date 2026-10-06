use anyhow::{Result, ensure};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{
    HeaderMap, Response, StatusCode, Uri,
    body::Incoming,
    header::{CONNECTION, HOST, UPGRADE},
};

pub(crate) type Body = UnsyncBoxBody<Bytes, anyhow::Error>;
pub(crate) const ERROR_HEADER: &str = "x-xe-connect-error";
pub(crate) const SOURCE_HEADER: &str = "x-xe-source-peer";
pub(crate) const GATEWAY_HEADER: &str = "x-xe-gateway-token";

pub(crate) fn body(body: Incoming) -> Body {
    body.map_err(anyhow::Error::new).boxed_unsync()
}

pub(crate) fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

pub(crate) fn json(value: impl serde::Serialize) -> Response<Body> {
    Response::builder()
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(full(
            serde_json::to_vec(&value).expect("serialize response"),
        ))
        .unwrap()
}

pub(crate) fn error(
    status: StatusCode,
    code: &str,
    message: impl std::fmt::Display,
) -> Response<Body> {
    let mut response = json(serde_json::json!({ "error": code, "message": message.to_string() }));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(ERROR_HEADER, code.parse().unwrap());
    response
}

/// Accept the launcher's one- and two-label .localhost routes, never arbitrary hosts.
pub(crate) fn hostname(authority: &str) -> Result<String> {
    let authority: hyper::http::uri::Authority = authority.parse()?;
    ensure!(!authority.as_str().contains('@'), "invalid hostname");
    if authority.as_str().contains(':') {
        ensure!(authority.port_u16().is_some(), "invalid port");
    }
    let name = authority.host().to_ascii_lowercase();
    let labels = name
        .strip_suffix(".localhost")
        .ok_or_else(|| anyhow::anyhow!("only .localhost services are routed"))?;
    let labels: Vec<_> = labels.split('.').collect();
    ensure!(
        (1..=2).contains(&labels.len()),
        "expected one or two labels before .localhost"
    );
    for label in labels {
        ensure!(
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
            "invalid service hostname"
        );
    }
    Ok(name)
}

pub(crate) fn request_hostname(uri: &Uri, headers: &HeaderMap) -> Result<String> {
    ensure!(
        uri.scheme_str().is_none_or(|s| s == "http"),
        "standalone listener accepts HTTP only"
    );
    let host = headers
        .get(HOST)
        .ok_or_else(|| anyhow::anyhow!("missing Host header"))?
        .to_str()?;
    if let Some(authority) = uri.authority() {
        ensure!(
            authority.as_str().eq_ignore_ascii_case(host),
            "request URL and Host disagree"
        );
    }
    hostname(host)
}

/// Strip connection-specific headers while preserving a requested HTTP upgrade.
pub(crate) fn clean_headers(headers: &mut HeaderMap, upgrade: bool) {
    let nominated: Vec<String> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|v| v.trim().to_ascii_lowercase())
        .collect();
    for name in nominated {
        if !(upgrade && name == "upgrade") {
            headers.remove(name);
        }
    }
    for name in [
        "connection",
        "proxy-connection",
        "proxy-authenticate",
        "proxy-authorization",
        "keep-alive",
        "transfer-encoding",
        "te",
    ] {
        headers.remove(name);
    }
    for name in [
        SOURCE_HEADER,
        ERROR_HEADER,
        GATEWAY_HEADER,
        "x-xe-target-peer",
        "forwarded",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
    ] {
        headers.remove(name);
    }
    if upgrade {
        headers.insert(CONNECTION, "upgrade".parse().unwrap());
    } else {
        headers.remove(UPGRADE);
    }
}
