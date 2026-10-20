//! Public static shell; all live data and actions use the authenticated control API.

use hyper::{Method, Response, StatusCode};

use crate::http::{self, Body};

include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));

pub(crate) fn serve(method: &Method, path: &str) -> Response<Body> {
    if !matches!(*method, Method::GET | Method::HEAD) {
        let mut response = http::error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "use GET or HEAD",
        );
        response
            .headers_mut()
            .insert("allow", "GET, HEAD".parse().unwrap());
        return response;
    }
    if path == "/_xe/ui" {
        return Response::builder()
            .status(StatusCode::PERMANENT_REDIRECT)
            .header("location", "/_xe/ui/")
            .body(http::full(""))
            .unwrap();
    }
    let name = path.strip_prefix("/_xe/ui/").unwrap_or_default();
    let name = if name.is_empty() { "index.html" } else { name };
    let Some((mime, bytes)) = asset(name) else {
        return http::error(StatusCode::NOT_FOUND, "asset_not_found", "unknown UI asset");
    };
    Response::builder()
        .header("content-type", mime)
        .header("cache-control", "no-store")
        .header("x-content-type-options", "nosniff")
        .header("referrer-policy", "no-referrer")
        .header("content-security-policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
        .body(http::full(if *method == Method::HEAD { &[][..] } else { bytes }))
        .unwrap()
}
