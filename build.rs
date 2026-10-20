use std::{env, fs, path::Path};

fn main() {
    println!("cargo:rerun-if-changed=ui/dist");
    if env::var_os("CARGO_FEATURE_WEB_UI").is_none() {
        return;
    }
    let root = env::current_dir().unwrap().join("ui/dist");
    assert!(
        root.join("index.html").is_file(),
        "Build the embedded UI first with `deno task build:ui`, or use --no-default-features."
    );
    let mut assets = Vec::new();
    collect(&root, &root, &mut assets);
    assets.sort();
    let mut source = String::from(
        "pub(super) fn asset(path: &str) -> Option<(&'static str, &'static [u8])> {\nmatch path {\n",
    );
    for (name, path) in assets {
        let mime = match path.extension().and_then(|value| value.to_str()) {
            Some("html") => "text/html; charset=utf-8",
            Some("js") => "text/javascript; charset=utf-8",
            Some("css") => "text/css; charset=utf-8",
            Some("svg") => "image/svg+xml",
            _ => panic!("unexpected UI asset: {}", path.display()),
        };
        source.push_str(&format!(
            "{name:?} => Some(({mime:?}, include_bytes!({path:?}))),\n"
        ));
    }
    source.push_str("_ => None,\n}\n}\n");
    fs::write(
        Path::new(&env::var_os("OUT_DIR").unwrap()).join("ui_assets.rs"),
        source,
    )
    .unwrap();
}

fn collect(root: &Path, directory: &Path, assets: &mut Vec<(String, std::path::PathBuf)>) {
    for entry in fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if entry.file_type().unwrap().is_dir() {
            collect(root, &path, assets);
        } else {
            assets.push((
                path.strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace('\\', "/"),
                path,
            ));
        }
    }
}
