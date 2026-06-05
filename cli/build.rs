use std::{env, fs, path::Path};

fn main() {
    println!("cargo:rerun-if-changed=.env.production");
    for key in ["GAME_BACKEND_URL", "GAME_CLI_REPO", "GAME_ENV"] {
        println!("cargo:rerun-if-env-changed={key}");
    }

    for key in ["GAME_BACKEND_URL", "GAME_CLI_REPO", "GAME_ENV"] {
        if let Ok(value) = env::var(key) {
            if !value.trim().is_empty() {
                println!("cargo:rustc-env={key}={value}");
            }
        }
    }

    if env::var("PROFILE").as_deref() != Ok("release") {
        return;
    }

    let path = Path::new(".env.production");
    let Ok(contents) = fs::read_to_string(path) else {
        return;
    };

    for line in contents.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        for key in ["GAME_BACKEND_URL", "GAME_CLI_REPO", "GAME_ENV"] {
            let prefix = format!("{key}=");
            if let Some(value) = line.strip_prefix(&prefix) {
                println!("cargo:rustc-env={key}={}", value.trim_matches('"'));
            }
        }
    }
}
