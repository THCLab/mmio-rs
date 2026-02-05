use std::process::Command;

fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");

    let pkg_version = std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string());
    let tag = run_git(&["describe", "--tags", "--exact-match"]);
    let sha = run_git(&["rev-parse", "--short", "HEAD"]);
    let dirty = run_git(&["status", "--porcelain"]).map_or(false, |s| !s.is_empty());

    let version = if tag.is_some() && !dirty {
        tag.unwrap()
    } else if dirty {
        match sha {
            Some(sha) => format!("{pkg_version} ({sha}-dirty)"),
            None => format!("{pkg_version} (dirty)"),
        }
    } else {
        match sha {
            Some(sha) => format!("{pkg_version} ({sha})"),
            None => pkg_version.clone(),
        }
    };

    println!("cargo:rustc-env=M2IO_VERSION={version}");
}
