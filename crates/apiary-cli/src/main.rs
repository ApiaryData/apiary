//! Apiary CLI — the command-line interface for the Apiary runtime.
//!
//! This is a placeholder for the v1 CLI. It will support:
//! - `apiary start [--storage <uri>]` — start a local node
//! - `apiary sql "SELECT ..."` — one-shot query
//! - `apiary shell` — Python REPL with Apiary pre-configured

use tracing_subscriber::EnvFilter;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    println!("Apiary v{}", env!("CARGO_PKG_VERSION"));
    println!("CLI not yet implemented. Use the Python SDK:");
    println!();
    println!("  from apiary import Apiary");
    println!("  ap = Apiary(\"production\")");
    println!("  ap.start()");
}
