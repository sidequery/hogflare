use tracing::error;

#[tokio::main]
async fn main() {
    if let Err(err) = hogflare::run().await {
        error!(error = %err, "failed to start server");
        eprintln!("failed to start server: {err}");
        std::process::exit(1);
    }
}
