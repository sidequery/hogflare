use hogflare::importer::{run_import, ImportConfig, ImportError};

#[tokio::main]
async fn main() {
    dotenvy::from_filename(".env.local")
        .or_else(|_| dotenvy::dotenv())
        .ok();

    match run().await {
        Ok(()) => {}
        Err(ImportError::Usage(message)) => {
            println!("{message}");
        }
        Err(err) => {
            eprintln!("posthog import failed: {err}");
            std::process::exit(1);
        }
    }
}

async fn run() -> Result<(), ImportError> {
    let config = ImportConfig::from_env_and_args(std::env::args().skip(1))?;
    let dry_run = config.dry_run;
    let summary = run_import(config).await?;

    let mode = if dry_run { "dry run" } else { "import" };
    println!(
        "PostHog {mode} complete: persons={}, person_snapshots={}, groups={}, events={}, skipped={}, pipeline_batches={}",
        summary.persons,
        summary.person_snapshots,
        summary.groups,
        summary.events,
        summary.skipped,
        summary.pipeline_batches
    );

    Ok(())
}
