use clap::Parser;
use fantasma_cli::{app::App, cli::Cli, config::default_config_path};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let app = App::new(default_config_path()?);
    let output = app.run(cli).await?;
    if !output.stdout.is_empty() {
        println!("{}", output.stdout);
    }
    Ok(())
}
