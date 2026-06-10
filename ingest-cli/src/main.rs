mod cli;
mod commands;

use clap::Parser;
use colored::*;
use ingest_core::ToolError;
use tracing_subscriber::{
    EnvFilter, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

use crate::cli::{Cli, Commands, LogFormat};

fn is_pipe() -> bool {
    !atty::is(atty::Stream::Stdout)
}

fn setup_logging(global: &cli::Global) {
    use tracing_subscriber::fmt::format;

    let log_format = match std::env::var("LOG_FORMAT").ok() {
        Some(s) if s.to_lowercase() == "json" => LogFormat::Json,
        _ => global.log_format,
    };

    let verbosity = std::env::var("INGEST_VERBOSE")
        .ok()
        .and_then(|v| v.parse::<u8>().ok())
        .unwrap_or(global.verbose);

    let builder = EnvFilter::builder().with_default_directive(LevelFilter::WARN.into());

    let filter: EnvFilter = if global.quiet {
        builder
            .parse(format!("{}=error", env!("CARGO_PKG_NAME").to_lowercase()).as_str())
            .expect("invalid filter directive")
    } else if verbosity > 0 {
        let level = match verbosity {
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        };
        builder
            .parse(format!("{}={}", env!("CARGO_PKG_NAME").to_lowercase(), level).as_str())
            .expect("invalid filter directive")
    } else {
        builder.from_env_lossy()
    };

    match log_format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
        LogFormat::Pretty => {
            let fmt = format().with_ansi(!global.no_color);
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().event_format(fmt))
                .init();
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), ToolError> {
    dotenvy::dotenv().ok();

    let cli = Cli::parse();
    let global = cli.global.clone();

    setup_logging(&global);

    if is_pipe() {
        eprintln!(
            "{}",
            "Detected piped output, auto-switching to JSON mode".yellow()
        );
    }

    let server_addr = global.server.clone();

    let result = tokio::select! {
        result = async {
            match cli.command {
                Commands::Run(args) => {
                    commands::handle_run(args, &server_addr).await
                }
                Commands::Server(args) => {
                    commands::handle_server(args).await
                }
                Commands::Status { scope } => commands::handle_status(scope, &server_addr).await,
                Commands::Cancel { scope } => commands::handle_cancel(scope, &server_addr).await,
                Commands::Retry { scope } => commands::handle_retry(scope, &server_addr).await,
                Commands::Files { scope } => commands::handle_files(scope, &server_addr).await,
            }
        } => result,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("{}", "Interrupted by SIGINT (Ctrl+C)".red());
            return Err(ToolError::Interrupted);
        }
    };

    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            let err_msg = format!("{e}");
            eprintln!("{} {}", "Error:".red(), err_msg);

            if err_msg.contains("connect") || err_msg.contains("transport") {
                eprintln!(
                    "{}",
                    "Is the ingest server running? Try `ingest server` or `ingest-server`"
                        .yellow()
                );
            }

            std::process::exit(1);
        }
    }
}
