pub mod artifacts;
pub mod config;
pub mod profile;
pub mod report;
pub mod runner;
#[cfg(feature = "grpc")]
pub mod sources;
pub mod validation;
pub mod verify;

use anyhow::Result;
use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum StreamCommand {
    /// Validate a stream config file (no network). Env names only; never paste secrets.
    CheckConfig {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run one fresh attempt. Source failures abort; there is no reconnect/retry.
    /// Requires a build with `--features grpc`.
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value = "artifacts")]
        artifact_root: PathBuf,
        #[arg(long)]
        smoke_matched_signatures: Option<usize>,
    },
    /// Verify checksums and eligibility without network access.
    Verify {
        #[arg(long)]
        artifact_dir: PathBuf,
    },
    /// Regenerate static reports from artifacts without network access.
    Report {
        #[arg(long)]
        artifact_dir: PathBuf,
        #[arg(long)]
        public_output: Option<PathBuf>,
        /// Publish only a 50k operator-host deshred-vs-processed lifecycle run.
        #[arg(long, requires = "public_output")]
        operator_lifecycle: bool,
    },
    /// Attach reviewed independent-tool evidence to an existing attempt.
    AttachValidation {
        #[arg(long)]
        artifact_dir: PathBuf,
        #[arg(long)]
        attachment: PathBuf,
    },
}

pub fn execute(command: StreamCommand) -> Result<()> {
    match command {
        StreamCommand::CheckConfig { config } => {
            let cfg = config::StreamConfig::load(&config)?;
            println!(
                "ok: profile={} sources={} target={}",
                cfg.run.profile,
                cfg.sources.len(),
                cfg.run.target_matched_signatures
            );
            Ok(())
        }
        StreamCommand::Run {
            config,
            artifact_root,
            smoke_matched_signatures,
        } => runner::run(&config, &artifact_root, smoke_matched_signatures),
        StreamCommand::Verify { artifact_dir } => verify::verify(&artifact_dir),
        StreamCommand::Report {
            artifact_dir,
            public_output,
            operator_lifecycle,
        } => report::render(&artifact_dir, public_output.as_deref(), operator_lifecycle),
        StreamCommand::AttachValidation {
            artifact_dir,
            attachment,
        } => validation::attach(&artifact_dir, &attachment),
    }
}
