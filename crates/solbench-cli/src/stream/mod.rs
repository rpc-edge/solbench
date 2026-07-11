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
    /// Run one fresh attempt. Source failures abort; there is no reconnect/retry.
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
        /// Publish only a 50k RPCEdge deshred-vs-processed operator-host run.
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
