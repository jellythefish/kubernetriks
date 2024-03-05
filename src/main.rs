mod core;
mod simulator;
mod trace;

use clap::Parser;
use log::info;
use std::env;

use crate::simulator::{run_simulator, SimulatorConfig};
use crate::trace::generic::GenericTrace;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    config_path: std::path::PathBuf,
    #[clap(short, long)]
    trace_path: std::path::PathBuf,
}

fn main() {
    // log level INFO by default
    let mut env_logger_builder = env_logger::builder();
    if env::var("RUST_LOG").is_err() {
        env_logger_builder.filter_level(log::LevelFilter::Info);
    }
    env_logger_builder.init();

    let args = Args::parse();

    info!(
        "Path to config file: {:?}",
        args.config_path.canonicalize().unwrap()
    );
    info!(
        "Path to trace file: {:?}",
        args.trace_path.canonicalize().unwrap()
    );

    let config_yaml =
        std::fs::read_to_string(&args.config_path).expect("could not read config file");
    let trace_yaml = std::fs::read_to_string(&args.trace_path).expect("could not read trace file");

    let config: SimulatorConfig = serde_yaml::from_str(&config_yaml).unwrap();
    let trace: GenericTrace = serde_yaml::from_str(&trace_yaml).unwrap();

    run_simulator(config, trace);
}
