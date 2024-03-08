mod core;
mod simulator;
mod trace;

use clap::Parser;
use log::info;
use std::env;
use std::rc::Rc;

use crate::simulator::{run_simulator, SimulatorConfig};
use crate::trace::generic::GenericTrace;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    config_file: std::path::PathBuf,
    #[clap(short, long)]
    trace_file: std::path::PathBuf,
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
        args.config_file.canonicalize().unwrap()
    );
    info!(
        "Path to trace file: {:?}",
        args.trace_file.canonicalize().unwrap()
    );

    let config_yaml =
        std::fs::read_to_string(&args.config_file).expect("could not read config file");
    let trace_yaml = std::fs::read_to_string(&args.trace_file).expect("could not read trace file");

    let config = Rc::new(serde_yaml::from_str::<SimulatorConfig>(&config_yaml).unwrap());
    let trace = serde_yaml::from_str::<GenericTrace>(&trace_yaml).unwrap();

    run_simulator(config, trace);
}
