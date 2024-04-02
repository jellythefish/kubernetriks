mod core;
mod simulator;
mod trace;

use clap::Parser;
use log::info;
use std::env;
use std::rc::Rc;

use crate::simulator::{KubernetriksSimulation, SimulationConfig};

use crate::trace::generic::{GenericClusterTrace, GenericWorkloadTrace};

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    config_file: std::path::PathBuf,
    #[clap(short, long)]
    workload_trace_file: std::path::PathBuf,
    #[clap(long)]
    cluster_trace_file: std::path::PathBuf,
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
        "Path to cluster trace file: {:?}",
        args.cluster_trace_file.canonicalize().unwrap()
    );
    info!(
        "Path to workload trace file: {:?}",
        args.workload_trace_file.canonicalize().unwrap()
    );

    let config_yaml =
        std::fs::read_to_string(&args.config_file).expect("could not read config file");
    let cluster_trace_yaml =
        std::fs::read_to_string(&args.cluster_trace_file).expect("could not read trace file");
    let workload_trace_yaml =
        std::fs::read_to_string(&args.workload_trace_file).expect("could not read trace file");

    let config = Rc::new(serde_yaml::from_str::<SimulationConfig>(&config_yaml).unwrap());
    let mut cluster_trace =
        serde_yaml::from_str::<GenericClusterTrace>(&cluster_trace_yaml).unwrap();
    let mut workload_trace =
        serde_yaml::from_str::<GenericWorkloadTrace>(&workload_trace_yaml).unwrap();

    let mut kubernetriks_simulation = KubernetriksSimulation::new(config);
    kubernetriks_simulation.initialize(&mut cluster_trace, &mut workload_trace);
    kubernetriks_simulation.run();
}
