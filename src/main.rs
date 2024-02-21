mod core;
mod simulator;
mod trace;

use crate::simulator::{run_simulator, SimulatorConfig};

use clap::Parser;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    config_path: std::path::PathBuf,
}

fn main() {
    let args = Args::parse();

    println!(
        "Path to config file: {:?}",
        args.config_path
            .canonicalize()
            .expect(&format!("no such config file {:?}", args.config_path))
    );
    let config_yaml =
        std::fs::read_to_string(&args.config_path).expect("could not read config file");
    let config: SimulatorConfig = serde_yaml::from_str(&config_yaml).unwrap();

    run_simulator(config);
}
