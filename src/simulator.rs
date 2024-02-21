//! Represents entry point for simulator and its config.

use serde::Deserialize;

use dslab_core::simulation::Simulation;

#[derive(Debug, Deserialize)]
pub struct SimulatorConfig {
    sim_name: String,
}

pub fn run_simulator(config: SimulatorConfig) {
    println!("Starting simulator with config: {:?}", config);

    let mut sim = Simulation::new(456);

    println!("{:?}", sim.rand());
    println!("{:?}", sim.time());
    println!("{:?}", sim.random_string(21));
}
