mod core;
mod trace;

use dslab_core::simulation::Simulation;

fn main() {
    let mut sim = Simulation::new(456);

    println!("{:?}", sim.rand());
    println!("{:?}", sim.time());
    println!("{:?}", sim.random_string(21));
}
