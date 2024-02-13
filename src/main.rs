mod core;

use dslab_core::simulation::Simulation;

use crate::core::node::Node;
use crate::core::pod::Pod;

fn main() {
    let mut sim = Simulation::new(456);

    let node = Node::new();
    let pod = Pod::new();

    println!("{:?}", sim.rand());
    println!("{:?}", sim.time());
    println!("{:?}", sim.random_string(21));
    println!("Node: {:?}, pod {:?}", node, pod);
}
