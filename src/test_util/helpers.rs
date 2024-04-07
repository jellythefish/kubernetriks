use crate::core::node::Node;
use crate::simulator::{KubernetriksSimulation, SimulationConfig};

pub fn check_expected_node_is_equal_to_nodes_in_components(
    expected_node: Node,
    kube_sim: &KubernetriksSimulation,
) {
    let actual_node_api_server = kube_sim
        .api_server
        .borrow()
        .get_node(&expected_node.metadata.name);
    let actual_node_persistent_storage = kube_sim
        .persistent_storage
        .borrow()
        .get_node(&expected_node.metadata.name);
    let actual_node_scheduler = kube_sim
        .scheduler
        .borrow()
        .get_node(&expected_node.metadata.name);

    assert_eq!(&expected_node, &actual_node_api_server);
    assert_eq!(&expected_node, &actual_node_persistent_storage);
    assert_eq!(&expected_node, &actual_node_scheduler);
}

pub fn check_count_of_nodes_in_components_equals_to(
    count: usize,
    kube_sim: &KubernetriksSimulation,
) {
    assert_eq!(count, kube_sim.api_server.borrow_mut().node_count());
    assert_eq!(count, kube_sim.persistent_storage.borrow_mut().node_count());
    assert_eq!(count, kube_sim.scheduler.borrow_mut().node_count());
}

pub fn check_expected_node_appeared_in_components(
    node_name: &str,
    kube_sim: &KubernetriksSimulation,
) {
    // do not throw if exists
    kube_sim.api_server.borrow().get_node(node_name);
    kube_sim.persistent_storage.borrow().get_node(node_name);
    kube_sim.scheduler.borrow().get_node(node_name);
}

pub fn default_test_simulation_config() -> SimulationConfig {
    SimulationConfig {
        sim_name: "test_kubernetriks".to_string(),
        seed: 123,
        node_pool_capacity: 100,
        trace_config: None,
        default_cluster: None,
        scheduling_cycle_interval: 10.0,
        as_to_ps_network_delay: 0.050,
        ps_to_sched_network_delay: 0.010,
        sched_to_as_network_delay: 0.020,
        as_to_node_network_delay: 0.150,
    }
}
