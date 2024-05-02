use crate::core::node::Node;

use crate::simulator::KubernetriksSimulation;

use crate::config::SimulationConfig;

pub fn check_expected_node_is_equal_to_nodes_in_components(
    expected_node: &Node,
    kube_sim: &KubernetriksSimulation,
) {
    let api_server_borrowed = kube_sim.api_server.borrow();
    let persistent_storage_borrowed = kube_sim.persistent_storage.borrow();
    let scheduler_borrowed = kube_sim.scheduler.borrow();

    assert_eq!(
        expected_node,
        api_server_borrowed
            .get_node_component(&expected_node.metadata.name)
            .unwrap()
            .borrow()
            .get_node()
    );
    assert_eq!(
        expected_node,
        persistent_storage_borrowed
            .get_node(&expected_node.metadata.name)
            .unwrap()
    );
    assert_eq!(
        expected_node,
        scheduler_borrowed.get_node(&expected_node.metadata.name)
    );
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
    let api_server_borrowed = kube_sim.api_server.borrow();

    api_server_borrowed
        .get_node_component(node_name)
        .unwrap()
        .borrow()
        .get_node();
    kube_sim.persistent_storage.borrow().get_node(node_name);
    kube_sim.scheduler.borrow().get_node(node_name);
}

pub fn default_test_simulation_config(with_suffix: Option<&str>) -> SimulationConfig {
    let mut default = r#"
    sim_name: "test_kubernetriks"
    seed: 123
    node_pool_capacity: 10
    scheduling_cycle_interval: 10.0
    as_to_ps_network_delay: 0.050
    ps_to_sched_network_delay: 0.010
    sched_to_as_network_delay: 0.020
    as_to_node_network_delay: 0.150
    as_to_ca_network_delay: 0.30
    "#
    .to_string();

    if !with_suffix.is_none() {
        default.push_str(with_suffix.unwrap());
    }

    serde_yaml::from_str::<SimulationConfig>(&default).unwrap()
}
