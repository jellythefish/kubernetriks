use std::rc::Rc;
use std::vec;

use dslab_kubernetriks::core::node::Node;
use dslab_kubernetriks::simulator::{KubernetriksSimulation, NodeGroup, SimulationConfig};
use dslab_kubernetriks::trace::generic::{GenericClusterTrace, GenericWorkloadTrace};

use dslab_kubernetriks::test_util::helpers::{
    check_count_of_nodes_in_components_equals_to, check_expected_node_appeared_in_components,
    default_test_simulation_config,
};

#[test]
fn test_node_creation_from_trace_and_default_cluster() {
    // 1 node comes from default cluster and 1 node from trace
    let node1 = Node::new("my_node_1".to_string(), 16000, 8589934592);
    let node2 = Node::new("trace_node_25".to_string(), 16000, 17179869184);

    let mut config: SimulationConfig = default_test_simulation_config();
    config.default_cluster = Some(vec![NodeGroup {
        node_count: Some(1),
        node_template: node1.clone(),
    }]);
    let mut cluster_trace: GenericClusterTrace = serde_yaml::from_str(
        &r#"
    events:
    - timestamp: 30
      event_type:
        !CreateNode
          node:
            metadata:
              name: trace_node_25
            status:
              capacity:
                cpu: 16000
                ram: 17179869184
    "#,
    )
    .unwrap();
    let mut workload_trace: GenericWorkloadTrace = Default::default();

    let mut kube_sim = KubernetriksSimulation::new(Rc::new(config));

    kube_sim.initialize(&mut cluster_trace, &mut workload_trace);

    check_count_of_nodes_in_components_equals_to(1, &kube_sim);
    check_expected_node_appeared_in_components(&node1.metadata.name, &kube_sim);

    // handle trace events
    kube_sim.step_for_duration(1000.0);

    check_count_of_nodes_in_components_equals_to(2, &kube_sim);
    check_expected_node_appeared_in_components(&node2.metadata.name, &kube_sim);
}
