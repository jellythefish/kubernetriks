use std::{collections::HashMap, rc::Rc};

use dslab_kubernetriks::{
    core::{
        common::RuntimeResources,
        node::{Node, NodeCondition, NodeConditionType, NodeStatus},
    },
    simulator::KubernetriksSimulation,
};

fn get_default_config_yaml() -> &'static str {
    r#"
    sim_name: "kubernetriks"
    seed: 123
    node_pool_capacity: 10
    as_to_ps_network_delay: 50
    ps_to_sched_network_delay: 89
    sched_to_as_network_delay: 23
    as_to_node_network_delay: 152
    "#
}

fn make_default_node(cpu: u32, ram: u64) -> Node {
    Node {
        metadata: Default::default(),
        status: NodeStatus {
            capacity: RuntimeResources { cpu, ram },
            allocatable: RuntimeResources { cpu, ram },
            conditions: vec![NodeCondition {
                status: "True".to_owned(),
                condition_type: NodeConditionType::NodeCreated,
                last_transition_time: 0.0,
            }],
        },
        spec: Default::default(),
    }
}

fn check_count_of_nodes_in_components_equals_to(count: usize, kube_sim: &KubernetriksSimulation) {
    assert_eq!(count, kube_sim.api_server.borrow_mut().created_nodes.len());
    assert_eq!(
        count,
        kube_sim
            .persistent_storage
            .borrow_mut()
            .storage_data
            .nodes
            .len()
    );
    assert_eq!(
        count,
        kube_sim.scheduler.borrow_mut().objects_cache.nodes.len()
    );
}

fn check_expected_node_is_equal_to_nodes_in_components(
    expected_node: Node,
    kube_sim: &KubernetriksSimulation,
) {
    let actual_node_api_server = kube_sim
        .api_server
        .borrow()
        .created_nodes
        .get(&expected_node.metadata.name)
        .unwrap()
        .borrow()
        .runtime
        .as_ref()
        .unwrap()
        .node
        .clone();
    let actual_node_persistent_storage = kube_sim
        .persistent_storage
        .borrow()
        .storage_data
        .nodes
        .get(&expected_node.metadata.name)
        .unwrap()
        .clone();
    let actual_node_scheduler = kube_sim
        .scheduler
        .borrow()
        .objects_cache
        .nodes
        .get(&expected_node.metadata.name)
        .unwrap()
        .clone();
    assert_eq!(&expected_node, &actual_node_api_server);
    assert_eq!(&expected_node, &actual_node_persistent_storage);
    assert_eq!(&expected_node, &actual_node_scheduler);
}

#[test]
fn test_config_default_cluster_is_none() {
    let mut kube_sim = KubernetriksSimulation::new(Rc::new(
        serde_yaml::from_str(get_default_config_yaml()).unwrap(),
    ));
    kube_sim.initialize_default_cluster();

    assert_eq!(0, kube_sim.api_server.borrow_mut().created_nodes.len());
    assert_eq!(
        0,
        kube_sim
            .persistent_storage
            .borrow_mut()
            .storage_data
            .nodes
            .len()
    );
    assert_eq!(0, kube_sim.scheduler.borrow_mut().objects_cache.nodes.len());
}

#[test]
fn test_config_default_cluster_with_no_name_prefix() {
    let mut config_yaml = get_default_config_yaml().to_string();
    config_yaml.push_str(
        r#"
    default_cluster:
    - node_count: 10
      node_template:
          metadata:
            labels:
              storage_type: ssd
              proc_type: intel
          status:
            capacity:
              cpu: 18000
              ram: 18589934592
    - node_count: 20
      node_template:
          status:
            capacity:
              cpu: 24000
              ram: 18589934592
    "#,
    );
    let mut kube_sim =
        KubernetriksSimulation::new(Rc::new(serde_yaml::from_str(&config_yaml).unwrap()));
    kube_sim.initialize_default_cluster();

    check_count_of_nodes_in_components_equals_to(30, &kube_sim);

    for idx in 0..10 {
        let mut expected_node = make_default_node(18000, 18589934592);
        expected_node.metadata.name = format!("default_node_{}", idx);
        expected_node.metadata.labels = HashMap::from([
            ("storage_type".to_string(), "ssd".to_string()),
            ("proc_type".to_string(), "intel".to_string()),
        ]);

        check_expected_node_is_equal_to_nodes_in_components(expected_node, &kube_sim);
    }

    for idx in 10..30 {
        let mut expected_node = make_default_node(24000, 18589934592);
        expected_node.metadata.name = format!("default_node_{}", idx);

        check_expected_node_is_equal_to_nodes_in_components(expected_node, &kube_sim);
    }
}

#[test]
fn test_config_default_cluster_no_node_count() {
    let mut config_yaml = get_default_config_yaml().to_string();
    config_yaml.push_str(
        r#"
    default_cluster:
    - node_template:
        status:
          capacity:
            cpu: 24000
            ram: 18589934592
    - node_template:
        status:
          capacity:
            cpu: 12000
            ram: 10589934592
    - node_count: 1
      node_template:
        status:
          capacity:
            cpu: 6000
            ram: 185899345
    - node_count: 1
      node_template:
        status:
          capacity:
            cpu: 8000
            ram: 185899345
    "#,
    );

    let mut kube_sim =
        KubernetriksSimulation::new(Rc::new(serde_yaml::from_str(&config_yaml).unwrap()));
    kube_sim.initialize_default_cluster();

    check_count_of_nodes_in_components_equals_to(4, &kube_sim);

    let mut expected_node1 = make_default_node(24000, 18589934592);
    expected_node1.metadata.name = "default_node_0".to_string();
    check_expected_node_is_equal_to_nodes_in_components(expected_node1, &kube_sim);

    let mut expected_node2 = make_default_node(12000, 10589934592);
    expected_node2.metadata.name = "default_node_1".to_string();
    check_expected_node_is_equal_to_nodes_in_components(expected_node2, &kube_sim);

    let mut expected_node3 = make_default_node(6000, 185899345);
    expected_node3.metadata.name = "default_node_2".to_string();
    check_expected_node_is_equal_to_nodes_in_components(expected_node3, &kube_sim);

    let mut expected_node4 = make_default_node(8000, 185899345);
    expected_node4.metadata.name = "default_node_3".to_string();
    check_expected_node_is_equal_to_nodes_in_components(expected_node4, &kube_sim);
}

#[test]
fn test_config_default_cluster_has_name_prefix() {
    let mut config_yaml = get_default_config_yaml().to_string();
    config_yaml.push_str(
        r#"
    default_cluster:
    - node_count: 2
      node_template:
        metadata:
          name: node_group_1
        status:
          capacity:
            cpu: 32000
            ram: 18589934592
    - node_count: 1
      node_template:
        metadata:
          name: exact_node_name
        status:
          capacity:
            cpu: 6000
            ram: 185899345
    - node_template:
        metadata:
          name: exact_node_name_2
        status:
          capacity:
            cpu: 4000
            ram: 185899345
    "#,
    );

    let mut kube_sim =
        KubernetriksSimulation::new(Rc::new(serde_yaml::from_str(&config_yaml).unwrap()));
    kube_sim.initialize_default_cluster();

    check_count_of_nodes_in_components_equals_to(4, &kube_sim);

    let mut expected_node1 = make_default_node(32000, 18589934592);
    let mut expected_node2 = expected_node1.clone();
    expected_node1.metadata.name = "node_group_1_0".to_string();
    expected_node2.metadata.name = "node_group_1_1".to_string();
    check_expected_node_is_equal_to_nodes_in_components(expected_node1, &kube_sim);
    check_expected_node_is_equal_to_nodes_in_components(expected_node2, &kube_sim);

    let mut expected_node3 = make_default_node(6000, 185899345);
    expected_node3.metadata.name = "exact_node_name".to_string();
    check_expected_node_is_equal_to_nodes_in_components(expected_node3, &kube_sim);

    let mut expected_node4 = make_default_node(4000, 185899345);
    expected_node4.metadata.name = "exact_node_name_2".to_string();
    check_expected_node_is_equal_to_nodes_in_components(expected_node4, &kube_sim);
}
