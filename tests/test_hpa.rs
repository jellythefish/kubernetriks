use std::rc::Rc;

use dslab_kubernetriks::{
    autoscalers::horizontal_pod_autoscaler::kube_horizontal_pod_autoscaler::KubeHorizontalPodAutoscalerConfig,
    simulator::KubernetriksSimulation,
    test_util::helpers::default_test_simulation_config,
    trace::generic::{GenericClusterTrace, GenericWorkloadTrace},
};

fn get_cluster_trace() -> GenericClusterTrace {
    serde_yaml::from_str(
        &r#"
  events:
  - timestamp: 5.0
    event_type:
      !CreateNode
        node:
          metadata:
            name: trace_node_42
          status:
            capacity:
              cpu: 64000
              ram: 68719476736
  "#,
    )
    .unwrap()
}

fn get_workload_trace() -> GenericWorkloadTrace {
    serde_yaml::from_str(
        &r#"
  events:
  - timestamp: 59.5
    event_type:
      !CreatePodGroup
        pod_group:
          name: pod_group_1
          initial_pod_count: 5
          max_pod_count: 100
          pod_template:
            metadata:
              name: pod_group_1
            spec:
              resources:
                requests:
                  cpu: 100
                  ram: 104857600
                limits:
                  cpu: 100
                  ram: 104857600
          target_resources_usage:
            cpu_utilization: 0.6
          resources_usage_model_config:
            cpu_config:
              model_name: pod_group
              config: |
                - duration: 500.0
                  total_load: 8
                - duration: 200.0
                  total_load: 2
  "#,
    )
    .unwrap()
}

fn pod_group_len(kube_sim: &KubernetriksSimulation) -> usize {
    let pod_groups = &kube_sim
        .horizontal_pod_autoscaler
        .as_ref()
        .unwrap()
        .borrow()
        .pod_groups;
    pod_groups.get("pod_group_1").unwrap().created_pods.len()
}

#[test]
fn test_pod_group_created_and_scaled_by_cpu_utilization() {
    let mut config = default_test_simulation_config(None);
    config.horizontal_pod_autoscaler.enabled = true;
    config
        .horizontal_pod_autoscaler
        .kube_horizontal_pod_autoscaler_config = Some(KubeHorizontalPodAutoscalerConfig::default());

    let mut workload_trace = get_workload_trace();
    let mut cluster_trace = get_cluster_trace();

    let mut kube_sim = KubernetriksSimulation::new(Rc::new(config));
    kube_sim.initialize(&mut cluster_trace, &mut workload_trace);

    // HPA actions at 60, 120, 180, 240, ..., 1080
    kube_sim.step_until_time(61.0);
    assert_eq!(5, pod_group_len(&kube_sim));
    // after first hpa step at 60.0: load = 8, pods = 5, utilization = 8 / 5 = 1.6 = 1.0, desired = ceil(5 * 1.0 / 0.6) = 9

    kube_sim.step_until_time(121.0);
    assert_eq!(9, pod_group_len(&kube_sim));
    // after second hpa step at 120.0: load = 8, pods = 9, utilization = 8 / 9 = 0.8888, desired = ceil(9 * 0.8888 / 0.6) = 14

    kube_sim.step_until_time(181.0);
    assert_eq!(14, pod_group_len(&kube_sim));
    // after third hpa step at 180: load = 8, pods = 14, utilization = 8 / 14 = 0.5714 (/ 0.6 ~ 0.95 - within tolerance 0.1)

    kube_sim.step_until_time(450.0);
    assert_eq!(14, pod_group_len(&kube_sim));
    // stabilized at 14 until load decrease at time > 500.0

    // at 540: load = 2, pods = 14, utilization = 2 / 14 = 0.1428, desired = ceil(14 * 0.1428 / 0.6) = 4

    kube_sim.step_until_time(600.5);
    assert_eq!(4, pod_group_len(&kube_sim));
    // after hpa step at 540: load = 2, pods = 4, utilization = 2 / 4 = 0.5, desired = ceil(4 * 0.5 / 0.6) = 4

    kube_sim.step_until_time(759.5);
    assert_eq!(4, pod_group_len(&kube_sim));
    // stabilized at 4 until load increase at time > 759.5

    // and again load = 8 - cycled

    // at 720: load = 8, pods = 4, utilization = 8 / 4 = 2 = 1.0, desired = ceil(4 * 1.0 / 0.6) = 7

    kube_sim.step_until_time(781.0);
    assert_eq!(7, pod_group_len(&kube_sim));
    // after hpa step at 780: load = 8, pods = 7, utilization = 8 / 7 = 1.14 = 1.0, desired = ceil(7 * 1.0 / 0.6) = 12

    kube_sim.step_until_time(841.0);
    assert_eq!(12, pod_group_len(&kube_sim));
    // after hpa step at 840: load = 8, pods = 12, utilization = 8 / 12 = 0.66667, desired = ceil(12 * 0.66667 / 0.6) = 14

    kube_sim.step_until_time(901.0);
    assert_eq!(14, pod_group_len(&kube_sim));
    // after hpa step at 900: load = 8, pods = 14, utilization = 8 / 14 = 0.5714 (/ 0.6 ~ 0.95 - within tolerance 0.1)

    kube_sim.step_until_time(1200.0);
    assert_eq!(14, pod_group_len(&kube_sim));
    // stabilized
}
