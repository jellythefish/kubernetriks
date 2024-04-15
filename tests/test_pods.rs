use std::rc::Rc;

use dslab_kubernetriks::core::pod::PodConditionType;
use dslab_kubernetriks::simulator::{KubernetriksSimulation, RunUntilAllPodsAreFinishedCallbacks};
use dslab_kubernetriks::trace::generic::{GenericClusterTrace, GenericWorkloadTrace};

use dslab_kubernetriks::test_util::helpers::default_test_simulation_config;

fn make_cluster_trace() -> GenericClusterTrace {
    serde_yaml::from_str(
        &r#"
  events:
  - timestamp: 30
    event_type:
      !CreateNode
        node:
          metadata:
            name: trace_node_42
          status:
            capacity:
              cpu: 2000
              ram: 4294967296
  "#,
    )
    .unwrap()
}

#[test]
fn test_pod_arrived_before_a_node() {
    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config()));
    let mut workload_trace: GenericWorkloadTrace = serde_yaml::from_str(
        &r#"
    events:
    - timestamp: 5
      event_type:
        !CreatePod
          pod:
            metadata:
              name: pod_16
            spec:
              resources:
                requests:
                  cpu: 2000
                  ram: 4294967296
                limits:
                  cpu: 2000
                  ram: 4294967296
              running_duration: 100.0
    "#,
    )
    .unwrap();

    kube_sim.initialize(&mut make_cluster_trace(), &mut workload_trace);
    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks{}));

    let persistent_storage_borrowed = kube_sim.persistent_storage.borrow();

    let pod = persistent_storage_borrowed.get_pod("pod_16").unwrap();
    assert!(
        pod.get_condition(PodConditionType::PodRunning)
            .unwrap()
            .last_transition_time
            > 30.0
    );
    pod.get_condition(PodConditionType::PodSucceeded).unwrap();
}

#[test]
fn test_many_pods_running_one_at_a_time_at_slow_node() {
    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config()));

    let mut workload_trace: GenericWorkloadTrace = serde_yaml::from_str(
        &r#"
    events:
    - timestamp: 40
      event_type:
        !CreatePod
          pod:
            metadata:
              name: pod_0
            spec:
              resources:
                requests:
                  cpu: 2000
                  ram: 4294967296
                limits:
                  cpu: 2000
                  ram: 4294967296
              running_duration: 100.0
    - timestamp: 41
      event_type:
        !CreatePod
          pod:
            metadata:
              name: pod_1
            spec:
              resources:
                requests:
                  cpu: 2000
                  ram: 4294967296
                limits:
                  cpu: 2000
                  ram: 4294967296
              running_duration: 100.0
    - timestamp: 42
      event_type:
        !CreatePod
          pod:
            metadata:
              name: pod_2
            spec:
              resources:
                requests:
                  cpu: 2000
                  ram: 4294967296
                limits:
                  cpu: 2000
                  ram: 4294967296
              running_duration: 100.0
    - timestamp: 43
      event_type:
        !CreatePod
          pod:
            metadata:
              name: pod_3
            spec:
              resources:
                requests:
                  cpu: 2000
                  ram: 4294967296
                limits:
                  cpu: 2000
                  ram: 4294967296
              running_duration: 100.0
    "#,
    )
    .unwrap();

    kube_sim.initialize(&mut make_cluster_trace(), &mut workload_trace);
    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks{}));

    let persistent_storage_borrowed = kube_sim.persistent_storage.borrow();

    let pods = vec![
        persistent_storage_borrowed.get_pod("pod_0"),
        persistent_storage_borrowed.get_pod("pod_1"),
        persistent_storage_borrowed.get_pod("pod_2"),
        persistent_storage_borrowed.get_pod("pod_3"),
    ];

    // all pods succeeded and run sequentially
    for i in 0..pods.len() - 1 {
        let pod_finish_time = pods[i]
            .unwrap()
            .get_condition(PodConditionType::PodSucceeded)
            .unwrap()
            .last_transition_time;
        let next_pod_running_time = pods[i + 1]
            .unwrap()
            .get_condition(PodConditionType::PodRunning)
            .unwrap()
            .last_transition_time;
        assert!(pod_finish_time < next_pod_running_time);
    }

    // last pod succeeded
    pods[pods.len() - 1]
        .unwrap()
        .get_condition(PodConditionType::PodSucceeded)
        .unwrap();
}

#[test]
fn test_node_fits_all_pods() {
    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config()));

    let mut workload_trace: GenericWorkloadTrace = serde_yaml::from_str(
        &r#"
  events:
  - timestamp: 41
    event_type:
      !CreatePod
        pod:
          metadata:
            name: pod_0
          spec:
            resources:
              requests:
                cpu: 333
                ram: 294967296
              limits:
                cpu: 333
                ram: 294967296
            running_duration: 100.0
  - timestamp: 42
    event_type:
      !CreatePod
        pod:
          metadata:
            name: pod_1
          spec:
            resources:
              requests:
                cpu: 333
                ram: 294967296
              limits:
                cpu: 333
                ram: 294967296
            running_duration: 50.0
  - timestamp: 43
    event_type:
      !CreatePod
        pod:
          metadata:
            name: pod_2
          spec:
            resources:
              requests:
                cpu: 333
                ram: 294967296
              limits:
                cpu: 333
                ram: 294967296
            running_duration: 25.0
  "#,
    )
    .unwrap();

    kube_sim.initialize(&mut make_cluster_trace(), &mut workload_trace);
    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks{}));

    let persistent_storage_borrowed = kube_sim.persistent_storage.borrow();

    let pods = vec![
        persistent_storage_borrowed.get_pod("pod_0"),
        persistent_storage_borrowed.get_pod("pod_1"),
        persistent_storage_borrowed.get_pod("pod_2"),
    ];

    // all pods succeeded
    for pod in pods.iter() {
        pod.unwrap()
            .get_condition(PodConditionType::PodSucceeded)
            .unwrap();
    }

    // all pods run parallel
    for i in 0..pods.len() - 1 {
        let pod_finish_time = pods[i]
            .unwrap()
            .get_condition(PodConditionType::PodSucceeded)
            .unwrap()
            .last_transition_time;
        let next_pod_finish_time = pods[i + 1]
            .unwrap()
            .get_condition(PodConditionType::PodSucceeded)
            .unwrap()
            .last_transition_time;
        assert!(pod_finish_time > next_pod_finish_time);
    }
}
