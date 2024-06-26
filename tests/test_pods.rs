use std::rc::Rc;

use dslab_kubernetriks::core::node::Node;
use dslab_kubernetriks::core::pod::{Pod, PodConditionType};
use dslab_kubernetriks::simulation_callbacks::RunUntilAllPodsAreFinishedCallbacks;
use dslab_kubernetriks::simulator::KubernetriksSimulation;
use dslab_kubernetriks::trace::generic::{
    ClusterEvent, ClusterEventType, GenericClusterTrace, GenericWorkloadTrace, WorkloadEvent,
    WorkloadEventType,
};

use dslab_kubernetriks::test_util::helpers::default_test_simulation_config;

// Helper functions

fn get_cluster_trace() -> GenericClusterTrace {
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

fn get_workload_trace() -> GenericWorkloadTrace {
    serde_yaml::from_str(
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
                ram: 4967296
              limits:
                cpu: 333
                ram: 4967296
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
                ram: 4967296
              limits:
                cpu: 333
                ram: 4967296
            running_duration: 100.0
"#,
    )
    .unwrap()
}

#[test]
fn test_pod_arrived_before_a_node() {
    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config(None)));
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

    kube_sim.initialize(&mut get_cluster_trace(), &mut workload_trace);
    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks {}));

    let persistent_storage_borrowed = kube_sim.persistent_storage.borrow();

    let pod = persistent_storage_borrowed
        .succeeded_pods
        .get("pod_16")
        .unwrap();
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

    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config(None)));
    kube_sim.initialize(&mut get_cluster_trace(), &mut workload_trace);
    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks {}));

    let persistent_storage_borrowed = kube_sim.persistent_storage.borrow();

    let pods = vec![
        persistent_storage_borrowed
            .succeeded_pods
            .get("pod_0")
            .unwrap(),
        persistent_storage_borrowed
            .succeeded_pods
            .get("pod_1")
            .unwrap(),
        persistent_storage_borrowed
            .succeeded_pods
            .get("pod_2")
            .unwrap(),
        persistent_storage_borrowed
            .succeeded_pods
            .get("pod_3")
            .unwrap(),
    ];

    // all pods succeeded but ran in unspecified order
    for i in 0..pods.len() {
        pods[i]
            .get_condition(PodConditionType::PodSucceeded)
            .unwrap()
            .last_transition_time;
    }
}

#[test]
fn test_node_fits_all_pods() {
    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config(None)));

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

    kube_sim.initialize(&mut get_cluster_trace(), &mut workload_trace);
    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks {}));

    let persistent_storage_borrowed = kube_sim.persistent_storage.borrow();

    let pods = vec![
        persistent_storage_borrowed
            .succeeded_pods
            .get("pod_0")
            .unwrap(),
        persistent_storage_borrowed
            .succeeded_pods
            .get("pod_1")
            .unwrap(),
        persistent_storage_borrowed
            .succeeded_pods
            .get("pod_2")
            .unwrap(),
    ];

    // all pods succeeded
    for pod in pods.iter() {
        pod.get_condition(PodConditionType::PodSucceeded).unwrap();
    }

    // all pods run parallel
    for i in 0..pods.len() - 1 {
        let pod_finish_time = pods[i]
            .get_condition(PodConditionType::PodSucceeded)
            .unwrap()
            .last_transition_time;
        let next_pod_finish_time = pods[i + 1]
            .get_condition(PodConditionType::PodSucceeded)
            .unwrap()
            .last_transition_time;
        assert!(pod_finish_time > next_pod_finish_time);
    }
}

#[test]
fn test_node_remove_while_pods_were_running() {
    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config(None)));

    let mut workload_trace: GenericWorkloadTrace = get_workload_trace();

    let mut cluster_trace = get_cluster_trace();
    cluster_trace.events.push(ClusterEvent {
        timestamp: 60.0,
        event_type: ClusterEventType::RemoveNode {
            node_name: "trace_node_42".to_string(),
        },
    });

    cluster_trace.events.push(ClusterEvent {
        timestamp: 1100.0,
        event_type: ClusterEventType::CreateNode {
            node: Node::new("trace_node_42".to_string(), 2000, 4294967296),
        },
    });

    kube_sim.initialize(&mut cluster_trace, &mut workload_trace);
    kube_sim.step_for_duration(1000.0);

    let total_pods = kube_sim
        .metrics_collector
        .borrow()
        .accumulated_metrics
        .total_pods_in_trace;
    assert_eq!(2, total_pods);

    assert_eq!(
        0,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_succeeded
    );

    kube_sim.step_for_duration(2000.0);
    // node returns at 1100.0
    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_succeeded
    );
}

#[test]
fn test_node_removed_at_the_same_time_as_assignment() {
    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config(None)));

    let mut workload_trace: GenericWorkloadTrace = get_workload_trace();

    let mut cluster_trace = get_cluster_trace();
    // assignment also happens at timestamp near 50 as scheduling cycle goes with 10.0 sec step
    cluster_trace.events.push(ClusterEvent {
        timestamp: 50.0,
        event_type: ClusterEventType::RemoveNode {
            node_name: "trace_node_42".to_string(),
        },
    });

    kube_sim.initialize(&mut cluster_trace, &mut workload_trace);
    kube_sim.step_for_duration(1000.0);

    let total_pods = kube_sim
        .metrics_collector
        .borrow()
        .accumulated_metrics
        .total_pods_in_trace;
    let pods_succeeded = kube_sim
        .metrics_collector
        .borrow()
        .accumulated_metrics
        .pods_succeeded;

    assert_eq!(2, total_pods);
    assert_eq!(0, pods_succeeded);
}

#[test]
fn test_pod_removals() {
    let mut cluster_trace = get_cluster_trace();
    let mut workload_trace = get_workload_trace();
    workload_trace.events.push(WorkloadEvent {
        timestamp: 71.0,
        event_type: WorkloadEventType::RemovePod {
            pod_name: "pod_1".to_string(),
        },
    });

    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config(None)));
    kube_sim.initialize(&mut cluster_trace, &mut workload_trace);

    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks {}));

    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .internal
            .terminated_pods
    );
    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .total_pods_in_trace
    );
    assert_eq!(
        1,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_succeeded
    );
    assert_eq!(
        1,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_removed
    );
}

#[test]
fn test_pod_removal_concurrently_with_node_removal() {
    let mut cluster_trace = get_cluster_trace();
    let mut workload_trace = get_workload_trace();
    workload_trace.events.push(WorkloadEvent {
        timestamp: 70.9,
        event_type: WorkloadEventType::RemovePod {
            pod_name: "pod_0".to_string(),
        },
    });
    cluster_trace.events.push(ClusterEvent {
        timestamp: 71.0,
        event_type: ClusterEventType::RemoveNode {
            node_name: "trace_node_42".to_string(),
        },
    });
    workload_trace.events.push(WorkloadEvent {
        timestamp: 71.0001,
        event_type: WorkloadEventType::RemovePod {
            pod_name: "pod_1".to_string(),
        },
    });

    cluster_trace.events.push(ClusterEvent {
        timestamp: 500.0,
        event_type: ClusterEventType::CreateNode {
            node: Node::new("trace_node_42".to_string(), 2000, 4294967296),
        },
    });

    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config(None)));
    kube_sim.initialize(&mut cluster_trace, &mut workload_trace);
    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks {}));

    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .internal
            .terminated_pods
    );
    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .total_pods_in_trace
    );
    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_removed
    );
}

#[test]
fn test_removed_pod_frees_place_for_other_pod() {
    let mut cluster_trace = get_cluster_trace();
    let mut workload_trace = GenericWorkloadTrace { events: vec![] };
    workload_trace.events.push(WorkloadEvent {
        timestamp: 40.0,
        event_type: WorkloadEventType::CreatePod {
            pod: Pod::new("pod_0".to_string(), 2000, 4294967296, Some(200.0)),
        },
    });
    workload_trace.events.push(WorkloadEvent {
        timestamp: 41.0,
        event_type: WorkloadEventType::CreatePod {
            pod: Pod::new("pod_1".to_string(), 2000, 4294967296, Some(200.0)),
        },
    });

    workload_trace.events.push(WorkloadEvent {
        timestamp: 120.0,
        event_type: WorkloadEventType::RemovePod {
            pod_name: "pod_0".to_string(),
        },
    });

    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config(None)));
    kube_sim.initialize(&mut cluster_trace, &mut workload_trace);

    kube_sim.step_for_duration(100.0);
    assert_eq!(1, kube_sim.scheduler.borrow().unschedulable_pods.len());

    kube_sim.step_for_duration(240.0);

    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .internal
            .terminated_pods
    );
    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .total_pods_in_trace
    );

    assert_eq!(
        1,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_succeeded
    );
    assert_eq!(
        0,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_failed
    );
    assert_eq!(
        0,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_unschedulable
    );
    assert_eq!(
        1,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_removed
    );
}

#[test]
fn test_pod_removed_after_it_was_finished() {
    let mut cluster_trace = get_cluster_trace();
    let mut workload_trace = get_workload_trace();

    workload_trace.events.push(WorkloadEvent {
        timestamp: 150.2,
        event_type: WorkloadEventType::RemovePod {
            pod_name: "pod_0".to_string(),
        },
    });

    let mut kube_sim = KubernetriksSimulation::new(Rc::new(default_test_simulation_config(None)));
    kube_sim.initialize(&mut cluster_trace, &mut workload_trace);
    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks {}));

    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .internal
            .terminated_pods
    );
    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .total_pods_in_trace
    );
    assert_eq!(
        2,
        kube_sim
            .metrics_collector
            .borrow()
            .accumulated_metrics
            .pods_succeeded
    );
}
