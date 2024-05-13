use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use dslab_kubernetriks::config::SimulationConfig;
use dslab_kubernetriks::core::{node::Node, pod::Pod};
use dslab_kubernetriks::metrics::collector::MetricsCollector;
use dslab_kubernetriks::simulator::{KubernetriksSimulation, RunUntilAllPodsAreFinishedCallbacks};
use dslab_kubernetriks::test_util::helpers::default_test_simulation_config;
use dslab_kubernetriks::trace::generic::{
    ClusterEvent, ClusterEventType, GenericClusterTrace, GenericWorkloadTrace, WorkloadEvent,
    WorkloadEventType,
};

fn generate_cluster_trace(sim: &mut KubernetriksSimulation) -> GenericClusterTrace {
    let events = (sim.sim.rand() * 1000.0).ceil() as usize;

    let mut created_nodes: BTreeMap<String, Node> = Default::default();

    let mut trace = GenericClusterTrace { events: Vec::new() };
    for _ in 0..events {
        if (sim.sim.rand() * 10.0).ceil() % 3.0 == 0.0 && created_nodes.len() > 0 {
            // delete node
            let next_node_name = created_nodes.values().next().unwrap().metadata.name.clone();
            let node = created_nodes.remove_entry(&next_node_name).unwrap();

            trace.events.push(ClusterEvent {
                timestamp: node.1.metadata.creation_timestamp + sim.sim.rand() * 10000.0,
                event_type: ClusterEventType::RemoveNode { node_name: node.0 },
            })
        } else {
            // create node
            let mut node = Node::new(
                sim.sim.random_string(5),
                (sim.sim.rand() * 10000.0).ceil() as u32,
                (sim.sim.rand() * 100000000000.0) as u64,
            );
            node.metadata.creation_timestamp = sim.sim.rand() * 1000.0;
            created_nodes.insert(node.metadata.name.clone(), node.clone());

            trace.events.push(ClusterEvent {
                timestamp: node.metadata.creation_timestamp,
                event_type: ClusterEventType::CreateNode { node },
            })
        }
    }
    trace
}

fn generate_workload_trace(sim: &mut KubernetriksSimulation) -> GenericWorkloadTrace {
    let events = (sim.sim.rand() * 10000.0).ceil() as usize;

    let mut trace = GenericWorkloadTrace { events: Vec::new() };
    for _ in 0..events {
        trace.events.push(WorkloadEvent {
            timestamp: sim.sim.rand() * 100000.0,
            event_type: WorkloadEventType::CreatePod {
                pod: Pod::new(
                    sim.sim.random_string(5),
                    (sim.sim.rand() * 1000.0).ceil() as u32,
                    (sim.sim.rand() * 10000000000.0) as u64,
                    sim.sim.rand() * 1000.0,
                ),
            },
        })
    }

    trace
}

fn run_simulation() -> Rc<RefCell<MetricsCollector>> {
    let mut config: SimulationConfig = default_test_simulation_config(None);
    // fixing seed
    config.seed = 46;
    let mut kube_sim = KubernetriksSimulation::new(Rc::new(config));

    let mut cluster_trace = generate_cluster_trace(&mut kube_sim);
    let mut workload_trace = generate_workload_trace(&mut kube_sim);

    kube_sim.initialize(&mut cluster_trace, &mut workload_trace);

    kube_sim.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks {}));

    kube_sim.metrics_collector.clone()
}

#[test]
// #[ignore = "slow test, run with cargo test -- --ignored"]
pub fn test_simulation_determinism() {
    let first_metric_collector = run_simulation();

    for _ in 0..10 {
        let current = run_simulation();

        assert_eq!(
            first_metric_collector.borrow().pods_succeeded,
            current.borrow().pods_succeeded
        );
        assert_eq!(
            first_metric_collector.borrow().pod_queue_time_stats,
            current.borrow().pod_queue_time_stats
        );
        assert_eq!(
            first_metric_collector
                .borrow()
                .pod_scheduling_algorithm_latency_stats,
            current.borrow().pod_scheduling_algorithm_latency_stats
        );
        assert_eq!(
            first_metric_collector.borrow().pod_duration_stats,
            current.borrow().pod_duration_stats
        );
    }
}
