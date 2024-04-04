use std::rc::Rc;

use dslab_core::Simulation;
use dslab_kubernetriks::core::common::RuntimeResources;
use dslab_kubernetriks::core::scheduler::{KubeGenericScheduler, ScheduleError, Scheduler};

use dslab_kubernetriks::core::node::Node;
use dslab_kubernetriks::core::pod::Pod;
use dslab_kubernetriks::simulator::SimulationConfig;

fn create_scheduler() -> Box<dyn Scheduler> {
    let mut fake_sim = Simulation::new(0);

    Box::new(KubeGenericScheduler::new(
        0,
        fake_sim.create_context("scheduler"),
        Rc::<SimulationConfig>::new(SimulationConfig::default()),
    ))
}

fn register_nodes(scheduler: &mut dyn Scheduler, nodes: Vec<Node>) {
    match scheduler.downcast_mut::<KubeGenericScheduler>() {
        Some(generic_scheduler) => {
            for node in nodes.into_iter() {
                generic_scheduler
                    .objects_cache
                    .nodes
                    .insert(node.metadata.name.clone(), node);
            }
        }
        None => {
            panic!("Failed to cast scheduler to KubeGenericScheduler")
        }
    }
}

fn allocate_pod(scheduler: &mut dyn Scheduler, node_name: &str, requests: RuntimeResources) {
    match scheduler.downcast_mut::<KubeGenericScheduler>() {
        Some(generic_scheduler) => {
            let node = generic_scheduler
                .objects_cache
                .nodes
                .get_mut(node_name)
                .unwrap();
            node.status.allocatable.cpu -= requests.cpu;
            node.status.allocatable.ram -= requests.ram;
        }
        None => {
            panic!("Failed to cast scheduler to KubeGenericScheduler")
        }
    }
}

#[test]
fn test_no_nodes_no_schedule() {
    let scheduler = create_scheduler();
    let pod = Pod::new("pod_1".to_string(), 4000, 16000, 5.0);
    assert_eq!(
        scheduler.schedule_one(&pod).err().unwrap(),
        ScheduleError::NoNodesInCluster
    );
}

#[test]
fn test_pod_has_requested_zero_resources() {
    let scheduler = create_scheduler();
    let pod = Pod::new("pod_1".to_string(), 0, 0, 5.0);
    assert_eq!(
        scheduler.schedule_one(&pod).err().unwrap(),
        ScheduleError::RequestedResourcesAreZeros
    );
}

#[test]
fn test_no_sufficient_nodes_for_scheduling() {
    let mut scheduler = create_scheduler();
    let pod = Pod::new("pod_1".to_string(), 6000, 12884901888, 5.0);
    let node = Node::new("node1".to_string(), 3000, 8589934592);
    register_nodes(scheduler.as_mut(), vec![node]);
    assert_eq!(
        scheduler.schedule_one(&pod).err().unwrap(),
        ScheduleError::NoSufficientNodes
    );
}

#[test]
fn test_correct_pod_scheduling() {
    let _ = env_logger::try_init();

    let mut scheduler = create_scheduler();
    let pod = Pod::new("pod_1".to_string(), 6000, 12884901888, 5.0);
    let node1 = Node::new("node1".to_string(), 8000, 14589934592);
    let node2 = Node::new("node2".to_string(), 7000, 20589934592);
    let node3 = Node::new("node3".to_string(), 6000, 100589934592);
    // scores
    // node1: ((8000 - 6000) * 100 / 8000 + (14589934592 - 12884901888) * 100 / 14589934592) / 2 = 18.34
    // node2: ((7000 - 6000) * 100 / 7000 + (20589934592 - 12884901888) * 100 / 20589934592) / 2 = 25.85
    // node3: ((6000 - 6000) * 100 / 6000 + (100589934592 - 12884901888) * 100 / 100589934592) / 2 = 43.59
    // node3 - max score - choose it for scheduling
    register_nodes(scheduler.as_mut(), vec![node1, node2, node3]);
    assert_eq!(
        scheduler.schedule_one(&pod).ok().unwrap(),
        "node3".to_owned()
    );
}

#[test]
fn test_several_pod_scheduling() {
    let mut scheduler = create_scheduler();
    let node_name = "node1";
    let pod1 = Pod::new("pod_1".to_string(), 4000, 8589934592, 5.0);
    let pod2 = Pod::new("pod_2".to_string(), 2000, 4294967296, 5.0);
    let pod3 = Pod::new("pod_3".to_string(), 8000, 8589934592, 5.0);
    let pod4 = Pod::new("pod_4".to_string(), 10000, 8589934592, 5.0);
    let node1 = Node::new(node_name.to_string(), 16000, 100589934592);
    register_nodes(scheduler.as_mut(), vec![node1]);
    assert_eq!(
        scheduler.as_ref().schedule_one(&pod1).ok().unwrap(),
        node_name
    );
    // scheduler does not update cache itself, so we do it for persistent storage
    allocate_pod(scheduler.as_mut(), node_name, pod1.spec.resources.requests);
    assert_eq!(
        scheduler.as_ref().schedule_one(&pod2).ok().unwrap(),
        node_name
    );
    allocate_pod(scheduler.as_mut(), node_name, pod2.spec.resources.requests);
    assert_eq!(
        scheduler.as_ref().schedule_one(&pod3).ok().unwrap(),
        node_name
    );
    allocate_pod(scheduler.as_mut(), node_name, pod3.spec.resources.requests);
    // there is no place left on node for the fourth pod
    assert_eq!(
        scheduler.as_ref().schedule_one(&pod4).err().unwrap(),
        ScheduleError::NoSufficientNodes
    );
}
