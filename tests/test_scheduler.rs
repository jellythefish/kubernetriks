use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::Simulation;
use dslab_kubernetriks::core::common::{ObjectMeta, RuntimeResources};
use dslab_kubernetriks::core::persistent_storage::StorageData;
use dslab_kubernetriks::core::scheduler::{KubeGenericScheduler, ScheduleError, Scheduler};

use dslab_kubernetriks::core::node::{Node, NodeStatus};
use dslab_kubernetriks::core::pod::{Container, Pod, PodSpec, Resources};
use dslab_kubernetriks::simulator::SimulatorConfig;

fn create_scheduler() -> Rc<dyn Scheduler> {
    let mut fake_sim = Simulation::new(0);

    let persistent_storage_data = Rc::new(RefCell::new(StorageData {
        nodes: Default::default(),
        pods: Default::default(),
    }));
    Rc::new(KubeGenericScheduler::new(
        0,
        persistent_storage_data.clone(),
        fake_sim.create_context("scheduler"),
        Rc::<SimulatorConfig>::new(SimulatorConfig::default()),
    ))
}

fn create_pod(container_requests: Vec<RuntimeResources>) -> Pod {
    let mut containers = vec![];
    for request in container_requests {
        containers.push(Container {
            resources: Resources {
                limits: request.clone(),
                requests: request.clone(),
            },
            running_duration: Default::default(),
        })
    }
    Pod {
        metadata: Default::default(),
        spec: PodSpec { containers },
        status: Default::default(),
    }
}

fn create_node(node_name: String, resources: RuntimeResources) -> Node {
    Node {
        metadata: ObjectMeta {
            name: node_name,
            labels: Default::default(),
            creation_timestamp: Default::default(),
        },
        spec: Default::default(),
        status: NodeStatus {
            allocatable: resources.clone(),
            capacity: resources,
            conditions: Default::default(),
        },
    }
}

fn register_nodes(scheduler: Rc<dyn Scheduler>, nodes: Vec<Node>) {
    match scheduler.downcast_rc::<KubeGenericScheduler>() {
        Ok(generic_scheduler) => {
            let mut cluster_cache = generic_scheduler.cluster_cache.borrow_mut();
            for node in nodes.into_iter() {
                cluster_cache.nodes.insert(node.metadata.name.clone(), node);
            }
        }
        Err(_) => {
            panic!("Failed to cast scheduler to KubeGenericScheduler")
        }
    }
}

fn allocate_pod(scheduler: Rc<dyn Scheduler>, node_name: &str, requests: RuntimeResources) {
    match scheduler.downcast_rc::<KubeGenericScheduler>() {
        Ok(generic_scheduler) => {
            let mut cluster_cache = generic_scheduler.cluster_cache.borrow_mut();
            let node = cluster_cache.nodes.get_mut(node_name).unwrap();
            node.status.allocatable.cpu -= requests.cpu;
            node.status.allocatable.ram -= requests.ram;
        }
        Err(_) => {
            panic!("Failed to cast scheduler to KubeGenericScheduler")
        }
    }
}

#[test]
fn test_no_nodes_no_schedule() {
    let scheduler = create_scheduler();
    let pod = create_pod(vec![RuntimeResources {
        cpu: 4000,
        ram: 16000,
    }]);
    assert_eq!(
        scheduler.schedule_one(&pod).err().unwrap(),
        ScheduleError::NoNodesInCluster
    );
}

#[test]
fn test_pod_has_requested_zero_resources() {
    let scheduler = create_scheduler();
    let pod = create_pod(vec![
        // all containers requested zeros
        RuntimeResources { cpu: 0, ram: 0 },
        RuntimeResources { cpu: 0, ram: 0 },
        RuntimeResources { cpu: 0, ram: 0 },
    ]);
    assert_eq!(
        scheduler.schedule_one(&pod).err().unwrap(),
        ScheduleError::RequestedResourcesAreZeros
    );
}

#[test]
fn test_no_sufficient_nodes_for_scheduling() {
    let scheduler = create_scheduler();
    let pod = create_pod(vec![
        RuntimeResources {
            cpu: 4000,
            ram: 8589934592,
        },
        RuntimeResources {
            cpu: 2000,
            ram: 4294967296,
        },
    ]);
    let node = create_node(
        "node1".to_string(),
        RuntimeResources {
            cpu: 3000,
            ram: 8589934592,
        },
    );
    register_nodes(scheduler.clone(), vec![node]);
    assert_eq!(
        scheduler.schedule_one(&pod).err().unwrap(),
        ScheduleError::NoSufficientNodes
    );
}

#[test]
fn test_correct_pod_scheduling() {
    let _ = env_logger::try_init();

    let scheduler = create_scheduler();
    let pod = create_pod(vec![
        RuntimeResources {
            cpu: 4000,
            ram: 8589934592,
        },
        RuntimeResources {
            cpu: 2000,
            ram: 4294967296,
        },
    ]);
    let node1 = create_node(
        "node1".to_string(),
        RuntimeResources {
            cpu: 8000,
            ram: 14589934592,
        },
    );
    let node2 = create_node(
        "node2".to_string(),
        RuntimeResources {
            cpu: 7000,
            ram: 20589934592,
        },
    );
    let node3 = create_node(
        "node3".to_string(),
        RuntimeResources {
            cpu: 6000,
            ram: 100589934592,
        },
    );
    // scores
    // node1: ((8000 - 6000) * 100 / 8000 + (14589934592 - 12884901888) * 100 / 14589934592) / 2 = 18.34
    // node2: ((7000 - 6000) * 100 / 7000 + (20589934592 - 12884901888) * 100 / 20589934592) / 2 = 25.85
    // node3: ((6000 - 6000) * 100 / 6000 + (100589934592 - 12884901888) * 100 / 100589934592) / 2 = 43.59
    // node3 - max score - choose it for scheduling
    register_nodes(scheduler.clone(), vec![node1, node2, node3]);
    assert_eq!(
        scheduler.schedule_one(&pod).ok().unwrap(),
        "node3".to_owned()
    );
}

#[test]
fn test_several_pod_scheduling() {
    let scheduler = create_scheduler();
    let node_name = "node1";
    let pod1 = create_pod(vec![RuntimeResources {
        cpu: 4000,
        ram: 8589934592,
    }]);
    let pod2 = create_pod(vec![RuntimeResources {
        cpu: 2000,
        ram: 4294967296,
    }]);
    let pod3 = create_pod(vec![RuntimeResources {
        cpu: 8000,
        ram: 8589934592,
    }]);
    let pod4 = create_pod(vec![RuntimeResources {
        cpu: 10000,
        ram: 8589934592,
    }]);
    let node1 = create_node(
        node_name.to_string(),
        RuntimeResources {
            cpu: 16000,
            ram: 100589934592,
        },
    );
    register_nodes(scheduler.clone(), vec![node1]);
    assert_eq!(
        scheduler.clone().schedule_one(&pod1).ok().unwrap(),
        node_name
    );
    // scheduler does not update cache itself, so we do it for persistent storage
    allocate_pod(
        scheduler.clone(),
        node_name,
        pod1.calculate_requested_resources(),
    );
    assert_eq!(
        scheduler.clone().schedule_one(&pod2).ok().unwrap(),
        node_name
    );
    allocate_pod(
        scheduler.clone(),
        node_name,
        pod2.calculate_requested_resources(),
    );
    assert_eq!(
        scheduler.clone().schedule_one(&pod3).ok().unwrap(),
        node_name
    );
    allocate_pod(
        scheduler.clone(),
        node_name,
        pod3.calculate_requested_resources(),
    );
    // there is no place left on node for the fourth pod
    assert_eq!(
        scheduler.clone().schedule_one(&pod4).err().unwrap(),
        ScheduleError::NoSufficientNodes
    );
}