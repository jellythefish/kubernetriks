//! Implementation of kube-scheduler component which is responsible for scheduling pods for nodes.

use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{cast, log_debug, Event, EventHandler, SimulationContext};

use crate::core::common::ObjectsInfo;
use crate::core::common::SimComponentId;
use crate::core::events::{AssignPodToNodeRequest, PodScheduleRequest, UpdateNodeCacheRequest};
use crate::simulator::SimulationConfig;
use downcast_rs::{impl_downcast, Downcast};

use crate::core::node::Node;
use crate::core::pod::Pod;

use crate::core::common::RuntimeResources;

#[derive(Debug, PartialEq)]
pub enum ScheduleError {
    NoNodesInCluster,
    RequestedResourcesAreZeros,
    NoSufficientNodes,
}

pub trait Scheduler: Downcast {
    // Method which should implement any scheduler to assign a node on which the pod will be executed.
    // Returns result which if is Ok - the name of assigned node, Err - scheduling error.
    fn schedule_one(&self, pod: &Pod) -> Result<String, ScheduleError>;
}
impl_downcast!(Scheduler);

pub struct KubeGenericScheduler {
    api_server: SimComponentId,

    // Cache which is updated based on events from persistent storage
    objects_cache: ObjectsInfo,

    ctx: SimulationContext,
    config: Rc<SimulationConfig>,
}

impl KubeGenericScheduler {
    pub fn new(
        api_server: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            api_server,
            objects_cache: Default::default(),
            ctx,
            config,
        }
    }

    pub fn add_node(&mut self, node: Node) {
        self.objects_cache
            .nodes
            .insert(node.metadata.name.clone(), node);
    }

    pub fn add_pod(&mut self, pod: Pod) {
        self.objects_cache
            .pods
            .insert(pod.metadata.name.clone(), pod);
    }

    pub fn get_node(&self, node_name: &str) -> Node {
        self.objects_cache.nodes.get(node_name).unwrap().clone()
    }

    pub fn get_pod(&self, pod_name: &str) -> Pod {
        self.objects_cache.pods.get(pod_name).unwrap().clone()
    }

    pub fn node_count(&self) -> usize {
        self.objects_cache.nodes.len()
    }

    pub fn pod_count(&self) -> usize {
        self.objects_cache.pods.len()
    }

    fn reserve_node_resources(
        &mut self,
        requested_resources: &RuntimeResources,
        assigned_node: &str,
    ) {
        let node = self.objects_cache.nodes.get_mut(assigned_node).unwrap();
        node.status.allocatable.cpu -= requested_resources.cpu;
        node.status.allocatable.ram -= requested_resources.ram;
    }

    fn filter<'a>(
        nodes: &'a HashMap<String, Node>,
        requested_resources: &RuntimeResources,
    ) -> Vec<&'a Node> {
        nodes
            .values()
            .filter(|&node| {
                requested_resources.cpu <= node.status.allocatable.cpu
                    && requested_resources.ram <= node.status.allocatable.ram
            })
            .collect()
    }

    fn score(requested: &RuntimeResources, allocatable: &RuntimeResources) -> f64 {
        let cpu_score = (allocatable.cpu - requested.cpu) as f64 * 100.0 / allocatable.cpu as f64;
        let ram_score = (allocatable.ram - requested.ram) as f64 * 100.0 / allocatable.ram as f64;
        (cpu_score + ram_score) / 2.0
    }
}

impl Scheduler for KubeGenericScheduler {
    // Implementing basic least requested priority algorithm. It means that after subtracting pod's
    // requested resources from node's allocatable resources, the node with the highest
    // percentage difference (relatively to capacity) is prioritized for scheduling.
    //
    // Weights for cpu and memory are equal by default.
    fn schedule_one(&self, pod: &Pod) -> Result<String, ScheduleError> {
        let requested_resources = &pod.spec.resources.requests;
        if requested_resources.cpu == 0 && requested_resources.ram == 0 {
            return Err(ScheduleError::RequestedResourcesAreZeros);
        }

        let nodes = &self.objects_cache.nodes;
        if nodes.len() == 0 {
            return Err(ScheduleError::NoNodesInCluster);
        }

        let filtered_nodes = KubeGenericScheduler::filter(nodes, &requested_resources);
        if filtered_nodes.len() == 0 {
            return Err(ScheduleError::NoSufficientNodes);
        }

        let mut assigned_node = &filtered_nodes[0].metadata.name;
        let mut max_score = 0.0;

        for node in filtered_nodes {
            let score = KubeGenericScheduler::score(&requested_resources, &node.status.allocatable);
            if score >= max_score {
                assigned_node = &node.metadata.name;
                max_score = score;
            }
            log_debug!(
                self.ctx,
                "Pod {:?} score for node {:?} - {:?}",
                pod.metadata.name,
                node.metadata.name,
                score
            );
        }

        Ok(assigned_node.to_owned())
    }
}

impl EventHandler for KubeGenericScheduler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            UpdateNodeCacheRequest { node } => {
                self.add_node(node);
            }
            PodScheduleRequest { pod } => {
                let pod_name = pod.metadata.name.clone();
                let assigned_node = self.schedule_one(&pod).unwrap();
                self.reserve_node_resources(&pod.spec.resources.requests, &assigned_node);
                self.add_pod(pod);
                self.ctx.emit(
                    AssignPodToNodeRequest {
                        pod_name: pod_name,
                        node_name: assigned_node,
                    },
                    self.api_server,
                    self.config.sched_to_as_network_delay,
                );
            } // TODO: UPDATE NODE CACHE WHEN THE POD IS FINISHED
        })
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use dslab_core::Simulation;
    use crate::core::common::RuntimeResources;
    use crate::core::scheduler::{KubeGenericScheduler, ScheduleError, Scheduler};

    use crate::core::node::Node;
    use crate::core::pod::Pod;
    use crate::simulator::SimulationConfig;

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
}