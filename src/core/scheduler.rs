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
    pub objects_cache: ObjectsInfo,

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

    pub fn add_node_to_cache(&mut self, node: Node) {
        self.objects_cache
            .nodes
            .insert(node.metadata.name.clone(), node);
    }

    pub fn add_pod_to_cache(&mut self, pod: Pod) {
        self.objects_cache
            .pods
            .insert(pod.metadata.name.clone(), pod);
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
                pod.metadata.name, node.metadata.name, score
            );
        }

        Ok(assigned_node.to_owned())
    }
}

impl EventHandler for KubeGenericScheduler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            UpdateNodeCacheRequest { node } => {
                self.add_node_to_cache(node);
            }
            PodScheduleRequest { pod } => {
                let pod_name = pod.metadata.name.clone();
                let assigned_node = self.schedule_one(&pod).unwrap();
                self.reserve_node_resources(&pod.spec.resources.requests, &assigned_node);
                self.add_pod_to_cache(pod);
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
