//! Implementation of kube-scheduler component which is responsible for scheduling pods for nodes.

use std::cell::{Cell, Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{Event, EventHandler, SimulationContext};

use crate::core::common::SimComponentId;
use crate::core::persistent_storage::StorageData;
use crate::simulator::SimulatorConfig;

use crate::core::node::Node;
use crate::core::pod::Pod;

use super::common::RuntimeResources;

pub enum ScheduleError {
    NoNodesInCluster,
    RequestedResourcesAreZeros,
    NoSufficientNodes,
}

trait Scheduler {
    // Method which should implement any scheduler to assign a node on which the pod will be executed.
    // Returns result which if is Ok - the name of assigned node, Err - scheduling error.
    fn schedule_one(&self, pod: &Pod) -> Result<String, ScheduleError>;
}

pub struct KubeGenericScheduler {
    api_server: SimComponentId,

    // Cache which is updated from persistent storage
    cluster_cache: Rc<RefCell<StorageData>>,

    ctx: SimulationContext,
    config: Rc<SimulatorConfig>,
}

impl KubeGenericScheduler {
    pub fn new(
        api_server: SimComponentId,
        cluster_cache: Rc<RefCell<StorageData>>,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            api_server,
            cluster_cache,
            ctx,
            config,
        }
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
        let requested_resources = pod.calculate_requested_resources();
        if requested_resources.cpu == 0 && requested_resources.ram == 0 {
            return Err(ScheduleError::RequestedResourcesAreZeros);
        }

        let nodes = &self.cluster_cache.borrow().nodes;
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
        }

        Ok(assigned_node.to_owned())
    }
}

impl EventHandler for KubeGenericScheduler {
    fn on(&mut self, _event: Event) {}
}
