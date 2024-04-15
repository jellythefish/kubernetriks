//! Implementation of persistent storage for saving all information about kubernetes objects.
//! In k8s etcd plays this role, in our simulator it is a component which implements simple
//! in-memory key-value storage.

use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::{cast, log_debug, Event, EventHandler, SimulationContext};

use crate::core::common::ObjectsInfo;
use crate::core::common::SimComponentId;
use crate::core::events::{
    AddNodeToCacheRequest, AssignPodToNodeRequest, AssignPodToNodeResponse, CreateNodeRequest,
    CreateNodeResponse, CreatePodRequest, NodeAddedToTheCluster, PodFinishedRunning,
    PodScheduleRequest, PodStartedRunning,
};
use crate::metrics::collector::MetricsCollector;
use crate::core::node::{Node, NodeConditionType};
use crate::core::pod::{Pod, PodConditionType};
use crate::simulator::SimulationConfig;

pub struct PersistentStorage {
    // Identifier of persistent storage as a simulation component.
    api_server: SimComponentId,
    scheduler: SimComponentId,

    storage_data: ObjectsInfo,

    ctx: SimulationContext,
    config: Rc<SimulationConfig>,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

impl PersistentStorage {
    pub fn new(
        api_server_id: SimComponentId,
        scheduler_id: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
        metrics_collector: Rc<RefCell<MetricsCollector>>,
    ) -> Self {
        Self {
            api_server: api_server_id,
            scheduler: scheduler_id,
            storage_data: Default::default(),
            ctx,
            config,
            metrics_collector,
        }
    }

    pub fn add_node(&mut self, node: Node) {
        let node_name = node.metadata.name.clone();
        let existing_key = self
            .storage_data
            .nodes
            .insert(node.metadata.name.clone(), node);
        if !existing_key.is_none() {
            panic!(
                "Trying to add node {:?} to persistent storage which already exists",
                node_name
            );
        }
    }

    pub fn add_pod(&mut self, pod: Pod) {
        let pod_name = pod.metadata.name.clone();
        let existing_key = self.storage_data.pods.insert(pod_name.clone(), pod);
        if !existing_key.is_none() {
            panic!(
                "Trying to add pod {:?} to persistent storage which already exists",
                pod_name
            );
        }
    }

    pub fn get_node(&self, node_name: &str) -> Option<&Node> {
        self.storage_data.nodes.get(node_name)
    }

    pub fn get_pod(&self, pod_name: &str) -> Option<&Pod> {
        self.storage_data.pods.get(pod_name)
    }

    pub fn node_count(&self) -> usize {
        self.storage_data.nodes.len()
    }

    pub fn pod_count(&self) -> usize {
        self.storage_data.pods.len()
    }

    pub fn print_running_info(&self, pod_name: String) {
        let pod = self.storage_data.pods.get(&pod_name).unwrap();
        let mut finish_time: f64 = 0.0;
        let mut result: &str = "";
        if let Some(succeeded) = pod.get_condition(PodConditionType::PodSucceeded) {
            finish_time = succeeded.last_transition_time;
            result = "Succeeded";
        } else if let Some(failed) = pod.get_condition(PodConditionType::PodFailed) {
            finish_time = failed.last_transition_time;
            result = "Failed";
        }
        let start_time = pod
            .get_condition(PodConditionType::PodRunning)
            .unwrap()
            .last_transition_time;
        println!("###############################################################################");
        println!("Pod name: {:?}\nPod created at: {:?}\nPod scheduled at: {:?}\nNode assigned: {:?}\nPod started running at: {:?}\nPod finished running at: {:?}\nPod running duration: {:?}\nPod finish result: {:?}",
            pod_name,
            pod.get_condition(PodConditionType::PodCreated).unwrap().last_transition_time,
            pod.get_condition(PodConditionType::PodScheduled).unwrap().last_transition_time,
            pod.status.assigned_node,
            start_time,
            finish_time,
            finish_time - start_time,
            result,
        );
    }
}

impl EventHandler for PersistentStorage {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            CreateNodeRequest { node } => {
                let node_name = node.metadata.name.clone();
                self.add_node(node);
                self.ctx.emit(
                    CreateNodeResponse {
                        created: true,
                        node_name,
                    },
                    self.api_server,
                    self.config.as_to_ps_network_delay,
                );
            }
            NodeAddedToTheCluster {
                event_time,
                node_name,
            } => {
                self.storage_data
                    .nodes
                    .get_mut(&node_name)
                    .unwrap()
                    .update_condition(
                        "True".to_string(),
                        NodeConditionType::NodeCreated,
                        event_time,
                    );
                // tell scheduler about new node in the cluster
                let node = self.storage_data.nodes.get(&node_name).unwrap().clone();
                self.ctx.emit(
                    AddNodeToCacheRequest { node },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );
                log_debug!(
                    self.ctx,
                    "Updated node conditions: {:?}",
                    self.storage_data.nodes[&node_name].status.conditions
                );
            }
            CreatePodRequest { mut pod } => {
                pod.update_condition("True".to_string(), PodConditionType::PodCreated, event.time);
                self.add_pod(pod.clone());
                // send info about newly created pod to scheduler
                self.ctx.emit(
                    PodScheduleRequest { pod },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );
            }
            AssignPodToNodeRequest {
                pod_name,
                node_name,
            } => {
                let pod = self.storage_data.pods.get_mut(&pod_name).unwrap();
                pod.update_condition(
                    "True".to_string(),
                    PodConditionType::PodScheduled,
                    event.time,
                );
                pod.status.assigned_node = node_name.clone();
                self.ctx.emit(
                    AssignPodToNodeResponse {
                        pod_name,
                        pod_duration: pod.spec.running_duration,
                        node_name,
                    },
                    self.api_server,
                    self.config.as_to_ps_network_delay,
                );
            }
            PodStartedRunning {
                start_time,
                pod_name,
            } => {
                let pod = self.storage_data.pods.get_mut(&pod_name).unwrap();
                pod.update_condition("True".to_string(), PodConditionType::PodRunning, start_time);
            }
            PodFinishedRunning {
                pod_name,
                finish_time,
                finish_result,
            } => {
                let pod = self.storage_data.pods.get_mut(&pod_name).unwrap();
                pod.update_condition("True".to_string(), finish_result.clone(), finish_time);

                self.ctx.emit(
                    PodFinishedRunning {
                        finish_time,
                        finish_result,
                        pod_name: pod_name.clone(),
                    },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );

                self.metrics_collector.borrow_mut().increment_pod_duration(pod.spec.running_duration);

                // TODO: temporary (may be refactored) function for checking running results
                // self.print_running_info(pod_name);
            }
        })
    }
}
