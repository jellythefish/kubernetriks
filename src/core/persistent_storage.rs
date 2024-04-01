//! Implementation of persistent storage for saving all information about kubernetes objects.
//! In k8s etcd plays this role, in our simulator it is a component which implements simple
//! in-memory key-value storage.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};
use log::debug;

use crate::core::common::SimComponentId;
use crate::core::events::{
    AssignPodToNodeRequest, AssignPodToNodeResponse, CreateNodeRequest, CreateNodeResponse,
    CreatePodRequest, NodeAddedToTheCluster, PodCreated, PodFinishedRunning, PodStartedRunning,
};
use crate::core::node::{Node, NodeConditionType};
use crate::core::pod::{Pod, PodConditionType};
use crate::simulator::SimulatorConfig;

pub struct StorageData {
    // State about current nodes of a cluster: <Node name, Node>
    pub nodes: HashMap<String, Node>,
    // State about current pods of a cluster: <Pod name, Pod>
    pub pods: HashMap<String, Pod>,
}

pub struct PersistentStorage {
    // Identifier of persistent storage as a simulation component.
    api_server: SimComponentId,
    scheduler: SimComponentId,

    storage_data: Rc<RefCell<StorageData>>,

    ctx: SimulationContext,
    config: Rc<SimulatorConfig>,
}

impl PersistentStorage {
    pub fn new(
        api_server_id: SimComponentId,
        scheduler_id: SimComponentId,
        storage_data: Rc<RefCell<StorageData>>,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            api_server: api_server_id,
            scheduler: scheduler_id,
            storage_data,
            ctx,
            config,
        }
    }

    pub fn print_running_info(&self, pod_name: String) {
        let storage = self.storage_data.borrow();
        let pod = storage.pods.get(&pod_name).unwrap();
        let mut finish_time: f64 = 0.0;
        let mut result: &str = "";
        if let Some(succeeded) = pod.get_condition(PodConditionType::PodSucceeded) {
            finish_time = succeeded.last_transition_time;
            result = "Succeeded";
        } else if let Some(failed) = pod.get_condition(PodConditionType::PodFailed) {
            finish_time = failed.last_transition_time;
            result = "Failed";
        }
        let start_time = pod.get_condition(PodConditionType::PodRunning).unwrap().last_transition_time;
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
                self.storage_data
                    .borrow_mut()
                    .nodes
                    .insert(node_name.clone(), node);
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
                ..
            } => {
                self.storage_data
                    .borrow_mut()
                    .nodes
                    .get_mut(&node_name)
                    .unwrap()
                    .update_condition(
                        "True".to_string(),
                        NodeConditionType::NodeCreated,
                        event_time,
                    );
                debug!(
                    "Updated node conditions: {:?}",
                    self.storage_data.borrow_mut().nodes[&node_name]
                        .status
                        .conditions
                );
            }
            CreatePodRequest { mut pod } => {
                let pod_name = pod.metadata.name.clone();
                pod.update_condition("True".to_string(), PodConditionType::PodCreated, event.time);
                self.storage_data
                    .borrow_mut()
                    .pods
                    .insert(pod_name.clone(), pod);
                // send info about newly created pod to scheduler
                self.ctx.emit(
                    PodCreated { pod_name },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );
            }
            AssignPodToNodeRequest {
                pod_name,
                node_name,
            } => {
                let mut storage = self.storage_data.borrow_mut();
                let pod = storage.pods.get_mut(&pod_name).unwrap();
                pod.update_condition(
                    "True".to_string(),
                    PodConditionType::PodScheduled,
                    event.time,
                );
                pod.status.assigned_node = node_name.clone();
                self.ctx.emit(
                    AssignPodToNodeResponse {
                        pod_name,
                        pod_duration: pod.calculate_running_duration(),
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
                let mut storage = self.storage_data.borrow_mut();
                let pod = storage.pods.get_mut(&pod_name).unwrap();
                pod.update_condition(
                    "True".to_string(),
                    PodConditionType::PodRunning,
                    start_time,
                );
            }
            PodFinishedRunning {
                finish_time,
                finish_result,
                pod_name,
                ..
            } => {
                {
                    let mut storage = self.storage_data.borrow_mut();
                    let pod = storage.pods.get_mut(&pod_name).unwrap();
                    pod.update_condition(
                        "True".to_string(),
                        finish_result,
                        finish_time,
                    );
                }

                // temporary (may be refactored) function for checking running results
                self.print_running_info(pod_name);
            }
        })
    }
}
