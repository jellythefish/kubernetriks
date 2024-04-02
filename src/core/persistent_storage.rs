//! Implementation of persistent storage for saving all information about kubernetes objects.
//! In k8s etcd plays this role, in our simulator it is a component which implements simple
//! in-memory key-value storage.

use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};
use log::debug;

use crate::core::common::ObjectsInfo;
use crate::core::common::SimComponentId;
use crate::core::events::{
    AssignPodToNodeRequest, AssignPodToNodeResponse, CreateNodeRequest, CreateNodeResponse,
    CreatePodRequest, NodeAddedToTheCluster, PodFinishedRunning, PodScheduleRequest,
    PodStartedRunning, UpdateNodeCacheRequest,
};
use crate::core::node::{Node, NodeConditionType};
use crate::core::pod::PodConditionType;
use crate::simulator::SimulationConfig;

pub struct PersistentStorage {
    // Identifier of persistent storage as a simulation component.
    api_server: SimComponentId,
    scheduler: SimComponentId,

    storage_data: ObjectsInfo,

    ctx: SimulationContext,
    config: Rc<SimulationConfig>,
}

impl PersistentStorage {
    pub fn new(
        api_server_id: SimComponentId,
        scheduler_id: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            api_server: api_server_id,
            scheduler: scheduler_id,
            storage_data: Default::default(),
            ctx,
            config,
        }
    }

    pub fn add_node(&mut self, node: Node) {
        self.storage_data
            .nodes
            .insert(node.metadata.name.clone(), node);
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
                    UpdateNodeCacheRequest { node },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );
                debug!(
                    "Updated node conditions: {:?}",
                    self.storage_data.nodes[&node_name].status.conditions
                );
            }
            CreatePodRequest { mut pod } => {
                let pod_name = pod.metadata.name.clone();
                pod.update_condition("True".to_string(), PodConditionType::PodCreated, event.time);
                self.storage_data.pods.insert(pod_name.clone(), pod);
                // send info about newly created pod to scheduler
                let pod = self.storage_data.pods.get(&pod_name).unwrap().clone();
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
                finish_time,
                finish_result,
                pod_name,
                ..
            } => {
                let pod = self.storage_data.pods.get_mut(&pod_name).unwrap();
                pod.update_condition("True".to_string(), finish_result, finish_time);

                // temporary (may be refactored) function for checking running results
                self.print_running_info(pod_name);
            }
        })
    }
}
