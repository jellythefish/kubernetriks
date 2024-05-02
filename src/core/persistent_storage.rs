//! Implementation of persistent storage for saving all information about kubernetes objects.
//! In k8s etcd plays this role, in our simulator it is a component which implements simple
//! in-memory key-value storage.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashSet, HashMap};

use dslab_core::{cast, log_debug, Event, EventHandler, SimulationContext};

use crate::autoscaler::cluster_autoscaler::{ScaleUpInfo, ScaleDownInfo, CLUSTER_AUTOSCALER_ORIGIN_LABEL};

use crate::core::common::{SimComponentId, ObjectsInfo};
use crate::core::events::{
    AddNodeToCache, AssignPodToNodeRequest, AssignPodToNodeResponse, CreateNodeRequest,
    CreateNodeResponse, CreatePodRequest, NodeAddedToCluster, PodFinishedRunning,
    PodScheduleRequest, PodStartedRunning, RemoveNodeRequest, RemoveNodeResponse,
    NodeRemovedFromCluster, RemoveNodeFromCache, ClusterAutoscalerRequest, ClusterAutoscalerResponse,
    PodNotScheduled,
};
use crate::core::node::{Node, NodeConditionType};
use crate::core::pod::{Pod, PodConditionType};

use crate::metrics::collector::MetricsCollector;

use crate::config::SimulationConfig;

pub struct PersistentStorage {
    api_server: SimComponentId,
    scheduler: SimComponentId,

    storage_data: ObjectsInfo,
    /// Map of node name and pod names which were assigned to that node.
    assignments: HashMap<String, HashSet<String>>,
    /// Pods that finished running successfully. Transferred from `storage_data` upon finish.
    pub succeeded_pods: HashMap<String, Pod>,

    unscheduled_pods_cache: HashSet<String>,

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
            assignments: Default::default(),
            succeeded_pods: Default::default(),
            unscheduled_pods_cache: Default::default(),
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
        self.assignments.insert(node_name.clone(), HashSet::default());
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

    fn send_scale_up_info(&self) {
        let unscheduled_pods = self.unscheduled_pods_cache.iter().map(
            |pod_name| self.storage_data.pods.get(pod_name).unwrap().clone()
        ).collect::<Vec<Pod>>();

        self.ctx.emit(
            ClusterAutoscalerResponse {
                scale_up: Some(ScaleUpInfo {
                    unscheduled_pods
                }),
                scale_down: None,
            },
            self.api_server,
            self.config.as_to_ps_network_delay,
        );
    }

    fn send_scale_down_info(&self) {
        let nodes: Vec<Node> = self.storage_data.nodes.values().cloned().collect();
        let mut pods_on_autoscaled_nodes: HashMap<String, Pod> = Default::default();

        for node in nodes.iter() {
            let origin = node.metadata.labels.get("origin");
            if origin.is_none() || origin.unwrap() != CLUSTER_AUTOSCALER_ORIGIN_LABEL {
                continue
            }

            for pod_name in self.assignments.get(&node.metadata.name).unwrap() {
                let (name, pod) = self.storage_data.pods.get_key_value(pod_name).unwrap();
                pods_on_autoscaled_nodes.insert(name.clone(), pod.clone());
            }
        }

        self.ctx.emit(
            ClusterAutoscalerResponse {
                scale_up: None,
                scale_down: Some(ScaleDownInfo {
                    pods_on_autoscaled_nodes,
                    nodes,
                    assignments: self.assignments.clone(),
                })
            },
            self.api_server,
            self.config.as_to_ps_network_delay,
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
                        node_name,
                    },
                    self.api_server,
                    self.config.as_to_ps_network_delay,
                );
            }
            NodeAddedToCluster {
                add_time,
                node_name,
            } => {
                self.storage_data
                    .nodes
                    .get_mut(&node_name)
                    .unwrap()
                    .update_condition(
                        "True".to_string(),
                        NodeConditionType::NodeCreated,
                        add_time,
                    );
                // tell scheduler about new node in the cluster
                let node = self.storage_data.nodes.get(&node_name).unwrap().clone();
                self.ctx.emit(
                    AddNodeToCache { node },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );
                log_debug!(
                    self.ctx,
                    "Updated node conditions: {:?}",
                    self.storage_data.nodes[&node_name].status.conditions
                );

                self.metrics_collector.borrow_mut().internal.processed_nodes += 1;
            }
            CreatePodRequest { mut pod } => {
                // Considering creation time as the time pod added to the persistent storage,
                // because it is just an entry in hash map.
                pod.update_condition("True".to_string(), PodConditionType::PodCreated, event.time);
                self.add_pod(pod.clone());
                // Send info about newly created pod to scheduler.
                self.ctx.emit(
                    PodScheduleRequest { pod },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );
            }
            AssignPodToNodeRequest {
                assign_time,
                pod_name,
                node_name,
            } => {
                let pod = self.storage_data.pods.get_mut(&pod_name).unwrap();
                pod.update_condition(
                    "True".to_string(),
                    PodConditionType::PodScheduled,
                    assign_time,
                );
                pod.status.assigned_node = node_name.clone();
                self.unscheduled_pods_cache.remove(&pod_name);

                let node = self.storage_data.nodes.get_mut(&node_name).unwrap();
                node.status.allocatable.cpu -= pod.spec.resources.requests.cpu;
                node.status.allocatable.ram -= pod.spec.resources.requests.ram;

                self.assignments.get_mut(&node_name).unwrap().insert(pod_name.clone());

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
            PodNotScheduled {
                not_scheduled_time,
                pod_name,
            } => {
                // Need to update pod status for autoscaler.
                let pod = self.storage_data.pods.get_mut(&pod_name).unwrap();
                pod.update_condition("False".to_string(), PodConditionType::PodScheduled, not_scheduled_time);
                self.unscheduled_pods_cache.insert(pod_name.clone());
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
                node_name,
                finish_time,
                finish_result,
            } => {
                let (pod_name, mut pod) = self.storage_data.pods.remove_entry(&pod_name).unwrap();
                pod.update_condition("True".to_string(), finish_result.clone(), finish_time);

                // Releasing resources is optional as we could persist remove node request earlier
                // then pod could finish. In remove request handling we remove node from storage.
                if let Some(node) = self.storage_data.nodes.get_mut(&pod.status.assigned_node) {
                    node.status.allocatable.cpu += pod.spec.resources.requests.cpu;
                    node.status.allocatable.ram += pod.spec.resources.requests.ram;
                }

                // Same is true for assignments also.
                if let Some(node_assignments) = self.assignments.get_mut(&pod.status.assigned_node) {
                    node_assignments.remove(&pod_name);
                }

                self.ctx.emit(
                    PodFinishedRunning {
                        pod_name: pod_name.clone(),
                        node_name,
                        finish_time,
                        finish_result,
                    },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );

                self.metrics_collector.borrow_mut().increment_pod_duration(pod.spec.running_duration);
                self.succeeded_pods.insert(pod_name, pod);

                // TODO: temporary (may be refactored) function for checking running results
                // self.print_running_info(pod_name);
            }
            RemoveNodeRequest { node_name } => {
                self.storage_data.nodes.remove(&node_name).unwrap();
                self.assignments.remove(&node_name).unwrap();

                self.ctx.emit(
                    RemoveNodeResponse {
                        node_name
                    },
                    self.api_server,
                    self.config.as_to_ps_network_delay,
                );
            }
            NodeRemovedFromCluster {
                removal_time,
                node_name
            } => {
                log_debug!(self.ctx, "Node {} removed from cluster at time: {}", node_name, removal_time);

                // Redirects to scheduler for pod rescheduling. Scheduler knows which pods to
                // reschedule. 
                self.ctx.emit(
                    RemoveNodeFromCache { node_name },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );
            }
            ClusterAutoscalerRequest {} => {
                if self.unscheduled_pods_cache.len() == 0 {
                    self.send_scale_down_info();
                    return
                }

                self.send_scale_up_info();
            }
        })
    }
}
