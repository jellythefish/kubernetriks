//! Implementation of persistent storage for saving all information about kubernetes objects.
//! In k8s etcd plays this role, in our simulator it is a component which implements simple
//! in-memory key-value storage.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use dslab_core::{cast, log_debug, Event, EventHandler, SimulationContext};

use crate::autoscalers::cluster_autoscaler::interface::{
    AutoscaleInfoRequestType, ScaleDownInfo, ScaleUpInfo,
};
use crate::autoscalers::cluster_autoscaler::kube_cluster_autoscaler::CLUSTER_AUTOSCALER_ORIGIN_LABEL;

use crate::core::common::{ObjectsInfo, RuntimeResourcesUsageModelConfig, SimComponentId};
use crate::core::events::{
    AddNodeToCache, AssignPodToNodeRequest, AssignPodToNodeResponse, ClusterAutoscalerRequest,
    ClusterAutoscalerResponse, CreateNodeRequest, CreateNodeResponse, CreatePodRequest,
    NodeAddedToCluster, NodeRemovedFromCluster, PodFinishedRunning, PodNotScheduled,
    PodRemovedFromNode, PodScheduleRequest, PodStartedRunning, RemoveNodeFromCache,
    RemoveNodeRequest, RemoveNodeResponse, RemovePodFromCache, RemovePodRequest, RemovePodResponse,
};
use crate::core::node::{Node, NodeConditionType};
use crate::core::pod::{Pod, PodConditionType};
use crate::core::resource_usage::helpers::default_resource_usage_config;

use crate::config::SimulationConfig;
use crate::metrics::collector::MetricsCollector;

pub struct PersistentStorage {
    api_server: SimComponentId,
    scheduler: SimComponentId,

    storage_data: ObjectsInfo,
    /// Map of node name and pod names which were assigned to that node.
    /// Pods that finished running successfully. Transferred from `storage_data` upon finish.
    assignments: HashMap<String, BTreeSet<String>>,
    pub succeeded_pods: HashMap<String, Pod>,

    unscheduled_pods_cache: BTreeSet<String>,

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
        self.assignments
            .insert(node_name.clone(), BTreeSet::default());
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

    fn scale_up_info(&self) -> ScaleUpInfo {
        let unscheduled_pods = self
            .unscheduled_pods_cache
            .iter()
            .map(|pod_name| self.storage_data.pods.get(pod_name).unwrap().clone())
            .collect::<Vec<Pod>>();

        ScaleUpInfo { unscheduled_pods }
    }

    fn scale_down_info(&self) -> ScaleDownInfo {
        let nodes: Vec<Node> = self.storage_data.nodes.values().cloned().collect();
        let mut pods_on_autoscaled_nodes: HashMap<String, Pod> = Default::default();

        for node in nodes.iter() {
            let origin = node.metadata.labels.get("origin");
            if origin.is_none() || origin.unwrap() != CLUSTER_AUTOSCALER_ORIGIN_LABEL {
                continue;
            }

            for pod_name in self.assignments.get(&node.metadata.name).unwrap() {
                let (name, pod) = self.storage_data.pods.get_key_value(pod_name).unwrap();
                pods_on_autoscaled_nodes.insert(name.clone(), pod.clone());
            }
        }

        ScaleDownInfo {
            nodes,
            pods_on_autoscaled_nodes,
            assignments: self.assignments.clone(),
        }
    }

    /// Release node resources which pod has taken and remove pod from assignments.
    /// It is is optional as we could persist remove node request earlier then pod could finish or
    /// removed. In remove request handling we remove node from storage.
    fn clean_up_pod_info(&mut self, pod: &Pod) {
        if let Some(node) = self.storage_data.nodes.get_mut(&pod.status.assigned_node) {
            node.status.allocatable.cpu += pod.spec.resources.requests.cpu;
            node.status.allocatable.ram += pod.spec.resources.requests.ram;
        }

        if let Some(node_assignments) = self.assignments.get_mut(&pod.status.assigned_node) {
            node_assignments.remove(&pod.metadata.name);
        }
    }
}

impl EventHandler for PersistentStorage {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            CreateNodeRequest { node } => {
                let node_name = node.metadata.name.clone();
                self.add_node(node);
                self.ctx.emit(
                    CreateNodeResponse { node_name },
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
                    .update_condition("True".to_string(), NodeConditionType::NodeCreated, add_time);
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

                self.metrics_collector
                    .borrow_mut()
                    .metrics
                    .internal
                    .processed_nodes += 1;
            }
            CreatePodRequest { mut pod } => {
                // Considering creation time as the time pod added to the persistent storage,
                // because it is just an entry in hash map.
                pod.update_condition("True".to_string(), PodConditionType::PodCreated, event.time);

                if pod.spec.resources.usage_model_config.is_none() {
                    pod.spec.resources.usage_model_config =
                        Some(RuntimeResourcesUsageModelConfig {
                            cpu_config: Some(default_resource_usage_config(
                                pod.spec.resources.requests.cpu as f64,
                            )),
                            ram_config: Some(default_resource_usage_config(
                                pod.spec.resources.requests.ram as f64,
                            )),
                        });
                }

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

                self.assignments
                    .get_mut(&node_name)
                    .unwrap()
                    .insert(pod_name.clone());

                self.ctx.emit(
                    AssignPodToNodeResponse {
                        pod_name,
                        pod_group: pod.metadata.labels.get("pod_group").cloned(),
                        node_name,
                        pod_duration: pod.spec.running_duration,
                        resources_usage_model_config: pod
                            .spec
                            .resources
                            .usage_model_config
                            .clone()
                            .unwrap(),
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
                pod.update_condition(
                    "False".to_string(),
                    PodConditionType::PodScheduled,
                    not_scheduled_time,
                );
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
                // Remove request may come earlier and remove pod from storage, so we check if
                // it's still in there, update condition and clean up information.
                if self.storage_data.pods.contains_key(&pod_name) {
                    let (pod_name, mut pod) =
                        self.storage_data.pods.remove_entry(&pod_name).unwrap();
                    pod.update_condition("True".to_string(), finish_result.clone(), finish_time);

                    self.clean_up_pod_info(&pod);

                    self.metrics_collector
                        .borrow_mut()
                        .metrics
                        .increment_pod_duration(pod.spec.running_duration.unwrap());
                    self.succeeded_pods.insert(pod_name, pod);
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

                // TODO: temporary (may be refactored) function for checking running results
                // self.print_running_info(pod_name);
            }
            RemoveNodeRequest { node_name } => {
                self.storage_data.nodes.remove(&node_name).unwrap();
                self.assignments.remove(&node_name).unwrap();

                self.ctx.emit(
                    RemoveNodeResponse { node_name },
                    self.api_server,
                    self.config.as_to_ps_network_delay,
                );
            }
            NodeRemovedFromCluster {
                removal_time,
                node_name,
            } => {
                log_debug!(
                    self.ctx,
                    "Node {} removed from cluster at time: {}",
                    node_name,
                    removal_time
                );

                // Redirects to scheduler for pod rescheduling. Scheduler knows which pods to
                // reschedule.
                self.ctx.emit(
                    RemoveNodeFromCache { node_name },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );
            }
            ClusterAutoscalerRequest { request_type } => {
                let mut response = ClusterAutoscalerResponse {
                    scale_up: None,
                    scale_down: None,
                };

                match request_type {
                    AutoscaleInfoRequestType::Auto => {
                        if self.unscheduled_pods_cache.len() == 0 {
                            response.scale_down = Some(self.scale_down_info());
                        } else {
                            response.scale_up = Some(self.scale_up_info());
                        }
                    }
                    AutoscaleInfoRequestType::ScaleUpOnly => {
                        response.scale_up = Some(self.scale_up_info());
                    }
                    AutoscaleInfoRequestType::ScaleDownOnly => {
                        response.scale_down = Some(self.scale_down_info());
                    }
                    AutoscaleInfoRequestType::Both => {
                        response.scale_up = Some(self.scale_up_info());
                        response.scale_down = Some(self.scale_down_info());
                    }
                }

                self.ctx.emit(
                    response,
                    self.api_server,
                    self.config.as_to_ps_network_delay,
                );
            }
            RemovePodRequest { pod_name } => {
                if !self.storage_data.pods.contains_key(&pod_name) {
                    // pod has already been removed or finished running - do nothing
                    self.ctx.emit(
                        RemovePodResponse {
                            assigned_node: None,
                            pod_name,
                        },
                        self.api_server,
                        self.config.as_to_ps_network_delay,
                    );
                    return;
                }

                let mut pod = self.storage_data.pods.remove(&pod_name).unwrap();
                pod.update_condition("True".to_string(), PodConditionType::PodRemoved, event.time);

                let assigned_node_name = pod.status.assigned_node.clone();
                let mut assigned_node = None;

                if !assigned_node_name.is_empty() {
                    // If we have already assigned a node, we should release node resources and
                    // remove pod from assignments.
                    self.clean_up_pod_info(&pod);
                    assigned_node = Some(assigned_node_name);
                } else {
                    // Otherwise, pod has not been assigned, meaning that it is probably still in
                    // scheduling queues. So we can directly send request to scheduler to update its
                    // cache as well as response to api server.
                    self.ctx.emit(
                        RemovePodFromCache {
                            pod_name: pod_name.clone(),
                        },
                        self.scheduler,
                        self.config.ps_to_sched_network_delay,
                    );
                }

                self.ctx.emit(
                    RemovePodResponse {
                        assigned_node,
                        pod_name,
                    },
                    self.api_server,
                    self.config.as_to_ps_network_delay,
                );
            }
            PodRemovedFromNode {
                removed,
                removal_time,
                pod_name,
            } => {
                if !removed {
                    log_debug!(
                        self.ctx,
                        "Pod {} was not removed as it finished running earlier than remove request",
                        pod_name,
                    );
                    return;
                }

                log_debug!(
                    self.ctx,
                    "Pod {} removed from node at time: {}",
                    pod_name,
                    removal_time
                );

                // Pod is removed from a node, so tell scheduler to remove it from cache
                // (queue or assignments) too.
                self.ctx.emit(
                    RemovePodFromCache { pod_name },
                    self.scheduler,
                    self.config.ps_to_sched_network_delay,
                );
            }
        })
    }
}
