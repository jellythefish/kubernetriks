//! Implementation of kube-api-server component.
//! It contains node pool as a field which helps dynamically create and remove nodes from a cluster
//! as dslab simulation components.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dslab_core::{Event, EventHandler, SimulationContext};

use crate::autoscalers::horizontal_pod_autoscaler::interface::PodGroupInfo;
use crate::cast_box;
use crate::core::common::SimComponentId;
use crate::core::events::{
    AssignPodToNodeRequest, AssignPodToNodeResponse, BindPodToNodeRequest,
    ClusterAutoscalerRequest, ClusterAutoscalerResponse, CreateNodeRequest, CreateNodeResponse,
    CreatePodGroupRequest, CreatePodRequest, NodeAddedToCluster, NodeRemovedFromCluster,
    PodFinishedRunning, PodNotScheduled, PodRemovedFromNode, PodStartedRunning, RegisterPodGroup,
    RemoveNodeRequest, RemoveNodeResponse, RemovePodRequest, RemovePodResponse,
};
use crate::core::node::Node;
use crate::core::node_component::NodeComponent;
use crate::core::node_component_pool::NodeComponentPool;
use crate::metrics::collector::MetricsCollector;

use crate::config::SimulationConfig;

pub struct KubeApiServer {
    persistent_storage: SimComponentId,
    cluster_autoscaler: Option<SimComponentId>,
    horizontal_pod_autoscaler: Option<SimComponentId>,

    pub ctx: SimulationContext,
    config: Rc<SimulationConfig>,

    node_pool: NodeComponentPool,
    pending_node_creation_requests: HashMap<String, Node>,
    pending_node_removal_requests: HashSet<String>,
    pending_pod_removal_requests: HashSet<String>,
    // Mapping from node name to it's component
    created_nodes: HashMap<String, Rc<RefCell<NodeComponent>>>,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

impl KubeApiServer {
    pub fn new(
        persistent_storage_id: SimComponentId,
        cluster_autoscaler_id: Option<SimComponentId>,
        horizontal_pod_autoscaler_id: Option<SimComponentId>,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
        metrics_collector: Rc<RefCell<MetricsCollector>>,
    ) -> Self {
        Self {
            persistent_storage: persistent_storage_id,
            cluster_autoscaler: cluster_autoscaler_id,
            horizontal_pod_autoscaler: horizontal_pod_autoscaler_id,
            ctx,
            config,
            node_pool: Default::default(),
            pending_node_creation_requests: Default::default(),
            pending_node_removal_requests: Default::default(),
            pending_pod_removal_requests: Default::default(),
            created_nodes: Default::default(),
            metrics_collector,
        }
    }

    pub fn add_node_component(&mut self, node_component: Rc<RefCell<NodeComponent>>) {
        let node_name = node_component.borrow().node_name().to_string();
        let existing_key = self.created_nodes.insert(node_name.clone(), node_component);
        if !existing_key.is_none() {
            panic!(
                "Trying to add node {:?} to api server which already exists",
                node_name
            );
        }
    }

    pub fn all_created_nodes(&self) -> Vec<Rc<RefCell<NodeComponent>>> {
        self.created_nodes
            .values()
            .map(|node_ref| node_ref.clone())
            .collect()
    }

    pub fn get_node_component(&self, node_name: &str) -> Option<Rc<RefCell<NodeComponent>>> {
        self.created_nodes.get(node_name).cloned()
    }

    pub fn node_count(&self) -> usize {
        self.created_nodes.len()
    }

    fn handle_create_node(&mut self, node_name: &str, add_time: f64) {
        // Now we are ready to create node via node pool, because Node info is persisted.
        let node = self
            .pending_node_creation_requests
            .remove(node_name)
            .unwrap();
        let node_component =
            self.node_pool
                .allocate_component(node, self.ctx.id(), self.config.clone());
        self.add_node_component(node_component);

        self.ctx.emit(
            NodeAddedToCluster {
                add_time,
                node_name: node_name.to_string(),
            },
            self.persistent_storage,
            self.config.as_to_ps_network_delay,
        );
    }

    fn handle_node_removal(&mut self, node_name: &String) {
        let node_component = self.created_nodes.remove(node_name).unwrap();
        self.node_pool.reclaim_component(node_component);
    }

    pub fn set_node_pool(&mut self, node_pool: NodeComponentPool) {
        self.node_pool = node_pool
    }
}

impl EventHandler for KubeApiServer {
    fn on(&mut self, event: Event) {
        // Macro which is called when we are sure that event.data is a Box from arbitrary
        // Box<dyn SimulationEvent>
        cast_box!(match event.data {
            // Redirect to persistent storage first
            CreateNodeRequest { node } => {
                self.pending_node_creation_requests
                    .insert(node.metadata.name.clone(), node.clone());
                self.ctx.emit(
                    CreateNodeRequest { node },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            CreateNodeResponse { node_name } => {
                // Then create upon the info about creation is persisted
                self.handle_create_node(&node_name, event.time);
            }
            CreatePodRequest { pod } => {
                // Redirects to persistent storage
                self.ctx.emit(
                    CreatePodRequest { pod },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            AssignPodToNodeRequest {
                assign_time,
                pod_name,
                node_name,
            } => {
                // We could receive assign request from scheduler when the removal of pod or node
                // is in progress - should check it.
                if self.pending_node_removal_requests.contains(&node_name)
                    || !self.created_nodes.contains_key(&node_name)
                {
                    // Node is being removing or already removed - do nothing
                    // Scheduler will later reschedule this pod upon receiving RemoveNodeFromCache
                    // event
                    return;
                }
                if self.pending_pod_removal_requests.contains(&pod_name) {
                    // Similarly for cases when pod is being removed - do nothing
                    // Scheduler will find out about removal later from persistent storage
                    return;
                }
                // Otherwise, redirect to persistent storage
                self.ctx.emit(
                    AssignPodToNodeRequest {
                        assign_time,
                        pod_name,
                        node_name,
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            AssignPodToNodeResponse {
                pod_name,
                pod_group,
                pod_group_creation_time,
                node_name,
                pod_duration,
                resources_usage_model_config,
            } => {
                // Info about assignment is persisted - make bind request to node component
                let node_component = self.created_nodes.get(&node_name).unwrap();
                self.ctx.emit(
                    BindPodToNodeRequest {
                        pod_name,
                        pod_group,
                        pod_group_creation_time,
                        node_name,
                        pod_duration,
                        resources_usage_model_config,
                    },
                    node_component.borrow().id(),
                    self.config.as_to_node_network_delay,
                );
            }
            PodNotScheduled {
                not_scheduled_time,
                pod_name,
            } => {
                // Redirect to persistent storage
                self.ctx.emit(
                    PodNotScheduled {
                        not_scheduled_time,
                        pod_name,
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            PodStartedRunning {
                start_time,
                pod_name,
            } => {
                // Redirect to persistent storage
                self.ctx.emit(
                    PodStartedRunning {
                        start_time,
                        pod_name,
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            PodFinishedRunning {
                pod_name,
                node_name,
                finish_time,
                finish_result,
            } => {
                self.metrics_collector
                    .borrow_mut()
                    .metrics
                    .internal
                    .terminated_pods += 1;
                self.metrics_collector.borrow_mut().metrics.pods_succeeded += 1;
                // Redirect to persistent storage
                self.ctx.emit(
                    PodFinishedRunning {
                        pod_name,
                        node_name,
                        finish_time,
                        finish_result,
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            RemoveNodeRequest { node_name } => {
                self.pending_node_removal_requests.insert(node_name.clone());
                // Redirects to persistent storage first to persist removal request
                self.ctx.emit(
                    RemoveNodeRequest { node_name },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            RemoveNodeResponse { node_name } => {
                // Info about removal is persisted, send message to node component to terminate
                // as it's being removed.
                let node_component = self.created_nodes.get(&node_name).unwrap();
                self.ctx.emit(
                    RemoveNodeRequest { node_name },
                    node_component.borrow().id(),
                    self.config.as_to_node_network_delay,
                );
            }
            NodeRemovedFromCluster {
                removal_time,
                node_name,
            } => {
                // Event from node component about completed removal.
                self.handle_node_removal(&node_name);
                self.pending_node_removal_requests.remove(&node_name);

                // Redirect to persistent storage
                self.ctx.emit(
                    NodeRemovedFromCluster {
                        removal_time,
                        node_name: node_name.to_string(),
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            ClusterAutoscalerRequest { request_type } => {
                // Redirect to persistent storage
                self.ctx.emit(
                    ClusterAutoscalerRequest { request_type },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            ClusterAutoscalerResponse {
                scale_up,
                scale_down,
            } => {
                // Redirect to auto scaler
                self.ctx.emit(
                    ClusterAutoscalerResponse {
                        scale_up,
                        scale_down,
                    },
                    self.cluster_autoscaler.unwrap(),
                    self.config.as_to_ca_network_delay,
                );
            }
            RemovePodRequest { pod_name } => {
                self.pending_node_removal_requests.insert(pod_name.clone());
                // Redirect to persistent storage
                self.ctx.emit(
                    RemovePodRequest { pod_name },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            RemovePodResponse {
                assigned_node,
                pod_name,
            } => {
                if assigned_node.is_some() {
                    // If some node was assigned we should terminate pod first and wait for response
                    // from node component about removal in `PodRemovedFromNode` event.
                    let node_component = self.created_nodes.get(&assigned_node.unwrap()).unwrap();
                    self.ctx.emit(
                        RemovePodRequest { pod_name },
                        node_component.borrow().id(),
                        self.config.as_to_node_network_delay,
                    );
                } else {
                    // Otherwise, pod is not executing on any node - just finish with removing from
                    // pending.
                    self.pending_pod_removal_requests.remove(&pod_name);
                }
            }
            PodRemovedFromNode {
                removed,
                removal_time,
                pod_name,
            } => {
                self.pending_pod_removal_requests.remove(&pod_name);

                if removed {
                    // removed with our request or node removal - consider it terminated
                    self.metrics_collector
                        .borrow_mut()
                        .metrics
                        .internal
                        .terminated_pods += 1;
                    self.metrics_collector.borrow_mut().metrics.pods_removed += 1;
                }

                // Redirect to persistent storage
                self.ctx.emit(
                    PodRemovedFromNode {
                        removed,
                        removal_time,
                        pod_name,
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            CreatePodGroupRequest { pod_group } => {
                assert!(
                    pod_group.pod_template.spec.running_duration.is_none(),
                    "Pod groups with specified duration are not supported. Only long running services."
                );

                let mut info = PodGroupInfo {
                    creation_time: event.time,
                    created_pods: Default::default(),
                    total_created: 0,
                    pod_group,
                };

                for idx in 0..info.pod_group.initial_pod_count {
                    // TODO: generalize code as it's the same as in HPA
                    let mut pod = info.pod_group.pod_template.clone();
                    let pod_name = format!("{}_{}", info.pod_group.name, idx);
                    pod.metadata.name = pod_name.clone();
                    pod.metadata
                        .labels
                        .insert("pod_group".to_string(), info.pod_group.name.clone());
                    pod.metadata.labels.insert(
                        "pod_group_creation_time".to_string(),
                        event.time.to_string(),
                    );
                    pod.spec.resources.usage_model_config =
                        Some(info.pod_group.resources_usage_model_config.clone());

                    self.ctx.emit(
                        CreatePodRequest { pod },
                        self.persistent_storage,
                        self.config.as_to_ps_network_delay,
                    );

                    info.created_pods.insert(pod_name);
                    info.total_created += 1;
                }

                self.ctx.emit(
                    RegisterPodGroup { info },
                    self.horizontal_pod_autoscaler.unwrap(),
                    self.config.as_to_hpa_network_delay,
                );
            }
        })
    }
}
