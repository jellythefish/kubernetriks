//! Implementation of kube-api-server component.
//! It contains node pool as a field which helps dynamically create and remove nodes from a cluster
//! as dslab simulation components.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dslab_core::{Event, EventHandler, SimulationContext};

use crate::cast_box;
use crate::core::common::SimComponentId;
use crate::core::events::{
    AssignPodToNodeRequest, AssignPodToNodeResponse, BindPodToNodeRequest, CreateNodeRequest,
    CreateNodeResponse, CreatePodRequest, NodeAddedToCluster, NodeFailed, NodeRemovedFromCluster,
    PodFinishedRunning, PodStartedRunning, RemoveNodeRequest, RemoveNodeResponse, RemovePodRequest
};
use crate::metrics::collector::MetricsCollector;
use crate::core::node::Node;
use crate::core::node_component::NodeComponent;
use crate::core::node_component_pool::NodeComponentPool;

use crate::simulator::SimulationConfig;

pub struct KubeApiServer {
    persistent_storage: SimComponentId,
    scheduler: SimComponentId,

    pub ctx: SimulationContext,
    config: Rc<SimulationConfig>,

    node_pool: NodeComponentPool,
    pending_node_creation_requests: HashMap<String, Node>,
    pending_node_removal_requests: HashSet<String>,
    // Mapping from node name to it's component
    created_nodes: HashMap<String, Rc<RefCell<NodeComponent>>>,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

impl KubeApiServer {
    pub fn new(
        persistent_storage_id: SimComponentId,
        scheduler_id: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
        metrics_collector: Rc<RefCell<MetricsCollector>>,
    ) -> Self {
        Self {
            persistent_storage: persistent_storage_id,
            scheduler: scheduler_id,
            ctx,
            config,
            node_pool: Default::default(),
            pending_node_creation_requests: Default::default(),
            pending_node_removal_requests: Default::default(),
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

    pub fn get_node_component(&self, node_name: &str) -> Option<Rc<RefCell<NodeComponent>>> {
        self.created_nodes.get(node_name).cloned()
    }

    pub fn node_count(&self) -> usize {
        self.created_nodes.len()
    }

    fn handle_create_node(
        &mut self,
        node_name: &str,
        add_time: f64,
    ) {
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
                if self.pending_node_removal_requests.contains(&node_name) || !self.created_nodes.contains_key(&node_name) {
                    // Node is being removing or already removed - send back response with failure to scheduler
                    self.ctx.emit(
                        AssignPodToNodeResponse {
                            assigned: false,
                            pod_name,
                            pod_duration: 0.0,
                            node_name
                        },
                        self.scheduler,
                        self.config.sched_to_as_network_delay,
                    );
                    return
                }
                // Redirect to persistent storage
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
                assigned: _,
                pod_name,
                pod_duration,
                node_name,
            } => {
                // Info about assignment is persisted - make bind request to node component
                let node_component = self.created_nodes.get(&node_name).unwrap();
                self.ctx.emit(
                    BindPodToNodeRequest {
                        pod_name,
                        pod_duration,
                        node_name,
                    },
                    node_component.borrow().id(),
                    self.config.as_to_node_network_delay,
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
                finish_time,
                finish_result,
                pod_name,
            } => {
                self.metrics_collector.borrow_mut().pods_succeeded += 1;
                self.metrics_collector.borrow_mut().internal.terminated_pods += 1;
                // Redirect to persistent storage
                self.ctx.emit(
                    PodFinishedRunning {
                        finish_time,
                        finish_result,
                        pod_name,
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            RemoveNodeRequest { node_name } => {
                self.pending_node_removal_requests.insert(node_name.clone());
                // Redirects to persistent storage first to persist removal request
                self.ctx.emit_ordered(
                    RemoveNodeRequest {
                        node_name
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            RemoveNodeResponse { node_name } => {
                // Info about removal is persisted, send message to node component to terminate
                // as it's being removed.
                let node_component = self.created_nodes.get(&node_name).unwrap();
                self.ctx.emit_ordered(
                    RemoveNodeRequest {
                        node_name
                    },
                    node_component.borrow().id(),
                    self.config.as_to_node_network_delay,
                );
            }
            NodeRemovedFromCluster { removal_time, node_name } => {
                // Event from node component about completed removal.
                self.handle_node_removal(&node_name);
                self.pending_node_removal_requests.remove(&node_name);

                // Redirect to persistent storage
                self.ctx.emit_ordered(
                    NodeRemovedFromCluster {
                        removal_time,
                        node_name: node_name.to_string()
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay
                );
            }
            NodeFailed { .. } => {}
            RemovePodRequest { .. } => {}
        })
    }
}
