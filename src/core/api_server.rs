//! Implementation of kube-api-server component.
//! It contains node pool as a field which helps dynamically create and remove nodes from a cluster
//! as dslab simulation components.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{Event, EventHandler, SimulationContext};

use crate::cast_box;
use crate::core::common::SimComponentId;
use crate::core::events::{
    AssignPodToNodeRequest, AssignPodToNodeResponse, BindPodToNodeRequest, CreateNodeRequest,
    CreateNodeResponse, CreatePodRequest, NodeAddedToTheCluster, PodFinishedRunning,
    PodStartedRunning, RemoveNodeRequest, RemovePodRequest,
};
use crate::metrics::collector::MetricsCollector;
use crate::core::node::Node;
use crate::core::node_component::NodeComponent;
use crate::core::node_component_pool::NodeComponentPool;

use crate::simulator::SimulationConfig;

pub struct KubeApiServer {
    persistent_storage: SimComponentId,

    pub ctx: SimulationContext,
    config: Rc<SimulationConfig>,

    node_pool: NodeComponentPool,
    pending_node_creation_requests: HashMap<String, Node>,
    // Mapping from node name to it's component
    created_nodes: HashMap<String, Rc<RefCell<NodeComponent>>>,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

impl KubeApiServer {
    pub fn new(
        persistent_storage_id: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
        metrics_collector: Rc<RefCell<MetricsCollector>>,
    ) -> Self {
        Self {
            persistent_storage: persistent_storage_id,
            ctx,
            config,
            node_pool: Default::default(),
            pending_node_creation_requests: Default::default(),
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

    fn handle_create_node_response(
        &mut self,
        event_time: f64,
        src: SimComponentId,
        created: bool,
        node_name: &str,
    ) {
        assert!(
            created,
            "Something went wrong while creating node, component with id {:?} failed:",
            src
        );
        assert_eq!(
            src, self.persistent_storage,
            "Got create node response not from persistent storage: id - {:?}",
            src
        );
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
            NodeAddedToTheCluster {
                add_time: event_time,
                node_name: node_name.to_string(),
            },
            self.persistent_storage,
            self.config.as_to_ps_network_delay,
        );
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
            // Redirects to persistent storage
            CreateNodeRequest { node } => {
                self.pending_node_creation_requests
                    .insert(node.metadata.name.clone(), node.clone());
                self.ctx.emit(
                    CreateNodeRequest { node },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
            }
            CreateNodeResponse { created, node_name } => {
                self.handle_create_node_response(event.time, event.src, created, &node_name);
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
                // Redirects to persistent storage
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
                pod_duration,
                node_name,
            } => {
                // Make bind request to node cluster
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
                // Redirects to persistent storage
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
                // Redirects to persistent storage
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
            RemoveNodeRequest { .. } => {}
            RemovePodRequest { .. } => {}
        })
    }
}
