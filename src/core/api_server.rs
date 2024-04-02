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
use crate::core::node::Node;
use crate::core::node_component::NodeComponent;
use crate::core::node_pool::NodePool;

use crate::simulator::SimulatorConfig;

pub struct KubeApiServer {
    persistent_storage: SimComponentId,

    pub ctx: SimulationContext,
    config: Rc<SimulatorConfig>,

    node_pool: NodePool,
    pending_node_creation_requests: HashMap<String, Node>,
    // Mapping from node name to it's component
    created_nodes: HashMap<String, Rc<RefCell<NodeComponent>>>,
}

impl KubeApiServer {
    pub fn new(
        persistent_storage_id: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
        node_pool: NodePool,
    ) -> Self {
        Self {
            persistent_storage: persistent_storage_id,
            ctx,
            config,
            node_pool,
            pending_node_creation_requests: Default::default(),
            created_nodes: Default::default(),
        }
    }

    pub fn add_node_component(&mut self, node_component: Rc<RefCell<NodeComponent>>) {
        let node_name = node_component.borrow().name().to_string();
        self.created_nodes.insert(node_name, node_component);
    }

    fn handle_create_node_response(
        &mut self,
        event_time: f64,
        src: SimComponentId,
        created: bool,
        node_name: &str,
    ) {
        if !created {
            panic!(
                "Something went wrong while creating node, component with id {:?} failed:",
                src
            );
        }
        if src != self.persistent_storage {
            panic!(
                "api server got CreateNodeResponse event type from unexpected sender with id {:?}",
                src
            );
        }
        // Now we are ready to create node via node pool, because Node info is persisted.
        let node = self
            .pending_node_creation_requests
            .remove(node_name)
            .unwrap();
        let node_component = self
            .node_pool
            .allocate(node, self.ctx.id(), self.config.clone());
        let node_name = node_component.borrow().name().to_string();
        self.add_node_component(node_component);

        self.ctx.emit(
            NodeAddedToTheCluster {
                event_time,
                node_name,
            },
            self.persistent_storage,
            self.config.as_to_ps_network_delay,
        );
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
                pod_name,
                node_name,
            } => {
                // Redirects to persistent storage
                self.ctx.emit(
                    AssignPodToNodeRequest {
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
                let node_component = self.created_nodes.get(&node_name).unwrap_or_else(|| {
                    panic!(
                        "Trying to assign pod {:?} to a node {:?} which do not exist",
                        pod_name, node_name
                    );
                });
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
