//! Implementation of kube-api-server component

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
use crate::simulator::SimulatorConfig;

pub struct KubeApiServer {
    persistent_storage: SimComponentId,
    cluster_controller: SimComponentId,
    ctx: SimulationContext,
    config: Rc<SimulatorConfig>,

    pending_node_creation_requests: HashMap<String, Node>,
    // Mapping from node name to it's component id
    created_nodes: HashMap<String, SimComponentId>,
}

impl KubeApiServer {
    pub fn new(
        cluster_controller: SimComponentId,
        persistent_storage_id: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            cluster_controller,
            persistent_storage: persistent_storage_id,
            ctx,
            config,
            pending_node_creation_requests: Default::default(),
            created_nodes: Default::default(),
        }
    }

    fn handle_create_node_response(&mut self, src: SimComponentId, created: bool, node_name: &str) {
        if !created {
            panic!(
                "Something went wrong while creating node, component with id {:?} failed:",
                src
            );
        }
        if src == self.persistent_storage {
            // Now we are ready to send create request to node cluster, because Node is persisted.
            self.ctx.emit(
                CreateNodeRequest {
                    node: self
                        .pending_node_creation_requests
                        .remove(node_name)
                        .unwrap(),
                },
                self.cluster_controller,
                self.config.as_to_nc_network_delay,
            );
        } else {
            panic!(
                "api server got CreateNodeResponse event type from unexpected sender with id {:?}",
                src
            );
        }
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
                self.handle_create_node_response(event.src, created, &node_name);
            }
            NodeAddedToTheCluster {
                event_time,
                node_name,
                node_id,
            } => {
                self.created_nodes.insert(node_name.clone(), node_id);
                self.ctx.emit(
                    NodeAddedToTheCluster {
                        event_time,
                        node_name,
                        node_id,
                    },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );
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
                let node_component_id = self.created_nodes.get(&node_name).unwrap_or_else(|| {
                    panic!("Trying to assign pod {:?} to a node {:?} which do not exist", pod_name, node_name);
                });
                self.ctx.emit(
                    BindPodToNodeRequest {
                        pod_name,
                        pod_duration,
                        node_name,
                    },
                    *node_component_id,
                    self.config.as_to_nc_network_delay,
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
