//! Implementation of kube-api-server component

use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{Event, EventHandler, SimulationContext};

use crate::cast_box;
use crate::core::common::SimComponentId;
use crate::core::events::{
    CreateNodeRequest, CreateNodeResponse, CreatePodRequest, RemoveNodeRequest, RemovePodRequest,
};
use crate::core::node::Node;
use crate::simulator::SimulatorConfig;

pub struct KubeApiServer {
    persistent_storage: SimComponentId,
    node_cluster: SimComponentId,
    ctx: SimulationContext,
    config: Rc<SimulatorConfig>,

    pending_node_creation_requests: HashMap<String, Node>,
}

impl KubeApiServer {
    pub fn new(
        node_cluster: SimComponentId,
        persistent_storage_id: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            node_cluster,
            persistent_storage: persistent_storage_id,
            ctx,
            config,
            pending_node_creation_requests: Default::default(),
        }
    }
}

impl EventHandler for KubeApiServer {
    fn on(&mut self, event: Event) {
        // Macro which is called when we are sure that event.data is a Box from arbitrary
        // Box<dyn EventData>
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
                if !created {
                    panic!(
                        "Something went wrong while creating node, component with id {:?} failed:",
                        event.src
                    );
                }
                if event.src == self.persistent_storage {
                    // Now we are ready to send create request to node cluster, because Node is persisted.
                    self.ctx.emit(
                        CreateNodeRequest {
                            node: self
                                .pending_node_creation_requests
                                .remove(&node_name)
                                .unwrap(),
                        },
                        self.node_cluster,
                        self.config.as_to_nc_network_delay,
                    );
                }
                // skip response from cluster node, no reason to handle it
            }
            CreatePodRequest { .. } => {}
            RemoveNodeRequest { .. } => {}
            RemovePodRequest { .. } => {}
        })
    }
}
