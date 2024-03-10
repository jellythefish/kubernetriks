//! Implementation of persistent storage for saving all information about kubernetes objects.
//! In k8s etcd plays this role, in our simulator it is a component which implements simple
//! in-memory key-value storage.

use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::common::SimComponentId;
use crate::core::events::{CreateNodeRequest, CreateNodeResponse};
use crate::core::node::Node;
use crate::core::pod::Pod;
use crate::simulator::SimulatorConfig;

pub struct PersistentStorage {
    // Identifier of persistent storage as a simulation component.
    api_server: SimComponentId,
    scheduler: SimComponentId,
    // State about current nodes of a cluster: <Node name, Node>
    nodes: HashMap<String, Node>,
    // State about current pods of a cluster: <Pod name, Pod>
    pods: HashMap<String, Pod>,

    ctx: SimulationContext,
    config: Rc<SimulatorConfig>,
}

impl PersistentStorage {
    pub fn new(
        api_server_id: SimComponentId,
        scheduler_id: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            api_server: api_server_id,
            scheduler: scheduler_id,
            nodes: Default::default(),
            pods: Default::default(),
            ctx,
            config,
        }
    }
}

impl EventHandler for PersistentStorage {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            CreateNodeRequest { node } => {
                let node_name = node.metadata.name.clone();
                self.nodes.insert(node_name.clone(), node);
                self.ctx.emit(
                    CreateNodeResponse {
                        created: true,
                        node_name,
                    },
                    self.api_server,
                    self.config.as_to_ps_network_delay,
                );
            }
        })
    }
}
