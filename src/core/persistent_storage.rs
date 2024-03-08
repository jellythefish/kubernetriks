//! Implementation of persistent storage for saving all information about kubernetes objects.
//! In k8s etcd plays this role, in our simulator it is a component which implements simple
//! in-memory key-value storage.

use std::collections::HashMap;
use std::rc::Rc;

use log::debug;

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::common::SimComponentId;
use crate::core::events::NodeAddedToCluster;
use crate::core::node_info::{NodeId, NodeInfo};
use crate::core::pod::{PodId, PodInfo};
use crate::simulator::SimulatorConfig;

pub struct PersistentStorage {
    // Identifier of persistent storage as a simulation component.
    api_server: SimComponentId,
    scheduler: SimComponentId,
    // Information about current nodes of a cluster.
    nodes: HashMap<NodeId, NodeInfo>,
    // Information about current pods of a cluster.
    pods: HashMap<PodId, PodInfo>,

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
            NodeAddedToCluster { node_info } => {
                debug!(
                    "[{}] Received NodeAddedToCluster event with node_info {:?}",
                    event.time, node_info
                );
                let id = node_info.spec.id;
                self.nodes.insert(id, node_info);
            }
        })
    }
}
