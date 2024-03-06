//! Implementation of persistent storage for saving all information about kubernetes objects.
//! In k8s etcd plays this role, in our simulator it is a component which implements simple
//! in-memory key-value storage.

use std::collections::HashMap;

use dslab_core::{Event, EventHandler, Id, SimulationContext};

use crate::core::node::{NodeId, NodeInfo};
use crate::core::pod::{PodId, PodInfo};

pub struct PersistentStorage {
    // Identifier of persistent storage as a simulation component.
    id: Id,
    // Identifier of kube api server component.
    api_server: Id,
    // Identifier of kube scheduler component.
    scheduler: Id,
    // Information about current nodes of a cluster.
    nodes: HashMap<NodeId, NodeInfo>,
    // Information about current pods of a cluster.
    pods: HashMap<PodId, PodInfo>,
    ctx: SimulationContext,
}

impl PersistentStorage {
    pub fn new(api_server_id: Id, scheduler_id: Id, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            api_server: api_server_id,
            scheduler: scheduler_id,
            nodes: Default::default(),
            pods: Default::default(),
            ctx,
        }
    }
}

impl EventHandler for PersistentStorage {
    fn on(&mut self, event: Event) {}
}
