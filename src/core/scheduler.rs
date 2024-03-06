//! Implementation of kube-scheduler component which is responsible for scheduling pods for nodes.

use dslab_core::{Event, EventHandler, Id, SimulationContext};

pub struct KubeScheduler {
    // Identifier of kube scheduler as a simulation component.
    id: Id,
    // Identifier of kube api server component.
    api_server: Id,
    cache: u64, // ???
    ctx: SimulationContext,
}

impl KubeScheduler {
    pub fn new(api_server_id: Id, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            api_server: api_server_id,
            cache: 0,
            ctx,
        }
    }
}

impl EventHandler for KubeScheduler {
    fn on(&mut self, event: Event) {}
}
