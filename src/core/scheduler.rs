//! Implementation of kube-scheduler component which is responsible for scheduling pods for nodes.

use std::rc::Rc;

use dslab_core::{Event, EventHandler, SimulationContext};

use crate::core::common::SimComponentId;
use crate::simulator::SimulatorConfig;

pub struct KubeScheduler {
    api_server: SimComponentId,
    cache: u64, // ???
    ctx: SimulationContext,
    config: Rc<SimulatorConfig>,
}

impl KubeScheduler {
    pub fn new(
        api_server: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            api_server,
            cache: 0,
            ctx,
            config,
        }
    }
}

impl EventHandler for KubeScheduler {
    fn on(&mut self, event: Event) {}
}
