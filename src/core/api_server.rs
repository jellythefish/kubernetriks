//! Implementation of kube-api-server component

use std::collections::HashSet;

use log::debug;

use dslab_core::{Event, EventHandler, Id, SimulationContext};

use crate::cast_box;
use crate::core::events::{
    CreateNodeRequest, CreatePodRequest, RemoveNodeRequest, RemovePodRequest,
};

pub struct KubeApiServer {
    // Identifier of api server as a simulation component.
    id: Id,
    // Identifier of persistent storage component.
    persistent_storage: Id,
    ctx: SimulationContext,
}

impl KubeApiServer {
    pub fn new(persistent_storage_id: Id, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            persistent_storage: persistent_storage_id,
            ctx,
        }
    }
}

impl EventHandler for KubeApiServer {
    fn on(&mut self, event: Event) {
        // Macro which is called when we are sure that event.data is arbitrary Box<dyn EventData>
        cast_box!(match event.data {
            CreateNodeRequest { node } => {
                debug!(
                    "kube-api-server received CreateNodeRequest at timestamp {}. Node - {:?}",
                    event.time, node
                )
            }
            CreatePodRequest { pod } => {
                debug!(
                    "kube-api-server received CreatePodRequest at timestamp {}. Pod - {:?}",
                    event.time, pod
                )
            }
            RemoveNodeRequest { node_id } => {
                debug!(
                    "kube-api-server received RemoveNodeRequest at timestamp {}. Node id - {}",
                    event.time, node_id
                )
            }
            RemovePodRequest { pod_id } => {
                debug!(
                    "kube-api-server received RemovePodRequest at timestamp {}. Pod id - {}",
                    event.time, pod_id
                )
            }
        })
    }
}
