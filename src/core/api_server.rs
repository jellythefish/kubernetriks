//! Implementation of kube-api-server component

use std::rc::Rc;

use log::debug;

use dslab_core::{Event, EventHandler, SimulationContext};

use crate::cast_box;
use crate::core::common::SimComponentId;
use crate::core::events::{
    AddNodeToClusterRequest, CreateNodeRequest, CreatePodRequest, NodeAddedToCluster,
    RemoveNodeRequest, RemovePodRequest,
};
use crate::core::node_info::NodeInfo;
use crate::simulator::SimulatorConfig;

pub struct KubeApiServer {
    persistent_storage: SimComponentId,
    node_cluster: SimComponentId,
    ctx: SimulationContext,
    config: Rc<SimulatorConfig>,
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
        }
    }
}

impl EventHandler for KubeApiServer {
    fn on(&mut self, event: Event) {
        // Macro which is called when we are sure that event.data is a Box from arbitrary
        // Box<dyn EventData>
        cast_box!(match event.data {
            // Redirects to persistent storage
            CreateNodeRequest { node_spec } => {
                debug!(
                    "[{}] Received CreateNodeRequest event with spec {:?}",
                    event.time, node_spec
                );
                let node_id = node_spec.id;
                let node_info = NodeInfo::new(node_spec);
                self.ctx.emit(
                    NodeAddedToCluster { node_info },
                    self.persistent_storage,
                    self.config.as_to_ps_network_delay,
                );

                // we sent asynchronously add node request, not waiting for the answer
                // sending info about it to persistent storage
                // maybe better wait for it??
                self.ctx.emit(
                    AddNodeToClusterRequest { node_id },
                    self.node_cluster,
                    self.config.as_to_nc_network_delay,
                );
            }
            CreatePodRequest { pod_spec } => {}
            RemoveNodeRequest { node_id } => {}
            RemovePodRequest { pod_id } => {}
        })
    }
}
