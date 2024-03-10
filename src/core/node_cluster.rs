//! Implementation of node cluster.

use std::collections::{HashMap, HashSet};

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::common::{RuntimeResources, SimComponentId};
use crate::core::events::CreateNodeRequest;
use crate::core::node::Node;

pub struct NodeInfo {
    allocatable: RuntimeResources,
    capacity: RuntimeResources,
    running_pods: HashSet<String>,
}

pub struct NodeCluster {
    api_server_id: SimComponentId,
    ctx: SimulationContext,
    // <Node name, NodeInfo> pairs
    nodes: HashMap<String, NodeInfo>,
}

impl NodeCluster {
    pub fn new(api_server_id: SimComponentId, ctx: SimulationContext) -> Self {
        Self {
            api_server_id,
            ctx,
            nodes: Default::default(),
        }
    }

    pub fn add_node(&mut self, node: Node) {
        self.nodes.insert(
            node.metadata.name,
            NodeInfo {
                allocatable: node.status.capacity.clone(),
                capacity: node.status.capacity,
                running_pods: Default::default(),
            },
        );
    }
}

impl EventHandler for NodeCluster {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            CreateNodeRequest { node } => {
                self.add_node(node);
            }
        });
    }
}
