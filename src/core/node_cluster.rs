//! Implementation of node cluster.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::common::{RuntimeResources, SimComponentId};
use crate::core::events::{CreateNodeRequest, NodeAddedToTheCluster};
use crate::core::node::Node;
use crate::simulator::SimulatorConfig;

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
    config: Rc<SimulatorConfig>,
}

impl NodeCluster {
    pub fn new(
        api_server_id: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            api_server_id,
            ctx,
            nodes: Default::default(),
            config,
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
                let node_name = node.metadata.name.clone();
                self.add_node(node);
                self.ctx.emit(
                    // TODO: ? simulate delay for node creation, event_time = event.time + delta
                    NodeAddedToTheCluster {
                        event_time: event.time,
                        node_name,
                    },
                    self.api_server_id,
                    self.config.as_to_nc_network_delay,
                );
            }
        });
    }
}
