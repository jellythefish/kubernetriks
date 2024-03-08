//! A struct which simulates a real node being a component in dslab simulation which receives
//! and sends events from and to api server.

use ::std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use log::debug;

use crate::core::common::SimComponentId;
use crate::core::events::AddNodeToClusterRequest;
use crate::core::node_info::NodeId;

struct Node {}

impl Node {
    pub fn new() -> Self {
        Self {}
    }
}

pub struct NodeCluster {
    api_server_id: SimComponentId,
    ctx: SimulationContext,
    nodes: HashMap<NodeId, Rc<RefCell<Node>>>,
    // Strictly increasing node counter for ids
    node_counter: u64,
}

impl NodeCluster {
    pub fn new(api_server_id: SimComponentId, ctx: SimulationContext) -> Self {
        Self {
            api_server_id,
            ctx,
            nodes: Default::default(),
            node_counter: 0,
        }
    }

    pub fn add_node(&mut self, node_id: NodeId) {
        self.node_counter += 1;
        let node = Rc::new(RefCell::new(Node::new()));
        self.nodes.insert(node_id, node);
    }
}

impl EventHandler for NodeCluster {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            AddNodeToClusterRequest { node_id } => {
                debug!(
                    "[{}] Received AddNodeToClusterRequest event with node_id {:?}",
                    event.time, node_id
                );
                self.add_node(node_id);
            }
        });
    }
}
