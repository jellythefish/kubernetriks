//! Implementation of cluster controller - component which create and remove nodes in cluster.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::common::SimComponentId;
use crate::core::events::{
    CreateNodeRequest, NodeAddedToTheCluster
};
use crate::core::node::Node;
use crate::simulator::SimulatorConfig;

use crate::core::node_pool::NodePool;

use crate::core::node_component::NodeComponent;

pub struct ClusterController {
    api_server: SimComponentId,

    pub allocated_nodes: HashMap<String, Rc<RefCell<NodeComponent>>>,
    pub node_name_to_id: HashMap<String, SimComponentId>,

    node_pool: NodePool,
    ctx: SimulationContext,
    config: Rc<SimulatorConfig>,
}

impl ClusterController {
    pub fn new(
        api_server: SimComponentId,
        node_pool: NodePool,
        ctx: SimulationContext,
        config: Rc<SimulatorConfig>,
    ) -> Self {
        Self {
            api_server,
            allocated_nodes: Default::default(),
            node_name_to_id: Default::default(),
            node_pool,
            ctx,
            config,
        }
    }

    pub fn add_node(&mut self, node_name: String, node_id: SimComponentId, allocated: Rc<RefCell<NodeComponent>>) {
        self.allocated_nodes.insert(node_name.clone(), allocated);
        self.node_name_to_id.insert(node_name, node_id);
    }

    pub fn allocate_node(&mut self, node: Node) -> (SimComponentId, Rc<RefCell<NodeComponent>>) {
        let (id, allocated) = self.node_pool.allocate();
        allocated.borrow_mut().node = node;
        allocated.borrow_mut().api_server = Some(self.api_server);
        allocated.borrow_mut().config = Some(self.config.clone());
        (id, allocated)
    }
}

impl EventHandler for ClusterController {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            CreateNodeRequest { node } => {
                // TODO: ? simulate delay for node creation, event_time = event.time + delta
                let (node_id, allocated) = self.allocate_node(node);
                let node_name = allocated.borrow().node.metadata.name.clone();
                self.add_node(node_name.clone(), node_id, allocated);
                self.ctx.emit(
                    NodeAddedToTheCluster {
                        event_time: event.time,
                        node_name,
                        node_id,
                    },
                    self.api_server,
                    self.config.as_to_nc_network_delay,
                );
            }
        });
    }
}
