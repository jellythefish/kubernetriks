//! Node pool registers fixed number of nodes for whole simulation before it starts event handling.
//!
//! It's needed because we cannot dynamically register new simulation components in events which
//! are handled by other simulation components. Such event handlers indirectly depends on simulation
//! mutable reference, but for registering we need it too.
//!
//! So in node pool we preallocate (register as simulation components) fixed number of nodes which
//! will be enough for dynamic node creation/removals and then allocate (add node) them or reclaim
//! (remove node) on events.
//!

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use dslab_core::Simulation;

use crate::core::common::SimComponentId;
use crate::core::node::Node;
use crate::core::node_component::{NodeComponent, NodeRuntime};

use crate::simulator::SimulationConfig;

pub struct NodePool {
    pool: VecDeque<Rc<RefCell<NodeComponent>>>,
}

impl NodePool {
    pub fn new(node_number: u64, sim: &mut Simulation) -> Self {
        let mut pool = VecDeque::with_capacity(node_number as usize);
        for i in 0..node_number {
            let node_name = format!("pool_node_{}", i);
            let node_component = Rc::new(RefCell::new(NodeComponent::new(
                sim.create_context(&node_name),
            )));
            sim.add_handler(node_name, node_component.clone());
            pool.push_back(node_component)
        }
        Self { pool }
    }

    pub fn allocate(
        &mut self,
        node: Node,
        api_server: SimComponentId,
        config: Rc<SimulationConfig>,
    ) -> Rc<RefCell<NodeComponent>> {
        let node_component = self
            .pool
            .pop_front()
            .unwrap_or_else(|| panic!("Trying to allocate node component from empty pool"));
        node_component.borrow_mut().runtime = Some(NodeRuntime {
            api_server,
            node,
            running_pods: Default::default(),
            config,
        });
        node_component
    }

    pub fn reclaim(&mut self, id: SimComponentId, node_component: Rc<RefCell<NodeComponent>>) {
        node_component.borrow_mut().runtime = None;
        self.pool.push_back(node_component);
    }
}
