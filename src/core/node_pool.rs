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
use std::rc::Rc;
use std::collections::HashSet;
use std::collections::VecDeque;

use dslab_core::Event;
use dslab_core::EventHandler;
use dslab_core::{Simulation, SimulationContext};

use crate::core::node::Node;

use crate::core::common::SimComponentId;

struct NodeComponent {
    node: Node,

    running_pods: HashSet<String>,

    ctx: SimulationContext,
}

impl NodeComponent {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            node: Default::default(),
            running_pods: Default::default(),
            ctx
        }
    }

    pub fn clear_state(&mut self) {
        self.node = Default::default();
        self.running_pods.clear();
    }
}

impl EventHandler for NodeComponent {
    fn on(&mut self, event: Event) {
    }
}

pub struct NodePool {
    pool: VecDeque<(SimComponentId, Rc<RefCell<NodeComponent>>)>
}

impl NodePool {
    pub fn new(node_number: u64, sim: &mut Simulation) -> Self {
        let mut pool = VecDeque::with_capacity(node_number as usize);
        for i in 0..node_number {
            let node_name = format!("node_{}", i);
            let node_component = Rc::new(RefCell::new(NodeComponent::new(sim.create_context(&node_name))));
            let id = sim.add_handler(node_name, node_component.clone());
            pool.push_back((id, node_component))
        }
        Self { pool }
    }

    pub fn allocate(&mut self) -> (SimComponentId, Rc<RefCell<NodeComponent>>) {
        match self.pool.pop_front() {
            Some((id, node_component)) => return (id, node_component),
            None => panic!("Trying to allocate node component from empty pool"),
        }
    }

    pub fn reclaim(&mut self, id: SimComponentId, node_component: Rc<RefCell<NodeComponent>>) {
        node_component.borrow_mut().clear_state();
        self.pool.push_back((id, node_component));
    }
}
