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
use crate::metrics::collector::MetricsCollector;

use crate::config::SimulationConfig;

#[derive(Default)]
pub struct NodeComponentPool {
    pool: VecDeque<Rc<RefCell<NodeComponent>>>,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

impl NodeComponentPool {
    pub fn new(
        node_number: usize,
        sim: &mut Simulation,
        metrics_collector: Rc<RefCell<MetricsCollector>>,
    ) -> Self {
        let mut pool = VecDeque::with_capacity(node_number as usize);
        for i in 0..node_number {
            let context_name = format!("pool_node_context_{}", i);
            let node_component = Rc::new(RefCell::new(NodeComponent::new(
                sim.create_context(&context_name),
                metrics_collector.clone(),
            )));
            sim.add_handler(context_name, node_component.clone());
            pool.push_back(node_component)
        }
        Self {
            pool,
            metrics_collector,
        }
    }

    pub fn allocate_component(
        &mut self,
        node: Node,
        api_server: SimComponentId,
        config: Rc<SimulationConfig>,
    ) -> Rc<RefCell<NodeComponent>> {
        let node_component = self
            .pool
            .pop_front()
            .unwrap_or_else(|| panic!("No nodes to allocate in pool"));
        node_component.borrow_mut().runtime = Some(NodeRuntime {
            api_server,
            node,
            config,
        });
        node_component
    }

    pub fn reclaim_component(&mut self, node_component: Rc<RefCell<NodeComponent>>) {
        node_component.borrow_mut().runtime = None;
        node_component.borrow_mut().removed = false;
        node_component.borrow_mut().removal_time = 0.0;
        node_component.borrow_mut().canceled_pods.clear();
        node_component.borrow_mut().running_pods.clear();

        self.pool.push_back(node_component);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use dslab_core::Simulation;

    use crate::core::node::Node;
    use crate::core::node_component_pool::NodeComponentPool;

    use crate::metrics::collector::MetricsCollector;

    use crate::test_util::helpers::default_test_simulation_config;

    #[test]
    fn test_node_pool_init() {
        let mut sim = Simulation::new(123);
        let pool_size: usize = 10;
        let node_pool = NodeComponentPool::new(
            pool_size,
            &mut sim,
            Rc::new(RefCell::new(MetricsCollector::new())),
        );

        assert_eq!(node_pool.pool.len(), pool_size);
        for (idx, node_component) in node_pool.pool.iter().enumerate() {
            let context_name = format!("pool_node_context_{}", idx);
            assert_eq!(context_name, node_component.borrow().context_name());
            assert_eq!(sim.lookup_id(&context_name), node_component.borrow().id());
        }
    }

    #[test]
    #[should_panic]
    fn test_node_pool_allocate_too_much_throws() {
        let mut sim = Simulation::new(123);
        let pool_size: usize = 3;
        let mut node_pool = NodeComponentPool::new(
            pool_size,
            &mut sim,
            Rc::new(RefCell::new(MetricsCollector::new())),
        );

        for _ in 0..pool_size + 1 {
            node_pool.allocate_component(
                Node::new("node".to_string(), 0, 0),
                0,
                Rc::new(default_test_simulation_config(None)),
            );
        }
    }

    #[test]
    fn test_node_pool_allocation_and_reclamation() {
        let mut sim = Simulation::new(123);
        let pool_size: usize = 1;
        let mut node_pool = NodeComponentPool::new(
            pool_size,
            &mut sim,
            Rc::new(RefCell::new(MetricsCollector::new())),
        );

        assert_eq!(node_pool.pool.len(), pool_size);
        assert!(node_pool.pool[0].borrow().runtime.is_none());

        let node = Node::new("node_42".to_string(), 0, 0);

        let node_component = node_pool.allocate_component(
            node.clone(),
            0,
            Rc::new(default_test_simulation_config(None)),
        );
        assert_eq!(node_pool.pool.len(), 0);
        assert_eq!(node, node_component.borrow().runtime.as_ref().unwrap().node);

        node_pool.reclaim_component(node_component);
        assert_eq!(node_pool.pool.len(), pool_size);
        assert!(node_pool.pool[0].borrow().runtime.is_none())
    }
}
