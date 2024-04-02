//! Represents entry point for simulator and its config.

use log::info;
use std::time::Instant;
use std::{cell::RefCell, rc::Rc};

use serde::Deserialize;

use dslab_core::simulation::Simulation;

use crate::core::api_server::KubeApiServer;
use crate::core::node::{Node, NodeConditionType};
use crate::core::node_component::{NodeComponent, NodeRuntime};
use crate::core::persistent_storage::PersistentStorage;
use crate::core::scheduler::KubeGenericScheduler;
use crate::core::node_pool::NodePool;

use crate::trace::interface::Trace;

#[derive(Default, Debug, Deserialize)]
pub struct SimulatorConfig {
    pub sim_name: String,
    pub seed: u64,
    pub node_pool_capacity: u64,
    pub default_cluster: Vec<NodeGroup>,
    // Simulated network delays, as = api server, ps = persistent storage.
    // All delays are in seconds with fractional part. Assuming all delays are bidirectional.
    pub as_to_ps_network_delay: f64,
    pub ps_to_sched_network_delay: f64,
    pub sched_to_as_network_delay: f64,
    pub as_to_node_network_delay: f64,
}

#[derive(Clone, Default, Debug, Deserialize)]
pub struct NodeGroup {
    node_count: u64,
    node_template: Node,
}

pub fn initialize_default_cluster(
    config: Rc<SimulatorConfig>, 
    persistent_storage: Rc<RefCell<PersistentStorage>>, 
    api_server: Rc<RefCell<KubeApiServer>>,
    scheduler: Rc<RefCell<KubeGenericScheduler>>,
    sim: &mut Simulation)
{
    if config.default_cluster.len() == 0 {
        return;
    }
    for bundle in config.default_cluster.iter() {
        for _ in 0..bundle.node_count {
            let node_name = format!("default_node_{}", sim.random_string(5));
            let node_context = sim.create_context(node_name.clone());

            let mut node = bundle.node_template.clone();
            node.update_condition("True".to_string(), NodeConditionType::NodeCreated, 0.0);
            node.metadata.name = node_name.clone();
            node.status.allocatable = node.status.capacity.clone();

            // add to persistent storage
            persistent_storage.borrow_mut().add_node(node.clone());
            // add to api server
            let node_component = Rc::new(RefCell::new(NodeComponent::new(node_context)));
            node_component.borrow_mut().runtime = Some(NodeRuntime {
                api_server: api_server.borrow().ctx.id(),
                node: node.clone(),
                running_pods: Default::default(),
                config: config.clone(),
            });
            api_server.borrow_mut().add_node_component(node_component.clone());
            // add to scheduler
            scheduler.borrow_mut().add_node_to_cache(node.clone());

            sim.add_handler(node_name, node_component);
        }
    }
}

pub fn run_simulator(config: Rc<SimulatorConfig>, cluster_trace: &mut dyn Trace, workload_trace: &mut dyn Trace) {
    info!(
        "Starting simulator {:?} with config: {:?}",
        config.sim_name, config
    );

    let mut sim = Simulation::new(config.seed);

    // Register simulator components

    // Client context for submitting trace events to kube_api_server
    let client = sim.create_context("client");

    let kube_api_server_component_name = "kube_api_server";
    let persistent_storage_component_name = "persistent_storage";
    let scheduler_component_name = "scheduler";

    let kube_api_server_context = sim.create_context(kube_api_server_component_name);
    let persistent_storage_context = sim.create_context(persistent_storage_component_name);
    let scheduler_context = sim.create_context(scheduler_component_name);

    let kube_api_server = Rc::new(RefCell::new(KubeApiServer::new(
        persistent_storage_context.id(),
        kube_api_server_context,
        config.clone(),
        NodePool::new(config.node_pool_capacity, &mut sim),
    )));
    let kube_api_server_id =
        sim.add_handler(kube_api_server_component_name, kube_api_server.clone());

    let scheduler = Rc::new(RefCell::new(KubeGenericScheduler::new(
        kube_api_server_id,
        scheduler_context,
        config.clone(),
    )));
    let scheduler_id = sim.add_handler(scheduler_component_name, scheduler.clone());

    let persistent_storage = Rc::new(RefCell::new(PersistentStorage::new(
        kube_api_server_id,
        scheduler_id,
        persistent_storage_context,
        config.clone(),
    )));
    sim.add_handler(
        persistent_storage_component_name,
        persistent_storage.clone(),
    );

    // First, we fully read the trace and push all events from it to simulation queue to kube_api_server
    // at corresponding event timestamps.

    // Asserting we start with the current time = 0, then all delays in emit() calls are equal to
    // the timestamps of events in a trace.
    assert_eq!(sim.time(), 0.0);

    initialize_default_cluster(config, persistent_storage.clone(), kube_api_server.clone(), scheduler.clone(), &mut sim);

    for (ts, event) in cluster_trace.convert_to_simulator_events().into_iter() {
        client.emit(event, kube_api_server_id, ts);
    }
    for (ts, event) in workload_trace.convert_to_simulator_events().into_iter() {
        client.emit(event, kube_api_server_id, ts);
    }

    // Then, we run simulation until completion of all events and measure time.
    let t = Instant::now();
    sim.step_until_no_events();
    let duration = t.elapsed().as_secs_f64();
    info!(
        "Processed {} events in {:.2?}s ({:.0} events/s)",
        sim.event_count(),
        duration,
        sim.event_count() as f64 / duration
    );
}
