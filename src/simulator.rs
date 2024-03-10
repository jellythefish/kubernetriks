//! Represents entry point for simulator and its config.

use log::info;
use std::time::Instant;
use std::{cell::RefCell, rc::Rc};

use serde::Deserialize;

use dslab_core::simulation::Simulation;

use crate::core::api_server::KubeApiServer;
use crate::core::node_cluster::NodeCluster;
use crate::core::persistent_storage::PersistentStorage;
use crate::core::scheduler::KubeScheduler;
use crate::trace::generic::GenericTrace;
use crate::trace::interface::Trace;

#[derive(Debug, Deserialize)]
pub struct SimulatorConfig {
    sim_name: String,
    seed: u64,
    // Simulated network delays, as = api server, ps = persistent storage, nc = node cluster.
    // All delays are in seconds with fractional part. Assuming all delays are bidirectional.
    pub as_to_ps_network_delay: f64,
    pub ps_to_sched_network_delay: f64,
    pub sched_to_as_network_delay: f64,
    pub as_to_nc_network_delay: f64,
}

pub fn run_simulator(config: Rc<SimulatorConfig>, mut trace: GenericTrace) {
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
    let kube_scheduler_component_name = "kube_scheduler";
    let node_cluster_component_name = "node_cluster";

    let kube_api_server_context = sim.create_context(kube_api_server_component_name);
    let persistent_storage_context = sim.create_context(persistent_storage_component_name);
    let kube_scheduler_context = sim.create_context(kube_scheduler_component_name);
    let node_cluster_context = sim.create_context(node_cluster_component_name);

    let node_cluster = Rc::new(RefCell::new(NodeCluster::new(
        kube_api_server_context.id(),
        node_cluster_context,
    )));
    let node_cluster_id = sim.add_handler(node_cluster_component_name, node_cluster.clone());

    let kube_api_server = Rc::new(RefCell::new(KubeApiServer::new(
        node_cluster_id,
        persistent_storage_context.id(),
        kube_api_server_context,
        config.clone(),
    )));
    let kube_api_server_id =
        sim.add_handler(kube_api_server_component_name, kube_api_server.clone());

    let kube_scheduler = Rc::new(RefCell::new(KubeScheduler::new(
        kube_api_server_id,
        kube_scheduler_context,
        config.clone(),
    )));
    let kube_scheduler_id = sim.add_handler(kube_scheduler_component_name, kube_scheduler.clone());

    let persistent_storage = Rc::new(RefCell::new(PersistentStorage::new(
        kube_api_server_id,
        kube_scheduler_id,
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

    for (ts, event) in trace.convert_to_simulator_events() {
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
