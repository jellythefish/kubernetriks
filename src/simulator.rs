//! Represents entry point for simulator and its config.

use log::info;
use std::cmp::max;
use std::time::Instant;
use std::{cell::RefCell, rc::Rc};

use serde::Deserialize;

use dslab_core::simulation::Simulation;

use crate::core::api_server::KubeApiServer;
use crate::core::node::{Node, NodeConditionType};
use crate::core::node_component::{NodeComponent, NodeRuntime};
use crate::core::node_component_pool::NodeComponentPool;
use crate::core::persistent_storage::PersistentStorage;
use crate::core::scheduler::{AnyScheduler, LeastRequestedPriorityScheduler, Scheduler};

use crate::trace::interface::Trace;

#[derive(Debug, Deserialize, PartialEq)]
pub struct SimulationConfig {
    pub sim_name: String,
    pub seed: u64,
    pub trace_config: Option<TraceConfig>,
    pub node_pool_capacity: usize,
    pub default_cluster: Option<Vec<NodeGroup>>,
    pub scheduling_cycle_interval: f64, // in seconds
    // Simulated network delays, as = api server, ps = persistent storage.
    // All delays are in seconds with fractional part. Assuming all delays are bidirectional.
    pub as_to_ps_network_delay: f64,
    pub ps_to_sched_network_delay: f64,
    pub sched_to_as_network_delay: f64,
    pub as_to_node_network_delay: f64,
}
#[derive(Clone, Default, Debug, Deserialize, PartialEq)]
pub struct AlibabaWorkloadTraceV2017Paths {
    pub batch_instance_trace_path: String,
    pub batch_task_trace_path: String,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq)]
pub struct GenericTracePaths {
    pub workload_trace_path: String,
    pub cluster_trace_path: String,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq)]
pub struct TraceConfig {
    // should be one of, not both
    pub alibaba_cluster_trace_v2017: Option<AlibabaWorkloadTraceV2017Paths>,
    pub generic_trace: Option<GenericTracePaths>,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq)]
pub struct NodeGroup {
    // If node count is not none and node's metadata has name, then it's taken as a prefix of all nodes
    // in a group.
    // If node count is none or 1 and node's metadata has name, then it's a single node and its name is set
    // to metadata name.
    // If metadata has got no name, then prefix default_node(_<idx>)? is used.
    pub node_count: Option<u64>,
    pub node_template: Node,
}

pub struct KubernetriksSimulation {
    config: Rc<SimulationConfig>,
    sim: Simulation,

    pub api_server: Rc<RefCell<KubeApiServer>>,
    pub persistent_storage: Rc<RefCell<PersistentStorage>>,
    pub scheduler: Rc<RefCell<Scheduler>>,
}

impl KubernetriksSimulation {
    pub fn new(config: Rc<SimulationConfig>) -> Self {
        info!(
            "Creating kubernetriks simulation {:?} with config: {:?}",
            config.sim_name, config
        );

        let mut sim = Simulation::new(config.seed);

        // Register simulator components
        let api_server_component_name = "kube_api_server";
        let persistent_storage_component_name = "persistent_storage";
        let scheduler_component_name = "scheduler";

        let kube_api_server_context = sim.create_context(api_server_component_name);
        let persistent_storage_context = sim.create_context(persistent_storage_component_name);
        let scheduler_context = sim.create_context(scheduler_component_name);

        let api_server = Rc::new(RefCell::new(KubeApiServer::new(
            persistent_storage_context.id(),
            kube_api_server_context,
            config.clone(),
        )));
        let api_server_id = sim.add_handler(api_server_component_name, api_server.clone());

        let default_scheduler_impl = Box::new(LeastRequestedPriorityScheduler {});

        let scheduler = Rc::new(RefCell::new(Scheduler::new(
            api_server_id,
            default_scheduler_impl,
            scheduler_context,
            config.clone(),
        )));
        let scheduler_id = sim.add_handler(scheduler_component_name, scheduler.clone());

        let persistent_storage = Rc::new(RefCell::new(PersistentStorage::new(
            api_server_id,
            scheduler_id,
            persistent_storage_context,
            config.clone(),
        )));
        sim.add_handler(
            persistent_storage_component_name,
            persistent_storage.clone(),
        );

        KubernetriksSimulation {
            config,
            sim,
            api_server,
            persistent_storage,
            scheduler,
        }
    }

    pub fn initialize(&mut self, cluster_trace: &mut dyn Trace, workload_trace: &mut dyn Trace) {
        // Client context for submitting trace events to kube_api_server
        let client = self.sim.create_context("client");

        // We fully read the traces and push all events from them to simulation queue to api server
        // at corresponding event timestamps.

        // Asserting we start with the current time = 0, then all delays in emit() calls are equal to
        // the timestamps of events in a trace.
        assert_eq!(self.sim.time(), 0.0);

        let node_pool_size = max(self.config.node_pool_capacity, cluster_trace.event_count());
        self.api_server
            .borrow_mut()
            .set_node_pool(NodeComponentPool::new(node_pool_size, &mut self.sim));

        self.initialize_default_cluster();

        for (ts, event) in cluster_trace.convert_to_simulator_events().into_iter() {
            client.emit(event, self.api_server.borrow().ctx.id(), ts);
        }
        for (ts, event) in workload_trace.convert_to_simulator_events().into_iter() {
            client.emit(event, self.api_server.borrow().ctx.id(), ts);
        }
    }

    pub fn add_node(&mut self, mut node: Node) {
        let node_name = node.metadata.name.clone();
        let node_context = self.sim.create_context(node_name.clone());

        node.update_condition("True".to_string(), NodeConditionType::NodeCreated, 0.0);
        node.metadata.name = node_name.clone();
        node.status.allocatable = node.status.capacity.clone();

        // add to persistent storage
        self.persistent_storage.borrow_mut().add_node(node.clone());
        // add to api server
        let node_component = Rc::new(RefCell::new(NodeComponent::new(node_context)));
        node_component.borrow_mut().runtime = Some(NodeRuntime {
            api_server: self.api_server.borrow().ctx.id(),
            node: node.clone(),
            config: self.config.clone(),
        });
        self.api_server
            .borrow_mut()
            .add_node_component(node_component.clone());
        // add to scheduler
        self.scheduler.borrow_mut().add_node(node.clone());

        self.sim.add_handler(node_name, node_component);
    }

    pub fn initialize_default_cluster(&mut self) {
        if self.config.default_cluster.is_none()
            || self.config.default_cluster.as_ref().unwrap().len() == 0
        {
            return;
        }
        let mut total_nodes = 0;
        for node_group in self
            .config
            .default_cluster
            .as_ref()
            .unwrap()
            .clone()
            .into_iter()
        {
            let name_prefix: String;
            let node_count_in_group = node_group.node_count.unwrap_or(1);

            if node_count_in_group == 1 && node_group.node_template.metadata.name.len() > 0 {
                let mut node = node_group.node_template.clone();
                // use name prefix as-is without suffix
                node.metadata.name = node_group.node_template.metadata.name.clone();
                self.add_node(node);
                continue;
            } else if node_group.node_template.metadata.name.len() > 0 {
                name_prefix = node_group.node_template.metadata.name.clone();
            } else {
                name_prefix = "default_node".to_string();
            }

            for _ in 0..node_count_in_group {
                let mut node = node_group.node_template.clone();
                node.metadata.name = format!("{}_{}", name_prefix, total_nodes);
                self.add_node(node);
                total_nodes += 1;
            }
        }
    }

    pub fn set_scheduler_impl(&mut self, scheduler_impl: Box<dyn AnyScheduler>) {
        self.scheduler
            .borrow_mut()
            .set_scheduler_impl(scheduler_impl)
    }

    pub fn run(&mut self) {
        // Run simulation until completion of all events and measure time.
        self.scheduler.borrow_mut().start();

        let t = Instant::now();
        self.sim.step_until_no_events();
        let duration = t.elapsed().as_secs_f64();
        info!(
            "Processed {} events in {:.2?}s ({:.0} events/s)",
            self.sim.event_count(),
            duration,
            self.sim.event_count() as f64 / duration
        );
    }

    pub fn step(&mut self) {
        self.sim.step();
    }
}
