//! Represents entry point for simulator and its config.

use log::info;
use std::cmp::max;
use std::collections::HashSet;
use std::time::Instant;
use std::{cell::RefCell, rc::Rc};

use serde::Deserialize;

use dslab_core::simulation::Simulation;

use crate::core::api_server::KubeApiServer;
use crate::core::common::SimulationEvent;
use crate::core::events::{CreateNodeRequest, CreatePodRequest, RemoveNodeRequest};
use crate::core::node::{Node, NodeConditionType};
use crate::core::node_component::{NodeComponent, NodeRuntime};
use crate::core::node_component_pool::NodeComponentPool;
use crate::core::persistent_storage::PersistentStorage;
use crate::core::pod::PodConditionType;
use crate::core::scheduler::interface::PodSchedulingAlgorithm;
use crate::core::scheduler::kube_scheduler::{default_kube_scheduler_config, KubeScheduler};
use crate::core::scheduler::scheduler::Scheduler;

use crate::trace::interface::Trace;

#[derive(Debug, Deserialize, PartialEq)]
pub struct SimulationConfig {
    pub sim_name: String,
    pub seed: u64,
    pub trace_config: Option<TraceConfig>,
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
    pub sim: Simulation,

    pub api_server: Rc<RefCell<KubeApiServer>>,
    pub persistent_storage: Rc<RefCell<PersistentStorage>>,
    pub scheduler: Rc<RefCell<Scheduler>>,

    pub pod_names: Vec<String>,
    pub node_names: Vec<String>,
}

pub trait SimulationCallbacks {
    /// Runs before starting a simulation run.
    fn on_simulation_start(&mut self, _sim: &mut KubernetriksSimulation) {}

    /// Runs on each step of a simulation run, returns false if the simulation must be stopped.
    fn on_step(&mut self, _sim: &mut KubernetriksSimulation) -> bool {
        true
    }

    /// Runs upon the completion of a simulation run, returns results of this run.
    fn on_simulation_finish(&mut self, _sim: &mut KubernetriksSimulation) {}
}

pub struct RunUntilAllPodsAreFinishedCallbacks {
    created_pods: HashSet<String>,
}

impl RunUntilAllPodsAreFinishedCallbacks {
    pub fn new() -> Self {
        Self {
            created_pods: Default::default(),
        }
    }

    fn check_pods(&mut self, sim: &mut KubernetriksSimulation) -> bool {
        let mut ready_pods: Vec<&str> = vec![];
        let persistent_storage_borrowed = sim.persistent_storage.borrow();
        for pod in self.created_pods.iter() {
            let pod_option = persistent_storage_borrowed.get_pod(&pod);
            if pod_option.is_none() {
                continue;
            }
            let pod = pod_option.unwrap();
            match pod.get_condition(PodConditionType::PodSucceeded) {
                Some(_) => ready_pods.push(&pod.metadata.name),
                None => {}
            }
        }

        for pod in ready_pods {
            self.created_pods.remove(pod);
        }

        !self.created_pods.is_empty()
    }
}

impl SimulationCallbacks for RunUntilAllPodsAreFinishedCallbacks {
    fn on_simulation_start(&mut self, sim: &mut KubernetriksSimulation) {
        for created_pod in sim.pod_names.iter() {
            self.created_pods.insert(created_pod.clone());
        }
    }

    fn on_step(&mut self, sim: &mut KubernetriksSimulation) -> bool {
        if sim.sim.time() % 1000.0 == 0.0 {
            return self.check_pods(sim);
        }
        true
    }
}

/// Calculates number of simultaneously existing nodes in trace by counting node creations and
/// removals. Used as node pool capacity.
fn max_nodes_in_trace(trace: &Vec<(f64, Box<dyn SimulationEvent>)>) -> usize {
    let mut count: usize = 0;
    let mut max_count: usize = 0;

    for (_, event) in trace.iter() {
        if let Some(_) = event.downcast_ref::<CreateNodeRequest>() {
            count += 1;
        } else if let Some(_) = event.downcast_ref::<RemoveNodeRequest>() {
            count -= 1;
        }
        max_count = max(count, max_count);
    }

    max_count
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

        let default_scheduler_impl = Box::new(KubeScheduler {
            config: default_kube_scheduler_config(),
        });

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
            pod_names: Default::default(),
            node_names: Default::default(),
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

        let cluster_trace_events = cluster_trace.convert_to_simulator_events();
        let max_nodes = max_nodes_in_trace(&cluster_trace_events);
        info!("Node pool capacity={:?} (from trace)", max_nodes);

        self.api_server
            .borrow_mut()
            .set_node_pool(NodeComponentPool::new(max_nodes, &mut self.sim));

        self.initialize_default_cluster();

        for (ts, event) in cluster_trace_events.into_iter() {
            client.emit(event, self.api_server.borrow().ctx.id(), ts);
        }
        for (ts, event) in workload_trace.convert_to_simulator_events().into_iter() {
            // TODO: make general trace preprocessors with preprocess callbacks and info stored as field in Simulation
            if let Some(create_pod_req) = event.downcast_ref::<CreatePodRequest>() {
                self.pod_names
                    .push(create_pod_req.pod.metadata.name.clone());
            }
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

    pub fn set_scheduler_algorithm(
        &mut self,
        scheduler_algorithm: Box<dyn PodSchedulingAlgorithm>,
    ) {
        self.scheduler
            .borrow_mut()
            .set_scheduler_algorithm(scheduler_algorithm)
    }

    pub fn run_with_callbacks(&mut self, mut callbacks: Box<dyn SimulationCallbacks>) {
        self.scheduler.borrow_mut().start();

        callbacks.on_simulation_start(self);

        let t = Instant::now();
        while callbacks.on_step(self) {
            self.sim.step();
        }
        let duration = t.elapsed().as_secs_f64();
        info!(
            "Processed {} events in {:.2?}s ({:.0} events/s)",
            self.sim.event_count(),
            duration,
            self.sim.event_count() as f64 / duration
        );
        info!("Finished at {}", self.sim.time());

        callbacks.on_simulation_finish(self);
    }

    pub fn run_until_no_events(&mut self) {
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

    /// Returns `true` if there could be more pending events and `false` otherwise.
    pub fn step_for_duration(&mut self, duration: f64) -> bool {
        self.sim.step_for_duration(duration)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        core::{
            common::SimulationEvent,
            events::{CreateNodeRequest, RemoveNodeRequest},
            node::Node,
        },
        simulator::max_nodes_in_trace,
    };

    #[test]
    fn test_max_nodes_in_trace_of_node_creations_only() {
        let trace: Vec<(f64, Box<dyn SimulationEvent>)> = vec![
            (
                10.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                15.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                20.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                350.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
        ];
        assert_eq!(4, max_nodes_in_trace(&trace));
    }

    #[test]
    fn test_max_nodes_in_trace_of_node_creations_and_removals() {
        let trace: Vec<(f64, Box<dyn SimulationEvent>)> = vec![
            (
                10.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                15.0f64,
                Box::new(RemoveNodeRequest {
                    node_name: "name".to_string(),
                }),
            ),
            (
                20.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                35.0f64,
                Box::new(RemoveNodeRequest {
                    node_name: "name".to_string(),
                }),
            ),
        ];
        assert_eq!(1, max_nodes_in_trace(&trace));

        let trace: Vec<(f64, Box<dyn SimulationEvent>)> = vec![
            (
                10.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                11.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                12.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                13.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                14.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                15.0f64,
                Box::new(RemoveNodeRequest {
                    node_name: "name".to_string(),
                }),
            ),
            (
                16.0f64,
                Box::new(RemoveNodeRequest {
                    node_name: "name".to_string(),
                }),
            ),
            (
                17.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
            (
                18.0f64,
                Box::new(CreateNodeRequest {
                    node: Node::new("name".to_string(), 0, 0),
                }),
            ),
        ];
        assert_eq!(5, max_nodes_in_trace(&trace));
    }
}
