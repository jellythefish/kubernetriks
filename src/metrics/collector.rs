//! Implements centralized storage for metrics. Any component may access this component to
//! report metrics about pods, nodes, etc.

use std::{cell::RefCell, collections::HashMap, rc::Rc};

use average::{concatenate, Estimate, Max, Mean, Min, Variance};
use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::{api_server::KubeApiServer, events::RunPodMetricsCollectionCycle};

concatenate!(
    Estimator,
    [Min, min],
    [Max, max],
    [Mean, mean],
    [Variance, population_variance]
);

#[derive(Debug, Default)]
pub struct EstimatorWrapper {
    estimator: Estimator,
}

impl std::fmt::Debug for Estimator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Estimator")
            .field("min", &self.min)
            .field("max", &self.max)
            .field("mean", &self.mean)
            .field("population_variance", &self.population_variance)
            .finish()
    }
}

impl EstimatorWrapper {
    pub fn new() -> Self {
        Self {
            estimator: Estimator::new(),
        }
    }

    pub fn add(&mut self, value: f64) {
        self.estimator.add(value);
    }

    pub fn min(&self) -> f64 {
        self.estimator.min()
    }

    pub fn max(&self) -> f64 {
        self.estimator.max()
    }

    pub fn mean(&self) -> f64 {
        self.estimator.mean()
    }

    pub fn population_variance(&self) -> f64 {
        self.estimator.population_variance()
    }
}

impl PartialEq for EstimatorWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.min() == other.min()
            && self.max() == other.max()
            && self.mean() == other.mean()
            && self.population_variance() == other.population_variance()
    }
}

#[derive(Default)]
pub struct InternalMetrics {
    /// The number of node creations that were processed. Increases with the progress of simulation.
    /// This counter does not include the nodes from default cluster.
    pub processed_nodes: u64,
    /// The number of pods that were terminated either with failure or success.
    /// Increases with the progress of simulation.
    /// Equals to pods succeeded + pods unschedulable + pods failed + pods removed.
    pub terminated_pods: u64,
}

pub struct SimulationMetrics {
    /// The number of created nodes in trace. Calculated before simulation starts.
    pub total_nodes_in_trace: u64,
    /// The number of created pods in trace. Calculated before simulation starts.
    pub total_pods_in_trace: u64,
    /// The number of successfully finished pods.
    pub pods_succeeded: u64,
    /// The number of pods which could not be scheduled and started during the simulation.
    /// Calculated at the stage of trace preprocessing.
    pub pods_unschedulable: u64,
    /// The number of failed pods which started but eventually not finished due to some reasons.
    pub pods_failed: u64,
    /// The number of removed pods due to the events in the trace.
    pub pods_removed: u64,

    /// Estimations for the pod running duration.
    pub pod_duration_stats: EstimatorWrapper,

    /// Estimations for the time a pod spent between it was popped from queue by scheduler
    /// and assigned a node. Considers only pods in one scheduling cycle which were successfully
    /// assigned a node.
    pub pod_scheduling_algorithm_latency_stats: EstimatorWrapper,

    /// Estimations for the time a pod spent between it was firstly pushed to the scheduling queue
    /// and lastly popped from it thus considering repushes due to insufficient resources
    /// scheduling error.
    pub pod_queue_time_stats: EstimatorWrapper,

    // Auto scaler metrics
    /// Total number of scaled up nodes
    pub total_scaled_up_nodes: u64,
    /// Total number of scaled down nodes
    pub total_scaled_down_nodes: u64,
    /// Total number of scaled up pods
    pub total_scaled_up_pods: u64,
    /// Total number of scaled down pods
    pub total_scaled_down_pods: u64,

    pub internal: InternalMetrics,

    /// Map of pod group to aggregated metrics of resource utilization for a group.
    /// First parameter in tuple is cpu and second - ram.
    pub pod_utilization_metrics: HashMap<String, (EstimatorWrapper, EstimatorWrapper)>,
}

impl SimulationMetrics {
    pub fn new() -> Self {
        Self {
            total_nodes_in_trace: 0,
            total_pods_in_trace: 0,
            pods_succeeded: 0,
            pods_unschedulable: 0,
            pods_failed: 0,
            pods_removed: 0,
            pod_duration_stats: EstimatorWrapper::new(),
            pod_scheduling_algorithm_latency_stats: EstimatorWrapper::new(),
            pod_queue_time_stats: EstimatorWrapper::new(),
            total_scaled_up_nodes: 0,
            total_scaled_down_nodes: 0,
            total_scaled_up_pods: 0,
            total_scaled_down_pods: 0,
            internal: InternalMetrics {
                processed_nodes: 0,
                terminated_pods: 0,
            },
            pod_utilization_metrics: Default::default(),
        }
    }

    pub fn increment_pod_duration(&mut self, value: f64) {
        self.pod_duration_stats.add(value);
    }

    pub fn increment_pod_scheduling_algorithm_latency(&mut self, value: f64) {
        self.pod_scheduling_algorithm_latency_stats.add(value);
    }

    pub fn increment_pod_queue_time(&mut self, value: f64) {
        self.pod_queue_time_stats.add(value);
    }
}

pub struct MetricsCollector {
    // `api_server_component` and `ctx` are options to resolve cyclic dependencies and to set them later in
    // setter methods.
    /// Direct access to api server for node components.
    pub api_server_component: Option<Rc<RefCell<KubeApiServer>>>,

    pub ctx: Option<SimulationContext>,

    pub metrics: SimulationMetrics,

    /// Collection interval only for pod utilization metrics pulling
    collection_interval: f64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            ctx: None,
            api_server_component: None,
            metrics: SimulationMetrics::new(),
            collection_interval: 60.0, // TODO: make configurable?
        }
    }

    pub fn set_api_server_component(&mut self, api_server_component: Rc<RefCell<KubeApiServer>>) {
        self.api_server_component = Some(api_server_component);
    }

    pub fn set_context(&mut self, ctx: SimulationContext) {
        self.ctx = Some(ctx);
    }

    pub fn start_pod_metrics_collection(&self) {
        self.ctx
            .as_ref()
            .unwrap()
            .emit_self_now(RunPodMetricsCollectionCycle {});
    }

    fn collect_pod_metrics(&mut self, event_time: f64) {
        // clear pod metrics as we have only last state for them
        self.metrics.pod_utilization_metrics.clear();
        let all_nodes = self
            .api_server_component
            .as_ref()
            .unwrap()
            .borrow()
            .all_created_nodes();
        let mut pod_count_in_pod_groups: HashMap<String, usize> = Default::default();

        // count pods in all pod groups
        for node in all_nodes.iter() {
            for pod in node.borrow().running_pods.values() {
                if pod.pod_group.is_some() {
                    let pod_group_name = pod.pod_group.as_ref().unwrap().clone();

                    if pod_count_in_pod_groups.contains_key(&pod_group_name) {
                        *pod_count_in_pod_groups.get_mut(&pod_group_name).unwrap() += 1;
                    } else {
                        pod_count_in_pod_groups.insert(pod_group_name, 1);
                    }
                }
            }
        }

        // collect utilization from each pod
        for node in all_nodes.iter() {
            for pod in node.borrow_mut().running_pods.values_mut() {
                if pod.pod_group.is_some() {
                    let pod_group_name = pod.pod_group.as_ref().unwrap().clone();
                    let total_pods_in_group = pod_count_in_pod_groups.get(&pod_group_name).unwrap();

                    let cpu_utilization = pod
                        .cpu_usage_model
                        .as_mut()
                        .and_then(|model| {
                            Some(model.current_usage(event_time, Some(*total_pods_in_group)))
                        })
                        .or(Some(0.0))
                        .unwrap();

                    let ram_utilization = pod
                        .ram_usage_model
                        .as_mut()
                        .and_then(|model| {
                            Some(model.current_usage(event_time, Some(*total_pods_in_group)))
                        })
                        .or(Some(0.0))
                        .unwrap();

                    if self
                        .metrics
                        .pod_utilization_metrics
                        .contains_key(&pod_group_name)
                    {
                        let utils = self
                            .metrics
                            .pod_utilization_metrics
                            .get_mut(&pod_group_name)
                            .unwrap();
                        utils.0.add(cpu_utilization);
                        utils.1.add(ram_utilization);
                    } else {
                        let mut utils = (EstimatorWrapper::new(), EstimatorWrapper::new());
                        utils.0.add(cpu_utilization);
                        utils.1.add(ram_utilization);
                        self.metrics
                            .pod_utilization_metrics
                            .insert(pod_group_name, utils);
                    }
                }
            }
        }
    }

    // Returns map of pod group name and pair of cpu mean utilization and ram mean utilization.
    pub fn pod_metrics_mean_utilization(&self) -> HashMap<String, (f64, f64)> {
        let mut metrics: HashMap<String, (f64, f64)> = Default::default();

        for (pod_group, (cpu_util, ram_util)) in self.metrics.pod_utilization_metrics.iter() {
            metrics.insert(pod_group.clone(), (cpu_util.mean(), ram_util.mean()));
        }

        metrics
    }
}

impl EventHandler for MetricsCollector {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            RunPodMetricsCollectionCycle {} => {
                self.collect_pod_metrics(event.time);

                self.ctx
                    .as_ref()
                    .unwrap()
                    .emit_self(RunPodMetricsCollectionCycle {}, self.collection_interval);
            }
        })
    }
}
