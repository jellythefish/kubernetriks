//! Implements centralized storage for metrics. Any component may access this component to
//! report metrics about pods, nodes, etc.

use std::{cell::RefCell, collections::HashMap, fs::File, rc::Rc};

use average::{concatenate, Estimate, Max, Mean, Min, Variance};
use csv::Writer;
use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::{
    api_server::KubeApiServer,
    events::{RecordGaugeMetricsCycle, RunPodMetricsCollectionCycle},
};

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

pub struct AccumulatedMetrics {
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

impl AccumulatedMetrics {
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

/// Metrics which represents state at certain point of simulation time.
pub struct GaugeMetrics {
    pub current_nodes: u64,
    pub current_pods: u64,
    pub pods_in_scheduling_queues: u64,
    pub node_average_cpu_utilization: f64,
    pub node_average_ram_utilization: f64,
    pub cluster_total_cpu_utilization: f64,
    pub cluster_total_ram_utilization: f64,
}

impl GaugeMetrics {
    pub fn new() -> Self {
        Self {
            current_nodes: 0,
            current_pods: 0,
            pods_in_scheduling_queues: 0,
            node_average_cpu_utilization: 0.0,
            node_average_ram_utilization: 0.0,
            cluster_total_cpu_utilization: 0.0,
            cluster_total_ram_utilization: 0.0,
        }
    }
}

pub struct MetricsCollector {
    // `api_server_component` and `ctx` are options to resolve cyclic dependencies and to set them later in
    // setter methods.
    /// Direct access to api server for node components.
    pub api_server_component: Option<Rc<RefCell<KubeApiServer>>>,

    pub ctx: Option<SimulationContext>,

    pub accumulated_metrics: AccumulatedMetrics,
    pub gauge_metrics: GaugeMetrics,

    /// Writer of metrics to file in csv format, including header.
    pub gauge_metrics_writer: Writer<File>,
    /// Record interval for writing gauge metrics.
    record_interval: f64,

    /// Collection interval only for pod utilization metrics pulling
    collection_interval: f64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let mut gauge_metrics_writer = Writer::from_path("gauge_metrics.csv").unwrap();
        gauge_metrics_writer
            .write_record(&[
                "timestamp",
                "current_nodes",
                "current_pods",
                "pods_in_scheduling_queues",
                "node_average_cpu_utilization",
                "node_average_ram_utilization",
                "cluster_total_cpu_utilization",
                "cluster_total_ram_utilization",
            ])
            .unwrap();

        Self {
            ctx: None,
            api_server_component: None,
            accumulated_metrics: AccumulatedMetrics::new(),
            gauge_metrics: GaugeMetrics::new(),
            gauge_metrics_writer,
            record_interval: 5.0,
            collection_interval: 60.0, // TODO: make configurable?
        }
    }

    pub fn set_api_server_component(&mut self, api_server_component: Rc<RefCell<KubeApiServer>>) {
        self.api_server_component = Some(api_server_component);
    }

    pub fn set_context(&mut self, ctx: SimulationContext) {
        self.ctx = Some(ctx);
    }

    pub fn start_gauge_metrics_recording(&self) {
        self.ctx
            .as_ref()
            .unwrap()
            .emit_self_now(RecordGaugeMetricsCycle {});
    }

    pub fn start_pod_metrics_collection(&self) {
        self.ctx
            .as_ref()
            .unwrap()
            .emit_self_now(RunPodMetricsCollectionCycle {});
    }

    fn collect_pod_metrics(&mut self, event_time: f64) {
        // clear pod metrics as we have only last state for them
        self.accumulated_metrics.pod_utilization_metrics.clear();
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
                        .accumulated_metrics
                        .pod_utilization_metrics
                        .contains_key(&pod_group_name)
                    {
                        let utils = self
                            .accumulated_metrics
                            .pod_utilization_metrics
                            .get_mut(&pod_group_name)
                            .unwrap();
                        utils.0.add(cpu_utilization);
                        utils.1.add(ram_utilization);
                    } else {
                        let mut utils = (EstimatorWrapper::new(), EstimatorWrapper::new());
                        utils.0.add(cpu_utilization);
                        utils.1.add(ram_utilization);
                        self.accumulated_metrics
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

        for (pod_group, (cpu_util, ram_util)) in
            self.accumulated_metrics.pod_utilization_metrics.iter()
        {
            metrics.insert(pod_group.clone(), (cpu_util.mean(), ram_util.mean()));
        }

        metrics
    }

    pub fn collect_utilizations(&mut self) {
        let all_nodes = self
            .api_server_component
            .as_ref()
            .unwrap()
            .borrow()
            .all_created_nodes();

        self.gauge_metrics.node_average_cpu_utilization = 0.0;
        self.gauge_metrics.node_average_ram_utilization = 0.0;
        let mut cluster_cpu_requests = 0u64;
        let mut cluster_ram_requests = 0u64;
        let mut cluster_cpu_capacity = 0u64;
        let mut cluster_ram_capacity = 0u64;
        let node_count = all_nodes.len();

        for node in all_nodes.iter() {
            let node_component = node.borrow();
            let status = &node_component.runtime.as_ref().unwrap().node.status;
            let cpu_request = status.capacity.cpu as u64 - status.allocatable.cpu as u64;
            let ram_request = status.capacity.ram as u64 - status.allocatable.ram as u64;

            self.gauge_metrics.node_average_cpu_utilization +=
                cpu_request as f64 / status.capacity.cpu as f64;
            self.gauge_metrics.node_average_ram_utilization +=
                ram_request as f64 / status.capacity.ram as f64;
            cluster_cpu_requests += cpu_request;
            cluster_ram_requests += ram_request;
            cluster_cpu_capacity += status.capacity.cpu as u64;
            cluster_ram_capacity += status.capacity.ram as u64;
        }

        self.gauge_metrics.node_average_cpu_utilization /= node_count as f64;
        self.gauge_metrics.node_average_ram_utilization /= node_count as f64;
        self.gauge_metrics.cluster_total_cpu_utilization =
            cluster_cpu_requests as f64 / cluster_cpu_capacity as f64;
        self.gauge_metrics.cluster_total_ram_utilization =
            cluster_ram_requests as f64 / cluster_ram_capacity as f64;
    }

    pub fn record_gauge_metrics(&mut self, current_time: f64) {
        self.collect_utilizations();

        self.gauge_metrics_writer
            .write_record(&[
                current_time.to_string(),
                self.gauge_metrics.current_nodes.to_string(),
                self.gauge_metrics.current_pods.to_string(),
                self.gauge_metrics.pods_in_scheduling_queues.to_string(),
                self.gauge_metrics.node_average_cpu_utilization.to_string(),
                self.gauge_metrics.node_average_ram_utilization.to_string(),
                self.gauge_metrics.cluster_total_cpu_utilization.to_string(),
                self.gauge_metrics.cluster_total_ram_utilization.to_string(),
            ])
            .unwrap();
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
            RecordGaugeMetricsCycle {} => {
                self.record_gauge_metrics(event.time);

                self.ctx
                    .as_ref()
                    .unwrap()
                    .emit_self(RecordGaugeMetricsCycle {}, self.record_interval);
            }
        })
    }
}
