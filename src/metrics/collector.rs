//! Implements centralized storage for metrics. Any component may access this component to
//! report metrics about pods, nodes, etc.

use average::{concatenate, Estimate, Max, Mean, Min, Variance};

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
            .field("population_variance", &self.population_variance).finish()
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
        self.min() == other.min() &&
        self.max() == other.max() &&
        self.mean() == other.mean() &&
        self.population_variance() == other.population_variance()
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

#[derive(Default)]
pub struct MetricsCollector {
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

    pub internal: InternalMetrics,
}

impl MetricsCollector {
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
            internal: InternalMetrics {
                processed_nodes: 0,
                terminated_pods: 0,
            },
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
