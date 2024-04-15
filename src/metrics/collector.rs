//! Implements centralized storage for metrics. Any component may access this component to
//! report metrics about pods, nodes, etc.

use std::fs::File;

use average::{concatenate, Estimate, Max, Mean, Min, Variance};

use prettytable::{row, Table};

concatenate!(
    Estimator,
    [Min, min],
    [Max, max],
    [Mean, mean],
    [Variance, population_variance]
);

#[derive(Default)]
pub struct MetricsCollector {
    /// total_pods = pods_succeeded + pods_failed
    pub total_pods: u64,
    pub pods_succeeded: u64,
    pub pods_failed: u64,

    /// Estimations for the pod running duration.
    pod_duration_stats: Estimator,

    /// Estimations for the time a pod spent between it was popped from queue by scheduler
    /// and assigned a node.
    pod_schedule_time_stats: Estimator,

    /// Estimations for the time a pod spent between it was pushed to the scheduling queue
    /// and popped from it.
    pod_queue_time_stats: Estimator,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            total_pods: 0,
            pods_succeeded: 0,
            pods_failed: 0,
            pod_duration_stats: Estimator::new(),
            pod_schedule_time_stats: Estimator::new(),
            pod_queue_time_stats: Estimator::new(),
        }
    }

    pub fn increment_pod_duration(&mut self, value: f64) {
        self.pod_duration_stats.add(value);
    }

    pub fn increment_pod_schedule_time(&mut self, value: f64) {
        self.pod_schedule_time_stats.add(value);
    }

    pub fn increment_pod_queue_time(&mut self, value: f64) {
        self.pod_queue_time_stats.add(value);
    }

    pub fn dump_metrics(&self) {
        let mut metrics_file = File::create("metrics.txt").unwrap();

        let mut aggregated_table = Table::new();
        aggregated_table.add_row(row!["Metric", "Count"]);
        aggregated_table.add_row(row!["Total pods", self.total_pods]);
        aggregated_table.add_row(row!["Pods succeeded", self.pods_succeeded]);
        aggregated_table.add_row(row!["Pods failed", self.pods_failed]);

        let mut stats_table = Table::new();
        stats_table.add_row(row!["Metric", "Min", "Max", "Mean", "Variance"]);
        stats_table.add_row(row![
            "Pod duration",
            self.pod_duration_stats.min(),
            self.pod_duration_stats.max(),
            self.pod_duration_stats.mean(),
            self.pod_duration_stats.population_variance()
        ]);
        stats_table.add_row(row![
            "Pod schedule time",
            self.pod_schedule_time_stats.min(),
            self.pod_schedule_time_stats.max(),
            self.pod_schedule_time_stats.mean(),
            self.pod_schedule_time_stats.population_variance()
        ]);
        stats_table.add_row(row![
            "Pod queue time",
            self.pod_queue_time_stats.min(),
            self.pod_queue_time_stats.max(),
            self.pod_queue_time_stats.mean(),
            self.pod_queue_time_stats.population_variance()
        ]);

        let _ = aggregated_table.print(&mut metrics_file);
        let _ = stats_table.print(&mut metrics_file);
    }
}
