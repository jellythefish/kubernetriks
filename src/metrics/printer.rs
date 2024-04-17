use std::{cell::RefCell, fs::File, rc::Rc};
use prettytable::{row, Table};

use crate::metrics::collector::MetricsCollector;

pub fn print_metrics_as_pretty_table(collector: Rc<RefCell<MetricsCollector>>) {
    let metrics = collector.borrow();

    let mut metrics_file = File::create("metrics.txt").unwrap();

    let mut aggregated_table = Table::new();
    aggregated_table.add_row(row!["Metric", "Count"]);
    aggregated_table.add_row(row!["Total nodes in trace", metrics.total_nodes_in_trace]);
    aggregated_table.add_row(row!["Total pods in trace", metrics.total_pods_in_trace]);
    aggregated_table.add_row(row!["Pods succeeded", metrics.pods_succeeded]);
    aggregated_table.add_row(row!["Pods unschedulable", metrics.pods_unschedulable]);

    let mut stats_table = Table::new();
    stats_table.add_row(row!["Metric", "Min", "Max", "Mean", "Variance"]);
    stats_table.add_row(row![
        "Pod duration",
        metrics.pod_duration_stats.min(),
        metrics.pod_duration_stats.max(),
        metrics.pod_duration_stats.mean(),
        metrics.pod_duration_stats.population_variance()
    ]);
    stats_table.add_row(row![
        "Pod schedule time",
        metrics.pod_schedule_time_stats.min(),
        metrics.pod_schedule_time_stats.max(),
        metrics.pod_schedule_time_stats.mean(),
        metrics.pod_schedule_time_stats.population_variance()
    ]);
    stats_table.add_row(row![
        "Pod queue time",
        metrics.pod_queue_time_stats.min(),
        metrics.pod_queue_time_stats.max(),
        metrics.pod_queue_time_stats.mean(),
        metrics.pod_queue_time_stats.population_variance()
    ]);

    let _ = aggregated_table.print(&mut metrics_file);
    let _ = stats_table.print(&mut metrics_file);
}
