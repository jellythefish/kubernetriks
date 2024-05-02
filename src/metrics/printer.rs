use prettytable::{row, Table};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, fs::File, io::Write, rc::Rc};

use crate::metrics::collector::MetricsCollector;

#[derive(Debug, Default, Deserialize, PartialEq)]
pub enum OutputFormat {
    #[default]
    JSON,
    PrettyTable,
}

#[derive(Debug, Default, Deserialize, PartialEq)]
pub struct MetricsPrinterConfig {
    format: OutputFormat,
    output_file: std::path::PathBuf,
}

pub fn print_metrics(collector: Rc<RefCell<MetricsCollector>>, config: &MetricsPrinterConfig) {
    match config.format {
        OutputFormat::PrettyTable => print_metrics_as_pretty_table(collector, &config.output_file),
        OutputFormat::JSON => print_metrics_as_json(collector, &config.output_file),
    }
}

pub fn print_metrics_as_pretty_table(
    collector: Rc<RefCell<MetricsCollector>>,
    output_file: &std::path::PathBuf,
) {
    let metrics = collector.borrow();
    let mut metrics_file = File::create(output_file).unwrap();

    let mut aggregated_table = Table::new();
    aggregated_table.add_row(row!["Metric", "Count"]);
    aggregated_table.add_row(row!["Total nodes in trace", metrics.total_nodes_in_trace]);
    aggregated_table.add_row(row!["Total pods in trace", metrics.total_pods_in_trace]);
    aggregated_table.add_row(row!["Pods succeeded", metrics.pods_succeeded]);
    aggregated_table.add_row(row!["Pods unschedulable", metrics.pods_unschedulable]);
    aggregated_table.add_row(row!["Pods failed", metrics.pods_failed]);
    aggregated_table.add_row(row!["Total scaled up nodes", metrics.total_scaled_up_nodes]);
    aggregated_table.add_row(row![
        "Total scaled down nodes",
        metrics.total_scaled_down_nodes
    ]);

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
        metrics.pod_scheduling_algorithm_latency_stats.min(),
        metrics.pod_scheduling_algorithm_latency_stats.max(),
        metrics.pod_scheduling_algorithm_latency_stats.mean(),
        metrics
            .pod_scheduling_algorithm_latency_stats
            .population_variance()
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

#[derive(Serialize)]
struct MetricsJSON {
    counters: Counters,
    timings: Timings,
}

#[derive(Serialize)]
struct Counters {
    total_nodes_in_trace: u64,
    total_pods_in_trace: u64,
    pods_succeeded: u64,
    pods_unschedulable: u64,
    pods_failed: u64,
    total_scaled_up_nodes: u64,
    total_scaled_down_nodes: u64,
}

#[derive(Serialize)]
struct Timings {
    pod_duration: TimingsStats,
    pod_schedule_time: TimingsStats,
    pod_queue_time: TimingsStats,
}

#[derive(Serialize)]
struct TimingsStats {
    min: f64,
    max: f64,
    mean: f64,
    variance: f64,
}

pub fn print_metrics_as_json(
    collector: Rc<RefCell<MetricsCollector>>,
    output_file: &std::path::PathBuf,
) {
    let metrics = collector.borrow();
    let mut metrics_file = File::create(output_file).unwrap();

    let metrics = MetricsJSON {
        counters: Counters {
            total_nodes_in_trace: metrics.total_nodes_in_trace,
            total_pods_in_trace: metrics.total_pods_in_trace,
            pods_succeeded: metrics.pods_succeeded,
            pods_unschedulable: metrics.pods_unschedulable,
            pods_failed: metrics.pods_failed,
            total_scaled_up_nodes: metrics.total_scaled_up_nodes,
            total_scaled_down_nodes: metrics.total_scaled_down_nodes,
        },
        timings: Timings {
            pod_duration: TimingsStats {
                min: metrics.pod_duration_stats.min(),
                max: metrics.pod_duration_stats.max(),
                mean: metrics.pod_duration_stats.mean(),
                variance: metrics.pod_duration_stats.population_variance(),
            },
            pod_schedule_time: TimingsStats {
                min: metrics.pod_scheduling_algorithm_latency_stats.min(),
                max: metrics.pod_scheduling_algorithm_latency_stats.max(),
                mean: metrics.pod_scheduling_algorithm_latency_stats.mean(),
                variance: metrics
                    .pod_scheduling_algorithm_latency_stats
                    .population_variance(),
            },
            pod_queue_time: TimingsStats {
                min: metrics.pod_queue_time_stats.min(),
                max: metrics.pod_queue_time_stats.max(),
                mean: metrics.pod_queue_time_stats.mean(),
                variance: metrics.pod_queue_time_stats.population_variance(),
            },
        },
    };

    let serialized_json = serde_json::to_string_pretty(&metrics).unwrap();
    metrics_file.write_all(serialized_json.as_bytes()).unwrap();
}
