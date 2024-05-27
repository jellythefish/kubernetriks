//! Config fields definitions for kubernetriks simulation

use serde::Deserialize;

use crate::autoscalers::cluster_autoscaler::cluster_autoscaler::ClusterAutoscalerConfig;
use crate::autoscalers::horizontal_pod_autoscaler::horizontal_pod_autoscaler::HorizontalPodAutoscalerConfig;

use crate::core::node::Node;

use crate::metrics::printer::MetricsPrinterConfig;

#[derive(Debug, Deserialize, PartialEq)]
pub struct SimulationConfig {
    pub sim_name: String,
    pub seed: u64,
    pub trace_config: Option<TraceConfig>,
    /// If not set default output of logs is stdout/stderr
    pub logs_filepath: Option<String>,
    #[serde(default)]
    pub cluster_autoscaler: ClusterAutoscalerConfig,
    #[serde(default)]
    pub horizontal_pod_autoscaler: HorizontalPodAutoscalerConfig,
    pub metrics_printer: Option<MetricsPrinterConfig>,
    pub default_cluster: Option<Vec<NodeGroup>>,
    // TODO: In SchedulerConfig struct two fields below:
    pub scheduling_cycle_interval: f64, // in seconds
    pub enable_unscheduled_pods_conditional_move: bool,
    // Simulated network delays, as = api server, ps = persistent storage, ca = cluster autoscaler,
    // hpa = horizontal pod autoscaler.
    // All delays are in seconds with fractional part. Assuming all delays are bidirectional.
    pub as_to_ps_network_delay: f64,
    pub ps_to_sched_network_delay: f64,
    pub sched_to_as_network_delay: f64,
    pub as_to_node_network_delay: f64,
    pub as_to_ca_network_delay: f64,
    pub as_to_hpa_network_delay: f64,
}

#[derive(Clone, Default, Debug, Deserialize, PartialEq)]
pub struct AlibabaWorkloadTraceV2017Paths {
    pub batch_instance_trace_path: String,
    pub batch_task_trace_path: String,
    /// Optional trace for alibaba cluster nodes
    pub machine_events_trace_path: Option<String>,
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
