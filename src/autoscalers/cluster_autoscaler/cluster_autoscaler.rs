//! Implements cluster autoscaler based on node resources utilization.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use dslab_core::{cast, log_debug, log_info, Event, EventHandler, SimulationContext};

use serde::Deserialize;

use crate::config::SimulationConfig;
use crate::core::common::SimComponentId;
use crate::core::events::{
    ClusterAutoscalerRequest, ClusterAutoscalerResponse, CreateNodeRequest, RemoveNodeRequest,
    RunClusterAutoscalerCycle,
};
use crate::core::node::Node;
use crate::metrics::collector::MetricsCollector;

use crate::autoscalers::cluster_autoscaler::interface::NodeGroup;
use crate::autoscalers::cluster_autoscaler::interface::{
    AutoscaleAction, AutoscaleInfo, ClusterAutoscalerAlgorithm,
};
use crate::autoscalers::cluster_autoscaler::kube_cluster_autoscaler::{
    KubeClusterAutoscaler, KubeClusterAutoscalerConfig, CLUSTER_AUTOSCALER_ORIGIN_LABEL,
};

/// This is general proxy for any cluster autoscaler algorithm.
/// Every `scan_interval` seconds it sends request to persistent storage for receiving cluster
/// autoscaler info, then passes it to autoscaling algorithm method `autoscale`.
///
/// Autoscaler receives information from persistent storage with some propagation delay, so the
/// state is not perfectly synced.
/// Processing actions it gets from `autoscale` method, autoscaler emits such events as
/// `CreateNodeRequest` or `RemoveNodeRequest`.
/// Due to such loosely coupled API there is no guarantee that unscheduled pods will be placed
/// directly on newly created nodes or pods that moves from deleting node will be placed to some
/// specific node. Scheduler can decide to place them somewhere else.
pub struct ClusterAutoscaler {
    api_server: SimComponentId,

    /// Last time when autoscaler started cycle with sending request to persistent storage to
    /// receive information about unscheduled pods and nodes.
    last_cycle_time: f64,

    /// Represents how much nodes were scaled up for each node group.
    node_groups: BTreeMap<String, NodeGroup>,
    autoscaling_algorithm: Box<dyn ClusterAutoscalerAlgorithm>,

    ctx: SimulationContext,
    config: Rc<SimulationConfig>,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct ClusterAutoscalerConfig {
    #[serde(default = "enabled_default")]
    pub enabled: bool,
    /// Name of implementation type for cluster autoscaler. Used in `resolve_cluster_autoscaler_impl`.
    #[serde(default = "autoscaler_type_default")]
    pub autoscaler_type: String,
    #[serde(default = "scan_interval_default")]
    pub scan_interval: f64,
    pub node_groups: Vec<NodeGroup>,
    /// One of implementation of cluster autoscaler
    pub kube_cluster_autoscaler: Option<KubeClusterAutoscalerConfig>,
}

impl Default for ClusterAutoscalerConfig {
    fn default() -> Self {
        Self {
            enabled: enabled_default(),
            autoscaler_type: autoscaler_type_default(),
            scan_interval: scan_interval_default(),
            node_groups: Default::default(),
            kube_cluster_autoscaler: None,
        }
    }
}

fn enabled_default() -> bool {
    false // disabled by default
}
fn autoscaler_type_default() -> String {
    "kube_cluster_autoscaler".to_string()
}
fn scan_interval_default() -> f64 {
    10.0 // 10 seconds
}

impl ClusterAutoscaler {
    pub fn new(
        api_server: SimComponentId,
        autoscaling_algorithm: Box<dyn ClusterAutoscalerAlgorithm>,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
        metrics_collector: Rc<RefCell<MetricsCollector>>,
    ) -> Self {
        assert!(
            config.cluster_autoscaler.node_groups.len() > 0,
            "node groups cannot be empty for CA"
        );
        let mut node_groups: BTreeMap<String, NodeGroup> = Default::default();
        for node_group in config.cluster_autoscaler.node_groups.iter() {
            assert!(!node_group.node_template.metadata.name.is_empty());
            let mut node_template = node_group.node_template.clone();
            node_template.status.allocatable = node_template.status.capacity.clone();
            node_template.metadata.labels.insert(
                "origin".to_string(),
                CLUSTER_AUTOSCALER_ORIGIN_LABEL.to_string(),
            );
            node_template.metadata.labels.insert(
                "node_group".to_string(),
                node_group.node_template.metadata.name.clone(),
            );
            let group = NodeGroup {
                max_count: node_group.max_count,
                current_count: 0,
                total_allocated: 0,
                node_template,
            };
            assert!(
                node_groups
                    .insert(node_group.node_template.metadata.name.clone(), group)
                    .is_none(),
                "unique node group name should be used"
            );
        }

        Self {
            api_server,
            last_cycle_time: 0.0,
            node_groups,
            autoscaling_algorithm,
            ctx,
            config,
            metrics_collector,
        }
    }

    pub fn max_nodes(&self) -> usize {
        self.autoscaling_algorithm.max_nodes(&self.node_groups)
    }

    pub fn start(&mut self) {
        log_info!(
            self.ctx,
            "Cluster autoscaler started running every {} seconds",
            self.config.cluster_autoscaler.scan_interval
        );
        self.ctx.emit_self_now(RunClusterAutoscalerCycle {});
    }

    /// Does not simulate and measure cluster autoscaler cycle time because of no interest,
    /// supposing it works instantly.
    /// Behavior may be changed later to simulate its time depending on node cluster and number of
    /// unscheduled pods.
    fn run_cluster_autoscaler_cycle(&mut self, event_time: f64) {
        self.last_cycle_time = event_time;
        self.ctx.emit(
            ClusterAutoscalerRequest {
                request_type: self.autoscaling_algorithm.info_request_type(),
            },
            self.api_server,
            self.config.as_to_ca_network_delay,
        );
        // Here we wait for response with information what to do and after that response and
        // actions we reschedule autoscaler cycle.
    }

    fn scale_up_request(&mut self, node: &Node) {
        log_debug!(self.ctx, "Scaling up new node {:?}", node);

        self.ctx.emit(
            CreateNodeRequest { node: node.clone() },
            self.api_server,
            self.config.as_to_ca_network_delay,
        );

        self.metrics_collector
            .borrow_mut()
            .metrics
            .total_scaled_up_nodes += 1;
    }

    fn scale_down_request(&mut self, node_name: &String) {
        log_debug!(self.ctx, "Scaling down node {:?}", &node_name);

        self.ctx.emit(
            RemoveNodeRequest {
                node_name: node_name.clone(),
            },
            self.api_server,
            self.config.as_to_ca_network_delay,
        );

        self.metrics_collector
            .borrow_mut()
            .metrics
            .total_scaled_down_nodes += 1;
    }

    fn take_actions(&mut self, actions: &Vec<AutoscaleAction>) {
        for action in actions {
            match action {
                AutoscaleAction::ScaleUp(node) => self.scale_up_request(&node),
                AutoscaleAction::ScaleDown(node_name) => {
                    self.scale_down_request(node_name);
                }
            }
        }
    }
}

pub fn resolve_cluster_autoscaler_impl(
    autoscaler_config: ClusterAutoscalerConfig,
    ctx: SimulationContext,
) -> Box<dyn ClusterAutoscalerAlgorithm> {
    match &autoscaler_config.autoscaler_type as &str {
        "kube_cluster_autoscaler" => {
            let config = match autoscaler_config.kube_cluster_autoscaler {
                Some(conf) => conf,
                None => KubeClusterAutoscalerConfig::default(),
            };
            Box::new(KubeClusterAutoscaler::new(config, ctx))
        }
        _ => panic!("Unsupported cluster autoscaler implementation"),
    }
}

impl EventHandler for ClusterAutoscaler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            RunClusterAutoscalerCycle {} => {
                self.run_cluster_autoscaler_cycle(event.time);
            }
            ClusterAutoscalerResponse {
                scale_up,
                scale_down,
            } => {
                let actions = self.autoscaling_algorithm.autoscale(
                    AutoscaleInfo {
                        scale_up,
                        scale_down,
                    },
                    &mut self.node_groups,
                );

                self.take_actions(&actions);

                let mut delay = self.config.cluster_autoscaler.scan_interval;
                if event.time - self.last_cycle_time > self.config.cluster_autoscaler.scan_interval
                {
                    // schedule now as response waiting took longer than scan interval
                    delay = 0.0;
                }
                self.ctx.emit_self(RunClusterAutoscalerCycle {}, delay);
            }
        })
    }
}
