//! Implements cluster autoscaler based on node resources utilization.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use dslab_core::{cast, log_debug, log_info, Event, EventHandler, SimulationContext};
use serde::{Deserialize, Serialize};

use crate::config::SimulationConfig;
use crate::core::common::SimComponentId;
use crate::core::events::{
    ClusterAutoscalerRequest, ClusterAutoscalerResponse, CreateNodeRequest, RemoveNodeRequest,
    RunClusterAutoscalerCycle,
};
use crate::core::node::Node;
use crate::core::pod::Pod;
use crate::metrics::collector::MetricsCollector;

pub const CLUSTER_AUTOSCALER_ORIGIN_LABEL: &'static str = "cluster autoscaler";

/// Scale-up: Cluster Autoscaler checks for any unschedulable pods every `scan_interval` seconds.
/// Unschedulable pods are recognized by their PodCondition, where condition is `PodScheduled` and
/// status is `False`. Such information about pods and nodes are received from persistent storage.
///
/// Scale-down: every `scan_interval` seconds if no scale-up is needed, Cluster Autoscaler checks
/// for holding of all three conditions:
///   1) The sum of cpu requests and sum of memory requests of all pods running on this node are
///      smaller than `scale_down_utilization_threshold` % of the node's allocatable.
///   2) All pods running on the node can be moved to other nodes.
///   3) Node belongs to autoscaler cluster (`origin=cluster autoscaler` kv-pair in node labels).
///
/// Autoscaler receives information from persistent storage with some propagation delay, so the
/// state is not perfectly synced.
/// In terms of simulation it emits such events as `CreateNodeRequest` or `RemoveNodeRequest`.
/// Due to such loosely coupled API there is no guarantee that unscheduled pods will be placed
/// directly on newly created nodes or pods that moves from deleting node will be placed to some
/// specific node. Scheduler can decide to place them somewhere else.
///
pub struct ClusterAutoscaler {
    api_server: SimComponentId,

    /// Last time when autoscaler started cycle with sending request to persistent storage to
    /// receive information about unscheduled pods and nodes.
    last_cycle_time: f64,

    /// Represents how much nodes were scaled up for each node group.
    state: HashMap<String, NodeGroup>,

    ctx: SimulationContext,
    config: Rc<SimulationConfig>,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ClusterAutoscalerConfig {
    #[serde(default = "enabled_default")]
    pub enabled: bool,
    #[serde(default = "scan_interval_default")]
    pub scan_interval: f64,
    #[serde(default = "scale_down_utilization_threshold_default")]
    /// Fraction in interval [0, 1] to define threshold of resource utilization to make scale down
    /// decisions.
    pub scale_down_utilization_threshold: f64,
    // TODO: #[serde(default = "node_provisioning_time_default")]
    // pub node_provisioning_time: f64,
    pub node_groups: Vec<NodeGroup>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct NodeGroup {
    /// Max number of nodes that can exist simultaneously for the node group.
    pub max_count: u64,
    #[serde(default)]
    pub current_count: u64,
    /// For monotonically incrementing counter used in unique scaled up node names.
    #[serde(default)]
    pub total_allocated: u64,
    pub node_template: Node,
}

/// Information from persistent storage what nodes failed to schedule due to insufficient resources.
#[derive(Serialize, Clone)]
pub struct ScaleUpInfo {
    pub unscheduled_pods: Vec<Pod>,
}

/// Information from persistent storage which contains current state of all nodes in the
/// cluster (`nodes`), map of pod names and pod objects that were bind to autoscaled nodes
/// (`pods_on_autoscaled_nodes`) and state of current assignments which is a map of node names to
/// a set of pod names (`assignments`).
#[derive(Serialize, Clone)]
pub struct ScaleDownInfo {
    pub nodes: Vec<Node>,
    pub pods_on_autoscaled_nodes: HashMap<String, Pod>,
    pub assignments: HashMap<String, HashSet<String>>,
}

impl ClusterAutoscaler {
    pub fn new(
        api_server: SimComponentId,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
        metrics_collector: Rc<RefCell<MetricsCollector>>,
    ) -> Self {
        let mut state: HashMap<String, NodeGroup> = Default::default();
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
                state
                    .insert(node_group.node_template.metadata.name.clone(), group)
                    .is_none(),
                "unique node group name should be used"
            );
        }

        Self {
            api_server,
            last_cycle_time: 0.0,
            ctx,
            config,
            metrics_collector,
            state,
        }
    }

    pub fn start(&mut self) {
        log_info!(
            self.ctx,
            "Cluster autoscaler started running every {} seconds",
            self.config.cluster_autoscaler.scan_interval
        );
        self.ctx.emit_self_now(RunClusterAutoscalerCycle {});
    }

    pub fn over_quota_for_all_groups(&self) -> bool {
        for group in self.state.values() {
            if group.current_count < group.max_count {
                return false;
            }
        }
        true
    }

    /// Does not simulate and measure cluster autoscaler cycle time because of no interest,
    /// supposing it works instantly.
    /// Behavior may be changed later to simulate its time depending on node cluster and number of
    /// unscheduled pods.
    fn run_cluster_autoscaler_cycle(&mut self, event_time: f64) {
        self.last_cycle_time = event_time;
        self.ctx.emit(
            ClusterAutoscalerRequest {},
            self.api_server,
            self.config.as_to_ca_network_delay,
        );
        // Here we wait for response with information what to do and after that response and
        // actions we reschedule autoscaler cycle.
    }

    fn node_fits_pod(pod: &Pod, node: &Node) -> bool {
        pod.spec.resources.requests.cpu <= node.status.allocatable.cpu
            && pod.spec.resources.requests.ram <= node.status.allocatable.ram
    }

    /// Searches through node group templates to find fitting one and returns a node of this template.
    fn try_find_fitting_template(&mut self, pod: &Pod) -> Option<Node> {
        for (_, node_group) in self.state.iter_mut() {
            if node_group.current_count >= node_group.max_count {
                continue;
            }
            if Self::node_fits_pod(pod, &node_group.node_template) {
                node_group.current_count += 1;
                node_group.total_allocated += 1;

                let mut node = node_group.node_template.clone();
                node.metadata.name =
                    format!("{}_{}", node.metadata.name, node_group.total_allocated);
                node.status.allocatable = node.status.capacity.clone();

                return Some(node);
            }
        }
        None
    }

    fn try_fit_in_allocated_nodes(allocated_nodes: &mut Vec<Node>, pod: &Pod) -> bool {
        for node in allocated_nodes.iter_mut() {
            if Self::node_fits_pod(&pod, node) {
                node.status.allocatable.cpu -= pod.spec.resources.requests.cpu;
                node.status.allocatable.ram -= pod.spec.resources.requests.ram;
                return true;
            }
        }
        return false;
    }

    /// Calculates utilization of a node, defined as maximum of cpu and memory.
    /// Per resource utilization is the sum of requests for it divided by allocatable.
    /// Returns whether the node is underutilized based on max resource.
    fn is_under_threshold_utilization(&self, node: &Node) -> bool {
        let cpu_utilization = (node.status.capacity.cpu - node.status.allocatable.cpu) as f64
            / node.status.capacity.cpu as f64;
        let ram_utilization = (node.status.capacity.ram - node.status.allocatable.ram) as f64
            / node.status.capacity.ram as f64;

        let utilization: f64;
        if cpu_utilization > ram_utilization {
            utilization = cpu_utilization;
        } else {
            utilization = ram_utilization;
        }

        return utilization
            < self
                .config
                .cluster_autoscaler
                .scale_down_utilization_threshold;
    }

    /// Returns `true` if every pod on node with index `current_node_idx` in vector `nodes` can be
    /// placed on any other node from `nodes`.
    /// If `true` then modifies allocatable of nodes to decrease available resources, otherwise
    /// leaves nodes' allocatable unchanged.
    fn all_pods_can_be_moved_to_other_nodes(
        pods: &Vec<&Pod>,
        nodes: &mut Vec<Node>,
        current_node_idx: usize,
    ) -> bool {
        if pods.is_empty() {
            return true;
        }

        let original = nodes.clone();

        for pod in pods.iter() {
            let mut placed = false;
            for (node_idx, node) in nodes.iter_mut().enumerate() {
                // Skipping node which pods we are processing currently.
                if node_idx == current_node_idx {
                    continue;
                }

                if Self::node_fits_pod(pod, node) {
                    node.status.allocatable.cpu -= pod.spec.resources.requests.cpu;
                    node.status.allocatable.ram -= pod.spec.resources.requests.ram;
                    placed = true;
                    break;
                }
            }
            if !placed {
                *nodes = original;
                return false;
            }
        }

        true
    }

    fn scale_up(&mut self, info: ScaleUpInfo) {
        let mut allocated_nodes: Vec<Node> = vec![];

        // Quick check for quota not to do useless node group search.
        if self.over_quota_for_all_groups() {
            log_debug!(
                self.ctx,
                "All node groups are scaled to their maximum node count"
            );
            return;
        }

        let mut not_scaled_up = 0;
        let total_unscheduled = info.unscheduled_pods.len();
        for pod in info.unscheduled_pods.into_iter() {
            if Self::try_fit_in_allocated_nodes(&mut allocated_nodes, &pod) {
                continue;
            } else if let Some(node) = self.try_find_fitting_template(&pod) {
                allocated_nodes.push(node);
            } else {
                not_scaled_up += 1;
            }
        }
        log_debug!(
            self.ctx,
            "Failed to scale up a node for {:?} pods out of {:?}",
            not_scaled_up,
            total_unscheduled
        );

        for mut node in allocated_nodes.into_iter() {
            // Restoring as we might decrease higher.
            node.status.allocatable = node.status.capacity.clone();

            log_debug!(self.ctx, "Scaling up new node {:?}", node);
            self.ctx.emit(
                CreateNodeRequest { node },
                self.api_server,
                self.config.as_to_ca_network_delay,
            );

            self.metrics_collector.borrow_mut().total_scaled_up_nodes += 1;
        }
    }

    fn scale_down(&mut self, mut info: ScaleDownInfo) {
        let mut node_indices_to_remove: Vec<usize> = Default::default();
        node_indices_to_remove.reserve(info.nodes.len());

        for idx in 0..info.nodes.len() {
            log_debug!(
                self.ctx,
                "Observing node {:?} for scaling down",
                info.nodes[idx]
            );

            let origin = info.nodes[idx].metadata.labels.get("origin");
            if origin.is_none() || origin.unwrap() != CLUSTER_AUTOSCALER_ORIGIN_LABEL {
                continue;
            }

            if !self.is_under_threshold_utilization(&info.nodes[idx]) {
                continue;
            }

            if let Some(assigned_pods) = info.assignments.get(&info.nodes[idx].metadata.name) {
                let pods_on_node = assigned_pods
                    .iter()
                    .map(|pod_name| info.pods_on_autoscaled_nodes.get(pod_name).unwrap())
                    .collect::<Vec<&Pod>>();

                if !Self::all_pods_can_be_moved_to_other_nodes(&pods_on_node, &mut info.nodes, idx)
                {
                    log_debug!(
                        self.ctx,
                        "Cannot scale down node {:?} as not all pods can be moved to other nodes",
                        &info.nodes[idx].metadata.name
                    );
                    continue;
                }
            }

            node_indices_to_remove.push(idx);
        }

        for idx in node_indices_to_remove.into_iter() {
            let node = &info.nodes[idx];
            let node_group_state = self
                .state
                .get_mut(node.metadata.labels.get("node_group").unwrap())
                .unwrap();
            node_group_state.current_count -= 1;

            log_debug!(self.ctx, "Scaling down node {:?}", &node.metadata.name);
            self.ctx.emit(
                RemoveNodeRequest {
                    node_name: node.metadata.name.clone(),
                },
                self.api_server,
                self.config.as_to_ca_network_delay,
            );

            self.metrics_collector.borrow_mut().total_scaled_down_nodes += 1;
        }
    }
}

impl Default for ClusterAutoscalerConfig {
    fn default() -> Self {
        Self {
            enabled: enabled_default(),
            scan_interval: scan_interval_default(),
            scale_down_utilization_threshold: scale_down_utilization_threshold_default(),
            node_groups: Default::default(),
        }
    }
}

fn enabled_default() -> bool {
    false
} // disabled by default
fn scan_interval_default() -> f64 {
    10.0
} // 10 seconds
fn scale_down_utilization_threshold_default() -> f64 {
    0.5
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
                if !scale_up.is_none() {
                    self.scale_up(scale_up.unwrap())
                } else if !scale_down.is_none() {
                    self.scale_down(scale_down.unwrap())
                }

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
