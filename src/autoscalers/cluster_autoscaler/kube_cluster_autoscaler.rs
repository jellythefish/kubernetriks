use std::collections::BTreeMap;

use crate::autoscalers::cluster_autoscaler::interface::{
    AutoscaleAction, AutoscaleInfo, AutoscaleInfoRequestType, ClusterAutoscalerAlgorithm,
    NodeGroup, ScaleDownInfo, ScaleUpInfo,
};
use crate::core::node::Node;
use crate::core::pod::Pod;

use dslab_core::{log_debug, SimulationContext};
use serde::Deserialize;

pub const CLUSTER_AUTOSCALER_ORIGIN_LABEL: &'static str = "cluster autoscaler";

/// Implementation of default kubernetes cluster autoscaler behavior.
///
/// Scale-up: checks for any unschedulable pods, trying to fit each into one of node group template.
/// Unschedulable pods are recognized by their PodCondition, where condition is `PodScheduled` and
/// status is `False`. Such information about pods and nodes are received from persistent storage.
///
/// Scale-down: if no scale-up is needed, Cluster Autoscaler checks
/// for holding of all three conditions:
///   1) The sum of cpu requests and sum of memory requests of all pods running on this node are
///      smaller than `scale_down_utilization_threshold` % of the node's allocatable.
///   2) All pods running on the node can be moved to other nodes.
///   3) Node belongs to autoscaler cluster (`origin=cluster autoscaler` kv-pair in node labels).
///
pub struct KubeClusterAutoscaler {
    ctx: SimulationContext,
    config: KubeClusterAutoscalerConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct KubeClusterAutoscalerConfig {
    #[serde(default = "scale_down_utilization_threshold_default")]
    /// Fraction in interval [0, 1] to define threshold of resource utilization to make scale down
    /// decisions.
    pub scale_down_utilization_threshold: f64,
    // TODO: #[serde(default = "node_provisioning_time_default")]
    // pub node_provisioning_time: f64,
}

impl Default for KubeClusterAutoscalerConfig {
    fn default() -> Self {
        Self {
            scale_down_utilization_threshold: scale_down_utilization_threshold_default(),
        }
    }
}

fn scale_down_utilization_threshold_default() -> f64 {
    0.5
}

impl KubeClusterAutoscaler {
    pub fn new(config: KubeClusterAutoscalerConfig, ctx: SimulationContext) -> Self {
        Self { ctx, config }
    }

    /// Check whether current node count quota exceeded with respect to `max_node_count` and each
    /// node group max count if it is set.
    pub fn node_count_over_quota(
        &self,
        node_groups: &mut BTreeMap<String, NodeGroup>,
        current_node_count: u64,
        max_node_count: u64,
    ) -> bool {
        if current_node_count >= max_node_count {
            return true;
        }

        for group in node_groups.values() {
            if group.max_count.is_none()
                || group.max_count.is_some() && group.current_count < group.max_count.unwrap()
            {
                return false;
            }
        }
        true
    }

    fn node_fits_pod(pod: &Pod, node: &Node) -> bool {
        pod.spec.resources.requests.cpu <= node.status.allocatable.cpu
            && pod.spec.resources.requests.ram <= node.status.allocatable.ram
    }

    /// Searches through node group templates to find fitting one and returns a node of this template.
    fn try_find_fitting_template(
        &mut self,
        pod: &Pod,
        node_groups: &mut BTreeMap<String, NodeGroup>,
    ) -> Option<Node> {
        for (_, node_group) in node_groups.iter_mut() {
            if node_group.max_count.is_some()
                && node_group.current_count >= node_group.max_count.unwrap()
            {
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

        return utilization < self.config.scale_down_utilization_threshold;
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

    fn current_node_count(&self, node_groups: &mut BTreeMap<String, NodeGroup>) -> u64 {
        let mut current_node_count = 0;
        for group in node_groups.values() {
            current_node_count += group.current_count;
        }
        current_node_count
    }

    fn scale_up(
        &mut self,
        info: ScaleUpInfo,
        node_groups: &mut BTreeMap<String, NodeGroup>,
        max_node_count: u64,
    ) -> Vec<AutoscaleAction> {
        let mut allocated_nodes: Vec<Node> = vec![];

        let mut current_node_count = self.current_node_count(node_groups);

        // Quick check for quota not to do useless node group search.
        if self.node_count_over_quota(node_groups, current_node_count, max_node_count) {
            log_debug!(
                self.ctx,
                "All node groups are scaled to their maximum node count or total max node count reached."
            );
            return vec![];
        }

        let mut not_scaled_up = 0;
        let total_unscheduled = info.unscheduled_pods.len();
        for pod in info.unscheduled_pods.into_iter() {
            if Self::try_fit_in_allocated_nodes(&mut allocated_nodes, &pod) {
                continue;
            }
            if current_node_count >= max_node_count {
                not_scaled_up += 1;
                continue;
            }

            if let Some(node) = self.try_find_fitting_template(&pod, node_groups) {
                allocated_nodes.push(node);
                current_node_count += 1;
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

        let mut scale_up_actions: Vec<AutoscaleAction> = Default::default();
        scale_up_actions.reserve(allocated_nodes.len());

        for mut node in allocated_nodes.into_iter() {
            // Restoring as we might decrease higher.
            node.status.allocatable = node.status.capacity.clone();
            scale_up_actions.push(AutoscaleAction::ScaleUp(node));
        }

        scale_up_actions
    }

    fn scale_down(
        &mut self,
        mut info: ScaleDownInfo,
        node_groups: &mut BTreeMap<String, NodeGroup>,
    ) -> Vec<AutoscaleAction> {
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

        let mut scale_down_actions: Vec<AutoscaleAction> = Default::default();
        scale_down_actions.reserve(node_indices_to_remove.len());

        for idx in node_indices_to_remove.into_iter() {
            let node = &info.nodes[idx];

            let node_group_state = node_groups
                .get_mut(node.metadata.labels.get("node_group").unwrap())
                .unwrap();
            node_group_state.current_count -= 1;

            scale_down_actions.push(AutoscaleAction::ScaleDown(node.metadata.name.clone()))
        }

        scale_down_actions
    }
}

impl ClusterAutoscalerAlgorithm for KubeClusterAutoscaler {
    fn info_request_type(&self) -> AutoscaleInfoRequestType {
        AutoscaleInfoRequestType::Auto
    }

    fn autoscale(
        &mut self,
        info: AutoscaleInfo,
        node_groups: &mut BTreeMap<String, NodeGroup>,
        max_node_count: u64,
    ) -> Vec<AutoscaleAction> {
        if !info.scale_up.is_none() {
            return self.scale_up(info.scale_up.unwrap(), node_groups, max_node_count);
        } else if !info.scale_down.is_none() {
            return self.scale_down(info.scale_down.unwrap(), node_groups);
        }
        vec![]
    }
}
