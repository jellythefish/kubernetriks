use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::core::{node::Node, pod::Pod};

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct NodeGroup {
    /// Max number of nodes that can exist simultaneously for the node group.
    /// If none - then unlimited and bounded only to max node count for cluster autoscaler.
    pub max_count: Option<u64>,
    #[serde(default)]
    pub current_count: u64,
    /// For monotonically incrementing counter used in unique scaled up node names.
    #[serde(default)]
    pub total_allocated: u64,
    pub node_template: Node,
}

pub enum AutoscaleAction {
    ScaleUp(Node),
    /// Scale down action with node name as string
    ScaleDown(String),
}

/// Information about pods that were failed to schedule due to insufficient resources
#[derive(Serialize, Clone)]
pub struct ScaleUpInfo {
    pub unscheduled_pods: Vec<Pod>,
}

/// Information about objects received from persistent storage
#[derive(Serialize, Clone)]
pub struct ScaleDownInfo {
    /// Current state of all nodes in the cluster
    pub nodes: Vec<Node>,
    /// Map of pod names to pod objects only which were bind to autoscaled nodes
    pub pods_on_autoscaled_nodes: HashMap<String, Pod>,
    /// Node assignments - map of node names to a set of assigned pod names
    pub assignments: HashMap<String, BTreeSet<String>>,
}

pub struct AutoscaleInfo {
    pub scale_up: Option<ScaleUpInfo>,
    pub scale_down: Option<ScaleDownInfo>,
}

#[derive(Serialize, Clone)]
pub enum AutoscaleInfoRequestType {
    /// Auto make persistent storage send only scale up or scale down info based on
    /// the state what should cluster autoscaler do with the cluster.
    /// the cluster. Optimization for reducing info size.
    Auto,
    ScaleUpOnly,
    ScaleDownOnly,
    Both,
}

pub trait ClusterAutoscalerAlgorithm {
    fn info_request_type(&self) -> AutoscaleInfoRequestType;

    fn autoscale(
        &mut self,
        info: AutoscaleInfo,
        node_groups: &mut BTreeMap<String, NodeGroup>,
        max_node_count: u64,
    ) -> Vec<AutoscaleAction>;
}
