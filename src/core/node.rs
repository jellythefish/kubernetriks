//! Type definitions for node specification and state used in persistent storage and trace formats

use serde::{Deserialize, Serialize};

use crate::core::common::{ObjectMeta, RuntimeResources};

#[derive(Default, Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct NodeSpec {
    // placeholder for future fields
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum NodeConditionType {
    NodeCreated,
    NodeReady,
    NodeFailed,
    NodeRemoved,
    // taken from https://kubernetes.io/docs/reference/node/node-status/#condition
    DiskPressure,
    MemoryPressure,
    PIDPressure,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct NodeCondition {
    // True, False or Unknown
    status: String,
    condition_type: NodeConditionType,
    // Last event time the condition transit from one status to another.
    last_transition_time: f64,
}

#[derive(Default, Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct NodeStatus {
    // How much resources left, defaults to capacity while node is created.
    #[serde(default)]
    pub allocatable: RuntimeResources,
    // Total amount of resources
    pub capacity: RuntimeResources,
    #[serde(default)]
    pub conditions: Vec<NodeCondition>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Node {
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: NodeSpec,
    pub status: NodeStatus,
}
