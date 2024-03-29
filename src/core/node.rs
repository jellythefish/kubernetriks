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

#[derive(Debug, Default, Deserialize, Serialize, Clone, PartialEq)]
pub struct Node {
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: NodeSpec,
    pub status: NodeStatus,
}

impl Node {
    pub fn update_condition(
        &mut self,
        status: String,
        condition_type: NodeConditionType,
        last_transition_time: f64,
    ) {
        let conditions = &mut self.status.conditions;
        match conditions
            .iter_mut()
            .find(|elem| elem.condition_type == condition_type)
        {
            Some(condition) => {
                condition.status = status;
                condition.last_transition_time = last_transition_time;
            }
            None => {
                conditions.push(NodeCondition {
                    status,
                    condition_type,
                    last_transition_time,
                });
            }
        }
    }
}
