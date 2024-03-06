//! Type definition for Node primitive of k8s cluster

use serde::{Deserialize, Serialize};

use std::collections::HashMap;

use crate::core::common::Resources;

pub type NodeId = u64;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct NodeSpec {
    id: NodeId,
    // Total resource of a node
    capacity: Resources,

    #[serde(default)]
    attributes: HashMap<String, String>,
}

#[derive(Default, Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum NodeState {
    #[default]
    Undefined,
}

pub struct NodeInfo {
    spec: NodeSpec,

    // Remaining amount of resources on a node
    allocatable: Resources,

    state: NodeState,
}

impl NodeSpec {
    pub fn new(id: NodeId, capacity: Resources, attributes: HashMap<String, String>) -> Self {
        Self {
            id,
            capacity,
            attributes,
        }
    }
}
