//! Type definition for Node primitive of k8s cluster

use serde::{Deserialize, Serialize};

use std::collections::HashMap;

use crate::core::common::Resources;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Node {
    id: u64,
    capacity: Resources,

    #[serde(default)]
    allocatable: Resources,

    #[serde(default)]
    attributes: HashMap<String, String>,

    #[serde(default)]
    state: NodeState,
}

impl Node {
    pub fn new(id: u64, capacity: Resources, attributes: HashMap<String, String>) -> Self {
        Self {
            id,
            capacity,
            allocatable: Default::default(),
            attributes,
            state: NodeState::Undefined,
        }
    }
}

#[derive(Default, Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum NodeState {
    #[default]
    Undefined,
}
