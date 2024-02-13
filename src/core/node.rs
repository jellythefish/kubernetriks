use std::collections::HashMap;

use crate::core::common::Resources;

#[derive(Default, Debug)]
pub struct Node {
    allocatable: Resources,
    capacity: Resources,
    state: NodeState,
    attributes: HashMap<String, String>,
}

impl Node {
    pub fn new() -> Self {
        Node::default()
    }
}

#[derive(Debug)]
enum NodeState {
    Undefined,
}

impl Default for NodeState {
    fn default() -> Self { NodeState::Undefined }
}
