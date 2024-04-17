//! Models which simulates the time of pod scheduling based on different parameters.

use std::collections::HashMap;

use crate::core::{node::Node, pod::Pod};

pub trait PodSchedulingTimeModel {
    fn simulate_time(&self, pod: &Pod, nodes: &HashMap<String, Node>) -> f64;
}

pub struct ConstantTimePerNodeModel {
    constant_time_per_node: f64
}

impl Default for ConstantTimePerNodeModel {
    fn default() -> Self {
        Self { constant_time_per_node: 0.000001 } // 1 us
    }
}

impl PodSchedulingTimeModel for ConstantTimePerNodeModel {
    fn simulate_time(&self, _pod: &Pod, nodes: &HashMap<String, Node>) -> f64 {
        self.constant_time_per_node * nodes.len() as f64
    }
}
