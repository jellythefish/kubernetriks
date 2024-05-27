use std::collections::HashMap;

use lazy_static::lazy_static;

use crate::core::node::Node;
use crate::core::pod::Pod;

pub enum PluginType {
    FilterPlugin(Box<dyn FilterPlugin>),
    ScorePlugin(Box<dyn ScorePlugin>),
}

lazy_static! {
    pub static ref PLUGIN_REGISTRY: HashMap<&'static str, PluginType> = {
        HashMap::from([
            ("Fit", PluginType::FilterPlugin(Box::new(Fit {}))),
            (
                "LeastAllocatedResources",
                PluginType::ScorePlugin(Box::new(LeastAllocatedResources {})),
            ),
        ])
    };
}

pub trait FilterPlugin: Send + Sync {
    fn filter<'a>(&self, pod: &'a Pod, nodes: Vec<&'a Node>) -> Vec<&'a Node>;
}

pub trait ScorePlugin: Send + Sync {
    fn score(&self, pod: &Pod, node: &Node) -> f64;
}

// Fit is a plugin that checks if a node has sufficient resources.
pub struct Fit {}
impl FilterPlugin for Fit {
    fn filter<'a>(&self, pod: &'a Pod, nodes: Vec<&'a Node>) -> Vec<&'a Node> {
        nodes
            .into_iter()
            .filter(|&node| {
                pod.spec.resources.requests.cpu <= node.status.allocatable.cpu
                    && pod.spec.resources.requests.ram <= node.status.allocatable.ram
            })
            .collect()
    }
}

// Least requested resources plugin is a score plugin. Its score means that after subtracting pod's
// requested resources from node's allocatable resources, the node with the highest
// percentage (relatively to current allocatable) is prioritized for scheduling.
//
// Weights for cpu and memory are equal by default.
pub struct LeastAllocatedResources {}
impl ScorePlugin for LeastAllocatedResources {
    fn score(&self, pod: &Pod, node: &Node) -> f64 {
        let cpu_score = (node.status.allocatable.cpu - pod.spec.resources.requests.cpu) as f64
            * 100.0
            / node.status.allocatable.cpu as f64;
        let ram_score = (node.status.allocatable.ram - pod.spec.resources.requests.ram) as f64
            * 100.0
            / node.status.allocatable.ram as f64;
        (cpu_score + ram_score) / 2.0
    }
}
