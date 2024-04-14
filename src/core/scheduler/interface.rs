use std::collections::HashMap;

use crate::core::node::Node;
use crate::core::pod::Pod;

#[derive(Debug, PartialEq)]
pub enum ScheduleError {
    RequestedResourcesAreZeros,
    NoSufficientNodes,
}

// Trait which should implement any scheduler in kubernetriks framework.
pub trait PodSchedulingAlgorithm {
    // A method to assign a node on which the pod will be executed.
    // `nodes` is a hash map of node names and nodes themselves.
    // Returns Result consisting of name of assigned node or scheduling error.
    fn schedule_one(
        &self,
        pod: &Pod,
        nodes: &HashMap<String, Node>,
    ) -> Result<String, ScheduleError>;
}
