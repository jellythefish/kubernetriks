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
    // Returns Result consisting of name of assigned node or scheduling error.
    fn schedule_one(&self, pod: &Pod, nodes: Vec<&Node>) -> Result<String, ScheduleError>;
}
