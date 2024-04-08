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
    fn schedule_one<'a>(
        &self,
        pod: &'a Pod,
        nodes: Vec<&'a Node>,
    ) -> Result<&'a Node, ScheduleError>;
}
