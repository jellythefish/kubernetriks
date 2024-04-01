//! Represents generic format for the trace that is simplified and convenient.

use std::mem::swap;

use serde::Deserialize;

use crate::core::common::SimulationEvent;
use crate::core::events::{
    CreateNodeRequest, CreatePodRequest, RemoveNodeRequest, RemovePodRequest,
};
use crate::core::node::Node;
use crate::core::pod::Pod;
use crate::trace::interface::Trace;

/// GenericTraces consist of timestamp-ordered events representing pod/node creation/removal in the
/// format corresponding to this trace.
/// These events differ from events which are emitted by simulator's components, so to get such
/// events GenericTrace implements Trace.
#[derive(Debug, Deserialize, PartialEq)]
pub struct GenericWorkloadTrace {
    pub events: Vec<WorkloadEvent>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct WorkloadEvent {
    pub timestamp: f64, // in seconds with fractional part
    pub event_type: WorkloadEventType,
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum WorkloadEventType {
    CreatePod { pod: Pod },
    RemovePod { pod_name: String },
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct GenericClusterTrace {
    pub events: Vec<ClusterEvent>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ClusterEvent {
    pub timestamp: f64, // in seconds with fractional part
    pub event_type: ClusterEventType,
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum ClusterEventType {
    CreateNode { node: Node },
    RemoveNode { node_name: String },
}

impl Trace for GenericWorkloadTrace {
    // Called once to convert and move events, TODO: better for call once semantic?
    fn convert_to_simulator_events(&mut self) -> Vec<(f64, Box<dyn SimulationEvent>)> {
        let mut converted_events: Vec<(f64, Box<dyn SimulationEvent>)> = vec![];
        converted_events.reserve(self.events.len());
        let mut events: Vec<WorkloadEvent> = vec![];
        swap(&mut events, &mut self.events);

        for event in events {
            match event.event_type {
                WorkloadEventType::CreatePod { pod } => {
                    converted_events.push((event.timestamp, Box::new(CreatePodRequest { pod })))
                }
                WorkloadEventType::RemovePod { pod_name } => converted_events
                    .push((event.timestamp, Box::new(RemovePodRequest { pod_name }))),
            }
        }

        converted_events
    }
}

impl Trace for GenericClusterTrace {
    // Called once to convert and move events, TODO: better for call once semantic?
    fn convert_to_simulator_events(&mut self) -> Vec<(f64, Box<dyn SimulationEvent>)> {
        let mut converted_events: Vec<(f64, Box<dyn SimulationEvent>)> = vec![];
        converted_events.reserve(self.events.len());
        let mut events: Vec<ClusterEvent> = vec![];
        swap(&mut events, &mut self.events);

        for event in events {
            match event.event_type {
                ClusterEventType::CreateNode { mut node } => {
                    node.status.allocatable = node.status.capacity.clone();
                    converted_events.push((event.timestamp, Box::new(CreateNodeRequest { node })))
                }
                ClusterEventType::RemoveNode { node_name } => converted_events
                    .push((event.timestamp, Box::new(RemoveNodeRequest { node_name }))),
            }
        }

        converted_events
    }
}
