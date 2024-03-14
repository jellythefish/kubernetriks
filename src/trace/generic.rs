//! Represents generic format for the trace that is simplified and convenient.

use std::mem::swap;

use serde::Deserialize;

use crate::core::events::{
    CreateNodeRequest, CreatePodRequest, RemoveNodeRequest, RemovePodRequest,
};
use crate::core::node::Node;
use crate::core::pod::Pod;
use crate::trace::interface::{SimulationEvent, Trace};

/// GenericTrace consists of timestamp-ordered events representing pod/node creation/removal,
/// but in the format corresponding to this trace.
/// These events differ from events which are emitted by simulator's components, so to get such
/// events GenericTrace implements Trace.
#[derive(Debug, Deserialize, PartialEq)]
pub struct GenericTrace {
    pub events: Vec<TraceEvent>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct TraceEvent {
    pub timestamp: f64, // in seconds with fractional part
    pub event_type: TraceEventType,
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum TraceEventType {
    CreatePod { pod: Pod },
    RemovePod { pod_name: String },
    CreateNode { node: Node },
    RemoveNode { node_name: String },
}

impl Trace for GenericTrace {
    // Called once to convert and move events, TODO: better for call once semantic?
    fn convert_to_simulator_events(&mut self) -> Vec<(f64, SimulationEvent)> {
        let mut converted_events: Vec<(f64, SimulationEvent)> = vec![];
        converted_events.reserve(self.events.len());

        let mut events: Vec<TraceEvent> = vec![];
        swap(&mut events, &mut self.events);

        for event in events {
            match event.event_type {
                TraceEventType::CreatePod { pod } => {
                    converted_events.push((event.timestamp, Box::new(CreatePodRequest { pod })))
                }
                TraceEventType::RemovePod { pod_name } => converted_events
                    .push((event.timestamp, Box::new(RemovePodRequest { pod_name }))),
                TraceEventType::CreateNode { mut node } => {
                    node.status.allocatable = node.status.capacity.clone();
                    converted_events.push((event.timestamp, Box::new(CreateNodeRequest { node })))
                }
                TraceEventType::RemoveNode { node_name } => converted_events
                    .push((event.timestamp, Box::new(RemoveNodeRequest { node_name }))),
            }
        }

        converted_events
    }
}
