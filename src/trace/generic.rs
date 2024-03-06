//! Represents generic format for the trace that is simplified and convenient.

use std::mem::swap;

use serde::Deserialize;

use crate::core::events::{
    CreateNodeRequest, CreatePodRequest, RemoveNodeRequest, RemovePodRequest,
};
use crate::core::node::{NodeId, NodeSpec};
use crate::core::pod::{PodId, PodSpec};
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
    pub timestamp: u64, // in milliseconds
    pub event_type: TraceEventType,
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum TraceEventType {
    CreatePod { pod: PodSpec },
    RemovePod { pod_id: PodId },
    CreateNode { node: NodeSpec },
    RemoveNode { node_id: NodeId },
}

impl Trace for GenericTrace {
    // Called once to convert and move events, TODO: better for call once semantic?
    fn convert_to_simulator_events(&mut self) -> Vec<(u64, SimulationEvent)> {
        let mut converted_events: Vec<(u64, SimulationEvent)> = vec![];
        converted_events.reserve(self.events.len());

        let mut events: Vec<TraceEvent> = vec![];
        swap(&mut events, &mut self.events);

        for event in events {
            match event.event_type {
                TraceEventType::CreatePod { pod } => {
                    converted_events.push((event.timestamp, Box::new(CreatePodRequest { pod })))
                }
                TraceEventType::RemovePod { pod_id } => {
                    converted_events.push((event.timestamp, Box::new(RemovePodRequest { pod_id })))
                }
                TraceEventType::CreateNode { node } => {
                    converted_events.push((event.timestamp, Box::new(CreateNodeRequest { node })))
                }
                TraceEventType::RemoveNode { node_id } => converted_events
                    .push((event.timestamp, Box::new(RemoveNodeRequest { node_id }))),
            }
        }

        converted_events
    }
}
