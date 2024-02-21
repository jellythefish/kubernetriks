//! Represents generic format for the trace that serves as input to the simulator.

use serde::Deserialize;

use crate::core::node::Node;
use crate::core::pod::Pod;

/// Trace consists of timestamp-ordered events representing pod/node creation/removal.
/// These events differ from events which are emitted by simulator's components.
#[derive(Debug, Deserialize, PartialEq)]
pub struct Trace {
    pub events: Vec<TraceEvent>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct TraceEvent {
    pub timestamp: u64, // in milliseconds
    pub event_type: TraceEventType,
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum TraceEventType {
    CreatePod { pod: Pod },
    RemovePod { pod_id: u64 },
    CreateNode { node: Node },
    RemoveNode { node_id: u64 },
}
