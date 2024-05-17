//! Represents generic format for the trace that is simplified and convenient.

use std::mem::swap;

use serde::Deserialize;

use crate::autoscalers::horizontal_pod_autoscaler::interface::PodGroup;
use crate::core::common::SimulationEvent;
use crate::core::events::{
    CreateNodeRequest, CreatePodGroupRequest, CreatePodRequest, RemoveNodeRequest, RemovePodRequest,
};
use crate::core::node::Node;
use crate::core::pod::Pod;
use crate::trace::interface::Trace;

/// GenericTraces consist of timestamp-ordered events representing pod/node creation/removal in the
/// format corresponding to this trace.
/// These events differ from events which are emitted by simulator's components, so to get such
/// events GenericTrace implements Trace.
#[derive(Default, Debug, Deserialize, PartialEq)]
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
    // TODO: simplify with round brackets: CreatePod(Pod)
    CreatePod { pod: Pod },
    RemovePod { pod_name: String },
    CreatePodGroup { pod_group: PodGroup },
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
                WorkloadEventType::CreatePodGroup { pod_group } => converted_events.push((
                    event.timestamp,
                    Box::new(CreatePodGroupRequest { pod_group }),
                )),
            }
        }
        // sort by timestamp in increasing order
        converted_events.sort_by(|lhs, rhs| lhs.0.partial_cmp(&rhs.0).unwrap());
        converted_events
    }

    fn event_count(&self) -> usize {
        self.events.len()
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
        converted_events.sort_by(|lhs, rhs| lhs.0.partial_cmp(&rhs.0).unwrap());
        converted_events
    }

    fn event_count(&self) -> usize {
        self.events.len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::core::common::{ObjectMeta, RuntimeResources};
    use crate::core::node::{Node, NodeStatus};
    use crate::core::pod::{Pod, PodSpec, Resources};
    use crate::trace::generic::{
        ClusterEvent, ClusterEventType, GenericClusterTrace, GenericWorkloadTrace, WorkloadEvent,
        WorkloadEventType,
    };

    #[test]
    fn test_deserialize_empty_trace_from_json() {
        let trace_json = r#"
        events: []
        "#;

        let cluster_deserialized: GenericClusterTrace = serde_yaml::from_str(&trace_json).unwrap();
        let workload_deserialized: GenericWorkloadTrace =
            serde_yaml::from_str(&trace_json).unwrap();

        let cluster_trace = GenericClusterTrace { events: vec![] };
        let workload_trace = GenericWorkloadTrace { events: vec![] };
        assert_eq!(cluster_trace, cluster_deserialized);
        assert_eq!(workload_trace, workload_deserialized);
    }

    #[test]
    fn test_deserialize_cluster_trace_from_yaml() {
        // Enums serialize using YAML’s !tag syntax to identify the variant name.
        // https://docs.rs/serde_yaml/latest/serde_yaml/#using-serde-derive
        let cluster_trace_yaml = r#"
        events:
        - timestamp: 1345
          event_type:
            !CreateNode
              node:
                metadata:
                  name: node_21
                  labels:
                    storage_type: ssd
                    proc_type: intel
                status:
                  capacity:
                    cpu: 16000
                    ram: 17179869184
        - timestamp: 4323.212
          event_type:
            !RemoveNode
              node_name: node_21 
        "#;

        let cluster_deserialized: GenericClusterTrace =
            serde_yaml::from_str(&cluster_trace_yaml).unwrap();
        let cluster_trace = GenericClusterTrace {
            events: vec![
                ClusterEvent {
                    timestamp: 1345.0,
                    event_type: ClusterEventType::CreateNode {
                        node: Node {
                            metadata: ObjectMeta {
                                name: "node_21".to_string(),
                                labels: HashMap::from([
                                    ("storage_type".to_string(), "ssd".to_string()),
                                    ("proc_type".to_string(), "intel".to_string()),
                                ]),
                                creation_timestamp: Default::default(),
                            },
                            status: NodeStatus {
                                capacity: RuntimeResources {
                                    cpu: 16000,
                                    ram: 17179869184,
                                },
                                allocatable: Default::default(),
                                conditions: Default::default(),
                            },
                            spec: Default::default(),
                        },
                    },
                },
                ClusterEvent {
                    timestamp: 4323.212,
                    event_type: ClusterEventType::RemoveNode {
                        node_name: "node_21".to_string(),
                    },
                },
            ],
        };
        assert_eq!(cluster_trace, cluster_deserialized);
    }

    #[test]
    fn test_deserialize_workload_trace_from_yaml() {
        let workload_trace_yaml = r#"
        events:
        - timestamp: 0
          event_type:
            !CreatePod
              pod:
                metadata:
                  name: pod_42
                spec:
                  resources:
                    limits:
                      cpu: 4000
                      ram: 8589934592
                    requests:
                      cpu: 8000
                      ram: 17179869184
                  running_duration: 21.0
        - timestamp: 432
          event_type:
            !RemovePod
              pod_name: pod_42
        "#;

        let workload_deserialized: GenericWorkloadTrace =
            serde_yaml::from_str(&workload_trace_yaml).unwrap();
        let workload_trace = GenericWorkloadTrace {
            events: vec![
                WorkloadEvent {
                    timestamp: 0.0,
                    event_type: WorkloadEventType::CreatePod {
                        pod: Pod {
                            metadata: ObjectMeta {
                                name: "pod_42".to_string(),
                                labels: Default::default(),
                                creation_timestamp: Default::default(),
                            },
                            spec: PodSpec {
                                resources: Resources {
                                    limits: RuntimeResources {
                                        cpu: 4000,
                                        ram: 8589934592,
                                    },
                                    requests: RuntimeResources {
                                        cpu: 8000,
                                        ram: 17179869184,
                                    },
                                    usage_model_config: None,
                                },
                                running_duration: Some(21.0),
                            },
                            status: Default::default(),
                        },
                    },
                },
                WorkloadEvent {
                    timestamp: 432.0,
                    event_type: WorkloadEventType::RemovePod {
                        pod_name: "pod_42".to_string(),
                    },
                },
            ],
        };
        assert_eq!(workload_trace, workload_deserialized);
    }
}
