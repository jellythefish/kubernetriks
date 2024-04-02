use std::collections::HashMap;

use dslab_kubernetriks::core::common::{ObjectMeta, RuntimeResources};
use dslab_kubernetriks::core::node::{Node, NodeStatus};
use dslab_kubernetriks::core::pod::{Pod, PodSpec, Resources};
use dslab_kubernetriks::trace::generic::{
    ClusterEvent, ClusterEventType, GenericClusterTrace, GenericWorkloadTrace, WorkloadEvent,
    WorkloadEventType,
};

#[test]
fn test_deserialize_empty_trace_from_json() {
    let trace_json = r#"
    events: []
    "#;

    let cluster_deserialized: GenericClusterTrace = serde_yaml::from_str(&trace_json).unwrap();
    let workload_deserialized: GenericWorkloadTrace = serde_yaml::from_str(&trace_json).unwrap();

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
                            },
                            running_duration: 21.0,
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
