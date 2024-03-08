use std::collections::HashMap;

use dslab_kubernetriks::core::common::Resources;
use dslab_kubernetriks::core::node_info::NodeSpec;
use dslab_kubernetriks::core::pod::PodSpec;
use dslab_kubernetriks::trace::generic::{GenericTrace, TraceEvent, TraceEventType};

#[test]
fn test_deserialize_empty_trace_from_json() {
    let trace_json = r#"
    events: []
    "#;

    let deserialized: GenericTrace = serde_yaml::from_str(&trace_json).unwrap();

    let trace = GenericTrace { events: vec![] };
    assert_eq!(trace, deserialized);
}

#[test]
fn test_deserialize_trace_from_json() {
    // Enums serialize using YAML’s !tag syntax to identify the variant name.
    // https://docs.rs/serde_yaml/latest/serde_yaml/#using-serde-derive
    let trace_yaml = r#"
    events:
    - timestamp: 0
      event_type:
        !CreatePod
          pod_spec:
            id: 42
            resources_request:
              cpu: 4000
              ram: 8589934592
            resources_limit:
              cpu: 8000
              ram: 17179869184
            running_duration: 21000
    - timestamp: 432
      event_type:
        !RemovePod
          pod_id: 42
    - timestamp: 1345
      event_type:
        !CreateNode
          node_spec:
            id: 21
            capacity:
              cpu: 16000
              ram: 17179869184
            attributes:
              storage_type: ssd
              proc_type: intel
    - timestamp: 4323.212
      event_type:
        !RemoveNode
          node_id: 21
    "#;

    let deserialized: GenericTrace = serde_yaml::from_str(&trace_yaml).unwrap();

    let trace = GenericTrace {
        events: vec![
            TraceEvent {
                timestamp: 0.0,
                event_type: TraceEventType::CreatePod {
                    pod_spec: PodSpec::new(
                        42,
                        Resources {
                            cpu: 4000,
                            ram: 8589934592,
                        },
                        Resources {
                            cpu: 8000,
                            ram: 17179869184,
                        },
                        21000,
                    ),
                },
            },
            TraceEvent {
                timestamp: 432.0,
                event_type: TraceEventType::RemovePod { pod_id: 42 },
            },
            TraceEvent {
                timestamp: 1345.0,
                event_type: TraceEventType::CreateNode {
                    node_spec: NodeSpec::new(
                        21,
                        Resources {
                            cpu: 16000,
                            ram: 17179869184,
                        },
                        HashMap::from([
                            ("storage_type".to_string(), "ssd".to_string()),
                            ("proc_type".to_string(), "intel".to_string()),
                        ]),
                    ),
                },
            },
            TraceEvent {
                timestamp: 4323.212,
                event_type: TraceEventType::RemoveNode { node_id: 21 },
            },
        ],
    };

    assert_eq!(trace, deserialized);
}
