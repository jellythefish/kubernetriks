use std::collections::HashSet;
use std::{mem::swap, path::PathBuf};

use serde::Deserialize;

use crate::trace::alibaba_cluster_trace_v2017::common::{CPU_BASE, DENORMALIZATION_BASE};
use crate::{
    core::{
        common::SimulationEvent,
        events::{CreateNodeRequest, RemoveNodeRequest},
        node::Node,
    },
    trace::interface::Trace,
};

#[derive(Debug, Deserialize, PartialEq)]
struct MachineEvent {
    timestamp: i64,
    machine_id: i64,
    /// There are three types of events: `add`, `softerror` and `harderror`.
    ///
    /// `softerror` represents a machine being temporarily unavailable due to software failures,
    /// such as low disk space and agent failures.
    ///
    /// `harderror` represents a machine being unavailable due to hardware failures,
    /// such as disk failures.
    ///
    /// From trace documentation: "In case of software and hardware errors, New online services and
    /// batch jobs should not be placed in the machines, but existing services and jobs may still
    /// function normally."
    ///
    /// Currently our simulator does not support such behavior, so in case of soft or hard error
    /// we emit RemoveNodeRequest only to terminate such nodes, so workload would be rescheduled.
    event_type: String,
    event_detail: Option<String>,
    number_of_cpus: Option<i64>,
    normalized_memory: Option<f64>,
    normalized_disk_space: Option<f64>,
}

pub struct AlibabaClusterTraceV2017 {
    machine_events: Vec<MachineEvent>,
}

impl AlibabaClusterTraceV2017 {
    pub fn new(machine_events_trace_path: PathBuf) -> Self {
        Self {
            machine_events: read_machine_events_from_file(machine_events_trace_path),
        }
    }
}

impl Trace for AlibabaClusterTraceV2017 {
    fn convert_to_simulator_events(
        &mut self,
    ) -> Vec<(f64, Box<dyn crate::core::common::SimulationEvent>)> {
        let mut converted_events: Vec<(f64, Box<dyn SimulationEvent>)> = vec![];
        converted_events.reserve(self.machine_events.len());
        let mut events: Vec<MachineEvent> = vec![];
        swap(&mut events, &mut self.machine_events);

        let mut created_nodes: HashSet<String> = Default::default();
        let mut removed_nodes: HashSet<String> = Default::default();

        for machine_event in events.into_iter() {
            let node_name = format!("alibaba_node_{}", machine_event.machine_id);
            if machine_event.event_type == "add" {
                created_nodes.insert(node_name.clone());
                let cpu = machine_event.number_of_cpus.unwrap();
                let ram = machine_event.normalized_memory.unwrap();
                let converted_cpu = (cpu * CPU_BASE) as u32; // in millicores
                let converted_ram = (ram * DENORMALIZATION_BASE as f64) as u64; // in bytes
                converted_events.push((
                    machine_event.timestamp as f64,
                    Box::new(CreateNodeRequest {
                        node: Node::new(node_name, converted_cpu, converted_ram),
                    }),
                ));
            } else if machine_event.event_type == "softerror"
                || machine_event.event_type == "harderror"
            {
                if removed_nodes.contains(&node_name) || !created_nodes.contains(&node_name) {
                    // already removed or does not exist - skip
                    continue;
                }
                removed_nodes.insert(node_name.clone());
                converted_events.push((
                    machine_event.timestamp as f64,
                    Box::new(RemoveNodeRequest { node_name }),
                ));
            } else {
                panic!(
                    "Unsupported operation for a node in alibaba cluster trace: {}",
                    machine_event.event_type
                )
            }
        }

        converted_events.sort_by(|lhs, rhs| lhs.0.partial_cmp(&rhs.0).unwrap());
        converted_events
    }

    fn event_count(&self) -> usize {
        self.machine_events.len()
    }
}

fn read_machine_events_from_str(trace_str: &str) -> Vec<MachineEvent> {
    let mut trace = vec![];

    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(trace_str.as_bytes());

    for row in csv_reader.deserialize() {
        let batch_instance: MachineEvent = row.unwrap();
        trace.push(batch_instance);
    }
    trace
}

fn read_machine_events_from_file(trace_path: PathBuf) -> Vec<MachineEvent> {
    let abs_path = trace_path.canonicalize().unwrap();
    let trace_str = std::fs::read_to_string(abs_path).unwrap();
    read_machine_events_from_str(&trace_str)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::trace::interface::Trace;

    use super::{read_machine_events_from_str, AlibabaClusterTraceV2017, MachineEvent};

    #[test]
    #[ignore = "too slow read from disk"]
    fn try_read_the_trace() {
        let machine_events = PathBuf::from(
            "/home/btkz/master/diploma/traces/alibaba-cluster-trace-v2017/server_event.csv",
        );
        let mut trace = AlibabaClusterTraceV2017::new(machine_events);
        let events = trace.convert_to_simulator_events();
        println!("event count: {:?}", events.len());
    }

    #[test]
    fn test_parsing_ok() {
        let machine_events = read_machine_events_from_str(
            r#"
49380,618,softerror,machine_fail,0,0,0
"#,
        );
        assert_eq!(
            machine_events[0],
            MachineEvent {
                timestamp: 49380,
                machine_id: 618,
                event_type: "softerror".to_string(),
                event_detail: Some("machine_fail".to_string()),
                number_of_cpus: Some(0),
                normalized_memory: Some(0.0),
                normalized_disk_space: Some(0.0),
            }
        );
    }

    #[test]
    fn test_parsing_optional_field() {
        let machine_events = read_machine_events_from_str(
            r#"
0,924,add,,64,0.6899697150104833,0.09572460286727773
0,925,add,,64,,
"#,
        );
        assert_eq!(
            machine_events[0],
            MachineEvent {
                timestamp: 0,
                machine_id: 924,
                event_type: "add".to_string(),
                event_detail: None,
                number_of_cpus: Some(64),
                normalized_memory: Some(0.6899697150104833),
                normalized_disk_space: Some(0.09572460286727773),
            }
        );
        assert_eq!(
            machine_events[1],
            MachineEvent {
                timestamp: 0,
                machine_id: 925,
                event_type: "add".to_string(),
                event_detail: None,
                number_of_cpus: Some(64),
                normalized_memory: None,
                normalized_disk_space: None,
            }
        );
    }
}
