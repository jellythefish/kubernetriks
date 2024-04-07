use std::{
    collections::{HashMap, HashSet},
    mem::swap,
    path::PathBuf,
};

use serde::Deserialize;

use crate::{
    core::{common::SimulationEvent, events::CreatePodRequest, pod::Pod},
    trace::interface::Trace,
};

#[derive(Debug, Deserialize, PartialEq)]
struct BatchTask {
    task_create_time: i64,
    task_end_time: i64,
    job_id: i64,
    task_id: i64,
    number_of_instances: i64,
    status: String,
    number_of_cpus_requested_per_instance_in_the_task: Option<i64>, // in cores
    normalized_memory_requested_per_instance_in_the_task: Option<f64>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct BatchInstance {
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    job_id: Option<i64>,
    task_id: Option<i64>,
    machine_id: Option<i64>,
    status: String,
    sequence_number: i64,
    total_sequence_number: i64,
    maximum_real_cpu_number: Option<f64>,
    average_real_cpu_number: Option<f64>,
    maximum_normalized_memory_usage: Option<f64>,
    average_normalized_memory_usage: Option<f64>,
}

struct AlibabaWorkloadTraceV2017 {
    batch_instances_events: Vec<BatchInstance>,
    batch_tasks: HashMap<i64, BatchTask>,
}

impl AlibabaWorkloadTraceV2017 {
    pub fn new(batch_instance_trace_path: PathBuf, batch_task_trace_path: PathBuf) -> Self {
        Self {
            batch_instances_events: read_batch_instance_from_file(batch_instance_trace_path),
            batch_tasks: read_batch_trace_from_file(batch_task_trace_path),
        }
    }

    fn make_pods_from_instances(&self, instances: Vec<BatchInstance>) -> Vec<(f64, Pod)> {
        let mut pods = vec![];
        pods.reserve(instances.len());

        let mut used_pod_names: HashSet<String> = Default::default();

        for instance in instances {
            if instance.start_timestamp.is_none()
                || instance.end_timestamp.is_none()
                || instance.task_id.is_none()
                || instance.job_id.is_none()
            {
                continue;
            }
            let start_timestamp = instance.end_timestamp.unwrap();
            let end_timestamp = instance.start_timestamp.unwrap();
            if start_timestamp <= 0 || end_timestamp <= 0 || start_timestamp < end_timestamp {
                continue;
            }
            let batch_task = self.batch_tasks.get(&instance.task_id.unwrap()).unwrap();
            if batch_task
                .number_of_cpus_requested_per_instance_in_the_task
                .is_none()
                || batch_task
                    .normalized_memory_requested_per_instance_in_the_task
                    .is_none()
            {
                continue;
            }
            let pod_name = format!(
                "{}_{}_{}",
                instance.job_id.unwrap(),
                instance.task_id.unwrap(),
                instance.sequence_number
            );
            if used_pod_names.contains(&pod_name) {
                continue;
            }
            used_pod_names.insert(pod_name.clone());

            let cpu = batch_task
                .number_of_cpus_requested_per_instance_in_the_task
                .unwrap();
            let converted_cpu = (cpu * 1000) as u32; // in millicores
            let ram = batch_task
                .normalized_memory_requested_per_instance_in_the_task
                .unwrap();
            // taking absolute of 128 GB for value 1.0 of normalized
            let base: u64 = 128 * 1024 * 1024 * 1024;
            let converted_ram = (ram * base as f64) as u64; // in bytes
            let running_duration = (end_timestamp - start_timestamp) as f64;

            let pod = Pod::new(pod_name, converted_cpu, converted_ram, running_duration);
            pods.push((start_timestamp as f64, pod));
        }

        pods
    }
}

impl Trace for AlibabaWorkloadTraceV2017 {
    fn convert_to_simulator_events(
        &mut self,
    ) -> Vec<(f64, Box<dyn crate::core::common::SimulationEvent>)> {
        let mut converted_events: Vec<(f64, Box<dyn SimulationEvent>)> = vec![];
        converted_events.reserve(self.batch_instances_events.len());
        let mut events: Vec<BatchInstance> = vec![];
        swap(&mut events, &mut self.batch_instances_events);

        let pods = self.make_pods_from_instances(events);
        for (start_ts, pod) in pods {
            converted_events.push((start_ts, Box::new(CreatePodRequest { pod })));
        }

        // do not need it anymore
        self.batch_tasks.clear();

        converted_events
    }

    fn event_count(&self) -> usize {
        self.batch_instances_events.len()
    }
}

// returning Hash Map where key is task_id
fn read_batch_trace_from_str(trace_str: &str) -> HashMap<i64, BatchTask> {
    let mut trace: HashMap<i64, BatchTask> = Default::default();

    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(trace_str.as_bytes());

    for row in csv_reader.deserialize() {
        let batch_task: BatchTask = row.unwrap();
        let task_id = batch_task.task_id;
        match trace.insert(task_id, batch_task) {
            Some(val) => panic!("duplicated task id: {:?}", task_id),
            _ => {}
        }
    }
    trace
}

// returning Hash Map where key is task_id
fn read_batch_trace_from_file(trace_path: PathBuf) -> HashMap<i64, BatchTask> {
    let abs_path = trace_path.canonicalize().unwrap();
    let trace_str = std::fs::read_to_string(abs_path).unwrap();
    read_batch_trace_from_str(&trace_str)
}

fn read_batch_instance_from_str(trace_str: &str) -> Vec<BatchInstance> {
    let mut trace = vec![];

    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(trace_str.as_bytes());

    for row in csv_reader.deserialize() {
        let batch_instance: BatchInstance = row.unwrap();
        trace.push(batch_instance);
    }
    trace
}

fn read_batch_instance_from_file(trace_path: PathBuf) -> Vec<BatchInstance> {
    let abs_path = trace_path.canonicalize().unwrap();
    let trace_str = std::fs::read_to_string(abs_path).unwrap();
    read_batch_instance_from_str(&trace_str)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::trace::interface::Trace;

    use super::{
        read_batch_instance_from_str, read_batch_trace_from_str, AlibabaWorkloadTraceV2017,
        BatchInstance, BatchTask,
    };

    #[test]
    fn try_read_the_trace() {
        let instance = PathBuf::from(
            "/home/btkz/master/diploma/traces/alibaba-cluster-trace-v2017/batch_instance.csv",
        );
        let task = PathBuf::from(
            "/home/btkz/master/diploma/traces/alibaba-cluster-trace-v2017/batch_task.csv",
        );

        let mut trace = AlibabaWorkloadTraceV2017::new(instance, task);
        let events = trace.convert_to_simulator_events();
        println!("event count: {:?}", events.len());
    }

    #[test]
    fn test_parsing_ok() {
        let batch_instance = read_batch_instance_from_str(
            r#"
41562,41618,120,686,299,Terminated,1,1,1.5,0.29,1.0,1.2
"#,
        );
        assert_eq!(
            batch_instance[0],
            BatchInstance {
                start_timestamp: Some(41562),
                end_timestamp: Some(41618),
                job_id: Some(120),
                task_id: Some(686),
                machine_id: Some(299),
                status: "Terminated".to_string(),
                sequence_number: 1,
                total_sequence_number: 1,
                maximum_real_cpu_number: Some(1.5),
                average_real_cpu_number: Some(0.29),
                maximum_normalized_memory_usage: Some(1.0),
                average_normalized_memory_usage: Some(1.2)
            }
        );

        let batch_task = read_batch_trace_from_str(
            r#"
10718,12897,15,64,2003,Terminated,50,0.01600704061294748
"#,
        );
        assert_eq!(
            batch_task.get(&64).unwrap(),
            &BatchTask {
                task_create_time: 10718,
                task_end_time: 12897,
                job_id: 15,
                task_id: 64,
                number_of_instances: 2003,
                status: "Terminated".to_string(),
                number_of_cpus_requested_per_instance_in_the_task: Some(50),
                normalized_memory_requested_per_instance_in_the_task: Some(0.01600704061294748),
            }
        );
    }

    #[test]
    fn test_parsing_optional_field() {
        let batch_instance = read_batch_instance_from_str(
            r#"
0,,120,686,,Interrupted,1,1,,,,
76058,76058,,,769,Terminated,1,1,0.5,0.5,,
"#,
        );
        assert_eq!(
            batch_instance[0],
            BatchInstance {
                start_timestamp: Some(0),
                end_timestamp: None,
                job_id: None,
                task_id: None,
                machine_id: None,
                status: "Interrupted".to_string(),
                sequence_number: 1,
                total_sequence_number: 1,
                maximum_real_cpu_number: None,
                average_real_cpu_number: None,
                maximum_normalized_memory_usage: None,
                average_normalized_memory_usage: None
            }
        );

        let batch_task = read_batch_trace_from_str(
            r#"
6036,6046,4,6,452,Waiting,,
"#,
        );
        assert_eq!(
            batch_task.get(&6).unwrap(),
            &BatchTask {
                task_create_time: 6036,
                task_end_time: 6046,
                job_id: 4,
                task_id: 6,
                number_of_instances: 452,
                status: "Waiting".to_string(),
                number_of_cpus_requested_per_instance_in_the_task: None,
                normalized_memory_requested_per_instance_in_the_task: None,
            }
        );
    }
}
