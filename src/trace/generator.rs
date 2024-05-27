use dslab_core::Simulation;

use crate::{core::pod::Pod, trace::generic::GenericWorkloadTrace};

use crate::trace::generic::{WorkloadEvent, WorkloadEventType};

// TODO: rework eventually - WIP
fn _generate_workload_trace() -> GenericWorkloadTrace {
    let mut trace: GenericWorkloadTrace = GenericWorkloadTrace{events: vec![]};
    trace.events.reserve(100000);
    
    let mut sim = Simulation::new(123);

    let bins = vec![
        (0, 0u64),
        (1000, 2147483648u64),
        (2000, 4294967296u64),
        (4000, 8589934592u64),
        (8000, 17179869184u64),
        (16000, 34359738368u64),
        (32000, 68719476736u64),
        (64000, 137438953472u64),
        (128000, 274877906944u64),
        (256000, 549755813888u64),
        (512000, 1099511627776u64),
    ];

    for i in 0..100000 {
        let pod_name = format!("pod_{}", i);
        let bin_no = sim.gen_range(1..=10);
        let cpu_lower = bins[bin_no - 1].0;
        let cpu_upper = bins[bin_no].0;
        let ram_lower = bins[bin_no - 1].1;
        let ram_upper = bins[bin_no].1;
        let cpu = sim.gen_range(cpu_lower..=cpu_upper);
        let ram = sim.gen_range(ram_lower..=ram_upper);
        let running_duration = sim.gen_range(1..=10000) as f64;
        let pod = Pod::new(pod_name, cpu, ram, Some(running_duration));
        trace.events.push(WorkloadEvent {timestamp: 0.0, event_type: WorkloadEventType::CreatePod { pod }});
    }

    trace
}
