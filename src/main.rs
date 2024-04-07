use clap::Parser;
use log::info;
use std::borrow::BorrowMut;
use std::path::PathBuf;
use std::rc::Rc;
use std::{env, vec};

use dslab_kubernetriks::simulator::{KubernetriksSimulation, SimulationConfig};
use dslab_kubernetriks::trace::alibaba_cluster_trace_v2017::workload::AlibabaWorkloadTraceV2017;
use dslab_kubernetriks::trace::generic::{GenericClusterTrace, GenericWorkloadTrace};
use dslab_kubernetriks::trace::interface::Trace;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    config_file: std::path::PathBuf,
}

fn main() {
    // log level INFO by default
    let mut env_logger_builder = env_logger::builder();
    if env::var("RUST_LOG").is_err() {
        env_logger_builder.filter_level(log::LevelFilter::Info);
    }
    env_logger_builder.init();

    let args = Args::parse();

    info!(
        "Path to config file: {:?}",
        args.config_file.canonicalize().unwrap()
    );
    let config_yaml =
        std::fs::read_to_string(&args.config_file).expect("could not read config file");
    let config = Rc::new(serde_yaml::from_str::<SimulationConfig>(&config_yaml).unwrap());

    let mut cluster_trace: Box<dyn Trace>;
    let mut workload_trace: Box<dyn Trace>;

    let trace_config = config.trace_config.as_ref().unwrap();

    assert!(
        !trace_config.alibaba_cluster_trace_v2017.is_none() ^ !trace_config.generic_trace.is_none(),
        "only one of trace config must be set"
    );
    if !trace_config.alibaba_cluster_trace_v2017.is_none() {
        info!("Reading alibaba cluster trace v2017 workload trace...");
        // dummy cluster trace
        cluster_trace = Box::new(GenericClusterTrace { events: vec![] });
        let batch_instance_path = PathBuf::from(
            &trace_config
                .alibaba_cluster_trace_v2017
                .as_ref()
                .unwrap()
                .batch_instance_trace_path,
        );
        let batch_task_path = PathBuf::from(
            &trace_config
                .alibaba_cluster_trace_v2017
                .as_ref()
                .unwrap()
                .batch_task_trace_path,
        );

        workload_trace = Box::new(AlibabaWorkloadTraceV2017::new(
            batch_instance_path,
            batch_task_path,
        ));
    } else {
        info!("Reading generic cluster and workload traces");
        let cluster_trace_yaml = std::fs::read_to_string(
            &trace_config
                .generic_trace
                .as_ref()
                .unwrap()
                .cluster_trace_path,
        )
        .expect("could not read trace file");
        let workload_trace_yaml = std::fs::read_to_string(
            &trace_config
                .generic_trace
                .as_ref()
                .unwrap()
                .workload_trace_path,
        )
        .expect("could not read trace file");

        cluster_trace =
            Box::new(serde_yaml::from_str::<GenericClusterTrace>(&cluster_trace_yaml).unwrap());
        workload_trace =
            Box::new(serde_yaml::from_str::<GenericWorkloadTrace>(&workload_trace_yaml).unwrap());
    }

    let mut kubernetriks_simulation = KubernetriksSimulation::new(config);
    kubernetriks_simulation.initialize(cluster_trace.as_mut(), workload_trace.as_mut());
    info!("Running simulation...");
    kubernetriks_simulation.run();
}
