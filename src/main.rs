use std::path::PathBuf;
use std::rc::Rc;
use std::{env, vec};

use env_logger::Target;
use file_rotate::{compression::Compression, suffix::AppendCount, ContentLimit, FileRotate};

use clap::Parser;
use log::info;

use dslab_kubernetriks::config::SimulationConfig;
use dslab_kubernetriks::simulation_callbacks::RunUntilAllPodsAreFinishedCallbacks;
use dslab_kubernetriks::simulator::KubernetriksSimulation;

use dslab_kubernetriks::trace::alibaba_cluster_trace_v2017::cluster::AlibabaClusterTraceV2017;
use dslab_kubernetriks::trace::alibaba_cluster_trace_v2017::workload::AlibabaWorkloadTraceV2017;
use dslab_kubernetriks::trace::generic::{GenericClusterTrace, GenericWorkloadTrace};
use dslab_kubernetriks::trace::interface::Trace;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    config_file: std::path::PathBuf,
}

fn main() {
    let args = Args::parse();

    let config_yaml =
        std::fs::read_to_string(&args.config_file).expect("could not read config file");
    let config = Rc::new(serde_yaml::from_str::<SimulationConfig>(&config_yaml).unwrap());

    let mut env_logger_builder = env_logger::builder();
    if env::var("RUST_LOG").is_err() {
        // log level INFO by default
        env_logger_builder.filter_level(log::LevelFilter::Info);
    }

    if let Some(log_filepath) = &config.logs_filepath {
        env_logger_builder.target(Target::Pipe(Box::new(FileRotate::new(
            log_filepath.clone(),
            AppendCount::new(50),
            ContentLimit::Bytes(104857600),
            Compression::None,
            #[cfg(unix)]
            None,
        ))));
    }

    env_logger_builder.init();

    info!(
        "Path to config file: {:?}",
        args.config_file.canonicalize().unwrap()
    );

    let mut cluster_trace: Box<dyn Trace>;
    let mut workload_trace: Box<dyn Trace>;

    let trace_config = config.trace_config.as_ref().unwrap();

    assert!(
        trace_config.alibaba_cluster_trace_v2017.is_some() ^ trace_config.generic_trace.is_some(),
        "only one of trace config must be set"
    );
    if trace_config.alibaba_cluster_trace_v2017.is_some() {
        info!("Reading alibaba trace v2017 workload trace...");
        let config = trace_config.alibaba_cluster_trace_v2017.as_ref().unwrap();

        if config.machine_events_trace_path.is_some() {
            info!("Reading alibaba trace v2017 cluster trace...");
            cluster_trace = Box::new(AlibabaClusterTraceV2017::new(PathBuf::from(
                &config.machine_events_trace_path.as_ref().unwrap(),
            )))
        } else {
            cluster_trace = Box::new(GenericClusterTrace { events: vec![] });
        }

        workload_trace = Box::new(AlibabaWorkloadTraceV2017::new(
            PathBuf::from(&config.batch_instance_trace_path),
            PathBuf::from(&config.batch_task_trace_path),
        ));
    } else {
        info!("Reading generic cluster and workload traces...");
        let config = trace_config.generic_trace.as_ref().unwrap();

        let cluster_trace_yaml =
            std::fs::read_to_string(&config.cluster_trace_path).expect("could not read trace file");
        let workload_trace_yaml = std::fs::read_to_string(&config.workload_trace_path)
            .expect("could not read trace file");

        cluster_trace =
            Box::new(serde_yaml::from_str::<GenericClusterTrace>(&cluster_trace_yaml).unwrap());
        workload_trace =
            Box::new(serde_yaml::from_str::<GenericWorkloadTrace>(&workload_trace_yaml).unwrap());
    }

    let mut kubernetriks_simulation = KubernetriksSimulation::new(config);
    kubernetriks_simulation.initialize(cluster_trace.as_mut(), workload_trace.as_mut());
    info!("Running simulation...");
    kubernetriks_simulation.run_with_callbacks(Box::new(RunUntilAllPodsAreFinishedCallbacks {}));
}
