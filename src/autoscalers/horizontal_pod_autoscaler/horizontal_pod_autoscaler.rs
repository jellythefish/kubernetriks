use std::{cell::RefCell, collections::HashMap, rc::Rc};

use dslab_core::{cast, log_debug, log_info, Event, EventHandler, SimulationContext};
use serde::Deserialize;

use crate::{
    config::SimulationConfig,
    core::{
        common::SimComponentId,
        events::{
            CreatePodRequest, RegisterPodGroup, RemovePodRequest, RunHorizontalPodAutoscalerCycle,
        },
        pod::Pod,
    },
    metrics::collector::MetricsCollector,
};

use crate::autoscalers::horizontal_pod_autoscaler::{
    interface::{AutoscaleAction, HorizontalPodAutoscalerAlgorithm, PodGroupInfo},
    kube_horizontal_pod_autoscaler::KubeHorizontalPodAutoscalerConfig,
};

use crate::autoscalers::horizontal_pod_autoscaler::kube_horizontal_pod_autoscaler::KubeHorizontalPodAutoscaler;

pub struct HorizontalPodAutoscaler {
    api_server: SimComponentId,

    /// Current state of pod groups.
    pub pod_groups: HashMap<String, PodGroupInfo>,
    autoscaling_algorithm: Box<dyn HorizontalPodAutoscalerAlgorithm>,

    ctx: SimulationContext,
    config: Rc<SimulationConfig>,

    metrics_collector: Rc<RefCell<MetricsCollector>>,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct HorizontalPodAutoscalerConfig {
    #[serde(default = "enabled_default")]
    pub enabled: bool,
    /// Name of implementation type for horizontal pod autoscaler. Used in `resolve_horizontal_pod_autoscaler_impl`.
    #[serde(default = "autoscaler_type_default")]
    pub autoscaler_type: String,
    #[serde(default = "scan_interval_default")]
    pub scan_interval: f64,
    /// One of implementation of horizontal pod autoscaler
    pub kube_horizontal_pod_autoscaler_config: Option<KubeHorizontalPodAutoscalerConfig>,
}

fn enabled_default() -> bool {
    false // disabled by default
}
fn autoscaler_type_default() -> String {
    "kube_horizontal_pod_autoscaler".to_string()
}
fn scan_interval_default() -> f64 {
    60.0 // 60 seconds
}

impl Default for HorizontalPodAutoscalerConfig {
    fn default() -> Self {
        Self {
            enabled: enabled_default(),
            autoscaler_type: autoscaler_type_default(),
            scan_interval: scan_interval_default(),
            kube_horizontal_pod_autoscaler_config: None,
        }
    }
}

impl HorizontalPodAutoscaler {
    pub fn new(
        api_server: SimComponentId,
        autoscaling_algorithm: Box<dyn HorizontalPodAutoscalerAlgorithm>,
        ctx: SimulationContext,
        config: Rc<SimulationConfig>,
        metrics_collector: Rc<RefCell<MetricsCollector>>,
    ) -> Self {
        Self {
            api_server,
            pod_groups: Default::default(),
            autoscaling_algorithm,
            ctx,
            config,
            metrics_collector,
        }
    }

    pub fn start(&mut self) {
        log_info!(
            self.ctx,
            "Horizontal pod autoscaler started running every {} seconds",
            self.config.horizontal_pod_autoscaler.scan_interval
        );
        self.ctx.emit_self_now(RunHorizontalPodAutoscalerCycle {});
    }

    fn scale_up_request(&mut self, pod: &Pod) {
        log_debug!(self.ctx, "Scaling up new pod {:?}", pod);

        self.ctx.emit(
            CreatePodRequest { pod: pod.clone() },
            self.api_server,
            self.config.as_to_ca_network_delay,
        );

        self.metrics_collector
            .borrow_mut()
            .metrics
            .total_scaled_up_pods += 1;
    }

    fn scale_down_request(&mut self, pod_name: &String) {
        log_debug!(self.ctx, "Scaling down pod {:?}", &pod_name);

        self.ctx.emit(
            RemovePodRequest {
                pod_name: pod_name.clone(),
            },
            self.api_server,
            self.config.as_to_ca_network_delay,
        );

        self.metrics_collector
            .borrow_mut()
            .metrics
            .total_scaled_down_pods += 1;
    }

    fn take_actions(&mut self, actions: &Vec<AutoscaleAction>) {
        for action in actions {
            match action {
                AutoscaleAction::ScaleUp(pod) => self.scale_up_request(&pod),
                AutoscaleAction::ScaleDown(pod_name) => {
                    self.scale_down_request(pod_name);
                }
            }
        }
    }

    /// Does not simulate and measure cluster autoscaler cycle time because of no interest,
    /// supposing it works instantly.
    /// Behavior may be changed later to simulate its time depending on node cluster and number of
    /// unscheduled pods.
    fn run_horizontal_pod_autoscaler_cycle(&mut self) {
        let metrics = self
            .metrics_collector
            .borrow()
            .pod_metrics_mean_utilization();
        let actions = self
            .autoscaling_algorithm
            .autoscale(metrics, &mut self.pod_groups);

        self.take_actions(&actions);

        self.ctx.emit_self(
            RunHorizontalPodAutoscalerCycle {},
            self.config.horizontal_pod_autoscaler.scan_interval,
        );
    }
}

pub fn resolve_horizontal_pod_autoscaler_impl(
    autoscaler_config: HorizontalPodAutoscalerConfig,
    ctx: SimulationContext,
) -> Box<dyn HorizontalPodAutoscalerAlgorithm> {
    match &autoscaler_config.autoscaler_type as &str {
        "kube_horizontal_pod_autoscaler" => {
            let config = match autoscaler_config.kube_horizontal_pod_autoscaler_config {
                Some(conf) => conf,
                None => KubeHorizontalPodAutoscalerConfig::default(),
            };
            Box::new(KubeHorizontalPodAutoscaler::new(config, ctx))
        }
        _ => panic!("Unsupported horizontal pod autoscaler implementation"),
    }
}

impl EventHandler for HorizontalPodAutoscaler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            RunHorizontalPodAutoscalerCycle {} => {
                self.run_horizontal_pod_autoscaler_cycle();
            }
            RegisterPodGroup { pod_group_info } => {
                self.pod_groups
                    .insert(pod_group_info.pod_group.name.clone(), pod_group_info);
            }
        })
    }
}
