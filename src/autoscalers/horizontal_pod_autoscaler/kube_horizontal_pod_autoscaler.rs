use std::cmp::{max, min};
use std::rc::Rc;

use dslab_core::{log_debug, SimulationContext};
use serde::Deserialize;

use crate::config::SimulationConfig;

use crate::autoscalers::horizontal_pod_autoscaler::interface::{
    AutoscaleAction, HorizontalPodAutoscalerAlgorithm, PodGroupInfo,
};

/// Default implementation of horizontal pod autoscaler which is used in Kubernetes.
///
/// Autoscaler runs every `scan_interval` and fetches metrics aggregated by pod group from metric collector.
/// For each pod group desired number of pod replicas is calculated as follows:
/// desiredReplicas = ceil[currentReplicas * ( currentMetricValue / desiredMetricValue )]
/// Then, CreatePodRequest or RemovePodRequest are issued to api server.
///
/// Pod Group may contains target thresholds for cpu metric and ram metric at the same time.
/// If so, KubeHorizontalPodAutoscaler evaluates deseiredReplicas for each metric and takes the
/// maximum scale recommended for each metric and sets the pod count to that size (provided that
/// this isn't larger than the overall maximum pod count configured in pod group).
///
pub struct KubeHorizontalPodAutoscaler {
    ctx: SimulationContext,
    config: KubeHorizontalPodAutoscalerConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct KubeHorizontalPodAutoscalerConfig {
    /// If the ratio of current metric and desired metric is sufficiently close to 1.0 within the
    /// `target_threshold_tolerance`, then autoscaling actions are skipped
    #[serde(default = "target_threshold_tolerance_default")]
    pub target_threshold_tolerance: f64,
}

impl Default for KubeHorizontalPodAutoscalerConfig {
    fn default() -> Self {
        Self {
            target_threshold_tolerance: target_threshold_tolerance_default(),
        }
    }
}

fn target_threshold_tolerance_default() -> f64 {
    0.1
}

impl KubeHorizontalPodAutoscaler {
    pub fn new(config: Rc<SimulationConfig>, ctx: SimulationContext) -> Self {
        Self {
            ctx: ctx,
            config: config
                .horizontal_pod_autoscaler
                .kube_horizontal_pod_autoscaler_config
                .clone()
                .unwrap(),
        }
    }

    /// Returns desired number of pods calculated by default kubernetes' hpa formula with respect to
    /// tolerance when ratio is close to 1.0.
    pub fn desired_number_of_pods_by_metric(
        &self,
        current_replicas: usize,
        current_value: f64,
        desired_value: f64,
    ) -> usize {
        let ratio = current_value / desired_value;
        log_debug!(
            self.ctx,
            "current_value / desired_value metric ratio {:?} (tolerance: {:?})",
            ratio,
            self.config.target_threshold_tolerance
        );
        if (ratio - 1.0).abs() <= self.config.target_threshold_tolerance {
            return current_replicas;
        }
        (current_replicas as f64 * (ratio)).ceil() as usize
    }

    /// Returns desired number of pods accounting current number of pods in pod group, target
    /// utilization thresholds for cpu and ram and current utilization.
    /// `current_cpu` and `current_ram` are mean utilizations for cpu and ram for all pods in group.
    pub fn desired_number_of_pods(
        &self,
        pod_group: &mut PodGroupInfo,
        current_cpu: f64,
        current_ram: f64,
    ) -> usize {
        let mut desired_by_cpu = None;
        let mut desired_by_ram = None;

        if pod_group
            .pod_group
            .target_resources_usage
            .cpu_utilization
            .is_some()
        {
            desired_by_cpu = Some(
                self.desired_number_of_pods_by_metric(
                    pod_group.created_pods.len(),
                    current_cpu,
                    pod_group
                        .pod_group
                        .target_resources_usage
                        .cpu_utilization
                        .unwrap(),
                ),
            );
        }
        if pod_group
            .pod_group
            .target_resources_usage
            .ram_utilization
            .is_some()
        {
            desired_by_ram = Some(
                self.desired_number_of_pods_by_metric(
                    pod_group.created_pods.len(),
                    current_ram,
                    pod_group
                        .pod_group
                        .target_resources_usage
                        .ram_utilization
                        .unwrap(),
                ),
            );
        }

        log_debug!(
            self.ctx,
            "Current mean utilization (cpu, ram) for pod group {:?}: ({:?}, {:?})",
            pod_group.pod_group.name,
            current_cpu,
            current_ram
        );
        log_debug!(
            self.ctx,
            "Target mean utilization (cpu, ram) for pod group {:?}: ({:?}, {:?})",
            pod_group.pod_group.name,
            pod_group.pod_group.target_resources_usage.cpu_utilization,
            pod_group.pod_group.target_resources_usage.ram_utilization
        );
        log_debug!(
            self.ctx,
            "Desired replicas by (cpu, ram): ({:?}, {:?})",
            desired_by_cpu,
            desired_by_ram
        );

        if desired_by_cpu.is_some() && desired_by_ram.is_some() {
            return min(
                pod_group.pod_group.max_pod_count,
                max(desired_by_cpu.unwrap(), desired_by_ram.unwrap()),
            );
        } else if desired_by_cpu.is_some() {
            return min(pod_group.pod_group.max_pod_count, desired_by_cpu.unwrap());
        } else if desired_by_ram.is_some() {
            return min(pod_group.pod_group.max_pod_count, desired_by_ram.unwrap());
        } else {
            // no thresholds are set for pod group - do not scale anything, leave the same pod count
            return pod_group.created_pods.len();
        }
    }

    pub fn make_actions_for_group(
        &self,
        pod_group: &mut PodGroupInfo,
        desired_number_of_pods: usize,
    ) -> Vec<AutoscaleAction> {
        let mut actions: Vec<AutoscaleAction> = Default::default();
        let current_pod_count = pod_group.created_pods.len();

        if current_pod_count == desired_number_of_pods {
            log_debug!(
                self.ctx,
                "Leaving pod count {:?} unchanged for pod group {:?}",
                current_pod_count,
                pod_group.pod_group.name
            );
            return actions;
        } else if current_pod_count < desired_number_of_pods {
            for _ in 0..desired_number_of_pods - current_pod_count {
                // TODO: this is a copypaste code from api server for new pod in group - generalize
                let mut new_pod = pod_group.pod_group.pod_template.clone();
                let pod_name = format!(
                    "{}_{}",
                    pod_group.pod_group.name.clone(),
                    pod_group.total_created
                );
                new_pod.metadata.name = pod_name.clone();
                new_pod
                    .metadata
                    .labels
                    .insert("pod_group".to_string(), pod_group.pod_group.name.clone());
                new_pod.spec.resources.usage_model_config =
                    Some(pod_group.pod_group.resources_usage_model_config.clone());

                actions.push(AutoscaleAction::ScaleUp(new_pod));

                pod_group.created_pods.insert(pod_name);
                pod_group.total_created += 1;
            }
        } else if current_pod_count > desired_number_of_pods {
            for _ in 0..current_pod_count - desired_number_of_pods {
                let next_pod_name = pod_group.created_pods.pop_first().unwrap();
                actions.push(AutoscaleAction::ScaleDown(next_pod_name));
            }
        }

        log_debug!(
            self.ctx,
            "Going to change number of pods {:?}->{:?} for pod group {:?}",
            current_pod_count,
            desired_number_of_pods,
            pod_group.pod_group.name
        );

        return actions;
    }
}

impl HorizontalPodAutoscalerAlgorithm for KubeHorizontalPodAutoscaler {
    fn autoscale(
        &mut self,
        metrics: std::collections::HashMap<String, (f64, f64)>,
        pod_groups: &mut std::collections::HashMap<String, PodGroupInfo>,
    ) -> Vec<AutoscaleAction> {
        let mut actions: Vec<AutoscaleAction> = Default::default();

        for (group_name, (cpu_mean_util, ram_mean_util)) in metrics.iter() {
            let pod_group = pod_groups.get_mut(group_name).unwrap();
            let desired_number_of_pods =
                self.desired_number_of_pods(pod_group, *cpu_mean_util, *ram_mean_util);
            actions.extend(self.make_actions_for_group(pod_group, desired_number_of_pods))
        }

        actions
    }
}
