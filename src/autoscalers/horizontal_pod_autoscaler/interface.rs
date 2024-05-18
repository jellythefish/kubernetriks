use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::core::pod::Pod;

use crate::core::common::RuntimeResourcesUsageModelConfig;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct TargetResourcesUsage {
    pub cpu_utilization: Option<f64>,
    pub ram_utilization: Option<f64>,
}

/// Pod group represents a set of pods which are considered as long running online services.
/// Each pod in a group has no running duration meaning it runs until remove request arrives from
/// trace or from horizontal pod autoscaler.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct PodGroup {
    /// Name of pod group used for prefix of pod names.
    pub name: String,
    /// Initial count of pods in a group.
    pub initial_pod_count: usize,
    /// Maximum count of pods for horizontal pod autoscaler.  
    pub max_pod_count: usize,
    /// Template of a pod for all pods in a group.
    pub pod_template: Pod,
    /// Target usage (utilization) of cpu and ram in ratio from 0 to 1.
    /// Relative to pod resources requests.
    pub target_resources_usage: TargetResourcesUsage,
    /// Model of pod's resources usage for each pod in a group.
    pub resources_usage_model_config: RuntimeResourcesUsageModelConfig,
}

/// Represents a state of the pod group in autoscaler.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct PodGroupInfo {
    /// Set of pod names which are currently active for pod group.
    pub created_pods: BTreeSet<String>,
    /// For monotonically incrementing counter used in unique scaled up pod names.
    pub total_created: usize,
    pub pod_group: PodGroup,
}

pub enum AutoscaleAction {
    ScaleUp(Pod),
    /// Scale down action with node name as string
    ScaleDown(String),
}

pub trait HorizontalPodAutoscalerAlgorithm {
    fn autoscale(
        &mut self,
        pod_group_metrics: (f64, f64),
        pod_group_info: &mut PodGroupInfo,
    ) -> Vec<AutoscaleAction>;
}
