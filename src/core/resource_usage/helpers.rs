//! Helpers for default model and builder from configuration

use crate::core::resource_usage::constant::ConstantResourceUsageModel;
use crate::core::resource_usage::interface::{ResourceUsageModel, ResourceUsageModelConfig};
use crate::core::resource_usage::pod_group::PodGroupResourceUsageModel;

/// Default model is constant usage
pub fn default_resource_usage_config(usage: f64) -> ResourceUsageModelConfig {
    ResourceUsageModelConfig {
        model_name: "constant".to_string(),
        config: format!("usage: {}", usage),
    }
}

pub fn resource_usage_model_from_config(
    config: ResourceUsageModelConfig,
    start_time: f64,
) -> Box<dyn ResourceUsageModel> {
    match &config.model_name as &str {
        "constant" => Box::new(ConstantResourceUsageModel::from_str(&config.config)),
        "pod_group" => Box::new(PodGroupResourceUsageModel::from_str(
            &config.config,
            start_time,
        )),
        _ => panic!("Unsupported resource usage model: {:?}", config.model_name),
    }
}
