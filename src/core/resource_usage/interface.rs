//! Interface for resource usage model

use serde::{Deserialize, Serialize};

/// Resource usage model is a method, which defines load of some resource at the moment `time` which
/// is current simulation time.
/// `pod_count` is optional argument which is needed for pod group resource usage model
pub trait ResourceUsageModel {
    fn current_usage(&mut self, time: f64, pod_count: Option<usize>) -> f64;
}

/// Config describes model name and configuration in arbitrary format which certain implementation of
/// `ResourceUsageModel` trait must be able to parse in form of yaml string.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct ResourceUsageModelConfig {
    pub model_name: String,
    pub config: String,
}
