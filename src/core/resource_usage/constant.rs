//! Constant usage model implementation

use serde::Deserialize;

use crate::core::resource_usage::interface::ResourceUsageModel;

#[derive(Deserialize)]
pub struct ConstantResourceUsageModel {
    usage: f64,
}

impl ConstantResourceUsageModel {
    pub fn new(usage: f64) -> Self {
        Self { usage }
    }

    /// Make model from configuration string.
    /// Should be in the following form:
    /// ```
    /// use dslab_kubernetriks::core::resource_usage::interface::ResourceUsageModel;
    /// use dslab_kubernetriks::core::resource_usage::constant::ConstantResourceUsageModel;
    ///
    /// let config = "usage: 32.0";
    /// let mut model = ConstantResourceUsageModel::from_str(config);
    ///
    /// assert_eq!(32.0, model.current_usage(32.5, None));
    /// ```
    ///
    pub fn from_str(config: &str) -> Self {
        serde_yaml::from_str::<ConstantResourceUsageModel>(&config).unwrap()
    }
}

impl ResourceUsageModel for ConstantResourceUsageModel {
    fn current_usage(&mut self, _time: f64, _pod_count: Option<usize>) -> f64 {
        self.usage
    }
}
