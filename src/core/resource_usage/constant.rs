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

#[cfg(test)]
mod tests {
    use crate::core::resource_usage::constant::ConstantResourceUsageModel;
    use crate::core::resource_usage::interface::ResourceUsageModel;

    #[test]
    fn test_any_time_constant_usage() {
        let config = "usage: 27.0";
        let mut model = ConstantResourceUsageModel::from_str(config);

        assert_eq!(27.0, model.current_usage(0.0, None));
        assert_eq!(27.0, model.current_usage(500.0, None));
        assert_eq!(27.0, model.current_usage(500.0, None));
        assert_eq!(27.0, model.current_usage(1000.0, None));
        assert_eq!(27.0, model.current_usage(1001.0, None));
    }
}
