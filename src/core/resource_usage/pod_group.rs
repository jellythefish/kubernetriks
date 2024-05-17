//! Implementation of resource usage model for Pod Group.

use serde::Deserialize;

use crate::core::resource_usage::interface::ResourceUsageModel;

#[derive(Deserialize)]
pub struct UsageUnit {
    pub duration: f64,
    pub total_load: f64,
}

/// The point of reference for all usage sequences with their durations is 0.0 of simulation time.
/// Such point was chosen for convenient synchronization of all pods usages.
/// For example if pod was created at 100.0, but have usage sequence durations: 10sec, 40sec, 50sec
/// then it's current usage element is third one.
pub struct PodGroupResourceUsageModel {
    last_unit_start_time: f64,
    last_poll_time: f64,
    usage_sequence: Vec<UsageUnit>,
    current_idx_in_sequence: usize,
}

impl PodGroupResourceUsageModel {
    pub fn new(current_time: f64, usage_sequence: Vec<UsageUnit>) -> Self {
        let mut model = Self {
            last_unit_start_time: 0.0,
            last_poll_time: 0.0,
            usage_sequence,
            current_idx_in_sequence: 0,
        };

        model.step_usage_until_current_time(current_time);

        model
    }

    /// Make model from configuration string.
    /// Should be in the following form:
    /// ```
    /// use dslab_kubernetriks::core::resource_usage::interface::ResourceUsageModel;
    /// use dslab_kubernetriks::core::resource_usage::pod_group::PodGroupResourceUsageModel;
    ///
    /// let config = "
    /// - duration: 1000.0
    ///   total_load: 10.0
    /// - duration: 500.0
    ///   total_load: 5.0
    /// - duration: 5000
    ///   total_load: 10.0
    /// ";
    /// let mut model = PodGroupResourceUsageModel::from_str(config, 0.0);
    ///
    /// assert_eq!(1.0, model.current_usage(32.5, Some(5)));
    /// assert_eq!(0.5, model.current_usage(1001.0, Some(10)));
    /// assert_eq!(0.25, model.current_usage(6500.0, Some(40)));
    /// ```
    ///
    pub fn from_str(config: &str, current_time: f64) -> Self {
        let usage_sequence = serde_yaml::from_str::<Vec<UsageUnit>>(&config).unwrap();
        PodGroupResourceUsageModel::new(current_time, usage_sequence)
    }

    /// Calculates utilization based on the load number at certain point of `time` and `pod count`.
    /// Considers that load is divided by pod count equally.
    /// The load number is relative to pod resources request:
    /// (Total Load / Pod Count) * Pod Resources Request is the desired request for pod considering
    /// current total load on Pod Group.
    /// Thus, utilization of resources on each pod is calculated as:
    /// min(1.0, (Total Load / Pod Count) * Pod Resources / Pod Resources) = min(1.0, Total Load / Pod Count)
    fn current_utilization(&mut self, time: f64, pod_count: usize) -> f64 {
        let current_load = self.current_load(time);
        f64::min(1.0, current_load / pod_count as f64)
    }

    fn step_usage_until_current_time(&mut self, time: f64) {
        // TODO: may be more optimal then linear step?
        let mut current_unit = &self.usage_sequence[self.current_idx_in_sequence];

        while self.last_unit_start_time + current_unit.duration <= time {
            self.last_unit_start_time += current_unit.duration;
            self.current_idx_in_sequence =
                (self.current_idx_in_sequence + 1) % self.usage_sequence.len();
            current_unit = &self.usage_sequence[self.current_idx_in_sequence];
        }
    }

    fn current_load(&mut self, time: f64) -> f64 {
        self.step_usage_until_current_time(time);
        self.usage_sequence[self.current_idx_in_sequence].total_load
    }
}

impl ResourceUsageModel for PodGroupResourceUsageModel {
    /// Time must be monotonically increasing for subsequent calls of this method.
    fn current_usage(&mut self, time: f64, pod_count: Option<usize>) -> f64 {
        if time < self.last_poll_time {
            panic!(
                "Trying to get current usage of time which is behind last poll time: {} vs {}",
                time, self.last_poll_time
            )
        }
        self.last_poll_time = time;
        self.current_utilization(time, pod_count.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::resource_usage::interface::ResourceUsageModel;
    use crate::core::resource_usage::pod_group::PodGroupResourceUsageModel;

    #[test]
    fn test_resource_usage_model_one_unit() {
        let config = "
        - duration: 1000.0
          total_load: 10.0
        ";
        let mut model = PodGroupResourceUsageModel::from_str(config, 0.0);

        assert_eq!(0.2, model.current_usage(0.0, Some(50)));
        assert_eq!(0.2, model.current_usage(500.0, Some(50)));
        assert_eq!(0.2, model.current_usage(500.0, Some(50)));
        assert_eq!(0.2, model.current_usage(1000.0, Some(50)));
        assert_eq!(0.2, model.current_usage(1001.0, Some(50)));
        assert_eq!(0.2, model.current_usage(7431.0, Some(50)));
        assert_eq!(0.2, model.current_usage(63431.0, Some(50)));
    }

    #[test]
    #[should_panic]
    fn test_request_in_past_causes_panic() {
        let config = "
        - duration: 1000.0
          total_load: 10.0
        ";
        let mut model = PodGroupResourceUsageModel::from_str(config, 0.0);
        assert_eq!(0.2, model.current_usage(0.0, Some(50)));
        assert_eq!(0.2, model.current_usage(500.0, Some(50)));
        assert_eq!(0.2, model.current_usage(250.0, Some(50)));
    }

    #[test]
    fn test_complex_resource_usage_model() {
        let config = "
        - duration: 1000.0
          total_load: 10.0
        - duration: 10.0
          total_load: 400.0
        - duration: 200.0
          total_load: 20.0
        - duration: 500.0
          total_load: 1.0
        ";
        let mut model = PodGroupResourceUsageModel::from_str(config, 0.0);

        assert_eq!(1.0, model.current_usage(0.0, Some(10)));
        assert_eq!(1.0, model.current_usage(1000.0, Some(10)));
        assert_eq!(0.25, model.current_usage(1000.0, Some(1600)));
        assert_eq!(0.8, model.current_usage(1000.1, Some(500)));
        assert_eq!(0.5, model.current_usage(1010.0, Some(40)));
        assert_eq!(1.0, model.current_usage(1010.0, Some(20)));
        assert_eq!(0.5, model.current_usage(8550.0, Some(20)));
        assert_eq!(0.25, model.current_usage(9560.0, Some(80)));
        assert_eq!(0.1, model.current_usage(9759.0, Some(200)));
        assert_eq!(0.05, model.current_usage(54376.0, Some(20)));
    }
}
