use crate::core::common::Resources;

#[derive(Default, Debug)]
pub struct Pod {
    resources_request: Resources,
    resources_limit: Resources,
    running_duration: u64, // in milliseconds
    state: PodState,
}

impl Pod {
    pub fn new() -> Self {
        Pod::default()
    }
}

#[derive(Debug)]
enum PodState {
    Undefined,
}

impl Default for PodState {
    fn default() -> Self { PodState::Undefined }
}
