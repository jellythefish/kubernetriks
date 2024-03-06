//! Type definition for Pod primitive in k8s cluster

use serde::{Deserialize, Serialize};

use crate::core::common::Resources;

pub type PodId = u64;

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct PodSpec {
    id: u64,
    resources_request: Resources,
    resources_limit: Resources,
    running_duration: u64, // in milliseconds
}

#[derive(Default, Debug, Deserialize, Serialize, PartialEq, Clone)]
pub enum PodState {
    #[default]
    Undefined,
}

pub struct PodInfo {
    spec: PodSpec,
    state: PodState,
}

impl PodSpec {
    pub fn new(
        id: PodId,
        resources_request: Resources,
        resources_limit: Resources,
        running_duration: u64,
    ) -> Self {
        Self {
            id,
            resources_request,
            resources_limit,
            running_duration,
        }
    }
}
