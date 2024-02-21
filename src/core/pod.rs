//! Type definition for Pod primitive in k8s cluster

use serde::Deserialize;

use crate::core::common::Resources;

#[derive(Debug, Deserialize, PartialEq)]
pub struct Pod {
    id: u64,
    resources_request: Resources,
    resources_limit: Resources,
    running_duration: u64, // in milliseconds

    #[serde(default)]
    state: PodState,
}

impl Pod {
    pub fn new(
        id: u64,
        resources_request: Resources,
        resources_limit: Resources,
        running_duration: u64,
    ) -> Self {
        Pod {
            id,
            resources_request,
            resources_limit,
            running_duration,
            state: PodState::Undefined,
        }
    }
}

#[derive(Default, Debug, Deserialize, PartialEq)]
pub enum PodState {
    #[default]
    Undefined,
}

// // Pod events

// struct PodCreatedEvent {

// }
