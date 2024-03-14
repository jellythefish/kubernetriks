use std::collections::HashMap;

use dslab_core::Id;

use serde::{Deserialize, Serialize};

use super::pod::Pod;

// Identifier of any component of kubernetes as a simulation component.
// Generated from sim.create_context.
pub type SimComponentId = Id;

// A partial implementation of ObjectMeta object from k8s
// https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta
#[derive(Default, Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ObjectMeta {
    // A client-provided string that refers to an object. Must be unique across all objects
    // in a simulation.
    pub name: String,
    // Map of kv pairs that can be used to organize and categorize (scope and select) objects.
    #[serde(default)]
    pub labels: HashMap<String, String>,
    // Timestamp of object creation in api-server
    #[serde(default)]
    pub creation_timestamp: f64,
}

#[derive(Default, Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct RuntimeResources {
    pub cpu: u32, // in millicores
    pub ram: u64, // in bytes
}
