use std::collections::HashMap;

use downcast_rs::impl_downcast;
use dyn_clone::clone_trait_object;

use dslab_core::{event::EventData, Id};

use serde::{Deserialize, Serialize};

// Identifier of any component of kubernetes as a simulation component.
// Generated from sim.create_context.
pub type SimComponentId = Id;

// SimulationEvent (also bounded to dslab's `EventData`) is a trait which all simulation events in
// kubernetriks should implement. In our simulator we explicitly use `SimulationEvent` to differ
// trace's events from events which are created directly by kubernetriks components.
// All traces must have converters from their events to simulation ones. Simulation events are
// described in `events.rs`.
// So, any event which implements `SimulationEvent` becomes emittable via `SimulationContext` of
// simulation components in kubernetriks.
pub trait SimulationEvent: EventData {}

impl_downcast!(SimulationEvent);
clone_trait_object!(SimulationEvent);
erased_serde::serialize_trait_object!(SimulationEvent);

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
