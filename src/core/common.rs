use dslab_core::Id;

use serde::{Deserialize, Serialize};

// Identifier of any component of kubernetes as a simulation component.
// Generated from sim.create_context.
pub type SimComponentId = Id;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Resources {
    pub cpu: u32, // in millicores
    pub ram: u64, // in bytes
}

impl Default for Resources {
    fn default() -> Self {
        Self { cpu: 0, ram: 0 }
    }
}
