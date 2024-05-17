//! Type definition for Pod primitive in k8s cluster

use serde::{Deserialize, Serialize};

use crate::core::common::{ObjectMeta, RuntimeResources, RuntimeResourcesUsageModelConfig};

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Resources {
    pub limits: RuntimeResources,
    pub requests: RuntimeResources,
    /// Cpu and ram usage model configuration
    pub usage_model_config: Option<RuntimeResourcesUsageModelConfig>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct PodSpec {
    /// Simplified: instead of vector of containers - one container with resources and duration
    pub resources: Resources,
    /// Custom field to simulate container workload duration.
    /// None is used for infinite duration to simulate long-running services.
    pub running_duration: Option<f64>, // in seconds
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum PodConditionType {
    // Pod is accepted via kube-api-server and written to persistent storage
    PodCreated,
    // Pod is scheduled to a node but is not bound to a node
    PodScheduled,
    // Pod is on a node and started to initialize (fetching container images, etc)
    // TODO: may be unused now
    PodInitializing,
    // Pod initialized all containers and started them, at least one container is still running in
    // this phase.
    PodRunning,
    // All containers in the pod have terminated successfully
    PodSucceeded,
    // All containers in the pod have terminated and at least one container has terminated
    // in failure.
    PodFailed,
    // Pod is manually (with event request) removed from a node, terminating all its running containers.
    PodRemoved,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct PodCondition {
    // True, False or Unknown
    pub status: String,
    pub condition_type: PodConditionType,
    // Last event time the condition transit from one status to another.
    pub last_transition_time: f64,
}

#[derive(Default, Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct PodStatus {
    // Time of a pod being accepted by a node before pulling container images.
    pub start_time: f64,
    pub conditions: Vec<PodCondition>,
    pub assigned_node: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Pod {
    pub metadata: ObjectMeta,
    pub spec: PodSpec,
    #[serde(default)]
    pub status: PodStatus,
}

impl Pod {
    pub fn new(name: String, cpu: u32, ram: u64, running_duration: Option<f64>) -> Self {
        Self {
            metadata: ObjectMeta {
                name: name,
                labels: Default::default(),
                creation_timestamp: Default::default(),
            },
            spec: PodSpec {
                resources: Resources {
                    limits: RuntimeResources { cpu, ram },
                    requests: RuntimeResources { cpu, ram },
                    usage_model_config: None,
                },
                running_duration,
            },
            status: Default::default(),
        }
    }

    // TODO: ? make this code general with update_node_condition
    pub fn update_condition(
        &mut self,
        status: String,
        condition_type: PodConditionType,
        last_transition_time: f64,
    ) {
        let conditions = &mut self.status.conditions;
        match conditions
            .iter_mut()
            .find(|elem| elem.condition_type == condition_type)
        {
            Some(condition) => {
                condition.status = status;
                condition.last_transition_time = last_transition_time;
            }
            None => {
                conditions.push(PodCondition {
                    status,
                    condition_type,
                    last_transition_time,
                });
            }
        }
    }

    // Ref to condition if it exists else None.
    pub fn get_condition(&self, condition_type: PodConditionType) -> Option<&PodCondition> {
        self.status
            .conditions
            .iter()
            .find(|c| c.condition_type == condition_type)
    }
}
