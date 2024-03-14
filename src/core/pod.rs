//! Type definition for Pod primitive in k8s cluster

use serde::{Deserialize, Serialize};

use crate::core::common::{ObjectMeta, RuntimeResources};

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Resources {
    pub limits: RuntimeResources,
    pub requests: RuntimeResources,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Container {
    pub resources: Resources,
    // Custom field to simulate container workload duration
    // -1.0 is used for infinite duration to simulate long-running services
    pub running_duration: f64, // in seconds
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct PodSpec {
    pub containers: Vec<Container>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum PodConditionType {
    // Pod is accepted via kube-api-server and written to persistent storage
    PodCreated,
    // Pod is scheduled to a node but is not bound to a node
    PodScheduled,
    // Pod is on a node and started to initialize (fetching container images, etc)
    // TODO: may be unused now
    PodReadyToStartContainers,
    // Pod initialized all containers and started them, at least one container is still running in
    // this phase.
    PodRunning,
    // All containers in the pod have terminated successfully
    PodSucceeded,
    // All containers in the pod have terminated and at least one container has terminated
    // in failure.
    PodFailed,
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
    pub fn calculate_requested_resources(&self) -> RuntimeResources {
        let mut resources = RuntimeResources { cpu: 0, ram: 0 };
        for container in self.spec.containers.iter() {
            resources.cpu += container.resources.requests.cpu;
            resources.ram += container.resources.requests.ram;
        }
        resources
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
        self.status.conditions.iter().find(|c| c.condition_type == condition_type)
    }

    pub fn calculate_running_duration(&self) -> f64 {
        let longest_running_container = self
            .spec
            .containers
            .iter()
            .max_by(|lhs, rhs| lhs.running_duration.total_cmp(&rhs.running_duration))
            .unwrap();
        longest_running_container.running_duration
    }
}
