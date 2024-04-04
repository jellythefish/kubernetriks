//! Node component simulates a real node running pods.

use std::collections::HashSet;
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, SimulationContext};

use crate::core::events::{BindPodToNodeRequest, PodFinishedRunning, PodStartedRunning};
use crate::core::node::Node;

use crate::core::pod::PodConditionType;

use crate::core::common::SimComponentId;
use crate::simulator::SimulationConfig;

pub struct NodeComponent {
    ctx: SimulationContext,
    // Initialized later when the node component is actually allocated from node pool.
    // Sets to None when the node gets back to node pool.
    pub runtime: Option<NodeRuntime>,
}

pub struct NodeRuntime {
    pub api_server: SimComponentId,
    pub node: Node,
    pub running_pods: HashSet<String>,
    pub config: Rc<SimulationConfig>,
}

impl NodeComponent {
    pub fn new(ctx: SimulationContext) -> Self {
        Self { ctx, runtime: None }
    }

    pub fn id(&self) -> SimComponentId {
        self.ctx.id()
    }

    pub fn node_name(&self) -> &str {
        &self.runtime.as_ref().unwrap().node.metadata.name
    }

    pub fn context_name(&self) -> &str {
        &self.ctx.name()
    }

    pub fn simulate_pod_runtime(&mut self, event_time: f64, pod_name: String, pod_duration: f64) {
        self.runtime
            .as_mut()
            .unwrap()
            .running_pods
            .insert(pod_name.clone());
        self.ctx.emit_self(
            PodFinishedRunning {
                pod_name,
                finish_time: event_time + pod_duration,
                finish_result: PodConditionType::PodSucceeded,
            },
            pod_duration,
        );
    }
}

impl EventHandler for NodeComponent {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            BindPodToNodeRequest {
                pod_name,
                pod_duration,
                node_name,
            } => {
                if node_name != self.node_name() {
                    panic!("Pod is assigned to node with different node name: pod - {:?}, current node - {:?}, assigned node - {:?}", pod_name, self.node_name(), node_name);
                }
                self.simulate_pod_runtime(event.time, pod_name.clone(), pod_duration);
                self.ctx.emit(
                    PodStartedRunning {
                        start_time: event.time,
                        pod_name,
                    },
                    self.runtime.as_ref().unwrap().api_server,
                    self.runtime
                        .as_ref()
                        .unwrap()
                        .config
                        .as_to_node_network_delay,
                );
            }
            PodFinishedRunning {
                finish_time,
                finish_result,
                pod_name,
            } => {
                self.runtime
                    .as_mut()
                    .unwrap()
                    .running_pods
                    .remove(&pod_name);
                // Redirect to api server
                self.ctx.emit(
                    PodFinishedRunning {
                        finish_time,
                        finish_result,
                        pod_name,
                    },
                    self.runtime.as_ref().unwrap().api_server,
                    self.runtime
                        .as_ref()
                        .unwrap()
                        .config
                        .as_to_node_network_delay,
                );
            }
        });
    }
}
